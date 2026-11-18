// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package serf

import (
	"log"
	"sync"
	"time"

	metrics "github.com/hashicorp/go-metrics"
)

const (
	// defaultEventChSize is the capacity of the bounded event channel that
	// sits in front of the application supplied EventCh. It absorbs short
	// event bursts (for example the tens-of-x event volume produced by a
	// bulk membership change) without blocking Serf state updates.
	defaultEventChSize = 1024

	// eventDropLogInterval rate limits the WARN logs emitted while events
	// are being dropped, so that a sustained overload cannot flood the
	// logger while the drop counters keep tracking every single drop.
	eventDropLogInterval = 5 * time.Second
)

// EventDropStats is a point-in-time snapshot of the event delivery
// degradation counters. The counters are cumulative over the lifetime of
// the dispatcher and safe to read while events are still flowing.
type EventDropStats struct {
	// TotalDropped is the number of non-critical events that could not be
	// delivered because the bounded event channel was full.
	TotalDropped uint64

	// DroppedByType breaks TotalDropped down by EventType string.
	DroppedByType map[string]uint64

	// CriticalDelivered is the number of critical events that were handed
	// to the dispatcher. Critical events are never dropped, so every
	// accepted critical event is eventually delivered (or abandoned on
	// shutdown, which is reported separately via CriticalAbandoned).
	CriticalDelivered uint64

	// CriticalBackpressure is the number of times a critical event found
	// the bounded event channel full and had to wait for capacity. A
	// non-zero value means the downstream consumer cannot keep up.
	CriticalBackpressure uint64

	// CriticalAbandoned is the number of critical events that were not
	// delivered because Serf shut down while waiting for capacity.
	CriticalAbandoned uint64
}

// eventDispatcher wraps an application facing event channel with a bounded
// buffer and explicit degradation behavior:
//
//   - Critical events (member leave/failed/reap, which drive downstream
//     resource cleanup) are never silently dropped. When the buffer is full
//     the caller applies backpressure until capacity frees up (or Serf
//     shuts down).
//   - Non-critical events (join/update/user/query) use a non-blocking send.
//     When the buffer is full the event is dropped, counted, logged at WARN
//     (rate limited) and recorded in the serf.events.dropped metric.
//
// The Event interface and the ordering semantics of the wrapped channel are
// unchanged: accepted events leave the buffer in FIFO order.
type eventDispatcher struct {
	ch         chan Event
	outCh      chan<- Event
	shutdownCh <-chan struct{}
	logger     *log.Logger
	labels     []metrics.Label
	name       string

	lock                sync.Mutex
	dropped             uint64
	droppedByType       map[string]uint64
	criticalDelivered   uint64
	backpressure        uint64
	abandoned           uint64
	lastDropLog         time.Time
	lastBackpressureLog time.Time
}

// newEventDispatcher returns a dispatcher that forwards buffered events to
// outCh. The pump goroutine exits when shutdownCh is closed, after which no
// further writes are attempted against outCh.
func newEventDispatcher(outCh chan<- Event, shutdownCh <-chan struct{},
	bufferSize int, logger *log.Logger, labels []metrics.Label, name string) *eventDispatcher {
	if bufferSize <= 0 {
		bufferSize = defaultEventChSize
	}
	d := &eventDispatcher{
		ch:            make(chan Event, bufferSize),
		outCh:         outCh,
		shutdownCh:    shutdownCh,
		logger:        logger,
		labels:        labels,
		name:          name,
		droppedByType: make(map[string]uint64),
	}
	go d.pump()
	return d
}

// newEventDispatcherManualStart creates a dispatcher whose forwarding pump
// is not started yet. It exists for deterministic unit testing of the
// bounded-buffer overflow behavior: while the pump is paused the buffer
// fills predictably and every overflow is dropped. Production code always
// uses newEventDispatcher, which starts the pump immediately.
func newEventDispatcherManualStart(outCh chan<- Event, shutdownCh <-chan struct{},
	bufferSize int, logger *log.Logger, labels []metrics.Label, name string) *eventDispatcher {
	if bufferSize <= 0 {
		bufferSize = defaultEventChSize
	}
	return &eventDispatcher{
		ch:            make(chan Event, bufferSize),
		outCh:         outCh,
		shutdownCh:    shutdownCh,
		logger:        logger,
		labels:        labels,
		name:          name,
		droppedByType: make(map[string]uint64),
	}
}

// inputCh returns the channel producers hand events to.
func (d *eventDispatcher) inputCh() chan<- Event {
	return d.ch
}

// pump forwards every accepted event downstream in FIFO order until
// shutdown, then releases any producers still waiting in dispatch().
func (d *eventDispatcher) pump() {
	for {
		select {
		case e := <-d.ch:
			select {
			case d.outCh <- e:
			case <-d.shutdownCh:
				return
			}
		case <-d.shutdownCh:
			return
		}
	}
}

// dispatch hands an event to the bounded buffer. Critical events block
// until capacity is available (backpressure) or Serf shuts down.
// Non-critical events are dropped, counted and logged when the buffer is
// full, so that a slow consumer can never stall Serf state updates.
func (d *eventDispatcher) dispatch(e Event) {
	if isCriticalEvent(e) {
		select {
		case d.ch <- e:
			d.lock.Lock()
			d.criticalDelivered++
			d.lock.Unlock()
		default:
			d.lock.Lock()
			d.backpressure++
			backpressure := d.backpressure
			shouldLog := time.Since(d.lastBackpressureLog) >= eventDropLogInterval
			if shouldLog {
				d.lastBackpressureLog = time.Now()
			}
			d.lock.Unlock()
			metrics.IncrCounterWithLabels([]string{"serf", "events", "critical_backpressure"}, 1, d.labels)
			if shouldLog && d.logger != nil {
				d.logger.Printf("[WARN] serf: %s event channel full, applying backpressure for critical %s event (backpressure events: %d)",
					d.name, e.EventType().String(), backpressure)
			}
			select {
			case d.ch <- e:
				d.lock.Lock()
				d.criticalDelivered++
				d.lock.Unlock()
			case <-d.shutdownCh:
				d.lock.Lock()
				d.abandoned++
				d.lock.Unlock()
				metrics.IncrCounterWithLabels([]string{"serf", "events", "critical_abandoned"}, 1, d.labels)
			}
		}
		return
	}

	select {
	case d.ch <- e:
	default:
		d.recordDrop(e)
	}
}

// recordDrop accounts for a dropped non-critical event: per-type and total
// counters for in-process inspection plus the serf.events.dropped metric
// used by monitoring to spot bulk change bursts.
func (d *eventDispatcher) recordDrop(e Event) {
	d.recordDropWithStage(e, "consumer")
}

// recordSnapshotDrop accounts for an event dropped because the on-disk
// snapshot ingestion channel was saturated. It shares the drop counters
// and metric of the application-facing path so a single dashboard query
// surfaces every event loss, with a stage label distinguishing the two.
func (d *eventDispatcher) recordSnapshotDrop(e Event) {
	d.recordDropWithStage(e, "snapshot")
}

func (d *eventDispatcher) recordDropWithStage(e Event, stage string) {
	eventType := e.EventType().String()

	d.lock.Lock()
	d.dropped++
	d.droppedByType[eventType]++
	dropped := d.dropped
	shouldLog := time.Since(d.lastDropLog) >= eventDropLogInterval
	if shouldLog {
		d.lastDropLog = time.Now()
	}
	d.lock.Unlock()

	labels := append([]metrics.Label{
		{Name: "event_type", Value: eventType},
		{Name: "stage", Value: stage},
	}, d.labels...)
	metrics.IncrCounterWithLabels([]string{"serf", "events", "dropped"}, 1, labels)

	// Emit the current queue depth alongside the drop so dashboards can
	// correlate the drop spike with buffer saturation.
	metrics.AddSampleWithLabels([]string{"serf", "events", "queue_depth"}, float32(len(d.ch)), d.labels)

	if shouldLog && d.logger != nil {
		d.logger.Printf("[WARN] serf: %s event channel full, dropping %s event (total dropped: %d)",
			d.name, eventType, dropped)
	}
}

// DropStats returns a consistent snapshot of the degradation counters.
func (d *eventDispatcher) DropStats() EventDropStats {
	d.lock.Lock()
	defer d.lock.Unlock()

	stats := EventDropStats{
		TotalDropped:         d.dropped,
		DroppedByType:        make(map[string]uint64, len(d.droppedByType)),
		CriticalDelivered:    d.criticalDelivered,
		CriticalBackpressure: d.backpressure,
		CriticalAbandoned:    d.abandoned,
	}
	for eventType, count := range d.droppedByType {
		stats.DroppedByType[eventType] = count
	}
	return stats
}

// isCriticalEvent reports whether an event must never be silently dropped.
// Member leave/failed/reap events drive downstream cleanup (releasing
// per-node resources); losing them leaks resources until an external
// reconciliation happens. Join/update/user/query events are best-effort.
func isCriticalEvent(e Event) bool {
	switch e.EventType() {
	case EventMemberLeave, EventMemberFailed, EventMemberReap:
		return true
	default:
		return false
	}
}
