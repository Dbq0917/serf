// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package serf

import (
	"log"
	"os"
	"testing"
	"time"

	metrics "github.com/hashicorp/go-metrics"
)

// testMetrics installs an in-memory metrics sink for the duration of the
// test and restores a blackhole sink afterwards. It returns the sink so
// tests can assert on the exact counters emitted.
func testMetrics(t *testing.T) *metrics.InmemSink {
	t.Helper()

	sink := metrics.NewInmemSink(10*time.Millisecond, time.Minute)
	conf := metrics.DefaultConfig("test")
	conf.ServiceName = ""
	conf.EnableHostname = false
	conf.EnableRuntimeMetrics = false
	if _, err := metrics.NewGlobal(conf, sink); err != nil {
		t.Fatalf("failed to install in-memory metrics sink: %v", err)
	}

	t.Cleanup(func() {
		metrics.NewGlobal(&metrics.Config{FilterDefault: true}, &metrics.BlackholeSink{}) //nolint:errcheck
	})
	return sink
}

// counterSum aggregates a counter across every retained interval.
func counterSum(sink *metrics.InmemSink, key string) int {
	total := 0
	for _, interval := range sink.Data() {
		interval.RLock()
		if sample, ok := interval.Counters[key]; ok && sample.AggregateSample != nil {
			total += sample.Count
		}
		interval.RUnlock()
	}
	return total
}

// sampleCount aggregates the number of points recorded for a sample key.
func sampleCount(sink *metrics.InmemSink, key string) int {
	total := 0
	for _, interval := range sink.Data() {
		interval.RLock()
		if sample, ok := interval.Samples[key]; ok && sample.AggregateSample != nil {
			total += sample.Count
		}
		interval.RUnlock()
	}
	return total
}

func testDispatcherLogger() *log.Logger {
	return log.New(os.Stderr, "", log.LstdFlags)
}

func memberEvent(eventType EventType, name string) MemberEvent {
	return MemberEvent{
		Type: eventType,
		Members: []Member{{
			Name: name,
			Addr: []byte{127, 0, 0, 1},
			Port: 5000,
		}},
	}
}

// newPausedDispatcher builds a dispatcher whose forwarding pump is not
// running yet. While paused the bounded buffer fills deterministically and
// every overflow is dropped, which lets tests assert exact drop counts.
// Call pump to begin delivering buffered events.
func newPausedDispatcher(outCh chan<- Event, shutdownCh <-chan struct{},
	bufferSize int) *eventDispatcher {
	return newEventDispatcherManualStart(outCh, shutdownCh, bufferSize,
		testDispatcherLogger(), nil, "test")
}

// startCollector runs a concurrent consumer that forwards every event it
// receives into the returned channel. It closes the returned channel when
// stopCh closes, so consumers can range over it deterministically.
func startCollector(outCh <-chan Event, stopCh <-chan struct{}) chan Event {
	collected := make(chan Event, 4096)
	go func() {
		defer close(collected)
		for {
			select {
			case e := <-outCh:
				collected <- e
			case <-stopCh:
				return
			}
		}
	}()
	return collected
}

// drainCollected reads want events from collected, failing the test if the
// timeout elapses first, and returns per-type counts.
func drainCollected(t *testing.T, collected <-chan Event, want int, timeout time.Duration) map[EventType]int {
	t.Helper()
	seen := map[EventType]int{}
	deadline := time.After(timeout)
	for total := 0; total < want; total++ {
		select {
		case e := <-collected:
			seen[e.EventType()]++
		case <-deadline:
			t.Fatalf("timed out, only received %d/%d events", total, want)
		}
	}
	return seen
}

// TestEventDispatcher_BurstDropsBestEffortAndPreservesCritical mirrors the
// acceptance scenario: more events than the buffer capacity arrive in a
// short burst. Non-critical events are dropped with exact counters, while
// critical events are never lost. A subsequent normal round is then
// received in full.
func TestEventDispatcher_BurstDropsBestEffortAndPreservesCritical(t *testing.T) {
	sink := testMetrics(t)

	const bufferSize = 16
	const burstNonCritical = 200
	const burstCritical = 40
	// outCh must hold every buffered event once the pump starts, so the
	// pump can fully drain the bounded channel without a second stall.
	outCh := make(chan Event, bufferSize+burstCritical)
	stopCh := make(chan struct{})
	t.Cleanup(func() { close(stopCh) })

	dispatcher := newPausedDispatcher(outCh, stopCh, bufferSize)

	// Bulk membership change: tens of times the normal volume. While the
	// pump is paused the bounded channel deterministically retains
	// bufferSize events and drops every other best-effort event.

	// Phase 1: best-effort burst with no consumer draining.
	for range burstNonCritical {
		dispatcher.dispatch(memberEvent(EventMemberJoin, "join-node"))
	}

	stats := dispatcher.DropStats()
	expectedDrops := burstNonCritical - bufferSize
	if stats.TotalDropped != uint64(expectedDrops) {
		t.Fatalf("expected %d dropped events, got %d", expectedDrops, stats.TotalDropped)
	}
	if stats.DroppedByType["member-join"] != uint64(expectedDrops) {
		t.Fatalf("expected %d member-join drops, got %d",
			expectedDrops, stats.DroppedByType["member-join"])
	}

	// Phase 2: critical events on the saturated, still-paused pipeline.
	// The first send parks in backpressure; start the pump (and a
	// collector) to prove all critical events are delivered, none dropped.
	criticalDone := make(chan struct{})
	go func() {
		for range burstCritical {
			dispatcher.dispatch(memberEvent(EventMemberLeave, "leave-node"))
		}
		close(criticalDone)
	}()

	// Confirm the producer is parked before relieving the pressure.
	time.Sleep(50 * time.Millisecond)
	select {
	case <-criticalDone:
		t.Fatal("critical dispatch should be applying backpressure")
	default:
	}

	go dispatcher.pump()
	collected := startCollector(outCh, stopCh)

	select {
	case <-criticalDone:
	case <-time.After(5 * time.Second):
		t.Fatal("critical event dispatch blocked indefinitely")
	}

	stats = dispatcher.DropStats()
	if stats.CriticalDelivered != burstCritical {
		t.Fatalf("expected %d critical events accepted, got %d",
			burstCritical, stats.CriticalDelivered)
	}
	if stats.CriticalBackpressure == 0 {
		t.Fatal("expected backpressure to be recorded while buffer was full")
	}
	if stats.CriticalAbandoned != 0 {
		t.Fatalf("expected no abandoned critical events, got %d", stats.CriticalAbandoned)
	}

	// The retained best-effort events plus every critical event must
	// physically reach the consumer.
	seen := drainCollected(t, collected, bufferSize+burstCritical, 5*time.Second)
	if seen[EventMemberLeave] != burstCritical {
		t.Fatalf("expected %d member-leave events delivered, got %d",
			burstCritical, seen[EventMemberLeave])
	}
	if seen[EventMemberJoin] != bufferSize {
		t.Fatalf("expected %d retained member-join events, got %d",
			bufferSize, seen[EventMemberJoin])
	}

	// The drop counter must be visible to monitoring with per-type and
	// stage labels so a burst can be attributed to bulk changes.
	dropKey := "serf.events.dropped;event_type=member-join;stage=consumer"
	if got := counterSum(sink, dropKey); got != expectedDrops {
		t.Fatalf("expected %d on %s metric, got %d", expectedDrops, dropKey, got)
	}
	if got := counterSum(sink, "serf.events.critical_backpressure"); got == 0 {
		t.Fatal("expected serf.events.critical_backpressure to be emitted")
	}
	if got := sampleCount(sink, "serf.events.queue_depth"); got == 0 {
		t.Fatal("expected serf.events.queue_depth samples alongside drops")
	}

	// Phase 3: a normal steady-state round, consumer keeping up. It must
	// be received in full with no additional drops.
	const steadyRound = 20
	for range steadyRound {
		dispatcher.dispatch(memberEvent(EventMemberUpdate, "steady-node"))
		dispatcher.dispatch(memberEvent(EventMemberFailed, "steady-fail"))
	}

	seen = drainCollected(t, collected, steadyRound*2, 5*time.Second)
	if seen[EventMemberUpdate] != steadyRound || seen[EventMemberFailed] != steadyRound {
		t.Fatalf("expected %d of each steady event, got updates=%d failures=%d",
			steadyRound, seen[EventMemberUpdate], seen[EventMemberFailed])
	}

	stats = dispatcher.DropStats()
	if stats.TotalDropped != uint64(expectedDrops) {
		t.Fatalf("steady round must not add drops, total changed to %d", stats.TotalDropped)
	}
	if stats.CriticalDelivered != burstCritical+steadyRound {
		t.Fatalf("expected %d critical events delivered overall, got %d",
			burstCritical+steadyRound, stats.CriticalDelivered)
	}
}

// TestEventDispatcher_ShutdownAbandonsCriticalWait verifies that a
// backpressured critical producer is released on shutdown instead of
// wedging the leaving process, and that the abandonment is counted.
func TestEventDispatcher_ShutdownAbandonsCriticalWait(t *testing.T) {
	// The single bounded slot is filled by the join event, so the
	// critical reap event has nowhere to go and parks in backpressure
	// (the pump is paused and outCh is unbuffered).
	outCh := make(chan Event)
	stopCh := make(chan struct{})
	dispatcher := newPausedDispatcher(outCh, stopCh, 1)

	dispatcher.dispatch(memberEvent(EventMemberJoin, "filler"))

	finished := make(chan struct{})
	go func() {
		dispatcher.dispatch(memberEvent(EventMemberReap, "reap-node"))
		close(finished)
	}()

	// Give the goroutine time to reach the backpressure select.
	time.Sleep(50 * time.Millisecond)
	select {
	case <-finished:
		t.Fatal("critical dispatch should be applying backpressure")
	default:
	}

	close(stopCh)

	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not release the backpressured producer")
	}

	stats := dispatcher.DropStats()
	if stats.CriticalAbandoned != 1 {
		t.Fatalf("expected 1 abandoned critical event, got %d", stats.CriticalAbandoned)
	}
}

// TestEventDispatcher_NormalRoundNoLoss is the steady-state half of the
// acceptance criteria: with the consumer keeping up, every event is
// received and no degradation counters move.
func TestEventDispatcher_NormalRoundNoLoss(t *testing.T) {
	// Pump is paused while the burst is produced, so dispatch is purely
	// channel writes: with the bounded buffer large enough for every
	// event, nothing can be dropped. Starting the pump afterwards proves
	// the whole round reaches the consumer unchanged.
	const total = 100
	outCh := make(chan Event, total+64)
	stopCh := make(chan struct{})
	t.Cleanup(func() { close(stopCh) })

	dispatcher := newPausedDispatcher(outCh, stopCh, total)
	collected := startCollector(outCh, stopCh)

	const perType = 25
	for range perType {
		dispatcher.dispatch(memberEvent(EventMemberJoin, "join-node"))
		dispatcher.dispatch(memberEvent(EventMemberUpdate, "update-node"))
		dispatcher.dispatch(memberEvent(EventMemberLeave, "leave-node"))
		dispatcher.dispatch(UserEvent{Name: "user-event"})
	}

	go dispatcher.pump()

	seen := drainCollected(t, collected, total, 5*time.Second)
	for eventType, count := range seen {
		if count != perType {
			t.Fatalf("expected %d %v events, got %d", perType, eventType, count)
		}
	}

	stats := dispatcher.DropStats()
	if stats.TotalDropped != 0 || stats.CriticalBackpressure != 0 ||
		stats.CriticalAbandoned != 0 {
		t.Fatalf("steady state must show no degradation, got %+v", stats)
	}
}

// TestIsCriticalEvent documents the exact event classification contract.
func TestIsCriticalEvent(t *testing.T) {
	critical := []EventType{EventMemberLeave, EventMemberFailed, EventMemberReap}
	for _, eventType := range critical {
		if !isCriticalEvent(memberEvent(eventType, "node")) {
			t.Fatalf("%s must be classified as critical", eventType)
		}
	}

	bestEffort := []EventType{EventMemberJoin, EventMemberUpdate}
	for _, eventType := range bestEffort {
		if isCriticalEvent(memberEvent(eventType, "node")) {
			t.Fatalf("%s must be classified as best-effort", eventType)
		}
	}

	if isCriticalEvent(UserEvent{Name: "user"}) {
		t.Fatal("user events must be classified as best-effort")
	}
	if isCriticalEvent(&Query{Name: "query"}) {
		t.Fatal("queries must be classified as best-effort")
	}
}
