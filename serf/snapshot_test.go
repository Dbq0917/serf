// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package serf

import (
	"fmt"
	"log"
	"os"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

func TestSnapshotter(t *testing.T) {
	td := t.TempDir()

	clock := new(LamportClock)
	outCh := make(chan Event, 64)
	stopCh := make(chan struct{})
	logger := log.New(os.Stderr, "", log.LstdFlags)
	inCh, snap, err := NewSnapshotter(td+"snap", snapshotSizeLimit, false,
		logger, clock, outCh, stopCh)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Write some user events
	ue := UserEvent{
		LTime: 42,
		Name:  "bar",
	}
	inCh <- ue

	// Write some queries
	q := &Query{
		LTime: 50,
		Name:  "uptime",
	}
	inCh <- q

	// Write some member events
	clock.Witness(100)
	meJoin := MemberEvent{
		Type: EventMemberJoin,
		Members: []Member{
			{
				Name: "foo",
				Addr: []byte{127, 0, 0, 1},
				Port: 5000,
			},
		},
	}
	meFail := MemberEvent{
		Type: EventMemberFailed,
		Members: []Member{
			{
				Name: "foo",
				Addr: []byte{127, 0, 0, 1},
				Port: 5000,
			},
		},
	}
	inCh <- meJoin
	inCh <- meFail
	inCh <- meJoin

	// Check these get passed through
	select {
	case e := <-outCh:
		if !reflect.DeepEqual(e, ue) {
			t.Fatalf("expected user event: %#v", e)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("timeout")
	}

	select {
	case e := <-outCh:
		if !reflect.DeepEqual(e, q) {
			t.Fatalf("expected query event: %#v", e)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("timeout")
	}

	select {
	case e := <-outCh:
		if !reflect.DeepEqual(e, meJoin) {
			t.Fatalf("expected member event: %#v", e)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("timeout")
	}

	select {
	case e := <-outCh:
		if !reflect.DeepEqual(e, meFail) {
			t.Fatalf("expected member event: %#v", e)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("timeout")
	}

	select {
	case e := <-outCh:
		if !reflect.DeepEqual(e, meJoin) {
			t.Fatalf("expected member event: %#v", e)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("timeout")
	}

	// Close the snapshoter
	close(stopCh)
	snap.Wait()

	// Open the snapshoter
	stopCh = make(chan struct{})
	_, snap, err = NewSnapshotter(td+"snap", snapshotSizeLimit, false,
		logger, clock, outCh, stopCh)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Check the values
	if snap.LastClock() != 100 {
		t.Fatalf("bad clock %d", snap.LastClock())
	}
	if snap.LastEventClock() != 42 {
		t.Fatalf("bad clock %d", snap.LastEventClock())
	}
	if snap.LastQueryClock() != 50 {
		t.Fatalf("bad clock %d", snap.LastQueryClock())
	}

	prev := snap.AliveNodes()
	if len(prev) != 1 {
		t.Fatalf("expected alive: %#v", prev)
	}
	if prev[0].Name != "foo" {
		t.Fatalf("bad name: %#v", prev[0])
	}
	if prev[0].Addr != "127.0.0.1:5000" {
		t.Fatalf("bad addr: %#v", prev[0])
	}

	// Close the snapshotter.
	close(stopCh)
	snap.Wait()

	// Open the snapshotter, make sure nothing dies reading with coordinates
	// disabled.
	stopCh = make(chan struct{})
	_, snap, err = NewSnapshotter(td+"snap", snapshotSizeLimit, false,
		logger, clock, outCh, stopCh)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	close(stopCh)
	snap.Wait()
}

func TestSnapshotter_forceCompact(t *testing.T) {
	td := t.TempDir()

	clock := new(LamportClock)
	stopCh := make(chan struct{})
	logger := log.New(os.Stderr, "", log.LstdFlags)

	// Create a very low limit
	inCh, snap, err := NewSnapshotter(td+"snap", 1024, false,
		logger, clock, nil, stopCh)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Write lots of user events
	for i := range 1024 {
		ue := UserEvent{
			LTime: LamportTime(i),
		}
		inCh <- ue
	}

	// Write lots of queries
	for i := range 1024 {
		q := &Query{
			LTime: LamportTime(i),
		}
		inCh <- q
	}

	// Wait for drain
	for len(inCh) > 0 {
		time.Sleep(20 * time.Millisecond)
	}

	// Close the snapshoter
	close(stopCh)
	snap.Wait()

	// Open the snapshoter
	stopCh = make(chan struct{})
	_, snap, err = NewSnapshotter(td+"snap", snapshotSizeLimit, false,
		logger, clock, nil, stopCh)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Check the values
	if snap.LastEventClock() != 1023 {
		t.Fatalf("bad clock %d", snap.LastEventClock())
	}

	if snap.LastQueryClock() != 1023 {
		t.Fatalf("bad clock %d", snap.LastQueryClock())
	}

	close(stopCh)
	snap.Wait()
}

func TestSnapshotter_leave(t *testing.T) {
	td := t.TempDir()

	clock := new(LamportClock)
	stopCh := make(chan struct{})
	logger := log.New(os.Stderr, "", log.LstdFlags)
	inCh, snap, err := NewSnapshotter(td+"snap", snapshotSizeLimit, false,
		logger, clock, nil, stopCh)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Write a user event
	ue := UserEvent{
		LTime: 42,
		Name:  "bar",
	}
	inCh <- ue

	// Write a query
	q := &Query{
		LTime: 50,
		Name:  "uptime",
	}
	inCh <- q

	// Write some member events
	clock.Witness(100)
	meJoin := MemberEvent{
		Type: EventMemberJoin,
		Members: []Member{
			{
				Name: "foo",
				Addr: []byte{127, 0, 0, 1},
				Port: 5000,
			},
		},
	}
	inCh <- meJoin

	// Wait for drain
	for len(inCh) > 0 {
		time.Sleep(20 * time.Millisecond)
	}

	// Leave the cluster!
	snap.Leave()

	// Close the snapshoter
	close(stopCh)
	snap.Wait()

	// Open the snapshoter
	stopCh = make(chan struct{})
	_, snap, err = NewSnapshotter(td+"snap", snapshotSizeLimit, false,
		logger, clock, nil, stopCh)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Check the values
	if snap.LastClock() != 0 {
		t.Fatalf("bad clock %d", snap.LastClock())
	}
	if snap.LastEventClock() != 0 {
		t.Fatalf("bad clock %d", snap.LastEventClock())
	}
	if snap.LastQueryClock() != 0 {
		t.Fatalf("bad clock %d", snap.LastQueryClock())
	}

	prev := snap.AliveNodes()
	if len(prev) != 0 {
		t.Fatalf("expected none alive: %#v", prev)
	}
}

func TestSnapshotter_leave_rejoin(t *testing.T) {
	td := t.TempDir()

	clock := new(LamportClock)
	stopCh := make(chan struct{})
	logger := log.New(os.Stderr, "", log.LstdFlags)
	inCh, snap, err := NewSnapshotter(td+"snap", snapshotSizeLimit, true,
		logger, clock, nil, stopCh)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Write a user event
	ue := UserEvent{
		LTime: 42,
		Name:  "bar",
	}
	inCh <- ue

	// Write a query
	q := &Query{
		LTime: 50,
		Name:  "uptime",
	}
	inCh <- q

	// Write some member events
	clock.Witness(100)
	meJoin := MemberEvent{
		Type: EventMemberJoin,
		Members: []Member{
			{
				Name: "foo",
				Addr: []byte{127, 0, 0, 1},
				Port: 5000,
			},
		},
	}
	inCh <- meJoin

	// Wait for drain
	for len(inCh) > 0 {
		time.Sleep(20 * time.Millisecond)
	}

	// Leave the cluster!
	snap.Leave()

	// Close the snapshoter
	close(stopCh)
	snap.Wait()

	// Open the snapshoter
	stopCh = make(chan struct{})
	_, snap, err = NewSnapshotter(td+"snap", snapshotSizeLimit, true,
		logger, clock, nil, stopCh)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Check the values
	if snap.LastClock() != 100 {
		t.Fatalf("bad clock %d", snap.LastClock())
	}
	if snap.LastEventClock() != 42 {
		t.Fatalf("bad clock %d", snap.LastEventClock())
	}
	if snap.LastQueryClock() != 50 {
		t.Fatalf("bad clock %d", snap.LastQueryClock())
	}

	prev := snap.AliveNodes()
	if len(prev) == 0 {
		t.Fatalf("expected alive: %#v", prev)
	}
}

func TestSnapshotter_slowDiskNotBlockingEventCh(t *testing.T) {
	t.Skip("Flaky test")
	td := t.TempDir()

	clock := new(LamportClock)
	stopCh := make(chan struct{})
	logger := log.New(os.Stderr, "", log.LstdFlags)

	outCh := make(chan Event, 1024)
	inCh, snap, err := NewSnapshotter(td+"snap", snapshotSizeLimit, true,
		logger, clock, outCh, stopCh)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// We need enough events to be much more than the buffers used which are size
	// 1024. This number processes easily within the 500ms we allow below on my
	// host provided there is no disk IO on the path (I verified that by just
	// returning early in tryAppend using the old blocking code). The new async
	// method should pass without disabling disk writes too!
	numEvents := 10000

	// Write lots of member updates (way bigger than our chan buffers)
	startCh := make(chan struct{})
	go func() {
		<-startCh
		for i := range numEvents {
			e := MemberEvent{
				Type: EventMemberJoin,
				Members: []Member{
					{
						Name: fmt.Sprintf("foo%d", i),
						Addr: []byte{127, 0, byte((i / 256) % 256), byte(i % 256)},
						Port: 5000,
					},
				},
			}
			if i%10 == 0 {
				// 1/10 events is a leave
				e.Type = EventMemberLeave
			}
			inCh <- e
			// Pace ourselves - if we just throw these out as fast as possible the
			// read loop below can't keep up and we end up dropping messages due to
			// backpressure. But we need to still send them all in well less than the
			// timeout, 10k messages at 1 microsecond should take 10 ms minimum. In
			// practice it's quite a bit more to actually process and because the
			// buffer here blocks.
			time.Sleep(1 * time.Microsecond)
		}
	}()

	// Wait for them all to process through and it should be in a lot less time
	// than if the disk IO was in serial. This was verified by running this test
	// against the old serial implementation and seeing it never come close to
	// passing on my laptop with an SSD. It's not the most robust thing ever but
	// it's at least a sanity check that we are non-blocking now, and it passes
	// reliably at least on my machine. I typically see this complete in around
	// 115ms on my machine so this should give plenty of headroom for slower CI
	// environments while still being low enough that actual disk IO would
	// reliably blow it.
	deadline := time.After(500 * time.Millisecond)
	numRecvd := 0
	start := time.Now()
	for numRecvd < numEvents {
		select {
		// Now we are ready to listen, start the generator goroutine off
		case startCh <- struct{}{}:
			continue
		case <-outCh:
			numRecvd++
		case <-deadline:
			t.Fatalf("timed out after %s waiting for messages blocked on fake disk IO? "+
				"got %d of %d", time.Since(start), numRecvd, numEvents)
		}
	}

	// Close the snapshoter
	close(stopCh)
	snap.Wait()
}

func TestSnapshotter_blockedUpstreamNotBlockingMemberlist(t *testing.T) {
	td := t.TempDir()

	clock := new(LamportClock)
	stopCh := make(chan struct{})
	logger := log.New(os.Stderr, "", log.LstdFlags)

	// OutCh is small, simulating a slow upstream that cannot keep up with
	// a bulk membership change. A collector drains it concurrently so the
	// pipeline can still make progress while remaining saturated.
	outCh := make(chan Event, 8)
	collectorDone := make(chan struct{})
	var criticalReceived atomic.Int64
	go func() {
		defer close(collectorDone)
		for {
			select {
			case e := <-outCh:
				if isCriticalEvent(e) {
					criticalReceived.Add(1)
				}
			case <-stopCh:
				return
			}
		}
	}()

	inCh, snap, err := NewSnapshotter(td+"snap", snapshotSizeLimit, true,
		logger, clock, outCh, stopCh)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// We need enough events to be more than the internal buffer sizes
	numEvents := 3000
	criticalProduced := 0

	// Send some updates
	for i := range numEvents {
		e := MemberEvent{
			Type: EventMemberJoin,
			Members: []Member{
				{
					Name: fmt.Sprintf("foo%d", i),
					Addr: []byte{127, 0, byte((i / 256) % 256), byte(i % 256)},
					Port: 5000,
				},
			},
		}
		if i%10 == 0 {
			// 1/10 events is a leave
			e.Type = EventMemberLeave
			criticalProduced++
		}
		select {
		case inCh <- e:
		default:
			t.Fatalf("inCh should never block")
		}
		// Allow just the tiniest time so that the runtime can schedule the
		// goroutine that's reading this even if they are both on the same physical
		// core (like in CI).
		time.Sleep(1 * time.Microsecond)
	}

	// Critical leave events drive downstream resource cleanup, so none of
	// them may have been dropped even under this sustained overload. The
	// bounded dispatcher applies backpressure instead of losing them.
	waitUntil := time.Now().Add(10 * time.Second)
	for time.Now().Before(waitUntil) {
		if criticalReceived.Load() >= int64(criticalProduced) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := criticalReceived.Load(); got != int64(criticalProduced) {
		t.Fatalf("critical events must not be dropped: got %d, want %d",
			got, criticalProduced)
	}

	stats := snap.DropStats()
	if stats.CriticalAbandoned != 0 {
		t.Fatalf("no critical event should be abandoned, got %d", stats.CriticalAbandoned)
	}

	// Close the snapshoter
	close(stopCh)
	snap.Wait()
}

// TestSnapshotter_saturationDropsBestEffortCountsAndPreservesCritical
// drives the snapshotter with a slow application channel to prove the
// degradation contract end to end: best-effort events are dropped and
// counted via DropStats, while critical member events are never lost.
func TestSnapshotter_saturationDropsBestEffortCountsAndPreservesCritical(t *testing.T) {
	td := t.TempDir()

	clock := new(LamportClock)
	stopCh := make(chan struct{})
	logger := log.New(os.Stderr, "", log.LstdFlags)

	// Small application channel: the consumer drains much more slowly than
	// the burst arrives, keeping the bounded dispatcher saturated so
	// best-effort events overflow while critical events apply backpressure.
	outCh := make(chan Event, 4)

	inCh, snap, err := NewSnapshotter(td+"snap", snapshotSizeLimit, true,
		logger, clock, outCh, stopCh)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Several times the combined internal capacity, so the best-effort
	// portion is guaranteed to overflow a bounded buffer.
	const total = eventChSize * 4
	const criticalEvery = 10
	criticalProduced := 0

	// A deliberately slow consumer keeps the downstream full, but still
	// drains so critical backpressure can eventually make progress.
	var criticalSeen atomic.Int64
	go func() {
		for {
			select {
			case e := <-outCh:
				if isCriticalEvent(e) {
					criticalSeen.Add(1)
				}
				time.Sleep(50 * time.Microsecond)
			case <-stopCh:
				return
			}
		}
	}()

	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		for i := range total {
			e := MemberEvent{
				Type: EventMemberJoin,
				Members: []Member{{
					Name: fmt.Sprintf("foo%d", i),
					Addr: []byte{127, 0, 0, byte(i % 256)},
					Port: 5000,
				}},
			}
			if i%criticalEvery == 0 {
				e.Type = EventMemberLeave
				criticalProduced++
			}
			inCh <- e
		}
	}()

	// Wait for the burst to be fully ingested.
	select {
	case <-producerDone:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out ingesting the burst")
	}

	stats := snap.DropStats()
	// Best-effort joins must have been dropped and the loss explicitly
	// counted rather than hidden.
	if stats.TotalDropped == 0 {
		t.Fatal("expected best-effort events to be dropped and counted")
	}
	if stats.DroppedByType["member-join"] == 0 {
		t.Fatalf("expected member-join drops to be counted per type, got %+v",
			stats.DroppedByType)
	}

	// Every critical leave event must reach the consumer. Critical events
	// apply backpressure, so none are silently dropped.
	waitUntil := time.Now().Add(10 * time.Second)
	for time.Now().Before(waitUntil) {
		if criticalSeen.Load() >= int64(criticalProduced) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if criticalSeen.Load() != int64(criticalProduced) {
		t.Fatalf("critical events must never be dropped: got %d, want %d",
			criticalSeen.Load(), criticalProduced)
	}

	close(stopCh)
	snap.Wait()
}
