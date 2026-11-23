// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package serf

import (
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/serf/testutil"
	"github.com/hashicorp/serf/testutil/retry"
)

func TestSerf_userEventHistoryBounded(t *testing.T) {
	ip, returnFn := testutil.TakeIP()
	defer returnFn()

	c := testConfig(t, ip)
	c.EventBuffer = 4
	c.EventBufferMax = 8
	c.EventCh = make(chan Event, 16)

	s, err := Create(c)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer s.Shutdown()

	const numEvents = 12
	for i := 0; i < numEvents; i++ {
		if err := s.UserEvent(fmt.Sprintf("event-%d", i), nil, false); err != nil {
			t.Fatalf("err: %v", err)
		}
	}

	s.eventLock.RLock()
	defer s.eventLock.RUnlock()

	if got, want := s.eventHistory.size, c.EventBufferMax; got != want {
		t.Fatalf("history size %d, want %d", got, want)
	}
	if got, want := s.eventHistory.events[0].LTime, LamportTime(5); got != want {
		t.Fatalf("oldest event time %d, want %d", got, want)
	}
	if got, want := s.eventHistory.events[len(s.eventHistory.events)-1].LTime, LamportTime(12); got != want {
		t.Fatalf("newest event time %d, want %d", got, want)
	}
	if got, want := len(s.eventBuffer), c.EventBuffer; got != want {
		t.Fatalf("recent buffer size %d, want %d", got, want)
	}

	state := s.localEventState()
	if got, want := len(state), c.EventBufferMax; got != want {
		t.Fatalf("state event groups %d, want %d", got, want)
	}
	for i, events := range state {
		want := fmt.Sprintf("event-%d", i+numEvents-c.EventBufferMax)
		if got := events.Events[0].Name; got != want {
			t.Fatalf("state event %d name %q, want %q", i, got, want)
		}
	}
}

func TestSerf_userEventHistoryCatchUp(t *testing.T) {
	ip1, returnFn1 := testutil.TakeIP()
	defer returnFn1()

	ip2, returnFn2 := testutil.TakeIP()
	defer returnFn2()

	c1 := testConfig(t, ip1)
	c1.EventBufferMax = 1024
	s1, err := Create(c1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer s1.Shutdown()

	const numEvents = 600
	names := make([]string, 0, numEvents)
	payloads := make([][]byte, 0, numEvents)
	for i := 0; i < numEvents; i++ {
		name := fmt.Sprintf("catchup-%03d", i)
		payload := []byte(name)
		if err := s1.UserEvent(name, payload, false); err != nil {
			t.Fatalf("err: %v", err)
		}
		names = append(names, name)
		payloads = append(payloads, payload)
	}

	eventCh := make(chan Event, numEvents)
	c2 := testConfig(t, ip2)
	c2.EventBuffer = 64
	c2.EventBufferMax = 1024
	c2.EventCh = eventCh

	s2, err := Create(c2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer s2.Shutdown()

	state := c1.MemberlistConfig.Delegate.LocalState(false)
	pp := messagePushPull{}
	if err := decodeMessage(state[1:], &pp); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if got, want := len(pp.Events), numEvents; got != want {
		t.Fatalf("state contains %d events, want %d", got, want)
	}

	c2.MemberlistConfig.Delegate.MergeRemoteState(state, false)
	testUserEvents(t, eventCh, names, payloads)

	s2.eventLock.RLock()
	if got, want := s2.eventHistory.size, numEvents; got != want {
		s2.eventLock.RUnlock()
		t.Fatalf("history size %d, want %d", got, want)
	}
	if got, want := s2.eventHistory.events[0].LTime, LamportTime(1); got != want {
		s2.eventLock.RUnlock()
		t.Fatalf("oldest history event %d, want %d", got, want)
	}
	s2.eventLock.RUnlock()

	// Replaying the same state must not deliver the events a second time.
	c2.MemberlistConfig.Delegate.MergeRemoteState(state, false)
	testUserEvents(t, eventCh, []string{}, [][]byte{})
}

func TestDelegate_MergeRemoteState_backfillsGap(t *testing.T) {
	ip, returnFn := testutil.TakeIP()
	defer returnFn()

	eventCh := make(chan Event, 16)
	c := testConfig(t, ip)
	c.EventBuffer = 4
	c.EventBufferMax = 32
	c.EventCh = eventCh

	s, err := Create(c)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer s.Shutdown()

	pp := messagePushPull{
		LTime:      42,
		EventLTime: 100,
	}
	names := make([]string, 0, 10)
	for i := 0; i < 10; i++ {
		name := fmt.Sprintf("gap-%d", i)
		pp.Events = append(pp.Events, &userEvents{
			LTime: LamportTime(10 + i),
			Events: []userEvent{{
				Name:    name,
				Payload: []byte(name),
			}},
		})
		names = append(names, name)
	}

	buf, err := encodeMessage(messagePushPullType, &pp, c.MsgpackUseNewTimeFormat)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	c.MemberlistConfig.Delegate.MergeRemoteState(buf, false)

	payloads := make([][]byte, 0, len(names))
	for _, name := range names {
		payloads = append(payloads, []byte(name))
	}
	testUserEvents(t, eventCh, names, payloads)

	s.eventLock.RLock()
	defer s.eventLock.RUnlock()
	if got, want := s.eventHistory.size, len(names); got != want {
		t.Fatalf("history size %d, want %d", got, want)
	}
	if got, want := s.eventHistory.events[0].LTime, LamportTime(10); got != want {
		t.Fatalf("oldest gap event %d, want %d", got, want)
	}
}

func TestSerf_reconnect_eventCatchUp(t *testing.T) {
	td := t.TempDir()

	ip1, returnFn1 := testutil.TakeIP()
	defer returnFn1()

	ip2, returnFn2 := testutil.TakeIP()
	defer returnFn2()

	c1 := testConfig(t, ip1)
	c1.EventBufferMax = 1024
	s1, err := Create(c1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer s1.Shutdown()

	snapshotPath := td + "/snap"
	c2 := testConfig(t, ip2)
	c2.SnapshotPath = snapshotPath
	c2.EventBufferMax = 1024
	s2, err := Create(c2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	waitUntilNumNodes(t, 1, s1, s2)
	if _, err := s1.Join([]string{c2.NodeName + "/" + c2.MemberlistConfig.BindAddr}, false); err != nil {
		t.Fatalf("err: %v", err)
	}
	waitUntilNumNodes(t, 2, s1, s2)

	if err := s2.Shutdown(); err != nil {
		t.Fatalf("err: %v", err)
	}
	time.Sleep(c2.MemberlistConfig.ProbeInterval * 5)

	const numEvents = 600
	names := make([]string, 0, numEvents)
	for i := 0; i < numEvents; i++ {
		name := fmt.Sprintf("offline-%03d", i)
		payload := []byte(name)
		if err := s1.UserEvent(name, payload, false); err != nil {
			t.Fatalf("err: %v", err)
		}
		names = append(names, name)
	}

	eventCh := make(chan Event, numEvents)
	c2 = testConfig(t, ip2)
	c2.SnapshotPath = snapshotPath
	c2.EventBuffer = 64
	c2.EventBufferMax = 1024
	c2.EventCh = eventCh
	s2, err = Create(c2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer s2.Shutdown()

	retry.Run(t, func(r *retry.R) {
		s2.eventLock.RLock()
		size := s2.eventHistory.size
		s2.eventLock.RUnlock()
		if size != numEvents {
			r.Fatalf("caught up %d events, want %d", size, numEvents)
		}
	})

	seen := make(map[string]int, numEvents)
	deadline := time.After(2 * time.Second)
	for len(seen) < numEvents {
		select {
		case event := <-eventCh:
			userEvent, ok := event.(UserEvent)
			if !ok {
				continue
			}
			seen[userEvent.Name]++
		case <-deadline:
			t.Fatalf("received %d of %d events", len(seen), numEvents)
		}
	}

	for _, name := range names {
		if got := seen[name]; got != 1 {
			t.Fatalf("event %q received %d times", name, got)
		}
	}
	if got := len(seen); got != numEvents {
		t.Fatalf("received %d unique events, want %d", got, numEvents)
	}
}
