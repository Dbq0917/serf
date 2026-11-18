// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package serf

import (
	"testing"

	"github.com/hashicorp/serf/testutil"
)

// TestSerf_EventDropStatsWiring verifies that a Serf instance created with a
// caller-supplied EventCh installs the bounded dispatcher and exposes the
// degradation counters, while an instance without an EventCh keeps the
// historical no-op behavior. It does not require a multi-node cluster.
func TestSerf_EventDropStatsWiring(t *testing.T) {
	ip, returnFn := testutil.TakeIP()
	defer returnFn()

	// With a caller-supplied EventCh, the dispatcher must be installed and
	// the stats accessor must report a usable, zero-initialized snapshot.
	eventCh := make(chan Event, 16)
	configWithCh := testConfig(t, ip)
	configWithCh.EventCh = eventCh

	serfWithCh, err := Create(configWithCh)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if serfWithCh.eventDispatcher == nil {
		t.Fatal("expected a bounded event dispatcher when EventCh is supplied")
	}
	stats := serfWithCh.EventDropStats()
	if stats.DroppedByType == nil {
		t.Fatal("DroppedByType map must be initialized")
	}
	if stats.TotalDropped != 0 || stats.CriticalDelivered != 0 {
		t.Fatalf("fresh dispatcher must report zero counters, got %+v", stats)
	}
	if err := serfWithCh.Shutdown(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Without a caller-supplied EventCh, no dispatcher is installed and the
	// accessor returns a safe empty snapshot rather than panicking.
	ip2, returnFn2 := testutil.TakeIP()
	defer returnFn2()

	configNoCh := testConfig(t, ip2)
	serfNoCh, err := Create(configNoCh)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if serfNoCh.eventDispatcher != nil {
		t.Fatal("did not expect a dispatcher without a caller-supplied EventCh")
	}
	emptyStats := serfNoCh.EventDropStats()
	if emptyStats.TotalDropped != 0 || emptyStats.DroppedByType == nil {
		t.Fatalf("expected empty stats without a dispatcher, got %+v", emptyStats)
	}
	if err := serfNoCh.Shutdown(); err != nil {
		t.Fatalf("err: %v", err)
	}
}
