// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package serf

import (
	"log"
	"os"
	"path/filepath"
	"testing"
)

func TestEventSequenceStore_freshStart(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "event-seq")
	clock := new(LamportClock)

	store, last, err := newEventSequenceStore(path, clock, log.New(os.Stderr, "", log.LstdFlags))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer store.Shutdown()

	if last != 0 {
		t.Fatalf("expected no persisted time, got %d", last)
	}
}

func TestEventSequenceStore_persistAndRecover(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "event-seq")
	logger := log.New(os.Stderr, "", log.LstdFlags)

	clock := new(LamportClock)
	clock.Witness(10) // clock.Time() == 11, last observed event is 10

	store, _, err := newEventSequenceStore(path, clock, logger)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	store.Observe()
	store.Shutdown()

	// Re-open with a fresh clock and confirm the high-water mark survives.
	recoveredClock := new(LamportClock)
	_, last, err := newEventSequenceStore(path, recoveredClock, logger)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if last != 10 {
		t.Fatalf("expected recovered time 10, got %d", last)
	}
}

func TestEventSequenceStore_observeNeverBlocks(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "event-seq")
	clock := new(LamportClock)

	store, _, err := newEventSequenceStore(path, clock, log.New(os.Stderr, "", log.LstdFlags))
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Flood the notification channel; Observe must never block the caller.
	for i := 0; i < eventSequenceChSize*4; i++ {
		store.Observe()
	}
	store.Shutdown()
}
