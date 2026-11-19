// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package serf

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

/*
The eventSequenceStore persists the highest user-event Lamport time that a
node has observed so that duplicate event delivery can still be detected
after a restart.

The user-event de-duplication state (eventClock/eventBuffer/eventMinTime)
otherwise lives only in memory: once a node restarts the Lamport clock
starts over and the events that are still being gossiped or replayed via
push/pull are mistaken for new events and delivered a second time.

Writes are performed by a background goroutine. The gossip path only hands
the observed time to a buffered channel, so no disk I/O or extra lock is
ever taken on the broadcast hot path.
*/

const (
	// eventSequenceFlushInterval is how often the persisted event time is
	// flushed to disk.
	eventSequenceFlushInterval = 500 * time.Millisecond

	// eventSequenceChSize is the size of the notification buffer. The
	// channel send is non-blocking: if the buffer is full the background
	// loop is already scheduled and the periodic flush will pick up the
	// latest clock value anyway.
	eventSequenceChSize = 64
)

// EventSequenceStore durably records the highest user-event Lamport time a
// node has observed. The zero value is not usable; create one with
// newEventSequenceStore.
type EventSequenceStore struct {
	clock  *LamportClock
	path   string
	logger *log.Logger

	notifyCh chan struct{}
	shutdown chan struct{}
	waitCh   chan struct{}

	lastPersisted LamportTime
}

// newEventSequenceStore opens (or creates) the file at path, restores any
// previously persisted event time, and starts the background persistence
// loop. The returned LamportTime is the highest event time observed before
// the previous process exited (0 when there is no usable state).
func newEventSequenceStore(path string, clock *LamportClock, logger *log.Logger) (*EventSequenceStore, LamportTime, error) {
	store := &EventSequenceStore{
		clock:    clock,
		path:     path,
		logger:   logger,
		notifyCh: make(chan struct{}, eventSequenceChSize),
		shutdown: make(chan struct{}),
		waitCh:   make(chan struct{}),
	}

	last, err := store.recover()
	if err != nil {
		return nil, 0, err
	}
	store.lastPersisted = last

	go store.loop()
	return store, last, nil
}

// Observe hands a newly observed event time to the background persistence
// loop. It never blocks and takes no lock, so it is safe to call from the
// gossip/broadcast hot path.
func (s *EventSequenceStore) Observe() {
	select {
	case s.notifyCh <- struct{}{}:
	default:
	}
}

// Shutdown flushes the current event time to disk and stops the background
// loop. It is safe to call more than once.
func (s *EventSequenceStore) Shutdown() {
	select {
	case <-s.shutdown:
		return
	default:
		close(s.shutdown)
	}
	<-s.waitCh
}

// loop is the long running routine that performs all disk I/O.
func (s *EventSequenceStore) loop() {
	defer close(s.waitCh)

	ticker := time.NewTicker(eventSequenceFlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.notifyCh:
			s.persist()
		case <-ticker.C:
			s.persist()
		case <-s.shutdown:
			s.persist()
			return
		}
	}
}

// persist writes the current highest observed event time when it is newer
// than what is already on disk.
func (s *EventSequenceStore) persist() {
	// Mirror the snapshot semantics: the clock points at the next time to
	// hand out, so the last fully observed event is one tick behind.
	current := s.clock.Time()
	if current > 0 {
		current--
	}
	if current <= s.lastPersisted {
		return
	}

	if err := s.write(current); err != nil {
		s.logger.Printf("[ERR] serf: failed to persist event sequence: %v", err)
		return
	}
	s.lastPersisted = current
}

// write atomically replaces the on-disk state with the given event time.
func (s *EventSequenceStore) write(eventTime LamportTime) error {
	tmpPath := s.path + ".tmp"
	fh, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to open event sequence file: %v", err)
	}

	writer := bufio.NewWriter(fh)
	if _, err := fmt.Fprintf(writer, "event-clock: %d\n", eventTime); err != nil {
		fh.Close()
		return err
	}
	if err := writer.Flush(); err != nil {
		fh.Close()
		return err
	}
	if err := fh.Sync(); err != nil {
		fh.Close()
		return err
	}
	if err := fh.Close(); err != nil {
		return err
	}

	// Windows does not allow renaming over an existing file, so remove the
	// previous state first. This mirrors the snapshot compaction logic.
	if err := os.Remove(s.path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to replace event sequence file: %v", err)
	}
	if err := os.Rename(tmpPath, s.path); err != nil {
		return fmt.Errorf("failed to install event sequence file: %v", err)
	}
	return nil
}

// recover reads the highest previously persisted event time. A missing file
// is treated as a fresh start.
func (s *EventSequenceStore) recover() (LamportTime, error) {
	fh, err := os.Open(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to open event sequence file: %v", err)
	}
	defer fh.Close()

	var last LamportTime
	scanner := bufio.NewScanner(fh)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		value, ok := strings.CutPrefix(line, "event-clock: ")
		if !ok {
			continue
		}
		parsed, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			s.logger.Printf("[WARN] serf: failed to parse event sequence line %q: %v", line, err)
			continue
		}
		if LamportTime(parsed) > last {
			last = LamportTime(parsed)
		}
	}
	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("failed to read event sequence file: %v", err)
	}
	return last, nil
}
