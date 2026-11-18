// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package serf

import (
	"bytes"
	"log"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/serf/testutil"
)

// slowNodeSet is shared by the responder goroutines of the test cluster.
// Nodes named here sleep for the configured delay before answering, which
// lets a short query time out while they are still processing.
type slowNodeSet struct {
	mu      sync.Mutex
	delays  map[string][]time.Duration
	waiters map[string]chan time.Duration
	phase   int
}

func newSlowNodeSet() *slowNodeSet {
	return &slowNodeSet{
		delays:  make(map[string][]time.Duration),
		waiters: make(map[string]chan time.Duration),
	}
}

// queue records a delay that the named node applies to its next response.
// A zero delay makes the node answer immediately.
func (s *slowNodeSet) queue(node string, delay time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if waiter, ok := s.waiters[node]; ok {
		select {
		case waiter <- delay:
			return
		default:
		}
	}
	s.delays[node] = append(s.delays[node], delay)
}

// waitForDelay blocks until a delay is queued for the node and returns it.
func (s *slowNodeSet) waitForDelay(node string) time.Duration {
	s.mu.Lock()
	if queued := s.delays[node]; len(queued) > 0 {
		delay := queued[0]
		s.delays[node] = queued[1:]
		s.mu.Unlock()
		return delay
	}
	waiter := make(chan time.Duration, 1)
	s.waiters[node] = waiter
	s.mu.Unlock()
	return <-waiter
}

// currentPhase returns the active query phase. Slow sleeps scheduled in an
// older phase are skipped so a new query is never blocked by an old one.
func (s *slowNodeSet) currentPhase() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.phase
}

// advancePhase marks the start of a new query phase, invalidating slow
// sleeps belonging to previous phases.
func (s *slowNodeSet) advancePhase() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.phase++
}

// sleepForPhase applies the delay only while the given phase is still
// active, returning early when the test moves on to the next phase.
func (s *slowNodeSet) sleepForPhase(phase int, delay time.Duration) {
	if delay <= 0 {
		return
	}
	deadline := time.Now().Add(delay)
	for time.Now().Before(deadline) {
		if s.currentPhase() != phase {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// partialQueryCluster builds a cluster of the requested size where every
// node answers query events, with per-node response delays driven by the
// shared slowNodeSet so that a short query can time out on selected members.
// The returned audit buffer captures the query completion records emitted by
// the originating node, providing a reviewable trail of the partial results.
func partialQueryCluster(t *testing.T, nodeCount int, slow *slowNodeSet) ([]*Serf, *bytes.Buffer) {
	t.Helper()

	serfs := make([]*Serf, 0, nodeCount)
	configs := make([]*Config, 0, nodeCount)
	eventChs := make([]chan Event, 0, nodeCount)
	var audit bytes.Buffer

	for i := 0; i < nodeCount; i++ {
		ip, returnFn := testutil.TakeIP()
		t.Cleanup(returnFn)

		config := testConfig(t, ip)
		eventCh := make(chan Event, 16)
		config.EventCh = eventCh
		if i == 0 {
			config.Logger = log.New(&audit, "audit[query]: ", log.LstdFlags)
			config.MemberlistConfig.Logger = config.Logger
		}

		node, err := Create(config)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		t.Cleanup(func() { node.Shutdown() })

		serfs = append(serfs, node)
		configs = append(configs, config)
		eventChs = append(eventChs, eventCh)
	}

	// Every node answers each query it receives, immediately except for the
	// configured slow nodes. Goroutines exit when the event channel closes
	// during shutdown.
	for i := range serfs {
		name := configs[i].NodeName
		eventCh := eventChs[i]
		go func() {
			for event := range eventCh {
				query, ok := event.(*Query)
				if !ok {
					continue
				}
				phase := slow.currentPhase()
				if delay := slow.waitForDelay(name); delay > 0 {
					slow.sleepForPhase(phase, delay)
					// The query timed out while we slept; do not answer a
					// stale query from a previous phase.
					if slow.currentPhase() != phase {
						continue
					}
				}
				if err := query.Respond([]byte("pong")); err != nil {
					t.Errorf("node %s failed to respond: %v", name, err)
				}
			}
		}()
	}

	// Join all nodes into one cluster.
	for i := 1; i < nodeCount; i++ {
		addr := configs[i].NodeName + "/" + configs[i].MemberlistConfig.BindAddr
		if _, err := serfs[0].Join([]string{addr}, false); err != nil {
			t.Fatalf("err: %v", err)
		}
	}
	waitUntilNumNodes(t, nodeCount, serfs...)

	return serfs, &audit
}

// drainResponses collects every response delivered until the query channel
// is closed at timeout.
func drainResponses(resp *QueryResponse) []NodeResponse {
	var responses []NodeResponse
	for nr := range resp.ResponseCh() {
		responses = append(responses, nr)
	}
	return responses
}

// TestSerf_Query_PartialResultsMarksMissing reproduces the incident: two
// nodes delay their response past the query timeout, so the query returns the
// partial results already received and explicitly names the missing nodes.
// A follow-up query with healthy timing returns the complete member set,
// proving callers can distinguish a partial cluster from a full one.
func TestSerf_Query_PartialResultsMarksMissing(t *testing.T) {
	const nodeCount = 4
	queryTimeout := 200 * time.Millisecond

	slow := newSlowNodeSet()
	serfs, audit := partialQueryCluster(t, nodeCount, slow)
	origin := serfs[0]

	allNames := make([]string, 0, nodeCount)
	for _, node := range serfs {
		allNames = append(allNames, node.config.NodeName)
	}
	slowNames := []string{allNames[2], allNames[3]}
	slices.Sort(slowNames)

	// Phase 1: two nodes cannot answer before the timeout, while the other
	// two (including the origin node) answer promptly.
	for _, node := range allNames {
		delay := time.Duration(0)
		if slices.Contains(slowNames, node) {
			delay = 750 * time.Millisecond
		}
		slow.queue(node, delay)
	}

	params := origin.DefaultQueryParams()
	params.Timeout = queryTimeout
	params.RequestAck = true

	start := time.Now()
	resp, err := origin.Query("load", []byte("phase1"), params)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	partial := drainResponses(resp)
	elapsed := time.Since(start)

	// The query must stay within its bounded timeout instead of hanging.
	if elapsed > queryTimeout+500*time.Millisecond {
		t.Fatalf("query took too long: %s", elapsed)
	}

	// Partial results are still delivered: the two healthy nodes answered.
	if len(partial) != nodeCount-len(slowNames) {
		t.Fatalf("expected %d partial responses, got %d: %v", nodeCount-len(slowNames), len(partial), partial)
	}

	// The missing nodes must be identifiable by name.
	missing := resp.MissingNodes()
	if !slices.Equal(missing, slowNames) {
		t.Fatalf("expected missing %v, got %v", slowNames, missing)
	}

	// Every expected member is accounted for, so callers never mistake the
	// partial set for the whole cluster.
	expected := resp.ExpectedNodes()
	if len(expected) != nodeCount {
		t.Fatalf("expected %d members, got %v", nodeCount, expected)
	}

	// Per-node timings expose who was slow, with data backing the claim.
	timings := resp.NodeTimings()
	responded := make(map[string]NodeTiming)
	for _, timing := range timings {
		responded[timing.Node] = timing
	}
	for _, node := range allNames {
		timing, ok := responded[node]
		if slices.Contains(slowNames, node) {
			if ok && timing.Responded {
				t.Fatalf("slow node %s unexpectedly responded in time", node)
			}
			continue
		}
		if !ok || !timing.Responded {
			t.Fatalf("healthy node %s missing from timings: %+v", node, timing)
		}
	}

	// The completion record must leave a reviewable audit trail naming the
	// missing nodes.
	trail := audit.String()
	for _, node := range slowNames {
		if !bytes.Contains(audit.Bytes(), []byte(node)) {
			t.Fatalf("audit trail does not name missing node %s:\n%s", node, trail)
		}
	}
	if !bytes.Contains(audit.Bytes(), []byte("missing=2")) {
		t.Fatalf("audit trail does not record missing count:\n%s", trail)
	}

	// Phase 2: every node responds promptly, so the next query is complete.
	slow.advancePhase()
	for _, node := range allNames {
		slow.queue(node, 0)
	}
	params2 := origin.DefaultQueryParams()
	params2.Timeout = queryTimeout
	params2.RequestAck = true

	resp2, err := origin.Query("load", []byte("phase2"), params2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	complete := drainResponses(resp2)

	if len(complete) != nodeCount {
		t.Fatalf("expected %d complete responses, got %d", nodeCount, len(complete))
	}
	if missing := resp2.MissingNodes(); len(missing) != 0 {
		t.Fatalf("expected no missing nodes, got %v", missing)
	}

	// All nodes now carry response latency data, including the previously
	// slow ones, proving the slow node was the cause rather than a failure.
	timings2 := resp2.NodeTimings()
	if len(timings2) != nodeCount {
		t.Fatalf("expected timings for %d nodes, got %d", nodeCount, len(timings2))
	}
	for _, timing := range timings2 {
		if !timing.Responded || timing.ResponseLatency <= 0 {
			t.Fatalf("node %s did not record response latency: %+v", timing.Node, timing)
		}
	}
}
