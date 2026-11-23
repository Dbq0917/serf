// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package serf

import (
	"fmt"
	"io"
	"log"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/serf/testutil"
)

// TestSerf_Query_ParallelFanout verifies that a query reaches every member
// without waiting for gossip retransmissions and that the response side closes
// as soon as all members have answered.
func TestSerf_Query_ParallelFanout(t *testing.T) {
	const nodeCount = 4
	const queryTimeout = 200 * time.Millisecond

	serfs := make([]*Serf, 0, nodeCount)
	configs := make([]*Config, 0, nodeCount)
	ctx := t.Context()
	for i := 0; i < nodeCount; i++ {
		ip, returnFn := testutil.TakeIP()
		t.Cleanup(returnFn)

		config := testConfig(t, ip)
		config.Logger = log.New(io.Discard, "", 0)
		config.MemberlistConfig.Logger = config.Logger

		eventCh := make(chan Event, 32)
		config.EventCh = eventCh

		node, err := Create(config)
		if err != nil {
			t.Fatalf("create node %d: %v", i, err)
		}
		t.Cleanup(func() { _ = node.Shutdown() })
		serfs = append(serfs, node)
		configs = append(configs, config)

		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case event := <-eventCh:
					if query, ok := event.(*Query); ok {
						_ = query.Respond([]byte("pong"))
					}
				}
			}
		}()
	}

	for i := 1; i < nodeCount; i++ {
		addr := configs[i].NodeName + "/" + configs[i].MemberlistConfig.BindAddr
		if _, err := serfs[0].Join([]string{addr}, false); err != nil {
			t.Fatalf("join node %d: %v", i, err)
		}
	}
	waitUntilNumNodes(t, nodeCount, serfs...)

	params := serfs[0].DefaultQueryParams()
	params.Timeout = queryTimeout
	params.RequestAck = true

	start := time.Now()
	resp, err := serfs[0].Query("load", []byte("ping"), params)
	if err != nil {
		t.Fatalf("query: %v", err)
	}

	responses := 0
	for range resp.ResponseCh() {
		responses++
	}
	elapsed := time.Since(start)
	t.Logf("collected %d responses in %s", responses, elapsed)

	if responses != nodeCount {
		t.Fatalf("responses=%d, want %d", responses, nodeCount)
	}
	if elapsed >= queryTimeout/2 {
		t.Fatalf("query took %s, want early completion before %s", elapsed, queryTimeout/2)
	}
}

// TestQueryResponse_BoundedParallelDelivery verifies that responses can be
// aggregated concurrently without allocating a per-member response queue.
func TestQueryResponse_BoundedParallelDelivery(t *testing.T) {
	const nodeCount = 200

	expected := make([]string, 0, nodeCount)
	for i := 0; i < nodeCount; i++ {
		expected = append(expected, fmt.Sprintf("node-%d", i))
	}

	mq := &messageQuery{
		LTime:   1,
		ID:      1,
		Timeout: time.Second,
		Flags:   queryFlagAck,
	}
	resp := newQueryResponseWithExpected(nodeCount, mq, expected)
	if got := cap(resp.ResponseCh()); got != 0 {
		t.Fatalf("response buffer=%d, want 0", got)
	}
	if got := cap(resp.AckCh()); got != nodeCount {
		t.Fatalf("ack buffer=%d, want %d", got, nodeCount)
	}

	countCh := make(chan int, 1)
	go func() {
		count := 0
		for range resp.ResponseCh() {
			count++
		}
		countCh <- count
	}()

	var wg sync.WaitGroup
	for i, node := range expected {
		wg.Add(1)
		go func(i int, node string) {
			defer wg.Done()
			err := resp.sendResponse(NodeResponse{
				From:    node,
				Payload: []byte(fmt.Sprintf("payload-%d", i)),
			})
			if err != nil {
				t.Errorf("send response from %s: %v", node, err)
			}
		}(i, node)
	}
	wg.Wait()
	resp.Close()

	if count := <-countCh; count != nodeCount {
		t.Fatalf("responses=%d, want %d", count, nodeCount)
	}
}

func TestExpectedQueryNodes(t *testing.T) {
	s := &Serf{
		members: map[string]*memberState{
			"alpha": {
				Member: Member{
					Name:   "alpha",
					Status: StatusAlive,
					Tags:   map[string]string{"role": "web"},
				},
			},
			"bravo": {
				Member: Member{
					Name:   "bravo",
					Status: StatusAlive,
					Tags:   map[string]string{"role": "db"},
				},
			},
			"charlie": {
				Member: Member{
					Name:   "charlie",
					Status: StatusFailed,
					Tags:   map[string]string{"role": "web"},
				},
			},
		},
	}

	got := s.expectedQueryNodes(&QueryParam{
		FilterTags: map[string]string{"role": "^web$"},
	})
	slices.Sort(got)
	if want := []string{"alpha"}; !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}

	got = s.expectedQueryNodes(&QueryParam{FilterNodes: []string{"bravo"}})
	if want := []string{"bravo"}; !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}
