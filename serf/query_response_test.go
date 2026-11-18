// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package serf

import (
	"reflect"
	"testing"
	"time"
)

func TestQueryResponse_MissingNodesAndTimings(t *testing.T) {
	mq := &messageQuery{
		LTime:   7,
		ID:      9,
		Timeout: 50 * time.Millisecond,
		Flags:   queryFlagAck,
	}
	expected := []string{"alpha", "bravo", "charlie"}
	resp := newQueryResponse(3, mq, expected, nil)

	// alpha acks and responds, bravo only acks, charlie never replies.
	if err := resp.sendAck(&messageQueryResponse{From: "alpha"}); err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := resp.sendAck(&messageQueryResponse{From: "bravo"}); err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := resp.sendResponse(NodeResponse{From: "alpha", Payload: []byte("ok")}); err != nil {
		t.Fatalf("err: %v", err)
	}

	// With at least one response, only alpha answered and charlie is missing
	// together with bravo (which acked but never sent a response).
	if got, want := resp.MissingNodes(), []string{"bravo", "charlie"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("missing: got %v want %v", got, want)
	}

	timings := resp.NodeTimings()
	if len(timings) != 2 {
		t.Fatalf("expected timings for 2 replying nodes, got %d", len(timings))
	}
	byNode := make(map[string]NodeTiming)
	for _, timing := range timings {
		byNode[timing.Node] = timing
	}
	alpha := byNode["alpha"]
	if !alpha.Acked || !alpha.Responded || alpha.AckLatency <= 0 || alpha.ResponseLatency <= 0 {
		t.Fatalf("bad alpha timing: %+v", alpha)
	}
	bravo := byNode["bravo"]
	if !bravo.Acked || bravo.Responded || bravo.AckLatency <= 0 || bravo.ResponseLatency != 0 {
		t.Fatalf("bad bravo timing: %+v", bravo)
	}

	// Elapsed is only populated after Close.
	if resp.Elapsed() != 0 {
		t.Fatalf("elapsed should be zero before close: %s", resp.Elapsed())
	}
	resp.Close()
	if resp.Elapsed() <= 0 {
		t.Fatalf("elapsed should be recorded after close")
	}
}

func TestQueryResponse_MissingNodesAckOnly(t *testing.T) {
	mq := &messageQuery{
		LTime:   8,
		ID:      10,
		Timeout: 50 * time.Millisecond,
		Flags:   queryFlagAck,
	}
	resp := newQueryResponse(3, mq, []string{"alpha", "bravo", "charlie"}, nil)

	// Ack-only query: any ack counts as an answer.
	if err := resp.sendAck(&messageQueryResponse{From: "alpha"}); err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := resp.sendAck(&messageQueryResponse{From: "bravo"}); err != nil {
		t.Fatalf("err: %v", err)
	}

	if got, want := resp.MissingNodes(), []string{"charlie"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("missing: got %v want %v", got, want)
	}
	if got := resp.ExpectedNodes(); len(got) != 3 {
		t.Fatalf("expected nodes: %v", got)
	}
}

func TestSerf_ExpectedQueryNodes(t *testing.T) {
	s := &Serf{
		members: map[string]*memberState{
			"alpha":   {Member: Member{Name: "alpha", Status: StatusAlive, Tags: map[string]string{"role": "web"}}},
			"bravo":   {Member: Member{Name: "bravo", Status: StatusAlive, Tags: map[string]string{"role": "db"}}},
			"charlie": {Member: Member{Name: "charlie", Status: StatusFailed, Tags: map[string]string{"role": "web"}}},
		},
	}

	got := s.expectedQueryNodes(&QueryParam{
		FilterTags: map[string]string{"role": "^web$"},
	})
	want := []string{"alpha"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}

	got = s.expectedQueryNodes(&QueryParam{FilterNodes: []string{"bravo"}})
	want = []string{"bravo"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}
