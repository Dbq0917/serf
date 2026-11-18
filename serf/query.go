// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package serf

import (
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"regexp"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
)

// QueryParam is provided to Query() to configure the parameters of the
// query. If not provided, sane defaults will be used.
type QueryParam struct {
	// If provided, we restrict the nodes that should respond to those
	// with names in this list
	FilterNodes []string

	// FilterTags maps a tag name to a regular expression that is applied
	// to restrict the nodes that should respond
	FilterTags map[string]string

	// If true, we are requesting an delivery acknowledgement from
	// every node that meets the filter requirement. This means nodes
	// the receive the message but do not pass the filters, will not
	// send an ack.
	RequestAck bool

	// RelayFactor controls the number of duplicate responses to relay
	// back to the sender through other nodes for redundancy.
	RelayFactor uint8

	// The timeout limits how long the query is left open. If not provided,
	// then a default timeout is used based on the configuration of Serf
	Timeout time.Duration
}

// DefaultQueryTimeout returns the default timeout value for a query
// Computed as GossipInterval * QueryTimeoutMult * log(N+1)
func (s *Serf) DefaultQueryTimeout() time.Duration {
	n := s.memberlist.NumMembers()
	timeout := s.config.MemberlistConfig.GossipInterval
	timeout *= time.Duration(s.config.QueryTimeoutMult)
	timeout *= time.Duration(math.Ceil(math.Log10(float64(n + 1))))
	return timeout
}

// DefaultQueryParam is used to return the default query parameters
func (s *Serf) DefaultQueryParams() *QueryParam {
	return &QueryParam{
		FilterNodes: nil,
		FilterTags:  nil,
		RequestAck:  false,
		Timeout:     s.DefaultQueryTimeout(),
	}
}

// encodeFilters is used to convert the filters into the wire format
func (q *QueryParam) encodeFilters() ([][]byte, error) {
	var filters [][]byte

	// Add the node filter
	if len(q.FilterNodes) > 0 {
		if buf, err := encodeFilter(filterNodeType, q.FilterNodes); err != nil {
			return nil, err
		} else {
			filters = append(filters, buf)
		}
	}

	// Add the tag filters
	for tag, expr := range q.FilterTags {
		filt := filterTag{tag, expr}
		if buf, err := encodeFilter(filterTagType, &filt); err != nil {
			return nil, err
		} else {
			filters = append(filters, buf)
		}
	}

	return filters, nil
}

// QueryResponse is returned for each new Query. It is used to collect
// Ack's as well as responses and to provide those back to a client.
type QueryResponse struct {
	// ackCh is used to send the name of a node for which we've received an ack
	ackCh chan string

	// startedAt is the time the query was initiated, used to measure the
	// per-node response latency.
	startedAt time.Time

	// closedAt is the time the query was closed, either by its timeout or
	// an explicit Close. It is zero until the query is finished.
	closedAt time.Time

	// deadline is the query end time (start + query timeout)
	deadline time.Time

	// Query ID
	id uint32

	// Stores the LTime of the query
	lTime LamportTime

	// respCh is used to send a response from a node
	respCh chan NodeResponse

	// acks/responses are used to track the nodes that have sent an ack/response
	acks      map[string]struct{}
	responses map[string]struct{}

	// expectedNodes is the snapshot of cluster members that were expected
	// to answer the query at the time it was dispatched. It lets callers
	// tell which nodes never replied once the query finishes.
	expectedNodes []string

	// timings records the per-node ack/response latency measured from the
	// start of the query.
	timings map[string]*NodeTiming

	// logger is used to emit an auditable summary when the query closes.
	logger *log.Logger

	closed    bool
	closeLock sync.Mutex
}

// NodeTiming captures the response timings of a single node for a query.
// AckLatency and ResponseLatency are zero when the node did not send the
// corresponding reply, in which case Acked/Responded are false.
type NodeTiming struct {
	// Node is the name of the member the timings belong to.
	Node string

	// Acked reports whether the node acknowledged the query.
	Acked bool

	// AckLatency is the elapsed time between the start of the query and
	// the receipt of the node's ack.
	AckLatency time.Duration

	// Responded reports whether the node sent a query response.
	Responded bool

	// ResponseLatency is the elapsed time between the start of the query
	// and the receipt of the node's response.
	ResponseLatency time.Duration
}

// newQueryResponse is used to construct a new query response
func newQueryResponse(n int, q *messageQuery, expectedNodes []string, logger *log.Logger) *QueryResponse {
	now := time.Now()
	resp := &QueryResponse{
		deadline:      now.Add(q.Timeout),
		startedAt:     now,
		id:            q.ID,
		lTime:         q.LTime,
		respCh:        make(chan NodeResponse, n),
		responses:     make(map[string]struct{}),
		expectedNodes: append([]string(nil), expectedNodes...),
		timings:       make(map[string]*NodeTiming, n),
		logger:        logger,
	}
	if q.Ack() {
		resp.ackCh = make(chan string, n)
		resp.acks = make(map[string]struct{})
	}
	return resp
}

// Close is used to close the query, which will close the underlying
// channels and prevent further deliveries
func (r *QueryResponse) Close() {
	r.closeLock.Lock()
	defer r.closeLock.Unlock()
	if r.closed {
		return
	}
	r.closed = true
	r.closedAt = time.Now()
	if r.ackCh != nil {
		close(r.ackCh)
	}
	if r.respCh != nil {
		close(r.respCh)
	}
	r.logSummary()
}

// Deadline returns the ending deadline of the query
func (r *QueryResponse) Deadline() time.Time {
	return r.deadline
}

// Finished returns if the query is finished running
func (r *QueryResponse) Finished() bool {
	r.closeLock.Lock()
	defer r.closeLock.Unlock()
	return r.closed || time.Now().After(r.deadline)
}

// ExpectedNodes returns the snapshot of member names that were expected to
// answer the query at dispatch time. The returned slice is safe to retain.
func (r *QueryResponse) ExpectedNodes() []string {
	r.closeLock.Lock()
	defer r.closeLock.Unlock()
	return append([]string(nil), r.expectedNodes...)
}

// MissingNodes returns the names of the expected nodes from which no query
// response was received before the query finished. For ack-only queries
// (RequestAck without any response), nodes that acknowledged are not
// considered missing. The result is sorted and safe to retain.
func (r *QueryResponse) MissingNodes() []string {
	r.closeLock.Lock()
	defer r.closeLock.Unlock()
	return r.missingNodesLocked()
}

// missingNodesLocked computes the missing node names. The caller must hold
// closeLock.
func (r *QueryResponse) missingNodesLocked() []string {
	ackOnly := r.ackCh != nil && len(r.responses) == 0
	missing := make([]string, 0)
	for _, node := range r.expectedNodes {
		if _, ok := r.responses[node]; ok {
			continue
		}
		if ackOnly {
			if _, ok := r.acks[node]; ok {
				continue
			}
		}
		missing = append(missing, node)
	}
	sort.Strings(missing)
	return missing
}

// NodeTimings returns the per-node ack/response latency measured from the
// start of the query. It includes every node that replied, even if that
// node joined after the query was dispatched and is not in ExpectedNodes.
// The result is sorted by node name and safe to retain.
func (r *QueryResponse) NodeTimings() []NodeTiming {
	r.closeLock.Lock()
	defer r.closeLock.Unlock()

	timings := make([]NodeTiming, 0, len(r.timings))
	for _, timing := range r.timings {
		timings = append(timings, *timing)
	}
	sort.Slice(timings, func(i, j int) bool {
		return timings[i].Node < timings[j].Node
	})
	return timings
}

// Elapsed returns the total wall-clock duration the query ran. It is zero
// until the query is finished, after which it is the time between dispatch
// and Close.
func (r *QueryResponse) Elapsed() time.Duration {
	r.closeLock.Lock()
	defer r.closeLock.Unlock()
	if r.closedAt.IsZero() {
		return 0
	}
	return r.closedAt.Sub(r.startedAt)
}

// AckCh returns a channel that can be used to listen for acks
// Channel will be closed when the query is finished. This is nil,
// if the query did not specify RequestAck.
func (r *QueryResponse) AckCh() <-chan string {
	return r.ackCh
}

// ResponseCh returns a channel that can be used to listen for responses.
// Channel will be closed when the query is finished.
func (r *QueryResponse) ResponseCh() <-chan NodeResponse {
	return r.respCh
}

// timingFor returns the NodeTiming tracked for the node, creating it on
// first reply. The caller must hold closeLock.
func (r *QueryResponse) timingFor(node string) *NodeTiming {
	timing, ok := r.timings[node]
	if !ok {
		timing = &NodeTiming{Node: node}
		r.timings[node] = timing
	}
	return timing
}

// sendResponse sends a response on the response channel ensuring the channel is not closed.
func (r *QueryResponse) sendResponse(nr NodeResponse) error {
	r.closeLock.Lock()
	defer r.closeLock.Unlock()
	if r.closed {
		return nil
	}
	select {
	case r.respCh <- nr:
		r.responses[nr.From] = struct{}{}
		timing := r.timingFor(nr.From)
		timing.Responded = true
		timing.ResponseLatency = time.Since(r.startedAt)
	default:
		return errors.New("serf: Failed to deliver query response, dropping")
	}
	return nil
}

// sendResponse sends a response on the response channel ensuring the channel is not closed.
func (r *QueryResponse) sendAck(nr *messageQueryResponse) error {
	r.closeLock.Lock()
	defer r.closeLock.Unlock()
	if r.closed {
		return nil
	}
	select {
	case r.ackCh <- nr.From:
		r.acks[nr.From] = struct{}{}
		timing := r.timingFor(nr.From)
		timing.Acked = true
		timing.AckLatency = time.Since(r.startedAt)
	default:
		return errors.New("serf: Failed to deliver query response, dropping")
	}
	return nil
}

// logSummary emits an auditable record of how the query completed, including
// the expected/answered counts, the names of nodes that never replied, and
// the per-node response latency so slow members can be identified.
func (r *QueryResponse) logSummary() {
	if r.logger == nil {
		return
	}

	missing := r.missingNodesLocked()
	elapsed := r.closedAt.Sub(r.startedAt)
	r.logger.Printf("[INFO] serf: query completed: id=%d expected=%d acks=%d responses=%d missing=%d elapsed=%s missing_nodes=%v",
		r.id, len(r.expectedNodes), len(r.acks), len(r.responses), len(missing), elapsed, missing)

	for _, node := range r.expectedNodes {
		timing, ok := r.timings[node]
		if !ok {
			r.logger.Printf("[DEBUG] serf: query node timing: id=%d node=%s acked=false responded=false", r.id, node)
			continue
		}
		r.logger.Printf("[DEBUG] serf: query node timing: id=%d node=%s acked=%v ack_latency=%s responded=%v response_latency=%s",
			r.id, node, timing.Acked, timing.AckLatency, timing.Responded, timing.ResponseLatency)
	}
}

// NodeResponse is used to represent a single response from a node
type NodeResponse struct {
	From    string
	Payload []byte
}

// shouldProcessQuery checks if a query should be proceeded given
// a set of filers.
func (s *Serf) shouldProcessQuery(filters [][]byte) bool {
	for _, filter := range filters {
		if len(filter) == 0 {
			return false
		}
		switch filterType(filter[0]) {
		case filterNodeType:
			// Decode the filter
			var nodes filterNode
			if err := decodeMessage(filter[1:], &nodes); err != nil {
				s.logger.Printf("[WARN] serf: failed to decode filterNodeType: %v", err)
				return false
			}

			// Check if we are being targeted
			found := slices.Contains(nodes, s.config.NodeName)
			if !found {
				return false
			}

		case filterTagType:
			// Decode the filter
			var filt filterTag
			if err := decodeMessage(filter[1:], &filt); err != nil {
				s.logger.Printf("[WARN] serf: failed to decode filterTagType: %v", err)
				return false
			}

			// Check if we match this regex
			tags := s.config.Tags
			matched, err := regexp.MatchString(filt.Expr, tags[filt.Tag])
			if err != nil {
				s.logger.Printf("[WARN] serf: failed to compile filter regex (%s): %v", filt.Expr, err)
				return false
			}
			if !matched {
				return false
			}

		default:
			s.logger.Printf("[WARN] serf: query has unrecognized filter type: %d", filter[0])
			return false
		}
	}
	return true
}

// expectedQueryNodes returns the sorted names of alive members that are
// expected to answer the given query based on its filters. It is a best
// effort snapshot taken at dispatch time; members that join afterwards are
// still able to reply but are not reported as missing if they do not.
func (s *Serf) expectedQueryNodes(params *QueryParam) []string {
	members := s.Members()
	expected := make([]string, 0, len(members))
	for _, member := range members {
		if member.Status != StatusAlive {
			continue
		}
		if s.memberMatchesQueryFilters(member, params) {
			expected = append(expected, member.Name)
		}
	}
	sort.Strings(expected)
	return expected
}

// memberMatchesQueryFilters reports whether a member would process a query
// configured with the given parameters. It mirrors the filter logic applied
// by the receiving node in shouldProcessQuery.
func (s *Serf) memberMatchesQueryFilters(member Member, params *QueryParam) bool {
	if len(params.FilterNodes) > 0 && !slices.Contains(params.FilterNodes, member.Name) {
		return false
	}
	for tag, expr := range params.FilterTags {
		matched, err := regexp.MatchString(expr, member.Tags[tag])
		if err != nil || !matched {
			return false
		}
	}
	return true
}

// relayResponse will relay a copy of the given response to up to relayFactor
// other members.
func (s *Serf) relayResponse(
	relayFactor uint8,
	addr net.UDPAddr,
	nodeName string,
	resp *messageQueryResponse,
) error {
	if relayFactor == 0 {
		return nil
	}

	// Needs to be worth it; we need to have at least relayFactor *other*
	// nodes. If you have a tiny cluster then the relayFactor shouldn't
	// be needed.
	members := s.Members()
	if len(members) < int(relayFactor)+1 {
		return nil
	}

	// Prep the relay message, which is a wrapped version of the original.
	raw, err := encodeRelayMessage(messageQueryResponseType, addr, nodeName, &resp)
	if err != nil {
		return fmt.Errorf("failed to format relayed response: %v", err)
	}
	if len(raw) > s.config.QueryResponseSizeLimit {
		return fmt.Errorf("relayed response exceeds limit of %d bytes", s.config.QueryResponseSizeLimit)
	}

	// Relay to a random set of peers.
	localName := s.LocalMember().Name
	relayMembers := kRandomMembers(int(relayFactor), members, func(m Member) bool {
		return m.Status != StatusAlive || m.ProtocolMax < 5 || m.Name == localName
	})
	for _, m := range relayMembers {
		udpAddr := net.UDPAddr{IP: m.Addr, Port: int(m.Port)}
		relayAddr := memberlist.Address{
			Addr: udpAddr.String(),
			Name: m.Name,
		}
		if err := s.memberlist.SendToAddress(relayAddr, raw); err != nil {
			return fmt.Errorf("failed to send relay response: %v", err)
		}
	}
	return nil
}

// kRandomMembers selects up to k members from a given list, optionally
// filtering by the given filterFunc
func kRandomMembers(k int, members []Member, filterFunc func(Member) bool) []Member {
	n := len(members)
	kMembers := make([]Member, 0, k)
OUTER:
	// Probe up to 3*n times, with large n this is not necessary
	// since k << n, but with small n we want search to be
	// exhaustive
	for i := 0; i < 3*n && len(kMembers) < k; i++ {
		// Get random member
		idx := rand.Intn(n)
		member := members[idx]

		// Give the filter a shot at it.
		if filterFunc != nil && filterFunc(member) {
			continue OUTER
		}

		// Check if we have this member already
		for j := 0; j < len(kMembers); j++ {
			if member.Name == kMembers[j].Name {
				continue OUTER
			}
		}

		// Append the member
		kMembers = append(kMembers, member)
	}

	return kMembers
}
