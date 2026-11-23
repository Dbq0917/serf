// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package serf

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net"
	"regexp"
	"slices"
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

// expectedQueryNodes returns the live members that match the query filters.
// The snapshot is used to decide when a query has collected every response
// and to avoid sending the query to members the filters exclude.
func (s *Serf) expectedQueryNodes(params *QueryParam) []string {
	nodeFilter := make(map[string]struct{}, len(params.FilterNodes))
	for _, node := range params.FilterNodes {
		nodeFilter[node] = struct{}{}
	}

	type tagFilter struct {
		tag string
		re  *regexp.Regexp
	}
	tagFilters := make([]tagFilter, 0, len(params.FilterTags))
	for tag, expr := range params.FilterTags {
		re, err := regexp.Compile(expr)
		if err != nil {
			// An invalid expression matches no nodes on the remote side.
			return nil
		}
		tagFilters = append(tagFilters, tagFilter{tag: tag, re: re})
	}

	members := s.Members()
	expected := make([]string, 0, len(members))
	for _, member := range members {
		if member.Status != StatusAlive {
			continue
		}
		if len(nodeFilter) > 0 {
			if _, ok := nodeFilter[member.Name]; !ok {
				continue
			}
		}

		matches := true
		for _, filter := range tagFilters {
			if !filter.re.MatchString(member.Tags[filter.tag]) {
				matches = false
				break
			}
		}
		if matches {
			expected = append(expected, member.Name)
		}
	}
	return expected
}

// QueryResponse is returned for each new Query. It is used to collect
// Ack's as well as responses and to provide those back to a client.
type QueryResponse struct {
	// ackCh is used to send the name of a node for which we've received an ack
	ackCh chan string

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

	// expected is the set of members that should answer this query based on
	// the filters in effect when it was dispatched. respondedExpected lets the
	// response close as soon as every expected member has replied instead of
	// always waiting for the full timeout.
	expectedSet       bool
	expected          map[string]struct{}
	respondedExpected int

	// finish deregisters the query and stops its timeout timer.
	finish func()

	// completionTimer closes the query shortly after every expected reply has
	// arrived, leaving callers a brief window to drain both result channels.
	completionTimer *time.Timer

	// done releases a response delivery blocked on a slow consumer when the
	// query finishes.
	done chan struct{}

	// responseWG ensures the response channel is only closed after every
	// in-flight delivery has returned.
	responseWG sync.WaitGroup

	closed    bool
	closeLock sync.Mutex
}

const queryCompletionGrace = 10 * time.Millisecond

// newQueryResponse is used to construct a new query response
func newQueryResponse(n int, q *messageQuery) *QueryResponse {
	resp := newQueryResponseInternal(n, q, nil, false)
	// Preserve the non-blocking delivery contract used by internal callers
	// that construct a response without an expected-member set.
	resp.respCh = make(chan NodeResponse, 1)
	return resp
}

func newQueryResponseWithExpected(n int, q *messageQuery, expected []string) *QueryResponse {
	return newQueryResponseInternal(n, q, expected, true)
}

func newQueryResponseInternal(n int, q *messageQuery, expected []string, expectedSet bool) *QueryResponse {
	resp := &QueryResponse{
		deadline:    time.Now().Add(q.Timeout),
		id:          q.ID,
		lTime:       q.LTime,
		respCh:      make(chan NodeResponse),
		responses:   make(map[string]struct{}),
		expectedSet: expectedSet,
		expected:    make(map[string]struct{}, len(expected)),
		done:        make(chan struct{}),
	}
	for _, node := range expected {
		resp.expected[node] = struct{}{}
	}
	if q.Ack() {
		// Acks only hold node names, so keeping one slot per known member is
		// cheap and lets completion account for every expected ack.
		resp.ackCh = make(chan string, n)
		resp.acks = make(map[string]struct{})
	}
	return resp
}

// Close is used to close the query, which will close the underlying
// channels and prevent further deliveries
func (r *QueryResponse) Close() {
	r.closeLock.Lock()
	if r.closed {
		r.closeLock.Unlock()
		return
	}
	if r.completionTimer != nil {
		r.completionTimer.Stop()
	}
	r.closed = true
	close(r.done)
	finish := r.finish
	r.closeLock.Unlock()

	// Wait for in-flight response delivers before closing the channels.
	r.responseWG.Wait()

	r.closeLock.Lock()
	if r.ackCh != nil {
		close(r.ackCh)
	}
	if r.respCh != nil {
		close(r.respCh)
	}
	r.closeLock.Unlock()

	if finish != nil {
		finish()
	}
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

func (r *QueryResponse) hasAck(node string) bool {
	r.closeLock.Lock()
	defer r.closeLock.Unlock()
	_, ok := r.acks[node]
	return ok
}

func (r *QueryResponse) hasResponse(node string) bool {
	r.closeLock.Lock()
	defer r.closeLock.Unlock()
	_, ok := r.responses[node]
	return ok
}

// setFinish installs the deregistration callback used by Close.
func (r *QueryResponse) setFinish(finish func()) {
	r.closeLock.Lock()
	if r.closed {
		r.closeLock.Unlock()
		finish()
		return
	}
	r.finish = finish
	r.closeLock.Unlock()
}

// completeIfDone schedules the query to close once every expected member has
// responded. The short grace period lets acks already in flight reach callers.
func (r *QueryResponse) completeIfDone() {
	r.closeLock.Lock()
	defer r.closeLock.Unlock()
	if !r.expectedSet || r.closed || r.completionTimer != nil {
		return
	}
	if r.respondedExpected < len(r.expected) {
		return
	}
	r.completionTimer = time.AfterFunc(queryCompletionGrace, r.Close)
}

// AckCh returns a channel that can be used to listen for acks
// Channel will be closed when the query is finished. This is nil,
// if the query did not specify RequestAck.
func (r *QueryResponse) AckCh() <-chan string {
	return r.ackCh
}

// ResponseCh returns a channel that can be used to listen for responses.
// Responses are delivered with backpressure to keep memory bounded, so callers
// should continue receiving until the channel is closed.
// Channel will be closed when the query is finished.
func (r *QueryResponse) ResponseCh() <-chan NodeResponse {
	return r.respCh
}

// sendResponse sends a response on the response channel ensuring the channel is not closed.
func (r *QueryResponse) sendResponse(nr NodeResponse) error {
	r.closeLock.Lock()
	if r.closed {
		r.closeLock.Unlock()
		return nil
	}
	if _, ok := r.responses[nr.From]; ok {
		r.closeLock.Unlock()
		return nil
	}
	r.responses[nr.From] = struct{}{}
	if _, ok := r.expected[nr.From]; ok {
		r.respondedExpected++
	}
	r.responseWG.Add(1)
	respCh := r.respCh
	done := r.done
	r.closeLock.Unlock()

	defer r.responseWG.Done()
	select {
	case respCh <- nr:
	case <-done:
	}
	return nil
}

// sendAck sends an ack on the ack channel ensuring the channel is not closed.
func (r *QueryResponse) sendAck(nr *messageQueryResponse) error {
	r.closeLock.Lock()
	defer r.closeLock.Unlock()
	if r.closed {
		return nil
	}
	if _, ok := r.acks[nr.From]; ok {
		return nil
	}
	select {
	case r.ackCh <- nr.From:
		r.acks[nr.From] = struct{}{}
	default:
		return errors.New("serf: Failed to deliver query response, dropping")
	}
	return nil
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
