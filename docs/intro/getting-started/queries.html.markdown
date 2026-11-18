---
layout: "intro"
page_title: "Custom Queries"
sidebar_current: "gettingstarted-queries"
description: |-
  While custom events provide an efficient "fire-and-forget" mechanism, queries send a request and nodes can provide responds. Custom queries provide even more flexibility than events, since the target nodes can be filtered, delivery can be acknowledged and custom responses can be sent back. This makes queries useful for gathering information about a running cluster in real-time.
---

# Custom Queries

While custom events provide an efficient "fire-and-forget" mechanism, queries
send a request and nodes can provide responses. Custom queries provide even more
flexibility than events, since the target nodes can be filtered, delivery
can be acknowledged and custom responses can be sent back. This makes queries
useful for gathering information about a running cluster in real-time.

## Sending Custom Queries

First, start a Serf agent so we can see the query being sent. Since we'll
just be running a single agent, running `serf agent` by itself is fine.
Then, to send a custom query, use the `serf query` command:

```
$ serf query load
```

If you look at the output of `serf agent`, you should see that it received
the user query:

```
...
2013/10/22 07:06:32 [INFO] agent: Received event: query: load
```

If the cluster were made up of multiple members, all of the reachable
members would have received this query.

Event handlers used with queries are even more powerful, as their
output is sent back to the query originator. This enables the query to make
a "request", while the event handler can generate the response.

For example, if we had a "load" custom event, we might return
the current load average of the machine.

Serf agents must be configured to handle queries before they will
respond to them. See the "Specifying Event Handlers" section in the
[Event Handlers documentation](/docs/agent/event-handlers.html.markdown) for
examples of the configuration format.

## Query Payloads

Queries are not limited to just a query name. The query can also contain
a payload: arbitrary data associated with the query. With our same agent
running, let's deliver an event with a payload: `serf query my-name-is Mitchell`

In practice, query payloads can contain information such as the git commit
to deploy if you're using Serf as a deployment tool. Or perhaps it contains
some updated configuration to modify on the nodes. It can contain anything
you'd like; it is up to the event handler to use it in some meaningful way.

## Limitations

Custom queries are delivered using the Serf gossip layer. The benefits of
this approach is that you get completely decentralized messaging across
your entire cluster that is fault tolerant.

Due to the mechanics of gossip, custom queries are highly scalable: Serf doesn't
need to connect to each and every node to send the message, it only needs to connect
to a handful, regardless of cluster size. It does require that all responses are
sent directly to the originator, which can be an issue if many nodes are responding.

Custom queries come with some trade-offs, however:

* Queries may not be delivered to all nodes: Because events are delivered over
  gossip, the message arrives at every node unless there is a network partition.
  Queries are designed for real-time use cases, and because responses to old queries
  are not useful, Serf will not attempt to redeliver queries to nodes that were
  offline or could not be reached. This is a critical distinction from custom events,
  which do attempt a later delivery.

* Responses and acknowledgements are sent over UDP back to the query originator.
  Since UDP does not provide any of the reliability or retry mechanisms of TCP,
  this means that although a query handler may be invoked, the response may
  not be received due to network issues.

* Payload size is limited: Serf gossips via UDP, so the payload must fit
  within a single UDP packet (alongside any other data Serf sends). This
  limits the potential size of a payload to less than 1 KB.

## Partial Results and Missing Nodes

When a query reaches its timeout, some nodes may not have replied yet. Rather
than failing the whole query, Serf keeps the responses already received and
makes the gap explicit so that callers never mistake a partial answer for the
full cluster. The query timeout itself bounds how long a caller waits.

The Go API exposes this through additive methods on `serf.QueryResponse`; the
existing `AckCh`, `ResponseCh`, and their streamed message formats are
unchanged:

* `ExpectedNodes() []string` - snapshot of the members expected to answer when
  the query was dispatched, including any node and tag filters applied.
* `MissingNodes() []string` - expected members from which no response (or ack,
  for ack-only queries) arrived before the timeout. An empty slice means the
  query completed fully.
* `NodeTimings() []serf.NodeTiming` - per-node ack and response latency measured
  from the start of the query, making slow members directly identifiable.
* `Elapsed() time.Duration` - total wall-clock duration of the query once it has
  finished.

When a query closes, Serf also emits an auditable log line recording the
expected, acknowledged, responded, and missing counts together with the missing
node names and the elapsed time, plus per-node timing records at debug level.
Metrics are emitted as `serf.query.missing_nodes`, `serf.query.duration`, and
the `serf.query.incomplete` counter so partial results can be alerted on.

