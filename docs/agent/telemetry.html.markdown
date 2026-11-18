---
layout: "docs"
page_title: "Telemetry"
sidebar_current: "docs-agent-telemetry"
description: |-
  The Serf agent collects various metrics data at runtime about the performance of different libraries and sub-systems. These metrics are aggregated on a ten second interval and are retained for one minute.
---

# Telemetry

The Serf agent collects various metrics data at runtime about the performance
of different libraries and sub-systems. These metrics are aggregated on a ten second
interval and are retained for one minute.

To view the telemetry information, you must send a `USR1` signal to the Serf
process. Windows users must use the `BREAK` signal instead.
Once Serf receives the signal, it will dump the current telemetry
information to the stderr of the agent.

In general, the telemetry information is useful for debugging or otherwise
getting a better view into what Serf is doing.

Below is an example output:

```
[2014-01-29 10:56:50 -0800 PST][G] 'serf-agent.runtime.num_goroutines': 19.000
[2014-01-29 10:56:50 -0800 PST][G] 'serf-agent.runtime.alloc_bytes': 755960.000
[2014-01-29 10:56:50 -0800 PST][G] 'serf-agent.runtime.malloc_count': 7550.000
[2014-01-29 10:56:50 -0800 PST][G] 'serf-agent.runtime.free_count': 4387.000
[2014-01-29 10:56:50 -0800 PST][G] 'serf-agent.runtime.heap_objects': 3163.000
[2014-01-29 10:56:50 -0800 PST][G] 'serf-agent.runtime.total_gc_pause_ns': 1151002.000
[2014-01-29 10:56:50 -0800 PST][G] 'serf-agent.runtime.total_gc_runs': 4.000
[2014-01-29 10:56:50 -0800 PST][C] 'serf-agent.agent.ipc.accept': Count: 5 Sum: 5.000
[2014-01-29 10:56:50 -0800 PST][C] 'serf-agent.agent.ipc.command': Count: 10 Sum: 10.000
[2014-01-29 10:56:50 -0800 PST][C] 'serf-agent.serf.events': Count: 5 Sum: 5.000
[2014-01-29 10:56:50 -0800 PST][C] 'serf-agent.serf.events.foo': Count: 4 Sum: 4.000
[2014-01-29 10:56:50 -0800 PST][C] 'serf-agent.serf.events.baz': Count: 1 Sum: 1.000
[2014-01-29 10:56:50 -0800 PST][S] 'serf-agent.memberlist.gossip': Count: 50 Min: 0.007 Mean: 0.020 Max: 0.041 Stddev: 0.007 Sum: 0.989
[2014-01-29 10:56:50 -0800 PST][S] 'serf-agent.serf.queue.Intent': Count: 10 Sum: 0.000
[2014-01-29 10:56:50 -0800 PST][S] 'serf-agent.serf.queue.Event': Count: 10 Min: 0.000 Mean: 2.500 Max: 5.000 Stddev: 2.121 Sum: 25.000
```

## Event Delivery Degradation Metrics

Serf wraps the application-facing event channel in a bounded buffer so that a
slow consumer cannot stall internal state updates. When a bulk membership
change produces events faster than they can be consumed, Serf degrades
gracefully and reports the degradation through the following metrics. A
sudden increase in the drop counter is the signal that someone is performing
a bulk change that the event consumer cannot keep up with.

* `serf.events.dropped` (counter): number of best-effort events
  (`member-join`, `member-update`, user events, queries) dropped because the
  bounded event channel was full. Emitted with an `event_type` label naming
  the event and a `stage` label (`consumer` for the application channel,
  `snapshot` for the on-disk snapshot stream). Critical `member-leave`,
  `member-failed`, and `member-reap` events are never counted here.
* `serf.events.critical_backpressure` (counter): number of times a critical
  event found the buffer full and had to wait for capacity. Critical events
  drive downstream resource cleanup, so Serf applies backpressure rather than
  dropping them.
* `serf.events.critical_abandoned` (counter): number of critical events that
  could not be delivered because Serf shut down while waiting for capacity.
* `serf.events.queue_depth` (sample): the bounded event channel depth sampled
  whenever an event is dropped, allowing drop spikes to be correlated with
  buffer saturation.

The same counters are available in-process through
`Serf.EventDropStats()`, and through `Snapshotter.DropStats()` when a
snapshot is configured, for tests and auditable verification.

