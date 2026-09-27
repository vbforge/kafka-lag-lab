# Problem Description & Solution — kafka-lag-lab

This document is the "why" behind `kafka-lag-lab`: the original question that
started it, each root cause investigated, the fix chosen for each, and the
practical engineering problems that came up along the way. See `README.md`
for how to actually run the project.

## Origin

A "senior backend interview" post posed this scenario: a Kafka consumer
handling 10,000 messages/second suddenly sees lag climb — 10K → 50K → 500K →
5M — while consumers are already scaled horizontally. The team's instinct is
to add more consumers. The post's point: that's only correct if
consumer-vs-partition count is actually the constraint, and a senior
engineer should ask where the real bottleneck is before scaling anything.

The posted checklist covered ten candidate causes: partition ceiling,
processing speed, hot partitions, slow downstream calls, retries, synchronous
processing, rebalancing, uneven producer distribution, commit frequency, and
batching.

Rather than answer the checklist from memory, the goal became: build
something that reproduces each root cause on purpose, observe the symptom
with real data, then apply a real fix and confirm it worked — turning ten
interview bullet points into five runnable, falsifiable experiments.

## Problems investigated, and what resolved each

### 1. Hot partition (key skew)

**Problem:** if one key gets disproportionate traffic, Kafka's partitioner
pins it to a single partition. That partition's consumer becomes the
bottleneck no matter how many consumer instances exist — the other
partitions' capacity just sits idle. This directly answers the article's
"is one partition much hotter than the others?" and "is the producer
creating an uneven distribution?"

**How it was reproduced:** a producer key strategy that always emits the
same fixed key. Watching per-partition lag confirmed the prediction exactly:
one partition climbed steadily while the other five stayed flat, and a
skew-ratio metric (max partition lag ÷ average lag) climbed well past 3x.

**Solution chosen:** key salting — append a random suffix to the hot key so
it spreads across N partitions, while the true entity id still travels in
the message payload so a downstream consumer can regroup by it if needed.
Re-running with the salted strategy brought all six partitions back to flat,
low lag.

**Trade-off worth naming:** salting sacrifices strict per-key ordering for
that entity across partitions. That's a real cost, not a free fix, whenever
downstream logic depends on ordering.

### 2. Slow / synchronous processing

**Problem:** if the consumer itself is too slow — e.g. blocking on a
downstream DB or API call per message — lag climbs regardless of
partitioning. This is the article's "are consumers actually processing
messages fast enough?" and "are we doing expensive processing
synchronously?"

**How it was reproduced:** a processing mode that blocks the poll loop for
a fixed delay per message, strictly one at a time. The visible signature was
the opposite of the hot-partition case: lag climbed **evenly across nearly
all partitions**, not concentrated in one — the clearest way to tell these
two root causes apart just by looking at the lag table.

**Solution chosen:** fan the same per-message work out across a bounded
thread pool so several calls overlap instead of queueing behind each other.
The pool size sets a concrete throughput ceiling
(`pool size ÷ per-call latency`), which lined up with the observed recovery
once traffic stayed under that ceiling — lag stabilized and started
draining.

### 3. Retry storms

**Problem:** unbounded retries with no backoff cap mean a single unlucky
message can stall an entire batch indefinitely. This is the article's "are
consumers spending too much time on retries?"

**How it was reproduced:** a processing mode where each message has a
chance to fail and retries immediately, forever, with no limit. Lag grew,
though — as noted below — not always as visibly *erratically* as expected at
low failure rates.

**Solution chosen:** capped attempts with exponential backoff; after
exhausting attempts, log a simulated dead-letter entry and move on. This
puts a hard, predictable ceiling on the worst case per message instead of
an unbounded one.

**Nuance discovered during testing:** at a moderate failure rate (~30%),
the *average* extra cost per message from retries converged statistically
close to the fixed-delay slow-processing scenario once averaged over a full
poll batch (500 records) — meaning the two scenarios could look similarly
smooth in the lag graph, blurring the "erratic vs. steady" distinction the
project set out to demonstrate. Raising the failure rate sharpens the
difference; documented here because it's a genuinely useful finding about
the limits of diagnosing purely from periodic lag snapshots, not a bug in
either the scenario or its fix.

### 4. Rebalance thrashing

**Problem:** if a batch takes longer to process than `max.poll.interval.ms`
allows, the consumer gets evicted from the group and must rejoin —
repeatedly, if the underlying slowness persists. This can look like a
processing problem but is really a group-coordination problem. This is the
article's "is the consumer group rebalancing frequently?"

**How it was reproduced:** pairing the slow-processing scenario with a
deliberately tight `max.poll.interval.ms`, so a batch reliably exceeded the
interval before finishing. A rebalance listener added to the consumer made
every partition revoke/assign event visible as `[REBALANCE]` log lines —
previously an invisible mechanic.

**Solution chosen:** two valid fixes, both tested — shrinking
`max.poll.records` so a batch finishes well under the interval, or reusing
the async/thread-pool fix from problem #2 so the same batch completes
faster. Either way, `[REBALANCE]` events stopped appearing entirely: a clear
"constant churn, then silence" before/after comparison.

### 5. Diagnosing which one is active (the stretch goal)

**Problem:** having five separate scenarios is only useful if you can also
tell, from the outside, *which one* is happening — the same way an on-call
engineer would have to in a real incident, without already knowing the
answer.

**Solution chosen:** a small standalone tool that samples per-partition lag
over a short rolling window and classifies the pattern using the same
signals discovered above, checked in this order:

1. Frequent rebalance events → rebalance thrashing (checked first, since
   thrashing can otherwise masquerade as a processing slowdown)
2. Lag flat and low → healthy
3. One partition far above the rest → hot partition
4. Erratic, bursty lag growth → retry storm
5. Even lag growth across nearly all partitions → slow/sync processing

Run against each scenario, it correctly named four of the five outright; the
fifth (retry storm at a moderate failure rate) surfaced the averaging nuance
described in problem #3 above — which became a documented limitation rather
than something to paper over.

## Engineering problems solved along the way

Not every problem in this project was about Kafka — a few were ordinary
build and tooling snags, each worth recording since they cost real time to
track down:

- **`SLF4J: Failed to load class "StaticLoggerBinder"`** — caused by
  `exec-maven-plugin` running the app inside Maven's own JVM process, where
  Maven's bundled (older) `slf4j-api` won out over the project's own
  binding. Fixed by switching to `maven-shade-plugin` fat jars run via
  `java -jar`, which sidesteps Maven's classloader entirely. When the
  warning persisted even after that switch, the actual cause turned out to
  be a second, older `slf4j-api` pulled in transitively by `kafka-clients`
  itself, winning under Maven's "nearest dependency" rule — resolved by
  dropping SLF4J entirely in favor of plain `System.out.printf` for the
  simple throughput counters this project actually needed.
- **No Python available** for the diagnosis tool — rather than requiring an
  install, the tool was ported to plain Java with zero extra dependencies
  (it only ever shelled out to `kafka-consumer-groups --describe`, a
  capability with no dependency on the host's scripting language), keeping
  the whole project single-language.
- **Shade-plugin build noise** (`module-info.class` warnings, overlapping
  `META-INF/MANIFEST.MF` entries, an unused `dependency-reduced-pom.xml`) —
  all cosmetic, not functional, but cleaned up via shade-plugin filters and
  `createDependencyReducedPom=false` for a quieter build log.
- **Copy-paste-modify fatigue between sprints** — early on, moving from a
  "problem" scenario to its "fix" meant editing the same `ProducerApp`/
  `ConsumerApp` file back and forth, which made the git history hard to
  follow. Resolved by refactoring to a Strategy pattern: each scenario
  (`HotKeyStrategy`, `SlowSyncProcessor`, `RetryBoundedProcessor`, etc.)
  became its own class behind a shared interface, selected at runtime by an
  environment variable. Every scenario now permanently coexists as its own
  file instead of overwriting the last one.

## Summary

Every scenario in this project produces the same outward symptom — rising
consumer lag — from a different underlying cause, and each needed a
different fix: salting instead of adding consumers, concurrency instead of
raw thread count, bounded retries instead of unbounded ones, and batch
sizing instead of blind scaling. The throughline back to the original
article's question: "add more consumers" was never wrong on its own merits,
just incomplete — it only helps for exactly one of the five causes
investigated here.
