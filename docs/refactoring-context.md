---
title: Refactoring Context
audience: contributor
doc_type: reference
owner: engine
---

# Refactoring Context

This page defines the current boundary and evidence requirements for the
point-operation hot-path cleanup sequence. It is durable planning context, not
an additional task ledger.

## Current Target

- Target boundary: resource ownership and measured per-operation overhead in
  `engine` routing, latency tracking, segment reads, and context logging, plus
  the focused JMH coverage needed to evaluate those paths.
- Desired end state: fix the confirmed concurrent searcher resource leak, then
  retain only small hot-path changes that show a repeatable allocation,
  throughput, or latency improvement without changing public behavior.
- Current status: items `100.1` through `100.3` and `100.5` are complete and
  archived in `docs/refactor-backlog.md`. Item `100.4` is intentionally
  deferred. Item `100.6` has run, but remains open because the canonical
  one-fork regression screen reported contradictory `worse` and `better`
  results and the focused throughput repeats did not clear the performance
  gate.
- Stop condition: when the next candidate is no longer visible in a
  representative profile, or its focused A/B result is neutral, inconclusive,
  or worse, do not retain that optimization. Record the evidence and proceed to
  the final profiling item instead of broadening the design.

### Planning Evidence

The following results are exploratory evidence from the rebuilt benchmark
runner at commit `ab14e0f69`; they are not completion evidence because no
canonical baseline/candidate report was captured:

- One-fork JMH with three warmup and five measurement iterations reported about
  `186.780 B/op` for four-thread live gets and `216.496 B/op` for
  twenty-thread hot puts.
- JFR allocation samples included `RouteMapSnapshot` on both paths.
  `OperationLatencyTracker.recordNanos(...)` also appeared in execution samples.
- A persisted, cache-enabled profile showed `LruChunkStoreCache.getCachedPage`
  in CPU samples but no cache contention events.
- `OperationResult` and `RouteLeaseAttempt` did not appear as material
  allocation sources in the sampled runs.
- Existing SegmentIndex benchmarks explicitly disable context logging, while
  the production configuration default enables it.

### Context-Logging Measurement

Item `100.2` added a focused Logback-backed profile without changing production
logging behavior. On the same dirty worktree at commit `ab14e0f69`, JDK 25 on
arm64 macOS, three forks with three warmup and five one-second measurement
iterations produced:

- Live get: `3.487 M ops/s`, `171.884 B/op` with context logging disabled and
  `3.122 M ops/s`, `204.007 B/op` enabled.
- Hot put: `2.527 M ops/s`, `201.239 B/op` with context logging disabled and
  `2.800 M ops/s`, `237.404 B/op` enabled.

The enabled-mode means were about `32.123 B/op` higher for live gets and
`36.165 B/op` higher for hot puts. The reported confidence intervals overlapped
for throughput and allocation in both workloads, so this run alone does not
establish either effect. Keep the enabled/disabled coverage for future
measurement, but make no performance claim from this result.

### Route-Map Snapshot Publication Measurement

Item `100.3` changed route snapshot publication from a per-call wrapper under a
read lock to one immutable snapshot per map version, published through a
volatile reference. On the same dirty worktree, JDK 25 on arm64 macOS, five
forks with three warmup and five one-second measurement iterations produced:

- Live get: baseline `3.493 M ops/s`, `168.175 B/op`; candidate
  `3.427 M ops/s`, `148.507 B/op`.
- Hot put: baseline `2.853 M ops/s`, `216.617 B/op`; first candidate
  `3.170 M ops/s`, `200.068 B/op`.

Live-get throughput changed by `-1.87%`, within overlapping reported confidence
intervals. Live-get allocation fell by `19.668 B/op` (`-11.70%`), and the
reported allocation confidence intervals did not overlap. Hot-put throughput
confidence intervals overlapped, while candidate and repeat allocation forks
were bimodal from about `136` to `216 B/op`; no hot-put improvement is claimed.
The retained benefit is the repeatable live-get allocation reduction and the
lock-free snapshot publication itself.

The affected live-get and hot-put throughput summaries passed the canonical
comparison script without a regression. A broader `segment-index-pr-smoke`
baseline attempt stopped at an unrelated multisegment benchmark setup timeout
and produced an empty result, so that full-profile comparison remains explicit
unfinished audit evidence for item `100.6`.

### Segment-ID Membership Measurement

Item `100.5` replaced list materialization followed by `contains(...)` with a
direct immutable map-value scan. A focused JMH benchmark measured a successful
middle-position lookup at the requested route counts. Both variants ran on the
same dirty worktree, JDK 25 on arm64 macOS, with one thread, five forks, three
warmup iterations, five one-second measurement iterations, and the GC profiler.

| Routes | List + contains | Direct scan | Speedup | Allocation before | Allocation after |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 10 | 63.807 ns | 5.627 ns | 11.34x | 384 B/op | ~0 B/op |
| 1,000 | 5.647 us | 1.649 us | 3.42x | 4,344 B/op | ~0 B/op |
| 10,000 | 64.027 us | 22.364 us | 2.86x | 40,361 B/op | ~0 B/op |
| 50,000 | 323.428 us | 115.267 us | 2.81x | 200,346 B/op | ~0 B/op |
| 100,000 | 660.687 us | 228.547 us | 2.89x | 400,355 B/op | ~0 B/op |

Reported confidence intervals did not overlap at any tested size, so the
improvement is measurable from 10 routes, the smallest tested value. The
production ceiling of 50,000 routes receives a `64.36%` latency reduction and
removes about `200 kB` of temporary allocation per membership check. The
remaining direct scan is linear, but an immutable segment-ID set is deferred
until an end-to-end profile proves that its extra snapshot memory and
publication cost are justified.

### Final Re-profile

Item `100.6` rebuilt commit `ab14e0f69` as the production baseline and used the
staged tree as the candidate on the same JDK 25 arm64 macOS host.

- The original canonical setup failure was reproduced. Repeated full
  `flushAndWait()` calls during multi-segment population could time out while
  reloading a segment as background splits progressed. Raising the segment
  cache limit from `3` through `64`, `128`, and `256` did not fix the failure.
  The read benchmark fixture now populates first and performs one final settled
  flush. With that benchmark-only change applied to both trees, the complete
  `segment-index-pr-smoke` profile ran successfully.
- The canonical one-fork comparison was not credible as a performance claim.
  It reported all get variants `22.07%` to `45.53%` worse while reporting hot
  puts and split-heavy work about `47%` to `57%` better. The reported JMH
  confidence intervals were extremely wide, so these opposing results are
  retained only as a failed regression screen.
- A five-fork live-get A/B with three warmup and five measurement iterations
  reported `3.081 M ops/s`, `179.293 B/op` for the baseline and
  `2.694 M ops/s`, `151.735 B/op` for the candidate. Throughput intervals
  overlapped. A reverse-order repeat reported `2.437 M ops/s`,
  `154.943 B/op` for the candidate and `2.633 M ops/s`, `154.445 B/op` for the
  baseline. The repeat made allocation bimodality visible on both sides, so
  item `100.6` does not add a new whole-live-get allocation or throughput
  claim.
- A five-fork hot-put A/B reported `1.572 M ops/s`, `211.983 B/op` for the
  baseline and `1.818 M ops/s`, `211.319 B/op` for the candidate. Throughput
  confidence intervals overlapped and allocation was effectively unchanged,
  so no hot-put improvement is claimed.
- Candidate JFR recorded no live-get monitor contention. It did not identify
  `OperationResult`, `RouteLeaseAttempt`, the remaining segment-ID scan, or
  `LruChunkStoreCache` as material hot-path candidates. In the artificial
  twenty-thread single-route hot-put workload, `RouteEntry` lease acquisition
  and release produced 14 monitor-blocked events totaling `143 ms`; production
  evidence is required before changing its drain-sensitive state model.

The measured boundary is therefore unchanged: retain the already demonstrated
snapshot-publication and exact membership improvements, accept no additional
cleanup candidate, and make no whole-application throughput claim. Item `100.6`
cannot be archived until a stable same-host comparison clears the canonical
regression gate.

## Source Of Truth

- `AGENTS.md` defines repository rules. In particular,
  `docs/refactor-backlog.md` is the authoritative and only active refactoring
  tracker. This file supplies the context role from the refactoring-plan
  workflow; do not create a competing `docs/refactoring-tracker.md`.
- `docs/development/code-quality-charter.md` defines the small-slice workflow
  and performance gates.
- `benchmarks/README.md`, `benchmarks/profiles/segment-index-pr-smoke.json`, and
  focused benchmark profiles define the comparison flow.
- `docs/development/rollout-quality-gates.md` defines the maximum accepted
  point-operation overhead for monitoring-related changes.
- `docs/architecture/segmentindex/performance.md`,
  `docs/architecture/segmentindex/read-path.md`, and
  `docs/architecture/segmentindex/caching.md` define current hot-path and cache
  behavior.
- `docs/configuration/logging.md` and `docs/operations/monitoring.md` define
  logging and latency-metric behavior that must remain externally consistent.
- Primary implementation owners are `PersistentSegmentRouteMap`,
  `RouteMapSnapshot`, `MappedSegmentLeaseService`, `OperationLatencyTracker`,
  `SegmentReadPath`, `SegmentIndexMdcLoggingAdapter`, and
  `LruChunkStoreCache`, with their matching tests.

## Guardrails

- Preserve route-map ordering, map-version semantics, split publication,
  persistence behavior, retry behavior, public APIs, and exact get/put/delete
  counts.
- Publish route data and its version as one immutable state. Do not expose or
  mutate a published `TreeMap`.
- Treat the `SegmentReadPath` CAS-loser cleanup as a correctness fix. Verify
  exactly-once cleanup with deterministic concurrent first access.
- Sampling experiments may reduce latency observations, not operation counts.
  Do not add public sampling configuration before an internal experiment shows
  useful improvement and acceptable percentile behavior.
- Measure context logging in both production-default enabled mode and disabled
  mode. The disabled bootstrap path already avoids the MDC adapter; do not add
  another disabled-path mechanism without evidence.
- Limit segment-ID membership work to exact-segment routing paths. Retain the
  direct snapshot scan; add a maintained immutable ID set only if an end-to-end
  profile shows that the remaining scan justifies duplicate snapshot state.
- Do not implement singleton `OperationResult` or `RouteLeaseAttempt` values,
  cache sharding, cache replacement, or per-key single-flight loading without
  new profile evidence showing that exact allocation or contention source is
  material.
- Keep each accepted production change in its own focused commit. Do not turn
  this sequence into an architectural rewrite.

## Validation Gates

- Documentation-only planning changes:
  `python3 scripts/check_docs_nav.py`, `mkdocs build --strict`, and
  `git diff --check`.
- Every production slice: matching focused tests, deterministic concurrency or
  resource-lifecycle coverage where applicable, then `mvn clean verify`.
- Benchmark coverage changes: the profile contract and benchmark history script
  smoke tests documented in `benchmarks/README.md`.
- Every retained hot-path optimization: a same-environment baseline/candidate
  comparison using the canonical compare flow, plus a focused three-fork JMH
  run with the GC profiler for the affected path.
- Acceptance: no `worse` canonical result, no point-operation regression beyond
  the documented three-percent budget, and a repeatable improvement in the
  metric targeted by the slice. Neutral or inconclusive code-only
  optimizations are not retained.
