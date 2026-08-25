# Refactor backlog

## Open Items

### To Solve

#### Point-operation hot-path cleanup

Items `100.1` through `100.3` and `100.5` are complete. Item `100.4` is
intentionally deferred from this sequence; item `100.6` is the final active
step.

- [ ] 100.6 Re-profile the accepted sequence and stop at the measured boundary
  (Risk: LOW). Owner: `benchmarks` and refactoring docs. Outcome: run the
  canonical SegmentIndex comparison and focused JFR profiles, record accepted,
  rejected, and still-deferred candidates, and update architecture docs only
  for behavior that actually changed. Validation: benchmark comparison reports,
  `python3 scripts/check_docs_nav.py`, `mkdocs build --strict`, and
  `git diff --check`.
  Status: blocked by the performance gate. The complete canonical profile now
  runs, but its one-fork comparison produced contradictory large deltas and
  several `worse` results. Two focused five-fork live-get A/B cycles retained
  overlapping throughput intervals and did not reproduce a stable whole-path
  allocation mean. Next safe action: rerun the same artifacts on a quiet,
  dedicated host before archiving this item.

[ ] 80. Make WAL durability explicit for non-fsync storage adapters so `SYNC` and `GROUP_SYNC` never claim guarantees that the active storage backend cannot provide.
[ ] 81. Split `WalRuntime` into focused writer, recovery, segment-catalog, and sync-policy responsibilities instead of keeping all lifecycle paths behind one shared monitor.
[ ] 82. Replace the slow generic `WalStorageDirectory` fallback with an explicit seekable/capability-aware storage path, or reject unsupported backends early.
[ ] 79.1 Freeze architecture, docs, and migration contract (Risk: HIGH)
[ ] 79.2 Introduce partition runtime and routing layer (Risk: HIGH)
[ ] 79.3 Switch `SegmentIndexSession` read/write/delete paths to partitions (Risk: HIGH)
[ ] 79.4 Implement drain, publish, flush, close, and WAL recovery (Risk: HIGH)
[ ] 79.6 Clean up config, metrics, control-plane tuning, and obsolete code (Risk: HIGH)
[ ] 79.7 Refresh unit tests, integration tests, and JMH gates (Risk: HIGH)
[ ] 78 Monitoring/Management platform rollout (Risk: HIGH)

### Regular Maintenance

[ ] M37 Audit `segment` package for unused or test-only code (Risk: LOW)
[ ] M38 Review `segment` package for test and Javadoc coverage (Risk: LOW)
[ ] M39 Audit `segmentindex` package for unused or test-only code (Risk: LOW)
[ ] M40 Review `segmentindex` package for test and Javadoc coverage (Risk: LOW)
[ ] M41 Audit `segmentregistry` package for unused or test-only code (Risk: LOW)
[ ] M42 Review `segmentregistry` package for test and Javadoc coverage (Risk: LOW)
[ ] M43 Avoid pattern-name abstractions unless they own lifecycle, resource opening, or rollback-sensitive cleanup (Risk: LOW)

## Unfinished Audit

- Deferred status-value reuse. Boundary: `engine` point-operation and route
  leasing results. Reason: exploratory JFR did not identify `OperationResult`
  or `RouteLeaseAttempt` as material allocation sources, and generic singleton
  reuse adds type-safety and semantic risk. Evidence still needed: a
  representative allocation profile with these constructors on a material hot
  stack. Item `100.6` JFR still did not show that evidence. Next safe action:
  revisit only after a new production-equivalent profile does.
- Deferred 100.4 sampled point-operation latency recording. Boundary: `engine`
  operation monitoring. Reason: explicitly excluded from the current
  refactoring sequence; operation counts and latency observations remain
  unchanged. Item `100.6` sampled latency recording in execution stacks but
  recorded no live-get monitor contention. Next safe action: reconsider only
  after a production profile identifies material contention.
- Deferred chunk-store cache redesign. Boundary:
  `engine` `LruChunkStoreCache`. Reason: the implementation has one synchronized
  access-ordered map, but the persisted cache-enabled profile recorded no
  contention events, and item `100.6` JFR did not make it material. Evidence
  still needed: representative blocked-time or duplicate same-page load
  measurements. Next safe action: shard or add minimal per-key single-flight
  only for the demonstrated failure mode.
- Canonical comparison gate remains blocked. Boundary: `benchmarks`
  `segment-index-pr-smoke`. The benchmark now completes for both baseline and
  candidate, but the one-fork report simultaneously marked all get paths
  `22.07%` to `45.53%` worse and hot-put/split-heavy paths roughly `47%` to
  `57%` better. Focused five-fork throughput intervals overlapped, and a
  reverse-order live-get repeat remained inconclusive. Evidence still needed:
  the same A/B artifacts on a quiet, dedicated host. Next safe action: rerun;
  do not tune code to these contradictory point estimates.
- Repeated explicit maintenance during background split population can time
  out. Boundary: multi-segment benchmark setup and the production
  `flushAndWait()`/split interaction. The failure reproduced at 8,192 and
  32,768 keys and with segment-cache limits from `3` through `256`; the stack
  timed out in `MappedSegmentMaintenanceService.awaitSegmentReady(...)` while
  reloading a closed segment. The read benchmark now performs one final settled
  flush so it can measure reads, but that does not fix the underlying
  maintenance interaction. Next safe action: investigate it as a dedicated
  lifecycle/concurrency issue outside item `100.6`.
- Route-lease monitor contention needs production confirmation. Boundary:
  `RouteEntry` lease acquisition/release. The item `100.6` JFR hot-put stress
  profile recorded 14 blocked monitor events totaling `143 ms` with twenty
  threads targeting one route; live get recorded none. Next safe action:
  redesign only if a production JFR shows the same monitor as material.
- Deferred immutable segment-ID set. Boundary: `RouteMapSnapshot`. Reason:
  direct map-value scanning removed the temporary list and improved the
  50,000-route lookup from `323.428 us/op` to `115.267 us/op` with effectively
  zero per-call allocation. Item `100.6` JFR did not show the remaining scan as
  material. Evidence still needed: an end-to-end exact-segment acquisition
  profile showing that it justifies duplicate snapshot state and extra
  publication cost.

## Done (Archive)

#### Senku Index implementation

Item `101` implements the first version described by
[`docs/development/senku-index.md`](development/senku-index.md). The sequence is
top-down: freeze the public contract first, then add the minimum storage and
merge machinery needed to make that contract executable, and finally prove the
whole index through its public API. Open or link one implementation issue before
starting the first production-code slice.

Package and implementation rules for every item:

- The supported public API belongs to `org.hestiastore.index.senku`; initial
  implementation classes belong to the single
  `org.hestiastore.index.senku.internal` package. Do not add speculative
  `util`, `spi`, factory, adapter, or one-class subpackages.
- Reuse `Directory`, `FileLock`, `PropertyStoreImpl`, `ChunkStoreFile`,
  `ChunkStoreWriterTx`, the existing Snappy and magic-number filters,
  `SingleChunkEntryWriterImpl`, `SingleChunkEntryIterator`, `Reader`,
  `EntryComparator`, `TypeDescriptor`, and
  `IndexConfigurationDefaults.DEFAULT_DISK_IO_BUFFER_SIZE_BYTES`. Do not change
  existing storage or Segment Index code merely to make Senku fit.
- Use `Vldtn` for every argument, state-independent configuration, constructor,
  and factory validation. Add a narrowly scoped `Vldtn` operation only when no
  existing operation expresses the required check; test that operation
  directly. Convert lower-level failures at the Senku boundary to
  `IndexException` as specified by the architecture.
- Prefer JDK `HashMap`, `PriorityQueue`, locks, conditions, latches, streams, and
  executors. Do not introduce an interface with one implementation, a recovery
  framework, runtime monitoring, persisted configuration, or an asynchronous
  ingestion layer in this sequence.

Testing rules for every item:

- Add the matching JUnit 5 `*Test` for every changed production class in the
  same package. A slice is not complete when only a later end-to-end test covers
  its new classes.
- Use a real `MemDirectory` for every Senku unit and integration test. Do not
  use `FsDirectory`, temporary filesystem directories, or mocked `Directory`
  objects. A small test-only `MemDirectory` subclass may inject one specific
  failure; do not build a generic fault-injection framework.
- Put whole-index tests under `engine/src/integration-test/java` with an `*IT`
  suffix and exercise only the public Senku API. Keep component tests under
  `engine/src/test/java`.
- Cover empty, one-element, exact-boundary, boundary-plus-one, duplicate-key,
  malformed-input, repeated-call, and cleanup/resource-ownership cases wherever
  they apply. Assert both returned data and the resulting in-memory directory
  tree.
- Make concurrency tests deterministic with latches, barriers, conditions,
  direct invocation of a package-private coordinator scan operation, and
  bounded awaits. Never use `Thread.sleep()` and never make the three-second
  production scan interval configurable solely for tests.

- [x] 101. Implement the first Senku Index without changing existing low-level
  storage behavior (Risk: HIGH). Owner: `engine` Senku package. Outcome: the
  documented write-once lifecycle, synchronous flushes, background sorted-run
  maintenance, finalization, and globally sorted ready stream are available
  through the new public API. Guardrails: all `101.x` slices and their tests are
  complete; deferred features remain in the architecture's limitations or
  technical-debt sections. Validation: items `101.1` through `101.20`, the
  Senku JMH gate, `mvn clean verify`, strict documentation build, and a clean
  static-analysis report for the new package.

##### Public contract and persisted format

- [x] 101.1 Add the Senku public contracts and flat builder configuration
  (Risk: MEDIUM). Owner: `engine/src/main/java/org/hestiastore/index/senku`.
  Outcome: add the documented merge function and registry, writing and ready
  handles, builder settings/defaults, and package Javadoc without placeholder
  operations that throw `UnsupportedOperationException`. Guardrails: expose no
  delete, abort-writing, writing close, monitoring, or persisted write tuning;
  keep object construction in the builder; use `Vldtn` for all inputs and every
  numeric boundary. Validation: direct tests for every default, missing required
  setting, null dependency, lower/upper boundary, overflow-sensitive capacity
  calculation, zero/duplicate merge-function registration, and repeated invalid
  calls.

- [x] 101.2 Implement deterministic path names and exact property codecs
  (Risk: HIGH). Owner: Senku internal metadata. Outcome: parse and write the
  documented `flush-N`, `shard-N`, `level-N`, `run-N`, part, manifest, and ready
  formats using canonical numeric names with a minimum width of five digits and
  `PropertyStoreImpl`; publish
  every manifest last through the existing directory transaction and rename
  operations. Guardrails: property files contain only documented keys, are
  authoritative, and have no compatibility/version framework; unknown,
  missing, extra, malformed, negative, non-contiguous, or contradictory data
  fails fast. Validation: round-trip and invalid-input tests for empty and
  non-empty runs, flush manifests, ready metadata, name limits, extra/missing
  parts, temporary files, and exact directory layouts in `MemDirectory`.

##### Storage primitives

- [x] 101.3 Implement the opaque packed `LargeFilePosition` value
  (Risk: MEDIUM). Owner: Senku internal storage. Outcome: encode and decode the
  documented non-negative `long` into a physical part number and local signed-
  `int` `CellPosition` without exposing a page ID. Guardrails: use overflow-safe
  `Vldtn` checks and keep shifting/masking inside this class. Validation: direct
  tests for zero, maximum local position, maximum supported manifest part,
  round trips, numeric ordering, sign-bit preservation, negative inputs, and a
  position beyond the declared part count.

- [x] 101.4 Build `LargeFileWriterTx` and `LargeFileReader` over existing chunk
  stores (Risk: HIGH). Owner: Senku internal storage. Outcome: append versioned,
  compressed entry pages, rotate by `maxEntriesPerPart`, commit contiguous
  `part-N.chunk` files, and read sequentially or from an opaque position while
  crossing parts. Guardrails: reuse the documented filter order and transaction
  lifecycle; do not virtualize `Directory`, expose page IDs, split a page, add
  random mutation, or promise rollback after failure. Validation: direct tests
  for empty commit, one page, exact part boundary, rotation, multi-part reads,
  positioned reads, stable EOF, idempotent close, read/append after close or
  commit, page-version mismatch, invalid/missing/extra parts, early failure, and
  underlying reader/writer closure.

- [x] 101.5 Implement the compressed flush `shard-index.dat` codec
  (Risk: HIGH). Owner: Senku internal storage. Outcome: write and read exactly
  one versioned 20-byte big-endian record for every configured shard containing
  shard ID, opaque `LargeFilePosition`, and record count. Guardrails: use one
  existing `ChunkStoreFile`; store no page IDs; use overflow-checking count
  arithmetic and reject a table that cannot fit the existing signed-`int`
  storage API. Validation: round trips for one and many shards, empty shards,
  maximum shard count, malformed length/order/ID/count/position, wrong version,
  extra chunks, truncated payload, and count overflow.

##### Sorting and ingestion

- [x] 101.6 Implement one reusable k-way sorted-entry merge loop
  (Risk: HIGH). Owner: Senku internal merge. Outcome: use a JDK `PriorityQueue`
  and the configured key comparator to lazily merge sorted cursors and collapse
  every duplicate key through the one configured merge function. Guardrails:
  do not materialize an input or output run, preserve no write order, and add no
  merge-function dispatch abstraction. Validation: pure unit tests for zero,
  one, two, and many inputs; empty inputs; all-equal keys; duplicates within and
  across inputs; skewed lengths; comparator edge cases; count overflow; merge
  callback failure; and closure of all cursors on success, early consumer close,
  and failure.

- [x] 101.7 Implement `SenkuIngestor` and synchronous flush publication
  (Risk: HIGH). Owner: Senku internal ingestion. Outcome: serialize `put()` and
  `finishWriting()` with one `ReentrantLock`, merge duplicate values in one
  `HashMap`, flush at the distinct-key limit on the caller thread, partition by
  `Math.floorMod(hash, shardCount)`, sort each shard, and publish one flush
  generation with its sparse index and manifest. Guardrails: validate keys and
  values with `Vldtn`, never use `ConcurrentHashMap`, never publish an empty
  flush, and make no copy or mutation-safety promise for caller objects.
  Validation: direct tests for nulls, distinct versus duplicate threshold
  accounting, exact threshold flush, final partial flush, negative hashes
  including `Integer.MIN_VALUE`, shard skew, sorted shard regions, page/part
  boundaries, concurrent caller serialization, merge callback failure, and
  manifest-last behavior under an injected in-memory write failure.

##### Maintenance data path

- [x] 101.8 Implement one flush-to-level-zero batch
  (Risk: HIGH). Owner: Senku internal merge. Outcome: select up to
  `mergeFanIn` committed flush generations, validate their manifests and sparse
  indexes once, run one independent merge job for each shard, publish one level-
  zero run per shard including empty runs, and delete the shared flush inputs
  only after every shard completion is accepted. Guardrails: jobs read only
  their persisted shard ranges; no job owns or removes the permanent `flush/`
  directory; L0 jobs may run in parallel. Validation: `MemDirectory` tests for
  full and drain-mode partial batches, empty and non-empty shards, duplicate
  collapse, multi-page/part input, one failed shard, completion in different
  orders, no early input deletion, manifest-last output, and exact batch
  cleanup.

- [x] 101.9 Implement same-level sorted-run merge and promotion
  (Risk: HIGH). Owner: Senku internal merge. Outcome: merge up to
  `mergeFanIn` runs from one shard and one numeric level into the next level,
  including drain-mode partial groups and one-input rewrites needed to leave one
  terminal run. Guardrails: never merge different levels, never run two sorted-
  run jobs for the same shard, and use the shared lazy merge loop and exact
  manifest counts. Validation: tests for normal fan-in, partial groups,
  one-input promotion, empty runs, duplicate keys, record-count overflow,
  early/late EOF, mixed-level rejection, publication failure, input cleanup,
  and removal of empty run/level/shard directories only when unowned.

##### Coordinator and lifecycle

- [x] 101.10 Implement the coordinator-owned source catalog and eligibility
  scan (Risk: HIGH). Owner: Senku internal maintenance. Outcome: one
  package-private `scanOnce()` traverses only structural directories and
  property metadata, discovers committed sources, ignores incomplete sources,
  and computes deterministic bottom-up merge candidates. Guardrails: do not
  enumerate data parts during ordinary scans, read shard data, repair metadata,
  add a catalog lock, or depend on one atomic directory-tree snapshot.
  Validation: direct no-sleep tests for empty trees, new committed sources,
  incomplete temporary sources, authoritative metadata, missing/extra/conflicting
  catalog entries, stable repeated scans, level ordering, shard ordering, and
  old-run selection.

- [x] 101.11 Add bounded scheduling, reservations, and completion handling
  (Risk: HIGH). Owner: Senku internal maintenance. Outcome: reserve exact input
  and output directory IDs before placing jobs into the fixed worker pool and
  bounded FIFO queue; serialize scans and completion processing on the one
  coordinator thread; accept a worker-published output and eagerly delete its
  obsolete inputs. Guardrails: keep pending work in the catalog/L0 batch rather
  than creating unbounded job objects; allow L0 beside a sorted-run merge for
  the same shard, but only one sorted-run job per shard; workers publish and
  return immutable results but never edit the catalog. Validation: deterministic
  executor tests for full capacity, FIFO queueing, duplicate/conflicting
  reservations, output observed before completion, concurrent L0 and run work,
  unique run IDs, completion order, worker failure, cancellation, and exact
  reservation release.

- [x] 101.12 Add queue-driven ingestion backpressure
  (Risk: HIGH). Owner: Senku internal maintenance and ingestion. Outcome: the
  coordinator pauses ingestion only when the bounded queue is full and known
  eligible work remains unqueued, and resumes it only when capacity exists and
  no eligible work remains pending. Guardrails: keep one boolean and one
  `Condition` under the ingestion lock; perform no directory I/O while holding
  that lock; preserve caller interruption; do not add byte/disk-space
  prediction. Validation: latch-based tests for high/low water transitions,
  spurious/repeated scans, queue-full-without-backlog, backlog-with-capacity,
  multiple blocked callers, wake on finish/error, interrupt preservation, and
  no lost signal or put after writing ends.

- [x] 101.13 Implement writing runtime ownership and fail-fast shutdown
  (Risk: HIGH). Owner: Senku internal lifecycle. Outcome: `create()` validates
  before mutation, acquires the single root `FileLock`, creates non-daemon
  maintenance executors, records one `firstFailure`, and performs the documented
  minimal shutdown without rollback or filesystem cleanup. Guardrails: catch
  `Exception`, never `Throwable`; do not interrupt running merge jobs; release
  every owned resource exactly once; restore interrupt status after
  uninterruptible lifecycle waits. Validation: direct tests for create failure
  at each acquisition step, second-handle lock rejection, caller-thread and
  background failures, racing failures, queued/running jobs, schedule failure,
  worker termination, lock release, no leaked live thread, suppressed cleanup
  errors, and generic later `ERROR` rejections.

- [x] 101.14 Implement `finishWriting()` and terminal layout publication
  (Risk: HIGH). Owner: Senku internal lifecycle and maintenance. Outcome: reject
  later writes, flush the remaining map, immediately enter drain mode, continue
  same-level merges until every configured shard owns exactly one terminal run,
  stop all maintenance threads, publish `ready.properties` last, and transfer
  the still-held root lock to `SenkuReady`. Guardrails: create an empty terminal
  run for every empty shard, keep `flush/` present and empty, do not equalize
  terminal levels, and do not add a final full-data rewrite. Validation:
  deterministic tests for an empty index, partial flush group, partial run
  groups, one-input promotions, uneven terminal levels, concurrent puts and
  finish calls, awakened blocked puts, interrupted caller, failed drain, exact
  cleanup/layout, thread termination, and lock transfer.

##### Ready streaming

- [x] 101.15 Implement ready-index open, global streaming, and close
  (Risk: HIGH). Owner: Senku public API and internal stream. Outcome: `open()`
  acquires the root lock, validates the authoritative ready tree without
  creating missing directories, and exposes one lazy globally sorted m-way
  stream across the terminal shard runs using the established Segment Index
  stream-closing behavior. Guardrails: permit only one active stream, retain no
  background thread, never materialize the dataset, make ready close idempotent,
  and make `SenkuReady.close()` break an active stream and release the lock.
  Validation: direct tests for empty and many-shard streams, global comparator
  order, laziness, early stream close, exhaustion, consumer failure, second
  stream rejection, repeated ready close, stream use after ready close, close
  during streaming, reopening after close, wrong I/O size behavior, and every
  missing/extra/malformed ready-layout case.

##### Whole-index proof and performance

- [x] 101.16 Add public-API functional integration coverage
  (Risk: HIGH). Owner: `engine/src/integration-test/java` Senku tests. Outcome:
  drive create, unsorted puts, synchronous flushes, maintenance, finalization,
  stream, close, and reopen through the public API only and compare every entry
  with an independent sorted reference map. Guardrails: use `MemDirectory` only
  and configuration values small enough to cross every boundary in one test
  run. Validation: `*IT` cases for empty, one key, all duplicate keys, one and
  many shards, one and many flushes/pages/parts/levels, highly skewed hashes,
  exact thresholds, threshold-plus-one, and multiple merge fan-ins.

- [x] 101.17 Add public-API concurrency and lifecycle integration coverage
  (Risk: HIGH). Owner: `engine/src/integration-test/java` Senku tests. Outcome:
  prove that concurrent callers, maintenance, backpressure, finalization,
  streaming, and root locking compose without data loss, deadlock, or resource
  leaks. Guardrails: real `MemDirectory`, bounded waits, no sleeping, no private
  state assertions, and no unsupported crash-recovery expectations.
  Validation: repeated `*IT` cases for disjoint and duplicate keyspaces,
  maintenance overlapping ingestion, queue saturation, finish races, active-
  stream close, second-handle rejection, injected storage/merge failure, and
  eventual termination of every Senku-owned non-daemon thread.

- [x] 101.18 Add deterministic reference-model and boundary stress tests
  (Risk: MEDIUM). Owner: Senku unit and integration tests. Outcome: seeded
  randomized sequences compare the final stream and duplicate reductions with a
  small independent model across a matrix of shard counts, flush limits, page
  limits, part limits, fan-ins, thread counts, and queue sizes. Guardrails: log
  the seed on failure, cap the matrix for normal CI, and do not introduce a
  property-testing dependency. Validation: repeat the focused suite enough to
  cover zero/one/max-small boundaries and multiple completion interleavings,
  then run it as part of normal `mvn verify`.

- [x] 101.19 Establish the first Senku performance baseline
  (Risk: MEDIUM). Owner: `benchmarks` and Senku implementation. Outcome: add the
  smallest JMH coverage that measures ingestion throughput, synchronous flush,
  flush-to-L0 merge, sorted-run merge, ready-stream throughput, and end-to-end
  ingest-to-first-sorted-result latency using `MemDirectory`. Guardrails: add no
  production tuning solely for the benchmark and claim no improvement without
  repeated forks and recorded confidence intervals. Validation: benchmark
  contract/smoke tests plus persisted baseline results for representative
  duplicate rates, shard skew, and boundary-crossing configurations.

- [x] 101.20 Complete the Senku implementation gate and documentation audit
  (Risk: MEDIUM). Owner: `engine`, `benchmarks`, and documentation. Outcome:
  every production class has its direct test, the public API and lifecycle have
  complete Javadocs, the actual package/layout/defaults match the architecture,
  and resolved implementation discoveries are incorporated without populating
  speculative features. Guardrails: keep the `Open Points` heading in
  `senku-index.md` even when empty; retain first-version limitations honestly;
  remove unused code/imports and do not leave placeholder types. Validation:
  focused Senku tests, `mvn clean verify`, `mvn clean site`,
  `python3 scripts/check_docs_nav.py`, `mkdocs build --strict`, and
  `git diff --check`.

- [x] 100.5 Replace list-building segment membership checks with direct
  snapshot lookup (Risk: MEDIUM).
    - `RouteMapSnapshot.containsSegmentId(...)` now scans immutable map values
      directly, and `MappedSegmentLeaseService` uses it for exact mapped-segment
      checks without constructing an unbounded list.
    - A focused one-thread JMH benchmark measured a successful middle-position
      lookup with five forks, three warmup iterations, five measurement
      iterations, and GC profiling on the same JDK 25 arm64 macOS environment.
    - Results were `63.807 -> 5.627 ns/op` at 10 routes,
      `5.647 -> 1.649 us/op` at 1,000, `64.027 -> 22.364 us/op` at 10,000,
      `323.428 -> 115.267 us/op` at 50,000, and
      `660.687 -> 228.547 us/op` at 100,000. Reported confidence intervals did
      not overlap at any tested size.
    - Allocation changed from `384 B/op`, `4,344 B/op`, `40,361 B/op`,
      `200,346 B/op`, and `400,355 B/op`, respectively, to effectively
      zero. The improvement is measurable from 10 routes, the smallest tested
      size; no claim is made below 10.
    - Focused engine tests passed with 23 tests, benchmark contract/tooling tests
      passed with 12 tests, and `mvn clean verify` passed across all 10 reactor
      modules.
- [x] 100.3 Publish and reuse one immutable `RouteMapSnapshot` per route-map
  version (Risk: MEDIUM).
    - `PersistentSegmentRouteMap` now publishes copied routes and their version
      together through one volatile `RouteMapSnapshot` reference. Repeated
      `snapshot()` calls perform no locking or wrapper allocation, and only real
      mutations publish the next instance.
    - Focused identity, no-op mutation, version-transition, and concurrent split
      publication coverage passed: 17 route-map tests. `mvn clean verify`
      passed across all 10 reactor modules.
    - Five-fork live-get JMH with GC data moved from
      `3.493 M ops/s`, `168.175 B/op` to
      `3.427 M ops/s`, `148.507 B/op`. Throughput changed by `-1.87%` with
      overlapping confidence intervals; allocation fell by `19.668 B/op`
      (`-11.70%`) with non-overlapping reported confidence intervals.
    - Five-fork hot-put throughput moved from `2.853 M ops/s` to
      `3.170 M ops/s`, but its confidence intervals overlapped. Allocation was
      bimodal across the candidate and repeat runs (`136` to `216 B/op`), so no
      hot-put allocation or throughput improvement is claimed.
    - The focused throughput summaries passed the canonical comparison tool
      with no regression: live get was `neutral` and hot put was `better` under
      its percentage thresholds. The broader canonical-profile setup failure is
      retained in the unfinished audit rather than hidden.
- [x] 100.2 Add production-equivalent context-logging benchmark coverage
  (Risk: LOW).
    - `SegmentIndexGetBenchmark` and `SegmentIndexHotRoutePutBenchmark` now
      expose the same enabled/disabled context-logging parameter. Existing
      canonical profiles pin it off, while
      `segment-index-context-logging.json` runs the focused comparison with a
      Logback MDC backend and log emission disabled.
    - The profile runner now rejects empty JMH result files instead of
      publishing a successful summary after all forks fail.
    - Benchmark profile contract and script smoke coverage passed with 11
      tests; the packaged runner also passed a direct enabled-MDC smoke run.
      `mvn clean verify` passed across all 10 reactor modules.
    - Three forks with three warmup and five measurement iterations reported
      live-get means of `3.487 M ops/s`, `171.884 B/op` disabled and
      `3.122 M ops/s`, `204.007 B/op` enabled. Hot-put means were
      `2.527 M ops/s`, `201.239 B/op` disabled and `2.800 M ops/s`,
      `237.404 B/op` enabled.
    - Enabled-MDC allocation means were about `32.123 B/op` higher for live
      gets and `36.165 B/op` higher for hot puts. Throughput and allocation
      confidence intervals overlapped in both comparisons, so this run alone
      does not establish either effect. Production logging behavior was
      unchanged.
- [x] 100.1 Close the losing `SegmentReadPath` searcher after concurrent first
  access (Risk: MEDIUM).
    - `SegmentReadPath` now closes a redundantly constructed searcher
      immediately after losing the cache CAS; the winner remains cached.
    - A deterministic two-thread `SegmentReadPathTest` verifies that exactly
      one supplier closes after the race and the winner closes with the read
      path.
    - `mvn -pl engine -Dtest=SegmentReadPathTest test` passed with 7 tests.
    - `mvn clean verify` passed across all 10 reactor modules.
[x] 99. Replace `SessionOperationGate` per-operation monitor contention with atomic in-flight tracking and bounded close-side drain polling (Risk: MEDIUM)
    - Focused session concurrency tests, benchmark module tests, and the full
      Maven verification pipeline pass.
    - Targeted JMH stack sampling removed all `SessionOperationGate` blocked
      frames and the per-operation `Object.notifyAll()` frame.
    - The targeted throughput result moved by -3.3%, with heavily overlapping
      confidence intervals; the canonical smoke profile was also too noisy to
      establish a throughput regression or improvement.
[x] 97. Clarify `core.session` responsibility boundaries by moving topology runtime composition to route/topology internals, giving core storage its own open spec and observer types, and preserving lifecycle behavior.
[x] 98. Simplify SegmentIndex initialization after the broad assembly refactor by keeping explicit opening points for bootstrap, session startup, and runtime resources while removing pattern-only assembler/request/components layers.
[x] 89. Rework split routing around a runtime `RouteTopology` so route handoff, draining, and split publish are owned by topology code while `SegmentRegistry` remains responsible only for physical segment instances and `SegmentRouteMap` remains responsible only for persisted routing.
[x] 90. Define the `RouteTopology` contract with route states such as `ACTIVE`, `DRAINING`, and `RETIRED`, plus `RouteLease` acquisition/release semantics and deterministic drain behavior for in-flight routed operations.
[x] 91. Add topology bootstrap from the versioned `SegmentRouteMap` snapshot so startup builds runtime route entries without changing `SegmentRegistry`, `BlockingSegment`, or `Segment` contracts.
[x] 92. Refactor foreground routed operations to resolve a `SegmentRouteMap` snapshot, acquire a `RouteTopology` lease for the resolved segment id and map version, use the existing `SegmentRegistry` to load the segment, and retry from the correct boundary on topology drain, stale version, registry unavailability, segment `BUSY`, or segment `CLOSED`.
[x] 93. Add focused tests for topology lease acquire/release, drain waiting, stale map-version rejection, route retirement, and the retry boundaries used by `put`, `delete` through tombstone writes, and `get`.
[x] 94. Rework split execution so the parent route moves to `DRAINING` before child materialization, in-flight leases drain, child materialization uses existing registry materialization, child routes publish in `RouteTopology`, `SegmentRouteMap` is updated and flushed, and the retired parent segment is cleaned up after publish.
[x] 95. Define and test the split failure policy for topology publish, `SegmentRouteMap` persistence failure, child materialization cleanup, parent cleanup retry, and startup recovery from the persisted map.
[x] 96. Remove the legacy split admission gate after routed operations and split publish use `RouteTopology` leases, then update concurrency documentation to make `RouteTopology`, `SegmentRegistry`, and `SegmentRouteMap` ownership boundaries explicit.
[x] 83. Define the new split runtime contract around `hintSplitCandidate(...)`, `awaitQuiescence(...)`, and managed lifecycle shutdown, and remove public scheduling concepts such as full-scan requests from the intended service shape.
[x] 84. Introduce a managed split runtime skeleton with explicit `OPENING -> RUNNING -> CLOSING -> CLOSED` state transitions and fail-fast behavior for calls made outside `RUNNING`.
[x] 85. Replace the current split-policy work-state loop with a candidate registry built from `Map<SegmentId, State>` plus a blocking ready queue so split hints are deduplicated and workers block instead of polling.
[x] 86. Split policy evaluation from split execution so policy workers only validate mapping and threshold eligibility, then hand off accepted candidates to the dedicated split executor.
[x] 87. Rebuild periodic reconciliation around the new candidate registry so the 250 ms scanner only offers over-threshold mapped segments that are not already queued or in process.
[x] 88. Rework split runtime tests around lifecycle, deduplicated candidate scheduling, blocking worker wakeup, quiescence, and close-drain behavior before removing the remaining legacy split-policy orchestration.
[x] 87 Decompose WAL into smaller durable-log subsystems (Risk: HIGH)
    - End-game achieved: `WalRuntime` now reads as a compatibility-facing
      orchestration facade instead of a monolithic durable-log implementation.
    - End-game achieved: WAL metadata/catalog, segment inventory, recovery,
      append-path writing, durability policy, and metrics ownership now live in
      dedicated collaborators under `segmentindex.wal`.
    - End-game achieved: public `WalRuntime` operations, metrics, and WAL
      on-disk compatibility remain stable while internal ownership boundaries
      are explicit and test-covered.
[x] 87.1 Freeze `WalRuntime` contract, state model, and migration invariants (Risk: HIGH)
[x] 87.2 Add characterization coverage for the current WAL orchestration seams (Risk: HIGH)
[x] 87.3 Extract WAL metadata/catalog state behind one internal model (Risk: HIGH)
[x] 87.4 Extract segment inventory, retention, and cleanup ownership (Risk: HIGH)
[x] 87.5 Extract recovery scan and corruption-repair ownership (Risk: HIGH)
[x] 87.6 Extract append writer and sync policy execution (Risk: HIGH)
[x] 87.7 Reduce `WalRuntime` to orchestration and lifecycle assembly (Risk: HIGH)
[x] 87.8 Refresh docs, benchmarks, and follow-on cleanup after extraction (Risk: HIGH)

[x] 83 Replace compatibility-shaped public model with domain-shaped public API (Risk: HIGH)
    - End-game achieved: canonical public configuration now exposes
      direct-to-segment write-path vocabulary via `IndexWritePathConfiguration`
      instead of making legacy partition naming the source of truth.
    - End-game achieved: canonical runtime metrics now expose a dedicated
      `SegmentIndexWritePathMetrics` model.
    - End-game achieved: legacy runtime configuration and partition/drain
      compatibility surfaces were removed from the SegmentIndex API and
      monitoring payloads.

[x] 82 Collapse split scheduling into a dedicated planner package (Risk: HIGH)
    - End-game achieved: `org.hestiastore.index.segmentindex.core.splitplanner`
      is now the control-plane package for split hint intake, periodic
      reconciliation, candidate selection, and admission into the split worker
      pool.
    - End-game achieved: split execution stays separate from planning. The
      planner owns "what should be scheduled"; split workers own "execute this
      already admitted split".
    - End-game achieved: planner is the only place allowed to submit split
      work. Write paths, maintenance finalization, and timer ticks emit only
      hints or rescan requests.
    - End-game achieved: thread topology is reduced to one planner thread plus
      one split worker pool, while route publish exclusivity, retry behavior,
      and candidate deduplication remain explicit and test-covered.

[x] 82.1 Freeze target split-planner architecture and migration invariants (Risk: HIGH)
[x] 82.2 Add characterization tests for current split trigger and admission behavior (Risk: HIGH)
[x] 82.3 Introduce `core.splitplanner` package with behavior-preserving type moves (Risk: MEDIUM)
[x] 82.4 Extract explicit planner state and planner-facing API (Risk: MEDIUM)
[x] 82.5 Introduce a single split-task dispatch seam (Risk: HIGH)
[x] 82.6 Route all split triggers through planner requests only (Risk: HIGH)
[x] 82.7 Separate candidate discovery from split execution in code and tests (Risk: HIGH)
[x] 82.8 Replace dual policy executors with a single planner thread model (Risk: HIGH)
[x] 82.9 Simplify executor topology and runtime assembly (Risk: HIGH)
[x] 82.10 Remove obsolete policy vocabulary and close the migration (Risk: MEDIUM)
[x] 81 Replace technical `segmentindex.core` package vocabulary with domain ownership packages (Risk: HIGH)
[x] 81.1 Freeze the target package model and migration rules (Risk: HIGH)
[x] 81.2 Move root/session ownership into `core.session` (Risk: HIGH)
[x] 81.3 Collapse `core.lifecycle` into `core.session` (Risk: HIGH)
[x] 81.4 Move state types under `core.session.state` (Risk: MEDIUM)
[x] 81.5 Merge `core.operation` and `core.split` into `core.routing` (Risk: HIGH)
[x] 81.6 Move durability and storage integrity into `core.storage` (Risk: HIGH)
[x] 81.7 Dissolve `core.runtime` into session, routing, storage, and maintenance (Risk: HIGH)
[x] 81.8 Dissolve `core.infrastructure` into owning domains (Risk: MEDIUM)
[x] 81.9 Replace `core.observability` with `segmentindex.monitoring` (Risk: MEDIUM)
[x] 81.10 Remove `core.internal` and `core.facade` as final package names (Risk: MEDIUM)
[x] 81.11 Collapse leftover assembly and access vocabulary after package moves (Risk: HIGH)
[x] 81.12 Finish when `segmentindex.core` reads as domain boundaries, not framework plumbing (Risk: MEDIUM)
