# Benchmark History and Per-Change Comparison

This module now contains a branch-based workflow for comparing benchmark
results across changes instead of relying only on ad-hoc local JMH runs.

## Goals

- Show what a specific change did to the most important SegmentIndex and
  storage hot-path scenarios.
- Keep the comparison reproducible by using fixed benchmark profiles.
- Preserve raw JMH JSON and machine-readable metadata for later inspection.
- Separate short per-change checks from longer nightly trend runs.

## Canonical Profiles

Profile definitions live in [profiles](/Users/jan/projects/HestiaStore/benchmarks/profiles):

- `segment-index-pr-smoke`
  - short per-change profile for PRs and local refactor validation
  - includes SegmentIndex hot paths plus the storage-core diff-key read and
    chunk-entry write checks
- `segment-index-nightly`
  - longer profile for trend tracking on `main`
  - extends the same coverage with broader SegmentIndex and storage-core
    parameter sets

The profiles currently include:

- `SegmentIndexGetBenchmark` with `readPathMode=persisted`
- `SegmentIndexGetBenchmark` with `readPathMode=overlay`
- `SegmentIndexMultiSegmentGetBenchmark` with `workingSetMode=hot`
- `SegmentIndexPersistedMutationBenchmark` for persisted `put`/`delete`
- `SegmentIndexHotPartitionPutBenchmark` (20-thread hot `put` + `putThenGet`)
- `SegmentIndexMixedDrainBenchmark` with `workloadMode=drainOnly`
- `SegmentIndexMixedDrainBenchmark` with `workloadMode=splitHeavy`

The nightly profile additionally includes:

- `SegmentIndexMultiSegmentGetBenchmark` with `workingSetMode=cold`
- `SegmentIndexLifecycleBenchmark` for `open`, `checkAndRepairConsistency`,
  and `compactAndWait`

- `DiffKeyReaderBenchmark`
- `SingleChunkEntryWriterBenchmark`
- compact and large storage-core parameter sets for both benchmarks

## Runner and Compare Scripts

Scripts live in [scripts](/Users/jan/projects/HestiaStore/benchmarks/scripts):

- [run_jmh_profile.py](/Users/jan/projects/HestiaStore/benchmarks/scripts/run_jmh_profile.py)
  - packages the benchmark jar
  - runs each scenario from a profile
  - writes raw JMH JSON, stdout logs, normalized summary, and metadata
- [compare_jmh_profile.py](/Users/jan/projects/HestiaStore/benchmarks/scripts/compare_jmh_profile.py)
  - compares two `summary.json` outputs
  - emits markdown and machine-readable comparison JSON
  - classifies metrics as `better`, `neutral`, `warning`, `worse`, `new`, or `removed`

Smoke coverage for the script entry points lives in
`BenchmarkHistoryScriptsSmokeTest` and executes the real Python CLIs against
temporary benchmark-history fixtures.

## Output Model

Each run produces:

- `raw/<label>.json`
- `logs/<label>.log`
- `summary.json`

`summary.json` is the stable compare input. It records:

- profile name
- git SHA / branch / dirty flag
- host metadata
- exact benchmark args
- normalized primary and secondary JMH metrics

## Local Usage

Run a canonical profile:

```sh
python3 benchmarks/scripts/run_jmh_profile.py \
  --repo-root . \
  --profile segment-index-pr-smoke \
  --output-dir /tmp/hestia-bench/current
```

Compare two runs:

```sh
python3 benchmarks/scripts/compare_jmh_profile.py \
  --baseline /tmp/hestia-bench/base/summary.json \
  --candidate /tmp/hestia-bench/current/summary.json \
  --markdown-out /tmp/hestia-bench/comparison.md \
  --json-out /tmp/hestia-bench/comparison.json
```

## CI Workflow

The workflow is [benchmark-compare.yml](/Users/jan/projects/HestiaStore/.github/workflows/benchmark-compare.yml).

Current behavior:

- preflight
  - run `BenchmarkProfileContractTest` before any JMH execution
  - fail fast when a canonical profile references a missing benchmark class,
    drifts from required SegmentIndex scenarios, or benchmark sources bring
    back removed public config names
- pull requests
  - run `segment-index-pr-smoke`
  - first try to compare PR candidate against the latest canonical baseline
    stored in the `perf-artifacts` branch
  - if no stored baseline exists yet, fall back to merge-base with the target
    branch
  - if the PR already has published history, also compare against the latest
    stored PR snapshot from the same PR number
  - publish the candidate run into a PR-scoped history path on
    `perf-artifacts`
  - update one sticky PR comment with the canonical baseline comparison,
    previous-PR delta when available, and history links
- push to `main`
  - run `segment-index-pr-smoke`
  - compare `HEAD` against the latest stored canonical smoke baseline from
    `perf-artifacts`
  - if no stored baseline exists yet, fall back to `HEAD~1`
  - publish the new candidate run into `perf-artifacts`, advancing
    `history/segment-index-pr-smoke/latest-main.json`
- nightly schedule
  - run `segment-index-nightly`
  - compare `HEAD` against the latest stored nightly baseline from
    `perf-artifacts`
  - if no stored baseline exists yet, fall back to `HEAD~1`
  - publish the new candidate run into `perf-artifacts`
- manual dispatch
  - can override profile, history branch, history channel, optional PR number,
    baseline ref, candidate ref, fail policy, and whether to publish into
    history

The workflow uses two git worktrees on the same runner so baseline and
candidate execute under the same machine conditions when a fallback git baseline
run is needed.

## `perf-artifacts` Branch

The intended long-lived store is a dedicated branch such as `perf-artifacts`.

The branch contains only generated benchmark history data, not source code.
Current layout:

```text
history/
  index.json
  <profile>/
    latest-main.json
    YYYY/
      MM/
        <timestamp>_<sha>_<run-id>/
          summary.json
          raw/
          logs/
          comparison-vs-previous.md
          comparison-vs-previous.json
    pull-requests/
      pr-<number>/
        latest.json
        YYYY/
          MM/
            <timestamp>_<sha>_<run-id>/
              summary.json
              raw/
              logs/
              comparison-vs-previous.md
              comparison-vs-previous.json
```

`latest-main.json` remains the canonical baseline pointer used by PR and
scheduled compare runs. PR snapshots publish under
`pull-requests/pr-<number>/latest.json` without overwriting the canonical main
pointer.

## Where To Look

After a PR benchmark run, the comparison is visible in three places:

- Actions job summary for the `Benchmark Compare` run
- a sticky PR comment with the latest delta table against canonical `main`,
  plus previous-PR delta when available, and history links
- `perf-artifacts/history/<profile>/pull-requests/pr-<number>/...`

After a canonical publish on `main`, the baseline moves forward in:

- `perf-artifacts/history/<profile>/latest-main.json`
- the timestamped run directory referenced by that pointer

## Recommended Interpretation Model

Use the per-change report for immediate review:

- `neutral`
  - within noise tolerance
- `warning`
  - noticeable regression, inspect before merging
- `worse`
  - large regression; block or justify
- `new` / `removed`
  - benchmark metric exists only on one side (profile evolved or baseline is older)

Current default thresholds in the compare script:

- `<= 3%` change: neutral
- `3-7%` regression: warning
- `> 7%` regression: worse

## What This Solves

- You can now compare one change against a concrete baseline on the same
  runner.
- Raw JMH outputs are not lost inside console logs.
- Group benchmarks with secondary metrics (`get` vs `put`) are preserved and
  compared separately.

## What Still Remains

This branch-based flow now gives us:

- canonical `main` baselines for both smoke and nightly profiles
- durable per-PR snapshots and comments for merged review history
- raw JMH outputs that remain inspectable after workflow log retention expires

The next step, if needed, is a separate report generator over
`perf-artifacts` to render long-term trend charts or release to release
summaries.
