# SessionOperationGate comparison

## Scope

- Benchmark: `SegmentIndexGetBenchmark.getHitSync`
- Read path: `live`
- Chunk-store cache pages: `0`
- Threads: `4`
- Warmup/measurement/forks: `3 x 1 s` / `5 x 1 s` / `3`
- Runtime: OpenJDK 25.0.3
- Both runs include the same uncommitted SegmentRegistry optimization baseline.

## Targeted throughput

| Version | Score | 99.9% confidence interval |
| --- | ---: | ---: |
| Monitor counter | 3,176,391 ops/s | 2,564,972–3,787,809 ops/s |
| Atomic counter | 3,071,857 ops/s | 2,570,314–3,573,401 ops/s |

The candidate score is 3.3% lower, but the confidence intervals overlap
heavily. This measurement does not establish either a regression or an
improvement.

## Stack sampling

Before the change, 5.1% of samples were `BLOCKED`. Gate increment and decrement
accounted for 3.3% of all samples and 64.1% of blocked samples. In addition,
`Object.notifyAll()` from gate completion accounted for 4.5% of all samples in
the `RUNNABLE` state.

After the change, no `SessionOperationGate` increment, decrement, monitor, or
`Object.notifyAll()` frame appeared in blocked or runnable hot-path samples.
The remaining blocked samples were in route-lease acquisition and release.

## Canonical smoke profile

The `segment-index-pr-smoke` profile was run before and after with identical
settings. Its one-fork, three-iteration workloads produced large changes in
both directions, with confidence errors commonly larger than the reported
scores. It is too noisy to establish a regression or improvement for this
change.

## Conclusion

The intended synchronization contention was removed. The collected throughput
measurements are inconclusive and do not demonstrate a regression outside
measurement noise.
