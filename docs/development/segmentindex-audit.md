# SegmentIndex Audit

Date: 2026-02-13
Scope: `src/main/java/org/hestiastore/index/segmentindex`

## M39 Findings (unused/test-only)

- No production class in `segmentindex` was found to be test-only.
- Tightened visibility for test-oriented API surface:
  - `SegmentAsyncExecutor#getQueueCapacity()` is now package-private.
  - `SplitAsyncExecutor#getQueueCapacity()` is now package-private.
- Tightened internal visibility:
  - `IndexInternalConcurrent` constructor is package-private.
  - `SegmentSplitter` constructor and `split(...)` are package-private.

## M40 Findings (tests + Javadocs)

- Added missing dedicated test:
  - `SegmentSplitAbortExceptionTest` for `SegmentSplitAbortException`.
- Added missing public Javadocs in:
  - `IndexRetryPolicy`
  - `SplitAsyncExecutor`
  - `SegmentSplitApplyPlan`
  - `SegmentIndexSplitPolicyThreshold#shouldSplit(...)`
- Coverage check result:
  - Every concrete class in `segmentindex` now has a corresponding JUnit test class.
  - `package-info.java` is intentionally excluded (documentation-only artifact).
