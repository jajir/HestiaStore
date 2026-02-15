# 🔗 Chain of Filters

`AbstractChainOfFilters` is the small engine that runs ordered pipelines inside the index. It accepts an immutable context object plus a mutable result carrier and iterates over a list of `Filter<Context, Result>` steps. Each step returns `true` to continue or `false` to short-circuit, which is how we cheaply exit when work is already finished (cache hits) or impossible (invalid plan).

## ⚙️ Mechanics

- The base class keeps the ordered `List<Filter<Context, Result>>` and exposes a single `filter(context, result)` method that drives the loop.
- Steps see the immutable context and may update the mutable result; they must return `false` once they have produced a terminal outcome so the chain stops immediately.
- Subclasses wrap the call to `filter` so they can prepare state/result objects and handle cleanup. Example: `SegmentSplitPipeline#run` closes the iterator in a `finally` block even when a step short-circuits.
- Filters are regular classes that implement `Filter<Context, Result>` and can be reused or composed in different orders when building a pipeline.

## 🛠️ Typical Usage

1) Define small, focused filters:

```java
final class ValidateInput implements Filter<RequestCtx, ProcessingState> {
    @Override
    public boolean filter(final RequestCtx ctx, final ProcessingState state) {
        if (!ctx.isValid()) {
            state.fail("missing fields");
            return false; // stop the chain early
        }
        return true;
    }
}
```

2) Compose them into a pipeline subclass that owns setup/teardown:

```java
final class ProcessingPipeline
        extends AbstractChainOfFilters<RequestCtx, ProcessingState> {

    ProcessingPipeline(
            final List<Filter<RequestCtx, ProcessingState>> steps) {
        super(List.copyOf(steps));
    }

    ProcessingState run(final RequestCtx ctx) {
        final ProcessingState state = new ProcessingState();
        filter(ctx, state); // short-circuit rules apply
        return state;
    }
}
```

## 📍 Where It Is Used

### Segment Searcher Pipeline (read path)

`SegmentSearcherPipeline` (`src/main/java/org/hestiastore/index/segment/SegmentSearcherPipeline.java`) runs a three-step lookup when serving `SegmentSearcher#get`:

```java
final SegmentSearcherPipeline<K, V> pipeline = new SegmentSearcherPipeline<>(
        List.of(
            new SegmentSearcherStepDeltaCache<>(valueTypeDescriptor), // stop if cached hit or tombstone
            new SegmentSearcherStepBloomFilter<>(),                   // stop if bloom says “definitely not”
            new SegmentSearcherStepIndexFile<>()));                   // stop after sparse-index + on-disk read

pipeline.run(ctx, result);
return result.getValue();
```

- Short-circuits: cache hit/tombstone, Bloom filter negative, or sparse-index probe missing/false-positive correction.
- Context/result types: `SegmentSearcherContext` carries the key plus caches/searcher; `SegmentSearcherResult` holds the found value (or `null` when absent).

### Segment Split Pipeline (split/compaction path)

`SegmentSplitPipeline` (`src/main/java/org/hestiastore/index/segment/SegmentSplitPipeline.java`) orchestrates the per-segment split flow inside `SegmentSplitter#split`:

```java
final SegmentSplitPipeline<K, V> pipeline = new SegmentSplitPipeline<>(
        List.of(
            new SegmentSplitStepValidateFeasibility<>(),      // throws if plan is invalid
            new SegmentSplitStepOpenIterator<>(),             // opens streaming iterator
            new SegmentSplitStepCreateLowerSegment<>(),       // prepares target segment
            new SegmentSplitStepFillLowerUntilTarget<>(),     // writes first half
            new SegmentSplitStepEnsureLowerNotEmpty<>(),      // guardrail for tiny splits
            new SegmentSplitStepReplaceIfNoRemaining<>(segmentReplacer), // compaction path, may stop here
            new SegmentSplitStepWriteRemainingToCurrent<>()   // split path, always stops after writing
        ));

final SegmentSplitterResult<K, V> result = pipeline.run(ctx);
```

- Short-circuits: compaction path (`SegmentSplitStepReplaceIfNoRemaining`) replaces the original segment and stops; the final write step stops after producing the split result.
- Context/result types: `SegmentSplitContext` carries the segment, version controller, plan, and IDs; `SegmentSplitState` owns the iterator, lower-segment handle, and final `SegmentSplitterResult`.
- Safety: `SegmentSplitPipeline#run` wraps `filter` so the opened iterator closes even when a step throws or short-circuits.

## ✅ Guidelines for Adding New Pipelines

- Keep steps single-purpose and side-effect aware; ensure they set a final result before returning `false`.
- Copy the incoming steps list (`List.copyOf(...)`) to avoid accidental reordering at runtime.
- When resources need cleanup, wrap the `filter` invocation in a `try/finally` inside your subclass.
- Pair unit tests with short-circuit cases; see `src/test/java/org/hestiastore/index/AbstractChainOfFiltersTest.java` for a minimal example.
