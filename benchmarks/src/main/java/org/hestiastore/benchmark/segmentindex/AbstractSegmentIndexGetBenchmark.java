package org.hestiastore.benchmark.segmentindex;

import java.io.File;
import java.io.IOException;

import org.hestiastore.index.directory.FsDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

public abstract class AbstractSegmentIndexGetBenchmark {

    private File tempDir;
    private SegmentIndex<Integer, String> index;
    private int queryKeyBound;
    private int missKeyStart;

    @Setup(Level.Trial)
    public final void setup() throws IOException {
        tempDir = SegmentIndexBenchmarkSupport.createTempDir(tempDirPrefix());
        final IndexConfiguration<Integer, String> conf = buildConfiguration();

        try (SegmentIndex<Integer, String> created = SegmentIndex
                .create(new FsDirectory(tempDir), conf)) {
            populateIndex(created);
            afterCreate(created);
        }

        index = SegmentIndex.open(new FsDirectory(tempDir), conf);
        configureReadState(index);
    }

    @TearDown(Level.Trial)
    public final void tearDown() {
        if (index != null) {
            index.close();
            index = null;
        }
        if (tempDir != null) {
            SegmentIndexBenchmarkSupport.deleteRecursively(tempDir);
            tempDir = null;
        }
    }

    @Benchmark
    public final String getHitSync(final QueryState queryState) {
        return index.get(Integer.valueOf(nextHitKey(queryState, queryKeyBound)));
    }

    @Benchmark
    public final String getHitAsyncJoin(final QueryState queryState) {
        return index
                .getAsync(Integer.valueOf(nextHitKey(queryState, queryKeyBound)))
                .toCompletableFuture().join();
    }

    @Benchmark
    public final String getMissSync(final QueryState queryState) {
        return index.get(Integer.valueOf(
                nextMissKey(queryState, queryKeyBound, missKeyStart)));
    }

    @Benchmark
    public final String getMissAsyncJoin(final QueryState queryState) {
        return index.getAsync(Integer.valueOf(
                nextMissKey(queryState, queryKeyBound, missKeyStart)))
                .toCompletableFuture().join();
    }

    protected abstract String tempDirPrefix();

    protected abstract IndexConfiguration<Integer, String> buildConfiguration();

    protected abstract void populateIndex(SegmentIndex<Integer, String> created);

    protected void afterCreate(final SegmentIndex<Integer, String> created) {
        // No-op by default.
    }

    protected abstract void configureReadState(
            SegmentIndex<Integer, String> openedIndex);

    protected abstract int nextHitKey(QueryState queryState, int boundExclusive);

    protected int nextMissKey(final QueryState queryState,
            final int boundExclusive, final int missKeyStart) {
        return missKeyStart + nextHitKey(queryState, boundExclusive);
    }

    protected final void setReadKeyBounds(final int queryKeyBound,
            final int missKeyStart) {
        this.queryKeyBound = queryKeyBound;
        this.missKeyStart = missKeyStart;
    }

    @State(Scope.Thread)
    public static class QueryState {

        private int cursor;
        private int randomState;

        @Setup(Level.Iteration)
        public void setup() {
            cursor = 0;
            randomState = 0x13579BDF;
        }

        int nextSequential(final int boundExclusive) {
            final int key = cursor;
            cursor++;
            if (cursor >= boundExclusive) {
                cursor = 0;
            }
            return key;
        }

        int nextRandom(final int boundExclusive) {
            randomState = randomState * 1664525 + 1013904223;
            return (randomState & Integer.MAX_VALUE) % boundExclusive;
        }
    }
}
