package org.hestiastore.index.segmentindex.routemap;

import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.segment.SegmentId;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Measures exact segment-id membership checks across route-map sizes.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(5)
@State(Scope.Benchmark)
public class RouteMapSnapshotMembershipBenchmark {

    @Param({ "10", "1000", "10000", "50000", "100000" })
    private int routeCount;

    private RouteMapSnapshot<Integer> snapshot;
    private SegmentId targetSegmentId;

    /**
     * Creates an immutable snapshot and selects its middle segment.
     */
    @Setup(Level.Trial)
    public void setup() {
        final TreeMap<Integer, SegmentId> routes = new TreeMap<>();
        for (int route = 0; route < routeCount; route++) {
            routes.put(Integer.valueOf(route), SegmentId.of(route));
        }
        snapshot = new RouteMapSnapshot<>(routes, 0);
        targetSegmentId = SegmentId.of(routeCount / 2);
    }

    /**
     * Checks membership for an average-position mapped segment.
     *
     * @return {@code true} when benchmark setup is valid
     */
    @Benchmark
    public boolean containsMiddleSegment() {
        return snapshot.containsSegmentId(targetSegmentId);
    }
}
