package org.hestiastore.benchmark.segmentindex;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.core.routing.RouteTopology;
import org.hestiastore.index.segmentindex.core.routing.RouteTopology.RouteLeaseResult;
import org.hestiastore.index.segmentindex.routemap.PersistentSegmentRouteMap;
import org.hestiastore.index.segmentindex.routemap.RouteMapSnapshot;
import org.hestiastore.index.segmentindex.routemap.RouteSplitPlan;
import org.hestiastore.index.segmentindex.routemap.SegmentRouteMap;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.ThreadParams;

/**
 * Measures steady-state route lease acquisition and release without segment
 * storage work.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(3)
@State(Scope.Benchmark)
public class RouteTopologyLeaseBenchmark {

    @Param({ "1", "64" })
    private int routeCount;

    private SegmentRouteMap<Integer> routeMap;
    private RouteTopology<Integer> topology;
    private List<SegmentId> routeIds;
    private long version;

    /**
     * Creates the requested number of runtime routes.
     */
    @Setup(Level.Trial)
    public void setup() {
        routeMap = new PersistentSegmentRouteMap<>(new MemDirectory(),
                new TypeDescriptorInteger());
        routeMap.extendMaxKeyIfNeeded(Integer.valueOf(routeCount));
        splitTailRoutes();
        final RouteMapSnapshot<Integer> snapshot = routeMap.snapshot();
        routeIds = snapshot.getSegmentIds(SegmentWindow.unbounded());
        if (routeIds.size() != routeCount) {
            throw new IllegalStateException(String.format(
                    "Expected %d routes but created %d.", routeCount,
                    routeIds.size()));
        }
        topology = RouteTopology.create(snapshot, 1, 1_000);
        version = snapshot.version();
    }

    /**
     * Closes the temporary route map after each fork.
     */
    @TearDown(Level.Trial)
    public void tearDown() {
        if (routeMap != null && !routeMap.wasClosed()) {
            routeMap.close();
        }
    }

    /**
     * Acquires and immediately releases one route lease.
     *
     * @param cursor thread-local route selector
     */
    @Benchmark
    public void acquireAndRelease(final RouteCursor cursor) {
        final SegmentId segmentId = routeIds.get(cursor.next(routeCount));
        final RouteLeaseResult result = topology.tryAcquire(segmentId, version);
        if (!result.isAcquired()) {
            throw new IllegalStateException(
                    "Steady-state route lease was not acquired.");
        }
        result.lease().close();
    }

    private void splitTailRoutes() {
        SegmentId tailSegmentId = SegmentId.of(0);
        int nextSegmentId = 1;
        for (int boundary = 1; boundary < routeCount; boundary++) {
            final SegmentId lowerSegmentId = SegmentId.of(nextSegmentId++);
            final SegmentId upperSegmentId = SegmentId.of(nextSegmentId++);
            final RouteSplitPlan<Integer> split = new RouteSplitPlan<>(
                    tailSegmentId, lowerSegmentId, upperSegmentId,
                    Integer.valueOf(boundary), Integer.valueOf(routeCount));
            if (!routeMap.tryReplaceRouteWithSplit(split)) {
                throw new IllegalStateException(
                        "Unable to create benchmark route topology.");
            }
            tailSegmentId = upperSegmentId;
        }
    }

    /**
     * Thread-local route selection state that avoids a shared benchmark
     * counter.
     */
    @State(Scope.Thread)
    public static class RouteCursor {

        private int nextRoute;

        /**
         * Seeds each benchmark thread at a different route.
         *
         * @param threadParams JMH thread metadata
         */
        @Setup(Level.Trial)
        public void setup(final ThreadParams threadParams) {
            nextRoute = threadParams.getThreadIndex();
        }

        /**
         * Returns the next route index for this benchmark thread.
         *
         * @param bound number of available routes
         * @return route index
         */
        int next(final int bound) {
            final int current = nextRoute >= bound ? nextRoute % bound
                    : nextRoute;
            nextRoute = current + 1 == bound ? 0 : current + 1;
            return current;
        }
    }
}
