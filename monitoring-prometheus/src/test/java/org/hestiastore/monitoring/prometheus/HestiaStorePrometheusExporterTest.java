package org.hestiastore.monitoring.prometheus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoring;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeSnapshot;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class HestiaStorePrometheusExporterTest {

    @SuppressWarnings("unchecked")
    @Test
    void scrape_containsExpectedMetricNames() {
        final SegmentIndex<Integer, String> index = Mockito.mock(
                SegmentIndex.class);
        final IndexRuntimeMonitoring runtimeMonitoring = Mockito.mock(
                IndexRuntimeMonitoring.class);
        final SegmentIndexMetricsSnapshot metricsSnapshot = snapshot(7L, 11L,
                13L, SegmentIndexState.READY);
        Mockito.when(index.runtimeMonitoring()).thenReturn(runtimeMonitoring);
        Mockito.when(runtimeMonitoring.snapshot())
                .thenReturn(new IndexRuntimeSnapshot("orders",
                        SegmentIndexState.READY, metricsSnapshot,
                        Instant.now()));

        final String scrape = HestiaStorePrometheusExporter.scrape(
                new PrometheusSegmentIndexSource("orders", index));

        assertTrue(scrape.contains("hestiastore_ops_get_total"));
        assertTrue(scrape.contains("hestiastore_ops_put_total"));
        assertTrue(scrape.contains("hestiastore_ops_delete_total"));
        assertTrue(scrape.contains("hestiastore_partition_count"));
        assertTrue(scrape.contains("hestiastore_partition_buffered_key_count"));
        assertTrue(scrape.contains("hestiastore_partition_drain_schedule_total"));
        assertTrue(
                scrape.contains("hestiastore_partition_drain_latency_p95_micros"));
        assertTrue(scrape.contains("hestiastore_split_schedule_total"));
        assertTrue(scrape.contains("hestiastore_split_in_flight"));
        assertTrue(scrape.contains("hestiastore_index_up"));
        assertTrue(scrape.contains("index=\"orders\""));
    }

    @SuppressWarnings("unchecked")
    @Test
    void scrape_exportsExactSplitDrainAndBacklogValuesAndRefreshes() {
        final SegmentIndex<Integer, String> index = Mockito.mock(
                SegmentIndex.class);
        final IndexRuntimeMonitoring runtimeMonitoring = Mockito.mock(
                IndexRuntimeMonitoring.class);
        final AtomicReference<SegmentIndexMetricsSnapshot> snapshotRef = new AtomicReference<>(
                snapshot(1L, 2L, 3L, SegmentIndexState.READY, 7, 2, 11, 29, 3,
                        2, 1, 4, 17, 19L, 23L, 31L, 5, 43L, 37L, 2));
        Mockito.when(index.runtimeMonitoring()).thenReturn(runtimeMonitoring);
        Mockito.when(runtimeMonitoring.snapshot()).thenAnswer(
                inv -> new IndexRuntimeSnapshot("orders",
                        snapshotRef.get().getState(), snapshotRef.get(),
                        Instant.now()));

        String scrape = HestiaStorePrometheusExporter.scrape(
                new PrometheusSegmentIndexSource("orders", index));

        assertSamples(scrape, 1D, 2D, 3D, 7D, 2D, 11D, 29D, 3D, 2D, 1D, 4D,
                17D, 19D, 23D, 31D, 5D, 43D, 37D, 2D, 1D);

        snapshotRef.set(snapshot(5L, 8L, 13L, SegmentIndexState.CLOSED, 9, 3,
                15, 41, 4, 1, 0, 2, 5, 29L, 31L, 37L, 0, 0L, 41L, 0));

        scrape = HestiaStorePrometheusExporter.scrape(
                new PrometheusSegmentIndexSource("orders", index));

        assertSamples(scrape, 5D, 8D, 13D, 9D, 3D, 15D, 41D, 4D, 1D, 0D, 2D,
                5D, 29D, 31D, 37D, 0D, 0D, 41D, 0D, 0D);
    }

    private void assertSampleValue(final String scrape, final String metricName,
            final String indexName, final double expected) {
        assertEquals(expected, sampleValue(scrape, metricName, indexName),
                0.0000001D,
                "Unexpected Prometheus sample for " + metricName
                        + " index=" + indexName);
    }

    private double sampleValue(final String scrape, final String metricName,
            final String indexName) {
        final Pattern pattern = Pattern.compile(
                "^" + Pattern.quote(metricName)
                        + "\\{index=\"" + Pattern.quote(indexName)
                        + "\"(?:,)?} ([+-]?(?:\\d+(?:\\.\\d+)?|NaN|Inf|-Inf)(?:[Ee][+-]?\\d+)?)$",
                Pattern.MULTILINE);
        final Matcher matcher = pattern.matcher(scrape);
        assertTrue(matcher.find(),
                "Missing sample for " + metricName + " index=" + indexName
                        + " in scrape:\n" + scrape);
        return Double.parseDouble(matcher.group(1));
    }

    private SegmentIndexMetricsSnapshot snapshot(final long getCount,
            final long putCount, final long deleteCount,
            final SegmentIndexState state) {
        return new SegmentIndexMetricsSnapshot(
                getCount, putCount, deleteCount,
                0L, 0L, 0L, 0L,
                0, 0,
                0, 0, 0,
                0, 0, 0, 0, 0, 0,
                0L, 0L, 0L, 0L,
                0L, 0L, 0L,
                0, 0, 0, 0, 0,
                0L, 0L, 0L,
                0L, 0L, 0L,
                0, 0, 0D,
                0L, 0L, 0L, 0L,
                List.<SegmentIndexMetricsSnapshot.SegmentMetricsSnapshot>of(),
                state);
    }

    private SegmentIndexMetricsSnapshot snapshot(final long getCount,
            final long putCount, final long deleteCount,
            final SegmentIndexState state,
            final int activePartitionLimit,
            final int immutableRunLimit,
            final int partitionBufferLimit, final int indexBufferLimit,
            final int partitionCount, final int activePartitionCount,
            final int drainingPartitionCount, final int immutableRunCount,
            final int partitionBufferedKeyCount,
            final long localThrottleCount,
            final long globalThrottleCount,
            final long drainScheduleCount, final int drainInFlightCount,
            final long drainLatencyP95Micros,
            final long splitScheduleCount, final int splitInFlightCount) {
        return new SegmentIndexMetricsSnapshot(
                getCount, putCount, deleteCount,
                0L, 0L, 0L, 0L,
                0, 0,
                0, activePartitionLimit, partitionBufferLimit,
                0, 0, 0, 0, 0, 0,
                0L, 0L, 0L, 0L,
                0L, 0L, splitScheduleCount,
                splitInFlightCount, 0, 0, 0, 0,
                0, 0L, 0L, 0, 0L, 0L, 0, 0, 0, 0L, 0L,
                0L, 0L, 0L,
                0L, 0L, 0L,
                0, 0, 0D,
                0L, 0L, 0L, 0L,
                false, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
                0, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
                immutableRunLimit, indexBufferLimit, partitionCount,
                activePartitionCount, drainingPartitionCount,
                immutableRunCount, partitionBufferedKeyCount,
                localThrottleCount, globalThrottleCount, drainScheduleCount,
                drainInFlightCount, drainLatencyP95Micros, 0L, 0L, 0L, 0L, 0,
                0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
                List.<SegmentIndexMetricsSnapshot.SegmentMetricsSnapshot>of(),
                state);
    }

    private void assertSamples(final String scrape, final double getCount,
            final double putCount, final double deleteCount,
            final double activePartitionLimit,
            final double immutableRunLimit,
            final double partitionBufferLimit, final double indexBufferLimit,
            final double partitionCount, final double activePartitionCount,
            final double drainingPartitionCount,
            final double immutableRunCount,
            final double partitionBufferedKeyCount,
            final double localThrottleCount,
            final double globalThrottleCount,
            final double drainScheduleCount, final double drainInFlightCount,
            final double drainLatencyP95Micros,
            final double splitScheduleCount, final double splitInFlightCount,
            final double indexUp) {
        assertSampleValue(scrape, "hestiastore_ops_get_total", "orders",
                getCount);
        assertSampleValue(scrape, "hestiastore_ops_put_total", "orders",
                putCount);
        assertSampleValue(scrape, "hestiastore_ops_delete_total", "orders",
                deleteCount);
        assertSampleValue(scrape, "hestiastore_partition_active_limit",
                "orders", activePartitionLimit);
        assertSampleValue(scrape, "hestiastore_partition_immutable_run_limit",
                "orders", immutableRunLimit);
        assertSampleValue(scrape, "hestiastore_partition_buffer_limit",
                "orders", partitionBufferLimit);
        assertSampleValue(scrape, "hestiastore_index_buffer_limit", "orders",
                indexBufferLimit);
        assertSampleValue(scrape, "hestiastore_partition_count", "orders",
                partitionCount);
        assertSampleValue(scrape, "hestiastore_partition_active_count",
                "orders", activePartitionCount);
        assertSampleValue(scrape, "hestiastore_partition_draining_count",
                "orders", drainingPartitionCount);
        assertSampleValue(scrape, "hestiastore_partition_immutable_run_count",
                "orders", immutableRunCount);
        assertSampleValue(scrape, "hestiastore_partition_buffered_key_count",
                "orders", partitionBufferedKeyCount);
        assertSampleValue(scrape, "hestiastore_partition_throttle_local_total",
                "orders", localThrottleCount);
        assertSampleValue(scrape, "hestiastore_partition_throttle_global_total",
                "orders", globalThrottleCount);
        assertSampleValue(scrape, "hestiastore_partition_drain_schedule_total",
                "orders", drainScheduleCount);
        assertSampleValue(scrape, "hestiastore_partition_drain_in_flight",
                "orders", drainInFlightCount);
        assertSampleValue(scrape,
                "hestiastore_partition_drain_latency_p95_micros", "orders",
                drainLatencyP95Micros);
        assertSampleValue(scrape, "hestiastore_split_schedule_total", "orders",
                splitScheduleCount);
        assertSampleValue(scrape, "hestiastore_split_in_flight", "orders",
                splitInFlightCount);
        assertSampleValue(scrape, "hestiastore_index_up", "orders", indexUp);
    }
}
