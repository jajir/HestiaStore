package org.hestiastore.monitoring.prometheus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class HestiaStorePrometheusExporterTest {

    @SuppressWarnings("unchecked")
    @Test
    void scrape_containsExpectedMetricNames() {
        final SegmentIndex<Integer, String> index = Mockito.mock(
                SegmentIndex.class);
        Mockito.when(index.metricsSnapshot())
                .thenReturn(snapshot(7L, 11L, 13L, SegmentIndexState.READY));
        Mockito.when(index.getState()).thenReturn(SegmentIndexState.READY);

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
        final AtomicReference<SegmentIndexMetricsSnapshot> snapshotRef = new AtomicReference<>(
                snapshot(1L, 2L, 3L, SegmentIndexState.READY, 7, 2, 11, 29, 3,
                        2, 1, 4, 17, 19L, 23L, 31L, 5, 43L, 37L, 2));
        Mockito.when(index.metricsSnapshot()).thenAnswer(inv -> snapshotRef.get());
        Mockito.when(index.getState()).thenAnswer(inv -> snapshotRef.get().getState());

        String scrape = HestiaStorePrometheusExporter.scrape(
                new PrometheusSegmentIndexSource("orders", index));

        assertSampleValue(scrape, "hestiastore_ops_get_total", "orders", 1D);
        assertSampleValue(scrape, "hestiastore_ops_put_total", "orders", 2D);
        assertSampleValue(scrape, "hestiastore_ops_delete_total", "orders", 3D);
        assertSampleValue(scrape, "hestiastore_partition_active_limit",
                "orders", 7D);
        assertSampleValue(scrape, "hestiastore_partition_immutable_run_limit",
                "orders", 2D);
        assertSampleValue(scrape, "hestiastore_partition_buffer_limit",
                "orders", 11D);
        assertSampleValue(scrape, "hestiastore_index_buffer_limit", "orders",
                29D);
        assertSampleValue(scrape, "hestiastore_partition_count", "orders", 3D);
        assertSampleValue(scrape, "hestiastore_partition_active_count",
                "orders", 2D);
        assertSampleValue(scrape, "hestiastore_partition_draining_count",
                "orders", 1D);
        assertSampleValue(scrape, "hestiastore_partition_immutable_run_count",
                "orders", 4D);
        assertSampleValue(scrape, "hestiastore_partition_buffered_key_count",
                "orders", 17D);
        assertSampleValue(scrape, "hestiastore_partition_throttle_local_total",
                "orders", 19D);
        assertSampleValue(scrape, "hestiastore_partition_throttle_global_total",
                "orders", 23D);
        assertSampleValue(scrape, "hestiastore_partition_drain_schedule_total",
                "orders", 31D);
        assertSampleValue(scrape, "hestiastore_partition_drain_in_flight",
                "orders", 5D);
        assertSampleValue(scrape,
                "hestiastore_partition_drain_latency_p95_micros", "orders",
                43D);
        assertSampleValue(scrape, "hestiastore_split_schedule_total", "orders",
                37D);
        assertSampleValue(scrape, "hestiastore_split_in_flight", "orders",
                2D);
        assertSampleValue(scrape, "hestiastore_index_up", "orders", 1D);

        snapshotRef.set(snapshot(5L, 8L, 13L, SegmentIndexState.CLOSED, 9, 3,
                15, 41, 4, 1, 0, 2, 5, 29L, 31L, 37L, 0, 0L, 41L, 0));

        scrape = HestiaStorePrometheusExporter.scrape(
                new PrometheusSegmentIndexSource("orders", index));

        assertSampleValue(scrape, "hestiastore_ops_get_total", "orders", 5D);
        assertSampleValue(scrape, "hestiastore_ops_put_total", "orders", 8D);
        assertSampleValue(scrape, "hestiastore_ops_delete_total", "orders",
                13D);
        assertSampleValue(scrape, "hestiastore_partition_active_limit",
                "orders", 9D);
        assertSampleValue(scrape, "hestiastore_partition_count", "orders", 4D);
        assertSampleValue(scrape, "hestiastore_partition_draining_count",
                "orders", 0D);
        assertSampleValue(scrape, "hestiastore_partition_buffered_key_count",
                "orders", 5D);
        assertSampleValue(scrape, "hestiastore_partition_throttle_global_total",
                "orders", 31D);
        assertSampleValue(scrape, "hestiastore_partition_drain_in_flight",
                "orders", 0D);
        assertSampleValue(scrape,
                "hestiastore_partition_drain_latency_p95_micros", "orders",
                0D);
        assertSampleValue(scrape, "hestiastore_split_schedule_total", "orders",
                41D);
        assertSampleValue(scrape, "hestiastore_split_in_flight", "orders",
                0D);
        assertSampleValue(scrape, "hestiastore_index_up", "orders", 0D);
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
                List.of(),
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
                drainInFlightCount, drainLatencyP95Micros, List.of(),
                state);
    }
}
