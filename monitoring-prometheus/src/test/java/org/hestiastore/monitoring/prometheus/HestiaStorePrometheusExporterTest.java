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
        assertTrue(scrape.contains("hestiastore_segment_write_cache_key_limit"));
        assertTrue(
                scrape.contains("hestiastore_segment_write_cache_key_limit_during_maintenance"));
        assertTrue(scrape.contains("hestiastore_index_buffered_write_key_limit"));
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
                snapshot(1L, 2L, 3L, SegmentIndexState.READY, 7, 11, 29, 37L,
                        2));
        Mockito.when(index.runtimeMonitoring()).thenReturn(runtimeMonitoring);
        Mockito.when(runtimeMonitoring.snapshot()).thenAnswer(
                inv -> new IndexRuntimeSnapshot("orders",
                        snapshotRef.get().getState(), snapshotRef.get(),
                        Instant.now()));

        String scrape = HestiaStorePrometheusExporter.scrape(
                new PrometheusSegmentIndexSource("orders", index));

        assertSamples(scrape, 1D, 2D, 3D, 7D, 11D, 29D, 37D, 2D, 1D);

        snapshotRef.set(snapshot(5L, 8L, 13L, SegmentIndexState.CLOSED, 9, 13,
                15, 41L, 0));

        scrape = HestiaStorePrometheusExporter.scrape(
                new PrometheusSegmentIndexSource("orders", index));

        assertSamples(scrape, 5D, 8D, 13D, 9D, 13D, 15D, 41D, 0D, 0D);
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
        return snapshot(getCount, putCount, deleteCount, state, 0, 0, 0, 0L,
                0);
    }

    private SegmentIndexMetricsSnapshot snapshot(final long getCount,
            final long putCount, final long deleteCount,
            final SegmentIndexState state, final int segmentWriteCacheKeyLimit,
            final int segmentWriteCacheKeyLimitDuringMaintenance,
            final int indexBufferedWriteKeyLimit,
            final long splitScheduleCount, final int splitInFlightCount) {
        return new SegmentIndexMetricsSnapshot(
                getCount, putCount, deleteCount,
                0L, 0L, 0L, 0L,
                0, 0,
                0, segmentWriteCacheKeyLimit,
                segmentWriteCacheKeyLimitDuringMaintenance,
                indexBufferedWriteKeyLimit, 0, 0, 0, 0, 0, 0,
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
                0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
                List.<SegmentIndexMetricsSnapshot.SegmentMetricsSnapshot>of(),
                state);
    }

    private void assertSamples(final String scrape, final double getCount,
            final double putCount, final double deleteCount,
            final double segmentWriteCacheKeyLimit,
            final double segmentWriteCacheKeyLimitDuringMaintenance,
            final double indexBufferedWriteKeyLimit,
            final double splitScheduleCount, final double splitInFlightCount,
            final double indexUp) {
        assertSampleValue(scrape, "hestiastore_ops_get_total", "orders",
                getCount);
        assertSampleValue(scrape, "hestiastore_ops_put_total", "orders",
                putCount);
        assertSampleValue(scrape, "hestiastore_ops_delete_total", "orders",
                deleteCount);
        assertSampleValue(scrape, "hestiastore_segment_write_cache_key_limit",
                "orders", segmentWriteCacheKeyLimit);
        assertSampleValue(scrape,
                "hestiastore_segment_write_cache_key_limit_during_maintenance",
                "orders", segmentWriteCacheKeyLimitDuringMaintenance);
        assertSampleValue(scrape, "hestiastore_index_buffered_write_key_limit",
                "orders", indexBufferedWriteKeyLimit);
        assertSampleValue(scrape, "hestiastore_split_schedule_total", "orders",
                splitScheduleCount);
        assertSampleValue(scrape, "hestiastore_split_in_flight", "orders",
                splitInFlightCount);
        assertSampleValue(scrape, "hestiastore_index_up", "orders", indexUp);
    }
}
