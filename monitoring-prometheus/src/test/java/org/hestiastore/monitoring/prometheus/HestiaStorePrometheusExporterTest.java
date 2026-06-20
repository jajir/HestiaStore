package org.hestiastore.monitoring.prometheus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.monitoring.SegmentIndexRuntimeMonitoring;
import org.hestiastore.index.segmentindex.monitoring.model.SegmentIndexRuntimeSnapshot;
import org.hestiastore.index.segmentindex.monitoring.model.SegmentIndexBloomFilterMetrics;
import org.hestiastore.index.segmentindex.monitoring.model.SegmentIndexChunkStoreCacheMetrics;
import org.hestiastore.index.segmentindex.monitoring.model.SegmentIndexExecutorMetrics;
import org.hestiastore.index.segmentindex.monitoring.model.SegmentIndexLatencyMetrics;
import org.hestiastore.index.segmentindex.monitoring.model.SegmentIndexMaintenanceMetrics;
import org.hestiastore.index.segmentindex.monitoring.model.SegmentIndexOperationMetrics;
import org.hestiastore.index.segmentindex.monitoring.model.SegmentIndexRegistryCacheMetrics;
import org.hestiastore.index.segmentindex.monitoring.model.SegmentIndexSegmentMetrics;
import org.hestiastore.index.segmentindex.monitoring.model.SegmentIndexSplitMetrics;
import org.hestiastore.index.segmentindex.monitoring.model.SegmentIndexWalMetrics;
import org.hestiastore.index.segmentindex.monitoring.model.SegmentIndexWritePathMetrics;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class HestiaStorePrometheusExporterTest {

    @SuppressWarnings("unchecked")
    @Test
    void scrape_containsExpectedMetricNames() {
        final SegmentIndex<Integer, String> index = Mockito.mock(
                SegmentIndex.class);
        final SegmentIndexRuntimeMonitoring runtimeMonitoring = Mockito.mock(
                SegmentIndexRuntimeMonitoring.class);
        final SegmentIndexRuntimeSnapshot runtimeSnapshot = snapshot(7L, 11L,
                13L, SegmentIndexState.READY);
        Mockito.when(index.runtimeMonitoring()).thenReturn(runtimeMonitoring);
        Mockito.when(runtimeMonitoring.snapshot()).thenReturn(runtimeSnapshot);

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
    void scrape_exportsExactSplitBacklogValuesAndRefreshes() {
        final SegmentIndex<Integer, String> index = Mockito.mock(
                SegmentIndex.class);
        final SegmentIndexRuntimeMonitoring runtimeMonitoring = Mockito.mock(
                SegmentIndexRuntimeMonitoring.class);
        final AtomicReference<SegmentIndexRuntimeSnapshot> snapshotRef = new AtomicReference<>(
                snapshot(1L, 2L, 3L, SegmentIndexState.READY, 7, 11, 29, 37L,
                        2));
        Mockito.when(index.runtimeMonitoring()).thenReturn(runtimeMonitoring);
        Mockito.when(runtimeMonitoring.snapshot()).thenAnswer(
                inv -> snapshotRef.get());

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

    private SegmentIndexRuntimeSnapshot snapshot(final long getCount,
            final long putCount, final long deleteCount,
            final SegmentIndexState state) {
        return snapshot(getCount, putCount, deleteCount, state, 0, 0, 0, 0L,
                0);
    }

    private SegmentIndexRuntimeSnapshot snapshot(final long getCount,
            final long putCount, final long deleteCount,
            final SegmentIndexState state, final int segmentWriteCacheKeyLimit,
            final int segmentWriteCacheKeyLimitDuringMaintenance,
            final int indexBufferedWriteKeyLimit,
            final long splitScheduleCount, final int splitInFlightCount) {
        final SegmentIndexExecutorMetrics emptyExecutor =
                new SegmentIndexExecutorMetrics(0, 0, 0, 0L, 0L, 0L);
        return new SegmentIndexRuntimeSnapshot(
                "orders",
                state,
                Instant.EPOCH,
                new SegmentIndexOperationMetrics(getCount, putCount,
                        deleteCount),
                new SegmentIndexRegistryCacheMetrics(0L, 0L, 0L, 0L, 0, 0),
                new SegmentIndexChunkStoreCacheMetrics(0, 0, 0L, 0L, 0L, 0L,
                        0L, 0L),
                new SegmentIndexSegmentMetrics(0, 0, 0, 0, 0, 0, 0, 0L, 0L,
                        0L, java.util.List.of()),
                new SegmentIndexWritePathMetrics(segmentWriteCacheKeyLimit,
                        segmentWriteCacheKeyLimitDuringMaintenance,
                        indexBufferedWriteKeyLimit, 0L),
                new SegmentIndexMaintenanceMetrics(0L, 0L, 0L, 0L, 0L, 0L,
                        emptyExecutor, emptyExecutor),
                new SegmentIndexSplitMetrics(splitScheduleCount,
                        splitInFlightCount, 0, 0L, 0L, emptyExecutor),
                new SegmentIndexLatencyMetrics(0L, 0L, 0L, 0L, 0L, 0L),
                new SegmentIndexBloomFilterMetrics(0, 0, 0D, 0L, 0L, 0L, 0L),
                new SegmentIndexWalMetrics(false, 0L, 0L, 0L, 0L, 0L, 0L,
                        0L, 0, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L));
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
