package org.hestiastore.monitoring.prometheus;

import static org.junit.jupiter.api.Assertions.assertTrue;

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
                .thenReturn(new SegmentIndexMetricsSnapshot(7L, 11L, 13L,
                        SegmentIndexState.READY));
        Mockito.when(index.getState()).thenReturn(SegmentIndexState.READY);

        final String scrape = HestiaStorePrometheusExporter.scrape(
                new PrometheusSegmentIndexSource("orders", index));

        assertTrue(scrape.contains("hestiastore_ops_get_total"));
        assertTrue(scrape.contains("hestiastore_ops_put_total"));
        assertTrue(scrape.contains("hestiastore_ops_delete_total"));
        assertTrue(scrape.contains("hestiastore_index_up"));
        assertTrue(scrape.contains("index=\"orders\""));
    }
}
