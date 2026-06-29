package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.datatype.ByteArray;
import org.hestiastore.index.datatype.NullValue;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorByteArray;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorNull;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.segmentindex.MemoryEstimateReport;
import org.hestiastore.index.segmentindex.configuration.api.IndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfigurationResolver;
import org.junit.jupiter.api.Test;

class IndexMemoryEstimatorTest {

    @Test
    void estimateCalculatesCompleteReportWithReadableInputs() {
        final EffectiveIndexConfiguration<Integer, String> configuration =
                effectiveConfiguration("memory-estimate-complete",
                        String.class, new TypeDescriptorShortString());

        final MemoryEstimateReport estimate = IndexMemoryEstimator.estimate(
                configuration, new TypeDescriptorInteger(),
                new TypeDescriptorShortString(), 0);
        printReport("complete", estimate);

        assertTrue(estimate.isComplete());
        assertEquals(98_460L, estimate.totalEstimatedBytes().orElseThrow());
        final String log = estimate.text();
        assertTrue(log.contains("Estimated memory use at startup"));
        assertTrue(log.contains("Estimated active heap: 96.15 KiB"));
        assertTrue(log.contains(
                "steady state: 76.92 KiB (memory expected after startup caches are loaded)"));
        assertTrue(log.contains(
                "temporary margin: 19.23 KiB (extra headroom for short-lived objects and snapshots)"));
        assertLineAppearsBefore(log, "Estimated active heap: 96.15 KiB",
                "Notes:");
        assertLineAppearsBefore(log, "Estimated active heap: 96.15 KiB",
                "Per-entry size assumptions:");
        assertTrue(log.contains(
                "Rough estimate only; not a JVM cap or measured allocation."));
        assertTrue(log.contains("Per-entry size assumptions:"));
        assertTrue(log.contains("entry: about 228 B (key + value + overhead)"));
        assertTrue(log.contains(
                "fixed per loaded/cached segment: about 16.00 KiB"));
        assertTrue(log.contains(
                "details: docs/operations/memory-estimate.md"));
        assertTrue(log.contains("Largest steady-state areas:"));
        assertTrue(log.contains(
                "loaded segment infrastructure: 48.00 KiB"));
        assertTrue(log.contains(
                "scarce indexes if loaded: 15.23 KiB"));
        assertTrue(log.contains("Configuration used for this estimate:"));
        assertTrue(log.contains(
                "cached segments=3, segment cache keys=10, maintenance keys=6"));
        assertTrue(log.contains("max keys per segment=100"));
        assertTrue(log.contains("Reported but not included:"));
        assertTrue(log.contains(
                "index write-buffer keys: 18"));
        assertTrue(log.contains(
                "chunk key limit=2, chunk-store pages=0, Bloom filter=1.00 KiB"));
        assertTrue(log.contains(
                "Estimated memory by area:"));
        assertTrue(log.contains(
                "inputs: 3 segments, 10 read keys, 6 maintenance keys, entry 228 B"));
        assertTrue(log.contains("End memory estimate"));
        assertFalse(log.contains("based on:"));
        assertFalse(log.contains("reported only"));
    }

    @Test
    void estimateIncludesChunkStoreCachePageLimit() {
        final EffectiveIndexConfiguration<Integer, String> configuration =
                effectiveConfiguration("memory-estimate-chunk-cache",
                        String.class, new TypeDescriptorShortString(), 10, 6,
                        2, 3, 7);

        final MemoryEstimateReport estimate = IndexMemoryEstimator.estimate(
                configuration, new TypeDescriptorInteger(),
                new TypeDescriptorShortString(), 0);
        printReport("chunk-store-cache", estimate);

        assertEquals(103_010L, estimate.totalEstimatedBytes().orElseThrow());
        assertTrue(estimate.text().contains(
                "chunk-store cache: 3.55 KiB"));
        assertTrue(estimate.text().contains(
                "inputs: 7 pages, 2 chunk keys, page overhead 64 B, entry 228 B"));
    }

    @Test
    void estimateScalesCachedSegmentLimit() {
        final EffectiveIndexConfiguration<Integer, String> configuration =
                effectiveConfiguration("memory-estimate-segment-limit",
                        String.class, new TypeDescriptorShortString(), 10, 6,
                        2, 5, 0);

        final MemoryEstimateReport estimate = IndexMemoryEstimator.estimate(
                configuration, new TypeDescriptorInteger(),
                new TypeDescriptorShortString(), 0);
        printReport("cached-segment-limit", estimate);

        assertEquals(164_100L, estimate.totalEstimatedBytes().orElseThrow());
        assertTrue(estimate.text().contains(
                "loaded segment cache: 17.81 KiB"));
        assertTrue(estimate.text().contains(
                "Bloom filters if loaded: 5.00 KiB"));
        assertTrue(estimate.text().contains(
                "loaded segment infrastructure: 80.00 KiB"));
    }

    @Test
    void estimateIncludesRouteCount() {
        final EffectiveIndexConfiguration<Integer, String> configuration =
                effectiveConfiguration("memory-estimate-route-count",
                        String.class, new TypeDescriptorShortString());

        final MemoryEstimateReport estimate = IndexMemoryEstimator.estimate(
                configuration, new TypeDescriptorInteger(),
                new TypeDescriptorShortString(), 4);
        printReport("route-count", estimate);

        assertEquals(99_220L, estimate.totalEstimatedBytes().orElseThrow());
        assertTrue(estimate.text().contains(
                "route map: 608 B"));
        assertTrue(estimate.text().contains(
                "inputs: 4 routes, key 4 B, segment id 16 B, tree entry 56 B"));
    }

    @Test
    void estimateReflectsCacheKeyLimitsInSegmentCacheAndHeadroom() {
        final EffectiveIndexConfiguration<Integer, String> configuration =
                effectiveConfiguration("memory-estimate-cache-limits",
                        String.class, new TypeDescriptorShortString(), 20, 10,
                        2, 3, 0);

        final MemoryEstimateReport estimate = IndexMemoryEstimator.estimate(
                configuration, new TypeDescriptorInteger(),
                new TypeDescriptorShortString(), 0);
        printReport("cache-key-limits", estimate);

        assertEquals(110_430L, estimate.totalEstimatedBytes().orElseThrow());
        assertTrue(estimate.text().contains(
                "loaded segment cache: 20.04 KiB"));
        assertTrue(estimate.text().contains(
                "inputs: 3 segments, 20 read keys, 10 maintenance keys, entry 228 B"));
        assertTrue(estimate.text().contains(
                "temporary memory margin: 21.57 KiB"));
        assertTrue(estimate.text().contains(
                "inputs: max(25% of steady state = 86.27 KiB, one segment cache = 6.68 KiB)"));
    }

    @Test
    void estimateFormatsGiBWhenSegmentCacheExceedsOneGiB() {
        final EffectiveIndexConfiguration<Integer, String> configuration =
                effectiveConfiguration("memory-estimate-segment-cache-gib",
                        String.class, new TypeDescriptorShortString(),
                        500_000, 200_000, 2, 10, 0);

        final MemoryEstimateReport estimate = IndexMemoryEstimator.estimate(
                configuration, new TypeDescriptorInteger(),
                new TypeDescriptorShortString(), 0);
        printReport("segment-cache-gib", estimate);

        assertEquals(1_995_282_600L,
                estimate.totalEstimatedBytes().orElseThrow());
        assertTrue(estimate.text().contains(
                "Estimated active heap: 1.86 GiB"));
        assertTrue(estimate.text().contains(
                "steady state: 1.49 GiB (memory expected after startup caches are loaded)"));
        assertTrue(estimate.text().contains(
                "loaded segment cache: 1.49 GiB"));
        assertTrue(estimate.text().contains(
                "inputs: 10 segments, 500000 read keys, 200000 maintenance keys, entry 228 B"));
    }

    @Test
    void estimateFormatsGiBWhenChunkStoreCacheExceedsOneGiB() {
        final EffectiveIndexConfiguration<Integer, String> configuration =
                effectiveConfiguration("memory-estimate-chunk-store-gib",
                        String.class, new TypeDescriptorShortString(), 10, 6,
                        100, 3, 50_000);

        final MemoryEstimateReport estimate = IndexMemoryEstimator.estimate(
                configuration, new TypeDescriptorInteger(),
                new TypeDescriptorShortString(), 0);
        printReport("chunk-store-gib", estimate);

        assertEquals(1_429_079_350L,
                estimate.totalEstimatedBytes().orElseThrow());
        assertTrue(estimate.text().contains(
                "Estimated active heap: 1.33 GiB"));
        assertTrue(estimate.text().contains(
                "chunk-store cache: 1.06 GiB"));
        assertTrue(estimate.text().contains(
                "inputs: 50000 pages, 100 chunk keys, page overhead 64 B, entry 228 B"));
    }

    @Test
    void estimateReportsIncompleteWhenValueDescriptorSizeIsUnknown() {
        final EffectiveIndexConfiguration<Integer, ByteArray> configuration =
                effectiveConfiguration("memory-estimate-incomplete",
                        ByteArray.class, new TypeDescriptorByteArray());

        final MemoryEstimateReport estimate = IndexMemoryEstimator.estimate(
                configuration, new TypeDescriptorInteger(),
                new TypeDescriptorByteArray(), 3);
        printReport("unknown-value-size", estimate);

        assertFalse(estimate.isComplete());
        assertTrue(estimate.totalEstimatedBytes().isEmpty());
        final String log = estimate.text();
        assertTrue(log.contains("value: TypeDescriptorByteArray, unknown"));
        assertTrue(log.contains(
                "loaded segment cache: unavailable"));
        assertTrue(log.contains(
                "reason: needs key and value descriptor estimates"));
        assertTrue(log.contains("Bloom filters if loaded"));
        assertTrue(log.contains("Estimated active heap: unavailable"));
    }

    @Test
    void estimateTreatsZeroValueSizeAsRealEstimate() {
        final EffectiveIndexConfiguration<Integer, NullValue> configuration =
                effectiveConfiguration("memory-estimate-zero",
                        NullValue.class, new TypeDescriptorNull());

        final MemoryEstimateReport estimate = IndexMemoryEstimator.estimate(
                configuration, new TypeDescriptorInteger(),
                new TypeDescriptorNull(), 1);
        printReport("zero-value-size", estimate);

        assertTrue(estimate.isComplete());
        assertTrue(estimate.totalEstimatedBytes().isPresent());
        assertTrue(estimate.text().contains(
                "entry: about 100 B (key + value + overhead)"));
    }

    private static <V> EffectiveIndexConfiguration<Integer, V> effectiveConfiguration(
            final String name, final Class<V> valueClass,
            final TypeDescriptor<V> valueTypeDescriptor) {
        return effectiveConfiguration(name, valueClass, valueTypeDescriptor,
                10, 6, 2, 3, 0);
    }

    private static <V> EffectiveIndexConfiguration<Integer, V> effectiveConfiguration(
            final String name, final Class<V> valueClass,
            final TypeDescriptor<V> valueTypeDescriptor,
            final int cacheKeyLimit,
            final int maintenanceWriteCacheKeyLimit,
            final int chunkKeyLimit,
            final int cachedSegmentLimit,
            final int chunkStorePageLimit) {
        return EffectiveIndexConfigurationResolver.resolveForCreate(
                IndexConfiguration.<Integer, V>builder()
                        .identity(identity -> identity.keyClass(Integer.class))
                        .identity(identity -> identity.valueClass(valueClass))
                        .identity(identity -> identity.keyTypeDescriptor(
                                new TypeDescriptorInteger()))
                        .identity(identity -> identity
                                .valueTypeDescriptor(valueTypeDescriptor))
                        .identity(identity -> identity.name(name))
                        .segment(segment -> segment
                                .cacheKeyLimit(cacheKeyLimit))
                        .writePath(writePath -> writePath
                                .segmentWriteCacheKeyLimit(5))
                        .writePath(writePath -> writePath
                                .maintenanceWriteCacheKeyLimit(
                                        maintenanceWriteCacheKeyLimit))
                        .segment(segment -> segment.maxKeys(100))
                        .segment(segment -> segment
                                .chunkKeyLimit(chunkKeyLimit))
                        .segment(segment -> segment
                                .cachedSegmentLimit(cachedSegmentLimit))
                        .bloomFilter(bloomFilter -> bloomFilter
                                .hashFunctions(1))
                        .bloomFilter(bloomFilter -> bloomFilter
                                .indexSizeBytes(1024))
                        .bloomFilter(bloomFilter -> bloomFilter
                                .falsePositiveProbability(0.01D))
                        .chunkStoreCache(cache -> cache
                                .pageLimit(chunkStorePageLimit))
                        .build());
    }

    private static void printReport(final String scenario,
            final MemoryEstimateReport report) {
        System.out.println();
        System.out.println("==== memory estimate report: " + scenario
                + " ====");
        System.out.println(report.text());
        System.out.println("==== end memory estimate report: " + scenario
                + " ====");
        assertReadableLineLengths(report);
        assertFalse(report.text().contains(" bytes"));
    }

    private static void assertReadableLineLengths(
            final MemoryEstimateReport report) {
        report.lines().forEach(line -> assertTrue(line.length() <= 100,
                () -> "Report line is too long: " + line));
    }

    private static void assertLineAppearsBefore(final String log,
            final String earlier, final String later) {
        final int earlierIndex = log.indexOf(earlier);
        final int laterIndex = log.indexOf(later);

        assertTrue(earlierIndex >= 0, () -> "Missing line: " + earlier);
        assertTrue(laterIndex >= 0, () -> "Missing line: " + later);
        assertTrue(earlierIndex < laterIndex,
                () -> earlier + " should appear before " + later);
    }
}
