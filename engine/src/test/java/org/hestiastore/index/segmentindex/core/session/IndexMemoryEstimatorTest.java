package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

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
        final EffectiveIndexConfiguration<Integer, String> configuration = effectiveConfiguration(
                "memory-estimate-complete",
                String.class, new TypeDescriptorShortString());

        final MemoryEstimateReport estimate = IndexMemoryEstimator.estimate(
                configuration, new TypeDescriptorInteger(),
                new TypeDescriptorShortString(), 0);
        printReport("complete", estimate);

        assertTrue(estimate.isComplete());
        assertEquals(93_330L, estimate.totalEstimatedBytes().orElseThrow());
        final String log = estimate.text();
        assertTrue(log.contains("Estimated memory use at startup"));
        assertTrue(log.contains("├─ Total index memory - 91.14 KiB"));
        assertTrue(log.contains("│  ├─ All segments - 72.91 KiB"));
        assertTrue(log.contains(
                "│  │  │              3 segment cache slots"));
        assertTrue(log.contains(
                "│  │  ├─ One cached segment - 24.30 KiB"));
        assertTrue(log.contains(
                "│  └─ Maintenance overhead - 18.23 KiB"));
        assertLineAppearsBefore(log, "├─ Total index memory - 91.14 KiB",
                "├─ Key/value entry - 228 B");
        assertTrue(log.contains(
                "not a JVM cap or measured allocation"));
        assertTrue(log.contains("├─ Key/value entry - 228 B"));
        assertLineAppearsBefore(log, "├─ Key/value entry - 228 B",
                "├─ Key/position entry - 104 B");
        assertTrue(log.contains("├─ Key/position entry - 104 B"));
        assertTrue(log.contains("overhead: 96 B"));
        assertTrue(log.contains(
                "├─ Formula constants - shown for transparency, not added separately"));
        assertTrue(log.contains(
                "fixed per loaded/cached segment: about 16.00 KiB"));
        assertTrue(log.contains(
                "details: docs/operations/memory-estimate.md"));
        assertTrue(log.contains(
                "│  │  │  └─ Segment runtime - 16.00 KiB"));
        assertTrue(log.contains(
                "│  │  │  ├─ Scarce index - 5.08 KiB"));
        assertTrue(log.contains(
                "│  │  │  │  ├─ Max number of keys in scarce index - 50"));
        assertDetailAlignsWithValue(estimate,
                "│  │  │  │  ├─ Max number of keys in scarce index - 50",
                "max number of keys in segment: 100");
        assertTrue(log.contains(
                "number of keys per page: 2"));
        assertFalse(log.contains(
                "│  │  │  │  │        max number of keys in segment: 100"));
        assertFalse(log.contains(
                "│  │  │  │  │  │                                    max number of keys in segment: 100"));
        assertTrue(log.contains(
                "│  │  │  │  └─ Key/position entry - 104 B"));
        assertTrue(log.contains(
                "key: TypeDescriptorInteger, about 4 B"));
        assertTrue(log.contains(
                "integer position: TypeDescriptorInteger, about 4 B"));
        assertTrue(log.contains(
                "├─ Other requirements - reported but not included"));
        assertTrue(log.contains("index write-buffer keys: 18"));
        assertTrue(log.contains("chunk keys per page: 2"));
        assertTrue(log.contains("configured bloom filter size: 1.00 KiB"));
        assertTrue(log.contains(
                "│  │  │  ├─ Delta cache - 2.23 KiB"));
        assertTrue(log.contains(
                "configured max number of keys in cache: 10"));
        assertTrue(log.contains("key/value entry: 228 B"));
        assertFalse(log.contains(
                "entry overhead: 96 B inside key/value entry"));
        assertDetailAlignsWithValue(estimate,
                "│  │  ├─ One cached segment - 24.30 KiB",
                "multiplied by 3 segment cache slots");
        assertTrue(log.contains("End memory estimate"));
        assertFalse(log.contains("based on:"));
        assertFalse(log.contains("reported only"));
    }

    @Test
    void estimateIncludesChunkStoreCachePageLimit() {
        final EffectiveIndexConfiguration<Integer, String> configuration = effectiveConfiguration(
                "memory-estimate-chunk-cache",
                String.class, new TypeDescriptorShortString(), 10, 6,
                2, 3, 7);

        final MemoryEstimateReport estimate = IndexMemoryEstimator.estimate(
                configuration, new TypeDescriptorInteger(),
                new TypeDescriptorShortString(), 0);
        printReport("chunk-store-cache", estimate);

        assertEquals(97_880L, estimate.totalEstimatedBytes().orElseThrow());
        assertTrue(estimate.text().contains(
                "│  ├─ Chunk-store page cache - 3.55 KiB"));
        assertTrue(estimate.text().contains("7 pages"));
        assertTrue(estimate.text().contains("chunk keys per page: 2"));
        assertTrue(estimate.text().contains("page overhead: 64 B"));
        assertTrue(estimate.text().contains("key/value entry: 228 B"));
    }

    @Test
    void estimateScalesCachedSegmentLimit() {
        final EffectiveIndexConfiguration<Integer, String> configuration = effectiveConfiguration(
                "memory-estimate-segment-limit",
                String.class, new TypeDescriptorShortString(), 10, 6,
                2, 5, 0);

        final MemoryEstimateReport estimate = IndexMemoryEstimator.estimate(
                configuration, new TypeDescriptorInteger(),
                new TypeDescriptorShortString(), 0);
        printReport("cached-segment-limit", estimate);

        assertEquals(155_550L, estimate.totalEstimatedBytes().orElseThrow());
        assertTrue(estimate.text().contains(
                "│  ├─ All segments - 121.52 KiB"));
        assertTrue(estimate.text().contains(
                "│  │  ├─ One cached segment - 24.30 KiB"));
        assertTrue(estimate.text().contains(
                "5 segment cache slots"));
    }

    @Test
    void estimateIncludesRouteCount() {
        final EffectiveIndexConfiguration<Integer, String> configuration = effectiveConfiguration(
                "memory-estimate-route-count",
                String.class, new TypeDescriptorShortString());

        final MemoryEstimateReport estimate = IndexMemoryEstimator.estimate(
                configuration, new TypeDescriptorInteger(),
                new TypeDescriptorShortString(), 10);
        printReport("route-count", estimate);

        assertEquals(93_430L, estimate.totalEstimatedBytes().orElseThrow());
        assertTrue(estimate.text().contains(
                "│  │  └─ Segment routing map - 80 B"));
        assertTrue(estimate.text().contains(
                "│  │     ├─ Number of segment routes - 10"));
        assertTrue(estimate.text().contains(
                "│  │     └─ Key/segment-id entry - 8 B"));
        assertDetailAlignsWithValue(estimate,
                "│  │     └─ Key/segment-id entry - 8 B",
                "key: 4 B");
        assertTrue(estimate.text().contains("segment id: 4 B"));
        assertFalse(estimate.text().contains("segment id: 16 B"));
        assertFalse(estimate.text().contains("route-map tree entry"));
    }

    @Test
    void estimateReflectsDeltaCacheKeyLimitAndHeadroom() {
        final EffectiveIndexConfiguration<Integer, String> configuration = effectiveConfiguration(
                "memory-estimate-cache-limits",
                String.class, new TypeDescriptorShortString(), 20, 10,
                2, 3, 0);

        final MemoryEstimateReport estimate = IndexMemoryEstimator.estimate(
                configuration, new TypeDescriptorInteger(),
                new TypeDescriptorShortString(), 0);
        printReport("cache-key-limits", estimate);

        assertEquals(101_880L, estimate.totalEstimatedBytes().orElseThrow());
        assertTrue(estimate.text().contains(
                "│  │  │  ├─ Delta cache - 4.45 KiB"));
        assertTrue(estimate.text().contains(
                "configured max number of keys in cache: 20"));
        assertTrue(estimate.text().contains("key/value entry: 228 B"));
        assertTrue(estimate.text().contains(
                "│  └─ Maintenance overhead - 19.90 KiB"));
        assertTrue(estimate.text().contains(
                "max(25% of memory before maintenance = 79.59 KiB,"));
        assertTrue(estimate.text().contains(
                "one delta cache = 4.45 KiB)"));
    }

    @Test
    void estimateFormatsGiBWhenSegmentCacheExceedsOneGiB() {
        final EffectiveIndexConfiguration<Integer, String> configuration = effectiveConfiguration(
                "memory-estimate-segment-cache-gib",
                String.class, new TypeDescriptorShortString(),
                500_000, 200_000, 2, 10, 0);

        final MemoryEstimateReport estimate = IndexMemoryEstimator.estimate(
                configuration, new TypeDescriptorInteger(),
                new TypeDescriptorShortString(), 0);
        printReport("segment-cache-gib", estimate);

        assertEquals(1_425_282_600L,
                estimate.totalEstimatedBytes().orElseThrow());
        assertTrue(estimate.text().contains(
                "├─ Total index memory - 1.33 GiB"));
        assertTrue(estimate.text().contains(
                "│  ├─ All segments - 1.06 GiB"));
        assertTrue(estimate.text().contains(
                "│  │  │  ├─ Delta cache - 108.72 MiB"));
        assertTrue(estimate.text().contains(
                "configured max number of keys in cache: 500,000"));
        assertTrue(estimate.text().contains("key/value entry: 228 B"));
    }

    @Test
    void estimateFormatsGiBWhenChunkStoreCacheExceedsOneGiB() {
        final EffectiveIndexConfiguration<Integer, String> configuration = effectiveConfiguration(
                "memory-estimate-chunk-store-gib",
                String.class, new TypeDescriptorShortString(), 10, 6,
                100, 3, 50_000);

        final MemoryEstimateReport estimate = IndexMemoryEstimator.estimate(
                configuration, new TypeDescriptorInteger(),
                new TypeDescriptorShortString(), 0);
        printReport("chunk-store-gib", estimate);

        assertEquals(1_429_074_220L,
                estimate.totalEstimatedBytes().orElseThrow());
        assertTrue(estimate.text().contains(
                "├─ Total index memory - 1.33 GiB"));
        assertTrue(estimate.text().contains(
                "│  ├─ Chunk-store page cache - 1.06 GiB"));
        assertTrue(estimate.text().contains("50,000 pages"));
        assertTrue(estimate.text().contains("chunk keys per page: 100"));
        assertTrue(estimate.text().contains("page overhead: 64 B"));
        assertTrue(estimate.text().contains("key/value entry: 228 B"));
    }

    @Test
    void estimateReportsIncompleteWhenValueDescriptorSizeIsUnknown() {
        final EffectiveIndexConfiguration<Integer, ByteArray> configuration = effectiveConfiguration(
                "memory-estimate-incomplete",
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
                "│  │  │  ├─ Delta cache - unknown"));
        assertTrue(log.contains(
                "reason: needs key and value descriptor estimates"));
        assertTrue(log.contains("Bloom filter"));
        assertTrue(log.contains("├─ Total index memory - unknown"));
    }

    @Test
    void estimateTreatsZeroValueSizeAsRealEstimate() {
        final EffectiveIndexConfiguration<Integer, NullValue> configuration = effectiveConfiguration(
                "memory-estimate-zero",
                NullValue.class, new TypeDescriptorNull());

        final MemoryEstimateReport estimate = IndexMemoryEstimator.estimate(
                configuration, new TypeDescriptorInteger(),
                new TypeDescriptorNull(), 1);
        printReport("zero-value-size", estimate);

        assertTrue(estimate.isComplete());
        assertTrue(estimate.totalEstimatedBytes().isPresent());
        assertTrue(estimate.text().contains(
                "├─ Key/value entry - 100 B"));
        assertTrue(estimate.text().contains("overhead: 96 B"));
    }

    @Test
    void estimateUsesKeyDescriptorForKeyPositionEntry() {
        final EffectiveIndexConfiguration<String, Integer> configuration = effectiveConfiguration(
                "memory-estimate-string-key",
                String.class, new TypeDescriptorShortString(),
                Integer.class, new TypeDescriptorInteger());

        final MemoryEstimateReport estimate = IndexMemoryEstimator.estimate(
                configuration, new TypeDescriptorShortString(),
                new TypeDescriptorInteger(), 0);
        printReport("string-key", estimate);

        final String log = estimate.text();
        assertTrue(estimate.isComplete());
        assertTrue(log.contains("│  │  │  ├─ Scarce index - 11.13 KiB"));
        assertTrue(log.contains(
                "│  │  │  │  └─ Key/position entry - 228 B"));
        assertLineAppearsBefore(log, "├─ Key/value entry - 228 B",
                "├─ Key/position entry - 228 B");
        assertDetailAlignsWithValue(estimate,
                "├─ Key/position entry - 228 B",
                "key: TypeDescriptorShortString, about 128 B");
        assertTrue(log.contains(
                "integer position: TypeDescriptorInteger, about 4 B"));
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
        return effectiveConfiguration(name, Integer.class,
                new TypeDescriptorInteger(), valueClass, valueTypeDescriptor,
                cacheKeyLimit, maintenanceWriteCacheKeyLimit, chunkKeyLimit,
                cachedSegmentLimit, chunkStorePageLimit);
    }

    private static <K, V> EffectiveIndexConfiguration<K, V> effectiveConfiguration(
            final String name, final Class<K> keyClass,
            final TypeDescriptor<K> keyTypeDescriptor,
            final Class<V> valueClass,
            final TypeDescriptor<V> valueTypeDescriptor) {
        return effectiveConfiguration(name, keyClass, keyTypeDescriptor,
                valueClass, valueTypeDescriptor, 10, 6, 2, 3, 0);
    }

    private static <K, V> EffectiveIndexConfiguration<K, V> effectiveConfiguration(
            final String name, final Class<K> keyClass,
            final TypeDescriptor<K> keyTypeDescriptor,
            final Class<V> valueClass,
            final TypeDescriptor<V> valueTypeDescriptor,
            final int cacheKeyLimit,
            final int maintenanceWriteCacheKeyLimit,
            final int chunkKeyLimit,
            final int cachedSegmentLimit,
            final int chunkStorePageLimit) {
        return EffectiveIndexConfigurationResolver.resolveForCreate(
                IndexConfiguration.<K, V>builder()
                        .identity(identity -> identity.keyClass(keyClass))
                        .identity(identity -> identity.valueClass(valueClass))
                        .identity(identity -> identity
                                .keyTypeDescriptor(keyTypeDescriptor))
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

    private static void assertDetailAlignsWithValue(
            final MemoryEstimateReport report, final String treeLine,
            final String detail) {
        final List<String> lines = report.lines();
        final int lineIndex = lines.indexOf(treeLine);

        assertTrue(lineIndex >= 0, () -> "Missing line: " + treeLine);
        final String detailLine = lines.get(lineIndex + 1);
        final int valueColumn = treeLine.indexOf(" - ") + " - ".length();

        assertEquals(valueColumn, detailLine.indexOf(detail));
        assertTrue(detailLine.substring(0, valueColumn).contains("│"),
                () -> "Missing tree connection before detail: " + detailLine);
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
