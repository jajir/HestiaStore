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
        assertContains(log, "Estimated memory use at startup");
        assertContains(log, "Included memory");
        assertContains(log,
                "+------------------------+------------+--------------------------------------+");
        assertContains(log,
                "| Total index memory     | 91.14 KiB  | segments + page cache + maintenance  |");
        assertContains(log,
                "| All segments           | 72.91 KiB  | cached segment slots: 3; cached      |");
        assertContains(log,
                "| One cached segment     | 24.30 KiB  | delta cache + bloom filter + scarce  |");
        assertContains(log,
                "| Delta cache            | 2.23 KiB   | cache key limit: 10; cache key limit |");
        assertContains(log,
                "| Bloom filter           | 1.00 KiB   | configured bloom filter size: 1.00   |");
        assertContains(log,
                "| Scarce index           | 5.08 KiB   | max scarce keys: 50; max scarce keys |");
        assertContains(log,
                "| Segment runtime        | 16.00 KiB  | fixed overhead per cached segment    |");
        assertContains(log,
                "| Maintenance overhead   | 18.23 KiB  | max(25% of memory before maintenance |");
        assertLineAppearsBefore(log, "| Total index memory",
                "Entry sizes");
        assertContains(log,
                "not a JVM cap or measured allocation");
        assertContains(log,
                "| Key/value entry        | 228 B      | key size + value size + overhead;    |");
        assertLineAppearsBefore(log, "| Key/value entry        | 228 B",
                "| Key/position entry     | 104 B");
        assertContains(log,
                "| Key/position entry     | 104 B      | key size + integer position +        |");
        assertContains(log, "overhead: 96 B");
        assertContains(log,
                "| Entry overhead         | 96 B       | fixed overhead in key/value and      |");
        assertContains(log,
                "details: docs/operations/memory-estimate.md");
        assertContains(log,
                "| Max scarce keys        | 50         | ceil(max segment keys / chunk keys   |");
        assertContains(log, "per page)");
        assertContains(log,
                "key: TypeDescriptorInteger, about 4");
        assertContains(log,
                "integer position:");
        assertContains(log, "Write-buffer keys");
        assertContains(log, "reported, not included in total");
        assertContains(log, "Chunk keys per page");
        assertContains(log,
                "configured bloom filter size: 1.00");
        assertContains(log, "key/value entry 228 B");
        assertFalse(log.contains(
                "entry overhead: 96 B inside key/value entry"));
        assertContains(log, "End memory estimate");
        assertFalse(log.contains("├─"));
        assertFalse(log.contains("│"));
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
        assertContains(estimate.text(),
                "| Chunk-store page cache | 3.55 KiB   | pages: 7; pages * (page overhead +   |");
        assertContains(estimate.text(),
                "| Chunk-store pages      | 7          | configured page cache size           |");
        assertContains(estimate.text(),
                "| Chunk keys per page    | 2          | keys stored in one chunk-store page  |");
        assertContains(estimate.text(),
                "| Page overhead          | 64 B       | fixed overhead per chunk-store page  |");
        assertContains(estimate.text(),
                "| Key/value entry        | 228 B");
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
        assertContains(estimate.text(),
                "| All segments           | 121.52 KiB | cached segment slots: 5; cached      |");
        assertContains(estimate.text(),
                "| One cached segment     | 24.30 KiB");
        assertContains(estimate.text(),
                "| Cached segment slots   | 5          | multiplier for one cached segment    |");
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
        assertContains(estimate.text(),
                "| Segment routing map    | 80 B       | route count: 10; route count *       |");
        assertContains(estimate.text(),
                "| Segment routes         | 10         | route-map entries                    |");
        assertContains(estimate.text(),
                "| Key/segment-id entry   | 8 B        | key size + segment id size; key: 4   |");
        assertContains(estimate.text(), "B; segment id: 4 B");
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
        assertContains(estimate.text(),
                "| Delta cache            | 4.45 KiB   | cache key limit: 20; cache key limit |");
        assertContains(estimate.text(), "key/value entry 228 B");
        assertContains(estimate.text(),
                "| Maintenance overhead   | 19.90 KiB  | max(25% of memory before maintenance |");
        assertContains(estimate.text(),
                "= 79.59 KiB, one delta cache = 4.45");
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
        assertContains(estimate.text(),
                "| Total index memory     | 1.33 GiB");
        assertContains(estimate.text(),
                "| All segments           | 1.06 GiB");
        assertContains(estimate.text(),
                "| Delta cache            | 108.72 MiB");
        assertContains(estimate.text(),
                "cache key limit: 500,000");
        assertContains(estimate.text(), "key/value entry 228 B");
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
        assertContains(estimate.text(),
                "| Total index memory     | 1.33 GiB");
        assertContains(estimate.text(),
                "| Chunk-store page cache | 1.06 GiB");
        assertContains(estimate.text(),
                "| Chunk-store pages      | 50,000");
        assertContains(estimate.text(),
                "| Chunk keys per page    | 100");
        assertContains(estimate.text(),
                "| Page overhead          | 64 B");
        assertContains(estimate.text(),
                "| Key/value entry        | 228 B");
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
        assertContains(log, "value: TypeDescriptorByteArray,");
        assertContains(log,
                "| Delta cache            | unknown    | reason: needs key and value          |");
        assertContains(log,
                "reason: needs key and value");
        assertContains(log, "Bloom filter");
        assertContains(log,
                "| Total index memory     | unknown    | reason: needs segment, page-cache,   |");
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
        assertContains(estimate.text(),
                "| Key/value entry        | 100 B");
        assertContains(estimate.text(), "overhead: 96 B");
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
        assertContains(log, "| Scarce index           | 11.13 KiB");
        assertContains(log, "| Key/position entry     | 228 B");
        assertLineAppearsBefore(log, "| Key/value entry        | 228 B",
                "| Key/position entry     | 228 B");
        assertContains(log, "TypeDescriptorShortString, about 128");
        assertContains(log, "integer position:");
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
        report.lines().forEach(line -> assertTrue(line.length() <= 80,
                () -> "Report line is too long: " + line));
    }

    private static void assertContains(final String log,
            final String expected) {
        assertTrue(log.contains(expected), () -> "Missing text: " + expected);
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
