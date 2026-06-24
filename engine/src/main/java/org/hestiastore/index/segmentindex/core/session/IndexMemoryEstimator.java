package org.hestiastore.index.segmentindex.core.session;

import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalInt;
import java.util.OptionalLong;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.segmentindex.MemoryEstimateReport;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;

/**
 * Builds a rough heap-pressure estimate from resolved startup configuration.
 */
final class IndexMemoryEstimator {

    static final int ENTRY_OVERHEAD_BYTES = 96;
    static final int PAGE_OVERHEAD_BYTES = 64;
    static final int TREE_MAP_ENTRY_OVERHEAD_BYTES = 56;
    static final int SEGMENT_ID_ESTIMATE_BYTES = 16;
    static final int SCARCE_INDEX_POSITION_BYTES = 4;
    static final int FIXED_SEGMENT_RUNTIME_OVERHEAD_BYTES = 16 * 1024;
    static final int TEMPORARY_MEMORY_MARGIN_PERCENT = 25;

    private static final String LINE_SEPARATOR = System.lineSeparator();
    private static final String UNAVAILABLE = "unavailable";
    private static final String NOTICE =
            "Rough estimate only; not a JVM cap or measured allocation.";
    private static final String RUNTIME_NOTE =
            "Real usage depends on JVM layout, GC, workload, and temporary snapshots.";

    private IndexMemoryEstimator() {
    }

    /**
     * Estimates startup memory pressure for an index configuration.
     *
     * @param <K> key type
     * @param <V> value type
     * @param configuration resolved effective configuration
     * @param keyTypeDescriptor resolved key descriptor
     * @param valueTypeDescriptor resolved value descriptor
     * @param routeCount current number of route-map entries
     * @return memory estimate report
     */
    static <K, V> MemoryEstimateReport estimate(
            final EffectiveIndexConfiguration<K, V> configuration,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final int routeCount) {
        final EffectiveIndexConfiguration<K, V> resolved =
                Vldtn.requireNonNull(configuration, "configuration");
        final TypeDescriptor<K> keyDescriptor = Vldtn.requireNonNull(
                keyTypeDescriptor, "keyTypeDescriptor");
        final TypeDescriptor<V> valueDescriptor = Vldtn.requireNonNull(
                valueTypeDescriptor, "valueTypeDescriptor");
        final int resolvedRouteCount = Vldtn
                .requireGreaterThanOrEqualToZero(routeCount, "routeCount");

        final OptionalInt keyEstimate =
                keyDescriptor.getEstimatedAverageSizeInBytes();
        final OptionalInt valueEstimate =
                valueDescriptor.getEstimatedAverageSizeInBytes();
        final OptionalLong entryEstimate = entryEstimate(keyEstimate,
                valueEstimate);
        final OptionalLong scarceEntryEstimate =
                scarceEntryEstimate(keyEstimate);
        final OptionalLong oneLoadedSegmentCache =
                estimateOneLoadedSegmentCache(resolved, entryEstimate);
        final OptionalLong loadedSegmentCaches =
                estimateLoadedSegmentCaches(resolved, entryEstimate);
        final OptionalLong chunkStoreCache =
                estimateChunkStoreCache(resolved, entryEstimate);
        final OptionalLong routeMap =
                estimateRouteMap(resolvedRouteCount, keyEstimate);
        final OptionalLong bloomFilters = OptionalLong
                .of(estimateBloomFilters(resolved));
        final OptionalLong scarceIndexes =
                estimateScarceIndexes(resolved, keyEstimate);
        final OptionalLong segmentInfrastructure = OptionalLong
                .of(estimateSegmentInfrastructure(resolved));

        long steadyStateBytes = 0L;
        steadyStateBytes = addIfPresent(steadyStateBytes,
                loadedSegmentCaches);
        steadyStateBytes = addIfPresent(steadyStateBytes, chunkStoreCache);
        steadyStateBytes = addIfPresent(steadyStateBytes, routeMap);
        steadyStateBytes = addIfPresent(steadyStateBytes, bloomFilters);
        steadyStateBytes = addIfPresent(steadyStateBytes, scarceIndexes);
        steadyStateBytes = addIfPresent(steadyStateBytes,
                segmentInfrastructure);

        final boolean complete = entryEstimate.isPresent()
                && loadedSegmentCaches.isPresent()
                && chunkStoreCache.isPresent()
                && routeMap.isPresent()
                && scarceIndexes.isPresent();
        final OptionalLong steadyState =
                complete ? OptionalLong.of(steadyStateBytes)
                        : OptionalLong.empty();
        final OptionalLong temporaryMemoryMargin = estimateTemporaryMemoryMargin(
                steadyState, oneLoadedSegmentCache);
        final OptionalLong total = total(steadyState, temporaryMemoryMargin);
        final List<Map.Entry<String, OptionalLong>> steadyAreas = List.of(
                Map.entry("loaded segment cache", loadedSegmentCaches),
                Map.entry("chunk-store cache", chunkStoreCache),
                Map.entry("route map", routeMap),
                Map.entry("Bloom filters if loaded", bloomFilters),
                Map.entry("scarce indexes if loaded", scarceIndexes),
                Map.entry("loaded segment infrastructure",
                        segmentInfrastructure));

        final StringBuilder out = new StringBuilder();
        appendTitle(out);
        appendSummary(out, steadyState, temporaryMemoryMargin, total);
        appendNotes(out);
        appendLargestAreas(out, steadyAreas);
        appendAssumptions(out, keyDescriptor, valueDescriptor, keyEstimate,
                valueEstimate, entryEstimate);
        appendConfiguration(out, resolved, resolvedRouteCount);
        appendReportedButNotIncluded(out, resolved);
        out.append("Estimated memory by area:").append(LINE_SEPARATOR);
        appendLine(out, "loaded segment cache", loadedSegmentCaches,
                "needs key and value descriptor estimates",
                String.format(Locale.ROOT,
                        "inputs: %d segments, %d read keys, %d maintenance keys, entry %s",
                        resolved.segment().cachedSegmentLimit(),
                        resolved.segment().cacheKeyLimit(),
                        resolved.writePath()
                                .segmentWriteCacheKeyLimitDuringMaintenance(),
                        estimateText(entryEstimate)));

        appendLine(out, "chunk-store cache", chunkStoreCache,
                "needs key and value descriptor estimates",
                String.format(Locale.ROOT,
                        "inputs: %d pages, %d chunk keys, page overhead %s, entry %s",
                        resolved.chunkStoreCache().pageLimit(),
                        resolved.segment().chunkKeyLimit(),
                        formatBytes(PAGE_OVERHEAD_BYTES),
                        estimateText(entryEstimate)));

        appendLine(out, "route map", routeMap,
                "needs key descriptor estimate",
                String.format(Locale.ROOT,
                        "inputs: %d routes, key %s, segment id %s, tree entry %s",
                        resolvedRouteCount, estimateText(keyEstimate),
                        formatBytes(SEGMENT_ID_ESTIMATE_BYTES),
                        formatBytes(TREE_MAP_ENTRY_OVERHEAD_BYTES)));

        appendLine(out, "Bloom filters if loaded", bloomFilters, "",
                String.format(Locale.ROOT, "inputs: %d segments, filter %s",
                        resolved.segment().cachedSegmentLimit(),
                        formatBytes(resolved.bloomFilter()
                                .indexSizeBytes())));

        appendLine(out, "scarce indexes if loaded", scarceIndexes,
                "needs key descriptor estimate",
                String.format(Locale.ROOT,
                        "inputs: %d segments, %d scarce entries each, scarce entry %s",
                        resolved.segment().cachedSegmentLimit(),
                        ceilDiv(resolved.segment().maxKeys(),
                                resolved.segment().chunkKeyLimit()),
                        estimateText(scarceEntryEstimate)));

        appendLine(out, "loaded segment infrastructure",
                segmentInfrastructure, "",
                String.format(Locale.ROOT, "inputs: %d segments, overhead %s",
                        resolved.segment().cachedSegmentLimit(),
                        formatBytes(FIXED_SEGMENT_RUNTIME_OVERHEAD_BYTES)));

        appendLine(out, "temporary memory margin",
                temporaryMemoryMargin,
                "needs complete steady-state estimate and entry estimate",
                String.format(Locale.ROOT,
                        "inputs: max(%d%% of steady state = %s, one segment cache = %s)",
                        TEMPORARY_MEMORY_MARGIN_PERCENT,
                        memoryText(steadyState),
                        memoryText(oneLoadedSegmentCache)));

        out.append("End memory estimate").append(LINE_SEPARATOR);
        return new MemoryEstimateReport(out.toString().lines().toList(),
                total.isPresent(), total);
    }

    private static void appendTitle(final StringBuilder out) {
        out.append("Estimated memory use at startup").append(LINE_SEPARATOR);
    }

    private static void appendSummary(final StringBuilder out,
            final OptionalLong steadyState,
            final OptionalLong temporaryMemoryMargin,
            final OptionalLong total) {
        out.append("Estimated active heap: ")
                .append(memoryText(total)).append(LINE_SEPARATOR);
        appendIndented(out, "steady state: " + memoryText(steadyState)
                + " (memory expected after startup caches are loaded)");
        appendIndented(out, "temporary margin: "
                + memoryText(temporaryMemoryMargin)
                + " (extra headroom for short-lived objects and snapshots)");
    }

    private static void appendNotes(final StringBuilder out) {
        out.append("Notes:").append(LINE_SEPARATOR);
        appendIndented(out, NOTICE);
        appendIndented(out, RUNTIME_NOTE);
        appendIndented(out, "details: docs/operations/memory-estimate.md");
    }

    private static void appendLargestAreas(final StringBuilder out,
            final List<Map.Entry<String, OptionalLong>> steadyAreas) {
        out.append("Largest steady-state areas:").append(LINE_SEPARATOR);
        final List<Map.Entry<String, OptionalLong>> availableAreas = steadyAreas
                .stream()
                .filter(area -> area.getValue().isPresent()
                        && area.getValue().getAsLong() > 0L)
                .sorted(Comparator
                        .comparingLong((Map.Entry<String, OptionalLong> area) ->
                                area.getValue().getAsLong())
                        .reversed())
                .limit(3L)
                .toList();
        if (availableAreas.isEmpty()) {
            appendIndented(out, UNAVAILABLE);
            return;
        }
        availableAreas.forEach(area -> appendIndented(out,
                area.getKey() + ": " + memoryText(area.getValue())));
    }

    private static <K, V> void appendAssumptions(final StringBuilder out,
            final TypeDescriptor<K> keyDescriptor,
            final TypeDescriptor<V> valueDescriptor,
            final OptionalInt keyEstimate, final OptionalInt valueEstimate,
            final OptionalLong entryEstimate) {
        out.append("Per-entry size assumptions:").append(LINE_SEPARATOR);
        appendIndented(out, "key: " + descriptorName(keyDescriptor) + ", "
                + aboutText(keyEstimate));
        appendIndented(out, "value: " + descriptorName(valueDescriptor) + ", "
                + aboutText(valueEstimate));
        appendIndented(out, "entry: " + aboutText(entryEstimate)
                + " (key + value + overhead)");
        out.append("Estimator assumptions:").append(LINE_SEPARATOR);
        appendIndented(out, "fixed per loaded/cached segment: about "
                + formatBytes(FIXED_SEGMENT_RUNTIME_OVERHEAD_BYTES));
        appendIndented(out, "temporary margin: max("
                + TEMPORARY_MEMORY_MARGIN_PERCENT
                + "% of steady state, one segment cache)");
    }

    private static void appendConfiguration(final StringBuilder out,
            final EffectiveIndexConfiguration<?, ?> resolved,
            final int routeCount) {
        out.append("Configuration used for this estimate:")
                .append(LINE_SEPARATOR);
        appendIndented(out, "cached segments="
                + resolved.segment().cachedSegmentLimit()
                + ", segment cache keys="
                + resolved.segment().cacheKeyLimit()
                + ", maintenance keys="
                + resolved.writePath()
                        .segmentWriteCacheKeyLimitDuringMaintenance());
        appendIndented(out, "max keys per segment="
                + resolved.segment().maxKeys());
        appendIndented(out, "chunk key limit="
                + resolved.segment().chunkKeyLimit()
                + ", chunk-store pages="
                + resolved.chunkStoreCache().pageLimit()
                + ", Bloom filter="
                + formatBytes(resolved.bloomFilter().indexSizeBytes()));
        appendIndented(out, "route map entries=" + routeCount);
    }

    private static void appendReportedButNotIncluded(final StringBuilder out,
            final EffectiveIndexConfiguration<?, ?> resolved) {
        out.append("Reported but not included:").append(LINE_SEPARATOR);
        appendIndented(out, "index write-buffer keys: "
                + resolved.writePath().indexBufferedWriteKeyLimit());
    }

    private static void appendLine(final StringBuilder out,
            final String label, final OptionalLong estimateBytes,
            final String unavailableReason, final String... details) {
        out.append("  ").append(label).append(": ")
                .append(memoryText(estimateBytes)).append(LINE_SEPARATOR);
        if (estimateBytes.isEmpty() && !unavailableReason.isBlank()) {
            appendDetail(out, "reason: " + unavailableReason);
        }
        for (final String detail : details) {
            appendDetail(out, detail);
        }
    }

    private static OptionalLong entryEstimate(final OptionalInt keyEstimate,
            final OptionalInt valueEstimate) {
        if (keyEstimate.isEmpty() || valueEstimate.isEmpty()) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(add(add(keyEstimate.getAsInt(),
                valueEstimate.getAsInt()), ENTRY_OVERHEAD_BYTES));
    }

    private static OptionalLong scarceEntryEstimate(
            final OptionalInt keyEstimate) {
        if (keyEstimate.isEmpty()) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(add(
                add(keyEstimate.getAsInt(), SCARCE_INDEX_POSITION_BYTES),
                ENTRY_OVERHEAD_BYTES));
    }

    private static OptionalLong estimateLoadedSegmentCaches(
            final EffectiveIndexConfiguration<?, ?> resolved,
            final OptionalLong entryEstimate) {
        if (entryEstimate.isEmpty()) {
            return OptionalLong.empty();
        }
        final long perSegmentKeys = add(resolved.segment().cacheKeyLimit(),
                resolved.writePath()
                        .segmentWriteCacheKeyLimitDuringMaintenance());
        return OptionalLong.of(multiply(
                multiply(resolved.segment().cachedSegmentLimit(),
                        perSegmentKeys),
                entryEstimate.getAsLong()));
    }

    private static OptionalLong estimateOneLoadedSegmentCache(
            final EffectiveIndexConfiguration<?, ?> resolved,
            final OptionalLong entryEstimate) {
        if (entryEstimate.isEmpty()) {
            return OptionalLong.empty();
        }
        final long perSegmentKeys = add(resolved.segment().cacheKeyLimit(),
                resolved.writePath()
                        .segmentWriteCacheKeyLimitDuringMaintenance());
        return OptionalLong.of(multiply(perSegmentKeys,
                entryEstimate.getAsLong()));
    }

    private static OptionalLong estimateChunkStoreCache(
            final EffectiveIndexConfiguration<?, ?> resolved,
            final OptionalLong entryEstimate) {
        if (entryEstimate.isEmpty()) {
            return OptionalLong.empty();
        }
        final long pageBytes = add(PAGE_OVERHEAD_BYTES,
                multiply(resolved.segment().chunkKeyLimit(),
                        entryEstimate.getAsLong()));
        return OptionalLong.of(multiply(resolved.chunkStoreCache().pageLimit(),
                pageBytes));
    }

    private static OptionalLong estimateRouteMap(final int routeCount,
            final OptionalInt keyEstimate) {
        if (keyEstimate.isEmpty()) {
            return OptionalLong.empty();
        }
        final long perEntryBytes = add(
                add(keyEstimate.getAsInt(), SEGMENT_ID_ESTIMATE_BYTES),
                TREE_MAP_ENTRY_OVERHEAD_BYTES);
        return OptionalLong.of(multiply(multiply(routeCount, 2L),
                perEntryBytes));
    }

    private static long estimateBloomFilters(
            final EffectiveIndexConfiguration<?, ?> resolved) {
        return multiply(resolved.segment().cachedSegmentLimit(),
                resolved.bloomFilter().indexSizeBytes());
    }

    private static OptionalLong estimateScarceIndexes(
            final EffectiveIndexConfiguration<?, ?> resolved,
            final OptionalInt keyEstimate) {
        if (keyEstimate.isEmpty()) {
            return OptionalLong.empty();
        }
        final long scarceKeysPerSegment = ceilDiv(resolved.segment().maxKeys(),
                resolved.segment().chunkKeyLimit());
        final long scarceEntryBytes = add(
                add(keyEstimate.getAsInt(), SCARCE_INDEX_POSITION_BYTES),
                ENTRY_OVERHEAD_BYTES);
        return OptionalLong.of(multiply(
                multiply(resolved.segment().cachedSegmentLimit(),
                        scarceKeysPerSegment),
                scarceEntryBytes));
    }

    private static long estimateSegmentInfrastructure(
            final EffectiveIndexConfiguration<?, ?> resolved) {
        return multiply(resolved.segment().cachedSegmentLimit(),
                FIXED_SEGMENT_RUNTIME_OVERHEAD_BYTES);
    }

    private static OptionalLong estimateTemporaryMemoryMargin(
            final OptionalLong steadyState,
            final OptionalLong oneLoadedSegmentCache) {
        if (steadyState.isEmpty() || oneLoadedSegmentCache.isEmpty()) {
            return OptionalLong.empty();
        }
        final long percentageHeadroom = multiply(steadyState.getAsLong(),
                TEMPORARY_MEMORY_MARGIN_PERCENT) / 100L;
        return OptionalLong.of(Math.max(percentageHeadroom,
                oneLoadedSegmentCache.getAsLong()));
    }

    private static OptionalLong total(final OptionalLong steadyState,
            final OptionalLong transientHeadroom) {
        if (steadyState.isEmpty() || transientHeadroom.isEmpty()) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(add(steadyState.getAsLong(),
                transientHeadroom.getAsLong()));
    }

    private static long addIfPresent(final long total,
            final OptionalLong value) {
        return value.isPresent() ? add(total, value.getAsLong()) : total;
    }

    private static long ceilDiv(final long dividend, final long divisor) {
        return (dividend + divisor - 1L) / divisor;
    }

    private static long add(final long left, final long right) {
        if (Long.MAX_VALUE - left < right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    private static long multiply(final long left, final long right) {
        if (left == 0L || right == 0L) {
            return 0L;
        }
        if (left > Long.MAX_VALUE / right) {
            return Long.MAX_VALUE;
        }
        return left * right;
    }

    private static String estimateText(final OptionalInt estimate) {
        return estimate.isPresent() ? formatBytes(estimate.getAsInt())
                : "unknown";
    }

    private static String estimateText(final OptionalLong estimate) {
        return estimate.isPresent() ? formatBytes(estimate.getAsLong())
                : UNAVAILABLE;
    }

    private static String memoryText(final OptionalLong estimate) {
        return estimate.isPresent() ? formatBytes(estimate.getAsLong())
                : UNAVAILABLE;
    }

    private static String aboutText(final OptionalInt estimate) {
        return estimate.isPresent()
                ? "about " + formatBytes(estimate.getAsInt())
                : "unknown";
    }

    private static String aboutText(final OptionalLong estimate) {
        return estimate.isPresent()
                ? "about " + formatBytes(estimate.getAsLong())
                : "unknown";
    }

    private static void appendIndented(final StringBuilder out,
            final String line) {
        out.append("  ").append(line).append(LINE_SEPARATOR);
    }

    private static void appendDetail(final StringBuilder out,
            final String line) {
        out.append("    ").append(line).append(LINE_SEPARATOR);
    }

    private static String descriptorName(final TypeDescriptor<?> descriptor) {
        final String simpleName = descriptor.getClass().getSimpleName();
        if (simpleName.length() <= 48) {
            return simpleName;
        }
        return simpleName.substring(0, 45) + "...";
    }

    private static String formatBytes(final long bytes) {
        if (bytes < 1024L) {
            return bytes + " B";
        }
        final String[] units = {"KiB", "MiB", "GiB", "TiB", "PiB", "EiB"};
        double value = bytes;
        int unitIndex = -1;
        while (value >= 1024D && unitIndex < units.length - 1) {
            value = value / 1024D;
            unitIndex++;
        }
        return String.format(Locale.ROOT, "%.2f %s", value,
                units[unitIndex]);
    }

}
