package org.hestiastore.index.segmentindex.core.session;

import java.util.Locale;
import java.util.OptionalInt;
import java.util.OptionalLong;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.segmentindex.MemoryEstimateReport;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;

/**
 * Builds a rough memory-pressure estimate from resolved startup configuration.
 */
final class IndexMemoryEstimator {

    static final int ENTRY_OVERHEAD_BYTES = 96;
    static final int PAGE_OVERHEAD_BYTES = 64;
    static final int SEGMENT_ID_ESTIMATE_BYTES = 4;
    static final int SCARCE_INDEX_POSITION_BYTES = 4;
    static final int FIXED_SEGMENT_RUNTIME_OVERHEAD_BYTES = 16 * 1024;
    static final int TEMPORARY_MEMORY_MARGIN_PERCENT = 25;

    private static final String LINE_SEPARATOR = System.lineSeparator();
    private static final String UNKNOWN = "unknown";

    private IndexMemoryEstimator() {
    }

    /**
     * Estimates startup memory pressure for an index configuration.
     *
     * @param <K>                 key type
     * @param <V>                 value type
     * @param configuration       resolved effective configuration
     * @param keyTypeDescriptor   resolved key descriptor
     * @param valueTypeDescriptor resolved value descriptor
     * @param routeCount          current number of route-map entries
     * @return memory estimate report
     */
    static <K, V> MemoryEstimateReport estimate(
            final EffectiveIndexConfiguration<K, V> configuration,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final int routeCount) {
        final EffectiveIndexConfiguration<K, V> resolved = Vldtn.requireNonNull(configuration, "configuration");
        final TypeDescriptor<K> keyDescriptor = Vldtn.requireNonNull(
                keyTypeDescriptor, "keyTypeDescriptor");
        final TypeDescriptor<V> valueDescriptor = Vldtn.requireNonNull(
                valueTypeDescriptor, "valueTypeDescriptor");
        final int resolvedRouteCount = Vldtn
                .requireGreaterThanOrEqualToZero(routeCount, "routeCount");

        final OptionalInt keyEstimate = keyDescriptor.getEstimatedAverageSizeInBytes();
        final OptionalInt valueEstimate = valueDescriptor.getEstimatedAverageSizeInBytes();
        final long maxScarceIndexKeys = maxScarceIndexKeys(resolved);
        final OptionalLong entryEstimate = entryEstimate(keyEstimate,
                valueEstimate);
        final OptionalLong scarceEntryEstimate = scarceEntryEstimate(keyEstimate);
        final OptionalLong routeEntryEstimate = routeMapEntryEstimate(keyEstimate);
        final OptionalLong oneDeltaCache = estimateOneDeltaCache(resolved, entryEstimate);
        final OptionalLong deltaCaches = estimateDeltaCaches(resolved, entryEstimate);
        final OptionalLong chunkStoreCache = estimateChunkStoreCache(resolved, entryEstimate);
        final OptionalLong routeMap = estimateRouteMap(resolvedRouteCount,
                routeEntryEstimate);
        final OptionalLong oneBloomFilter = OptionalLong.of(resolved.bloomFilter().indexSizeBytes());
        final OptionalLong bloomFilters = OptionalLong
                .of(estimateBloomFilters(resolved));
        final OptionalLong oneScarceIndex = estimateOneScarceIndex(resolved, keyEstimate);
        final OptionalLong scarceIndexes = estimateScarceIndexes(resolved, keyEstimate);
        final OptionalLong oneSegmentInfrastructure = OptionalLong.of(FIXED_SEGMENT_RUNTIME_OVERHEAD_BYTES);
        final OptionalLong segmentInfrastructure = OptionalLong
                .of(estimateSegmentInfrastructure(resolved));
        final OptionalLong oneCachedSegment = sumEstimates(
                oneDeltaCache, oneBloomFilter, oneScarceIndex,
                oneSegmentInfrastructure);
        final OptionalLong loadedSegments = sumEstimates(deltaCaches,
                bloomFilters, scarceIndexes, segmentInfrastructure);
        final OptionalLong allSegments = sumEstimates(loadedSegments,
                routeMap);

        long steadyStateBytes = 0L;
        steadyStateBytes = addIfPresent(steadyStateBytes, allSegments);
        steadyStateBytes = addIfPresent(steadyStateBytes, chunkStoreCache);

        final boolean complete = entryEstimate.isPresent()
                && allSegments.isPresent()
                && chunkStoreCache.isPresent()
                && routeMap.isPresent()
                && scarceIndexes.isPresent();
        final OptionalLong steadyState = complete ? OptionalLong.of(steadyStateBytes)
                : OptionalLong.empty();
        final OptionalLong temporaryMemoryMargin = estimateTemporaryMemoryMargin(
                steadyState, oneDeltaCache);
        final OptionalLong total = total(steadyState, temporaryMemoryMargin);

        final StringBuilder out = new StringBuilder();
        appendTitle(out);
        appendParentEstimateTreeLine(out, "", "├─ ", "Total index memory",
                total,
                "needs segment, page-cache, and maintenance estimates");
        appendParentEstimateTreeLine(out, "│  ", "├─ ", "All segments",
                allSegments, "needs complete segment and routing estimates",
                formatCount(resolved.segment().cachedSegmentLimit())
                        + " segment cache slots");
        appendParentEstimateTreeLine(out, "│  │  ", "├─ ",
                "One cached segment", oneCachedSegment,
                "needs key and value descriptor estimates",
                "multiplied by "
                        + formatCount(resolved.segment().cachedSegmentLimit())
                        + " segment cache slots");
        appendEstimateTreeLine(out, "│  │  │  ", "├─ ", "Delta cache",
                oneDeltaCache,
                "needs key and value descriptor estimates",
                "configured max number of keys in cache: "
                        + formatCount(resolved.segment().cacheKeyLimit()),
                "key/value entry: " + estimateText(entryEstimate));
        appendEstimateTreeLine(out, "│  │  │  ", "├─ ",
                "Bloom filter", oneBloomFilter, "",
                "configured bloom filter size: " + formatBytes(
                        resolved.bloomFilter().indexSizeBytes()));
        appendParentEstimateTreeLine(out, "│  │  │  ", "├─ ",
                "Scarce index", oneScarceIndex,
                "needs key descriptor estimate");
        appendTreeLine(out, "│  │  │  │  ", "├─ ",
                "Max number of keys in scarce index",
                formatCount(maxScarceIndexKeys),
                "max number of keys in segment: "
                        + formatCount(resolved.segment().maxKeys()),
                "number of keys per page: "
                        + formatCount(resolved.segment().chunkKeyLimit()));
        appendTreeLine(out, "│  │  │  │  ", "└─ ",
                "Key/position entry", estimateText(scarceEntryEstimate));
        appendEstimateTreeLine(out, "│  │  │  ", "└─ ", "Segment runtime",
                oneSegmentInfrastructure, "",
                "overhead: "
                        + formatBytes(FIXED_SEGMENT_RUNTIME_OVERHEAD_BYTES));
        appendParentEstimateTreeLine(out, "│  │  ", "└─ ", "Segment routing map",
                routeMap, "needs key descriptor estimate");
        appendTreeLine(out, "│  │     ", "├─ ",
                "Number of segment routes", formatCount(resolvedRouteCount));
        appendTreeLine(out, "│  │     ", "└─ ",
                "Key/segment-id entry", estimateText(routeEntryEstimate),
                "key: " + estimateText(keyEstimate),
                "segment id: " + formatBytes(SEGMENT_ID_ESTIMATE_BYTES));
        appendEstimateTreeLine(out, "│  ", "├─ ",
                "Chunk-store page cache", chunkStoreCache,
                "needs key and value descriptor estimates",
                formatCount(resolved.chunkStoreCache().pageLimit())
                        + " pages",
                "chunk keys per page: "
                        + formatCount(resolved.segment().chunkKeyLimit()),
                "page overhead: " + formatBytes(PAGE_OVERHEAD_BYTES),
                "key/value entry: " + estimateText(entryEstimate));
        appendEstimateTreeLine(out, "│  ", "└─ ", "Maintenance overhead",
                temporaryMemoryMargin,
                "needs memory before maintenance and key/value entry estimates",
                "maintenance threads: "
                        + formatCount(resolved.maintenance().indexThreads()),
                "max(" + TEMPORARY_MEMORY_MARGIN_PERCENT
                        + "% of memory before maintenance = "
                        + memoryText(steadyState) + ",",
                "one delta cache = "
                        + memoryText(oneDeltaCache) + ")");
        appendTreeLine(out, "", "├─ ", "Key/value entry",
                estimateText(entryEstimate),
                "key: " + descriptorName(keyDescriptor) + ", "
                        + aboutText(keyEstimate),
                "value: " + descriptorName(valueDescriptor) + ", "
                        + aboutText(valueEstimate),
                "overhead: " + formatBytes(ENTRY_OVERHEAD_BYTES));
        appendTreeLine(out, "", "├─ ", "Key/position entry",
                estimateText(scarceEntryEstimate),
                "key: " + descriptorName(keyDescriptor) + ", "
                        + aboutText(keyEstimate),
                "integer position: TypeDescriptorInteger, "
                        + "about " + formatBytes(SCARCE_INDEX_POSITION_BYTES),
                "overhead: " + formatBytes(ENTRY_OVERHEAD_BYTES));
        appendTreeLine(out, "", "├─ ", "Formula constants",
                "shown for transparency, not added separately",
                "fixed per loaded/cached segment: about "
                        + formatBytes(FIXED_SEGMENT_RUNTIME_OVERHEAD_BYTES),
                "chunk-store page overhead: "
                        + formatBytes(PAGE_OVERHEAD_BYTES),
                "route-map segment id: "
                        + formatBytes(SEGMENT_ID_ESTIMATE_BYTES),
                "maintenance overhead: max("
                        + TEMPORARY_MEMORY_MARGIN_PERCENT
                        + "% of memory before maintenance,",
                "one delta cache)");
        appendTreeLine(out, "", "├─ ", "Other requirements",
                "reported but not included",
                "index write-buffer keys: " + formatCount(
                        resolved.writePath().indexBufferedWriteKeyLimit()));
        appendTreeLine(out, "", "└─ ", "Notes", "rough estimate only",
                "not a JVM cap or measured allocation",
                "usage depends on JVM layout, GC, workload, and snapshots",
                "details: docs/operations/memory-estimate.md");
        out.append("End memory estimate").append(LINE_SEPARATOR);
        return new MemoryEstimateReport(out.toString().lines().toList(),
                total.isPresent(), total);
    }

    private static void appendTitle(final StringBuilder out) {
        out.append("Estimated memory use at startup").append(LINE_SEPARATOR);
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

    private static OptionalLong routeMapEntryEstimate(
            final OptionalInt keyEstimate) {
        if (keyEstimate.isEmpty()) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(add(keyEstimate.getAsInt(),
                SEGMENT_ID_ESTIMATE_BYTES));
    }

    private static OptionalLong estimateDeltaCaches(
            final EffectiveIndexConfiguration<?, ?> resolved,
            final OptionalLong entryEstimate) {
        if (entryEstimate.isEmpty()) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(multiply(
                multiply(resolved.segment().cachedSegmentLimit(),
                        resolved.segment().cacheKeyLimit()),
                entryEstimate.getAsLong()));
    }

    private static OptionalLong estimateOneDeltaCache(
            final EffectiveIndexConfiguration<?, ?> resolved,
            final OptionalLong entryEstimate) {
        if (entryEstimate.isEmpty()) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(multiply(resolved.segment().cacheKeyLimit(),
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
            final OptionalLong routeEntryEstimate) {
        if (routeEntryEstimate.isEmpty()) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(multiply(routeCount,
                routeEntryEstimate.getAsLong()));
    }

    private static long estimateBloomFilters(
            final EffectiveIndexConfiguration<?, ?> resolved) {
        return multiply(resolved.segment().cachedSegmentLimit(),
                resolved.bloomFilter().indexSizeBytes());
    }

    private static OptionalLong estimateOneScarceIndex(
            final EffectiveIndexConfiguration<?, ?> resolved,
            final OptionalInt keyEstimate) {
        if (keyEstimate.isEmpty()) {
            return OptionalLong.empty();
        }
        final long scarceKeysPerSegment = maxScarceIndexKeys(resolved);
        final long scarceEntryBytes = add(
                add(keyEstimate.getAsInt(), SCARCE_INDEX_POSITION_BYTES),
                ENTRY_OVERHEAD_BYTES);
        return OptionalLong.of(multiply(scarceKeysPerSegment,
                scarceEntryBytes));
    }

    private static OptionalLong estimateScarceIndexes(
            final EffectiveIndexConfiguration<?, ?> resolved,
            final OptionalInt keyEstimate) {
        final OptionalLong oneScarceIndex = estimateOneScarceIndex(resolved, keyEstimate);
        if (oneScarceIndex.isEmpty()) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(multiply(
                resolved.segment().cachedSegmentLimit(),
                oneScarceIndex.getAsLong()));
    }

    private static long maxScarceIndexKeys(
            final EffectiveIndexConfiguration<?, ?> resolved) {
        return ceilDiv(resolved.segment().maxKeys(),
                resolved.segment().chunkKeyLimit());
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

    private static OptionalLong sumEstimates(final OptionalLong... estimates) {
        long sum = 0L;
        for (final OptionalLong estimate : estimates) {
            if (estimate.isEmpty()) {
                return OptionalLong.empty();
            }
            sum = add(sum, estimate.getAsLong());
        }
        return OptionalLong.of(sum);
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
                : UNKNOWN;
    }

    private static String estimateText(final OptionalLong estimate) {
        return estimate.isPresent() ? formatBytes(estimate.getAsLong())
                : UNKNOWN;
    }

    private static String memoryText(final OptionalLong estimate) {
        return estimate.isPresent() ? formatBytes(estimate.getAsLong())
                : UNKNOWN;
    }

    private static String aboutText(final OptionalInt estimate) {
        return estimate.isPresent()
                ? "about " + formatBytes(estimate.getAsInt())
                : UNKNOWN;
    }

    private static String aboutText(final OptionalLong estimate) {
        return estimate.isPresent()
                ? "about " + formatBytes(estimate.getAsLong())
                : UNKNOWN;
    }

    private static void appendEstimateTreeLine(final StringBuilder out,
            final String prefix, final String connector, final String label,
            final OptionalLong estimateBytes, final String unknownReason,
            final String... details) {
        appendEstimateTreeLine(out, prefix, connector, label, estimateBytes,
                unknownReason, false, details);
    }

    private static void appendParentEstimateTreeLine(final StringBuilder out,
            final String prefix, final String connector, final String label,
            final OptionalLong estimateBytes, final String unknownReason,
            final String... details) {
        appendEstimateTreeLine(out, prefix, connector, label, estimateBytes,
                unknownReason, true, details);
    }

    private static void appendEstimateTreeLine(final StringBuilder out,
            final String prefix, final String connector, final String label,
            final OptionalLong estimateBytes, final String unknownReason,
            final boolean childBranch, final String... details) {
        final String[] resolvedDetails;
        if (estimateBytes.isEmpty() && !unknownReason.isBlank()) {
            resolvedDetails = new String[details.length + 1];
            resolvedDetails[0] = "reason: " + unknownReason;
            System.arraycopy(details, 0, resolvedDetails, 1,
                    details.length);
        } else {
            resolvedDetails = details;
        }
        appendTreeLine(out, prefix, connector, label,
                memoryText(estimateBytes), childBranch, resolvedDetails);
    }

    private static void appendTreeLine(final StringBuilder out,
            final String prefix, final String connector, final String label,
            final String value, final String... details) {
        appendTreeLine(out, prefix, connector, label, value, false, details);
    }

    private static void appendTreeLine(final StringBuilder out,
            final String prefix, final String connector, final String label,
            final String value, final boolean childBranch,
            final String... details) {
        final String lead = prefix + connector + label + " - ";
        out.append(lead).append(value).append(LINE_SEPARATOR);
        final String childMarker = childBranch ? "│" : "";
        final String detailPrefix = prefix + continuation(connector)
                + childMarker
                + " ".repeat(label.length() + " - ".length()
                        - childMarker.length());
        for (final String detail : details) {
            out.append(detailPrefix).append(detail).append(LINE_SEPARATOR);
        }
    }

    private static String continuation(final String connector) {
        return connector.startsWith("├") ? "│  " : "   ";
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
        final String[] units = { "KiB", "MiB", "GiB", "TiB", "PiB", "EiB" };
        double value = bytes;
        int unitIndex = -1;
        while (value >= 1024D && unitIndex < units.length - 1) {
            value = value / 1024D;
            unitIndex++;
        }
        return String.format(Locale.ROOT, "%.2f %s", value,
                units[unitIndex]);
    }

    private static String formatCount(final long count) {
        return String.format(Locale.ROOT, "%,d", count);
    }

}
