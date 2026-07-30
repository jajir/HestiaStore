package org.hestiastore.index.segmentindex.core.session;

import java.util.ArrayList;
import java.util.List;
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

    private static final int TABLE_WIDTH = 80;
    private static final int COMPONENT_WIDTH = 22;
    private static final int ESTIMATE_WIDTH = 10;
    private static final int TABLE_RESERVED_WIDTH = 12;
    private static final int DESCRIPTION_WIDTH = TABLE_WIDTH
            - COMPONENT_WIDTH - ESTIMATE_WIDTH - TABLE_RESERVED_WIDTH;

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
        appendIncludedMemoryTable(out, resolved, resolvedRouteCount,
                maxScarceIndexKeys, entryEstimate, total, allSegments,
                oneCachedSegment, oneDeltaCache, oneBloomFilter,
                oneScarceIndex, oneSegmentInfrastructure, routeMap,
                chunkStoreCache, steadyState, temporaryMemoryMargin);
        appendEntrySizeTable(out, keyDescriptor, valueDescriptor, keyEstimate,
                valueEstimate, entryEstimate, scarceEntryEstimate,
                routeEntryEstimate);
        appendInputTable(out, resolved, resolvedRouteCount,
                maxScarceIndexKeys);
        appendNotes(out);
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

    private static void appendIncludedMemoryTable(final StringBuilder out,
            final EffectiveIndexConfiguration<?, ?> resolved,
            final int resolvedRouteCount, final long maxScarceIndexKeys,
            final OptionalLong entryEstimate, final OptionalLong total,
            final OptionalLong allSegments,
            final OptionalLong oneCachedSegment,
            final OptionalLong oneDeltaCache,
            final OptionalLong oneBloomFilter,
            final OptionalLong oneScarceIndex,
            final OptionalLong oneSegmentInfrastructure,
            final OptionalLong routeMap,
            final OptionalLong chunkStoreCache,
            final OptionalLong steadyState,
            final OptionalLong temporaryMemoryMargin) {
        appendTableTitle(out, "Included memory");
        appendEstimateRow(out, "Total index memory", total,
                "needs segment, page-cache, and maintenance estimates",
                "segments + page cache + maintenance overhead");
        appendEstimateRow(out, "All segments", allSegments,
                "needs complete segment and routing estimates",
                "cached segment slots: "
                        + formatCount(resolved.segment().cachedSegmentLimit())
                        + "; cached segments + routing map");
        appendEstimateRow(out, "One cached segment", oneCachedSegment,
                "needs key and value descriptor estimates",
                "delta cache + bloom filter + scarce index + segment runtime");
        appendEstimateRow(out, "Delta cache", oneDeltaCache,
                "needs key and value descriptor estimates",
                "cache key limit: "
                        + formatCount(resolved.segment().cacheKeyLimit())
                        + "; cache key limit * key/value entry "
                        + estimateText(entryEstimate));
        appendEstimateRow(out, "Bloom filter", oneBloomFilter, "",
                "configured bloom filter size: "
                        + formatBytes(resolved.bloomFilter()
                                .indexSizeBytes()));
        appendEstimateRow(out, "Scarce index", oneScarceIndex,
                "needs key descriptor estimate",
                "max scarce keys: " + formatCount(maxScarceIndexKeys)
                        + "; max scarce keys * key/position entry");
        appendEstimateRow(out, "Segment runtime", oneSegmentInfrastructure, "",
                "fixed overhead per cached segment");
        appendEstimateRow(out, "Segment routing map", routeMap,
                "needs key descriptor estimate",
                "route count: " + formatCount(resolvedRouteCount)
                        + "; route count * key/segment-id entry");
        appendEstimateRow(out, "Chunk-store page cache", chunkStoreCache,
                "needs key and value descriptor estimates",
                "pages: " + formatCount(
                        resolved.chunkStoreCache().pageLimit())
                        + "; pages * (page overhead + chunk keys per page "
                        + "* key/value entry)");
        appendEstimateRow(out, "Maintenance overhead",
                temporaryMemoryMargin,
                "needs memory before maintenance and key/value entry estimates",
                "max(" + TEMPORARY_MEMORY_MARGIN_PERCENT
                        + "% of memory before maintenance = "
                        + memoryText(steadyState)
                        + ", one delta cache = "
                        + memoryText(oneDeltaCache) + ")");
        appendTableFooter(out);
    }

    private static void appendEntrySizeTable(final StringBuilder out,
            final TypeDescriptor<?> keyDescriptor,
            final TypeDescriptor<?> valueDescriptor,
            final OptionalInt keyEstimate,
            final OptionalInt valueEstimate,
            final OptionalLong entryEstimate,
            final OptionalLong scarceEntryEstimate,
            final OptionalLong routeEntryEstimate) {
        appendTableTitle(out, "Entry sizes");
        appendEstimateRow(out, "Key/value entry", entryEstimate,
                "needs key and value descriptor estimates",
                "key size + value size + overhead; key: "
                        + descriptorName(keyDescriptor) + ", "
                        + aboutText(keyEstimate) + "; value: "
                        + descriptorName(valueDescriptor) + ", "
                        + aboutText(valueEstimate) + "; overhead: "
                        + formatBytes(ENTRY_OVERHEAD_BYTES));
        appendEstimateRow(out, "Key/position entry", scarceEntryEstimate,
                "needs key descriptor estimate",
                "key size + integer position + overhead; key: "
                        + descriptorName(keyDescriptor) + ", "
                        + aboutText(keyEstimate)
                        + "; integer position: TypeDescriptorInteger, about "
                        + formatBytes(SCARCE_INDEX_POSITION_BYTES)
                        + "; overhead: " + formatBytes(ENTRY_OVERHEAD_BYTES));
        appendEstimateRow(out, "Key/segment-id entry", routeEntryEstimate,
                "needs key descriptor estimate",
                "key size + segment id size; key: "
                        + estimateText(keyEstimate) + "; segment id: "
                        + formatBytes(SEGMENT_ID_ESTIMATE_BYTES));
        appendTableFooter(out);
    }

    private static void appendInputTable(final StringBuilder out,
            final EffectiveIndexConfiguration<?, ?> resolved,
            final int resolvedRouteCount, final long maxScarceIndexKeys) {
        appendTableTitle(out, "Inputs and constants");
        appendValueRow(out, "Cached segment slots",
                formatCount(resolved.segment().cachedSegmentLimit()),
                "multiplier for one cached segment");
        appendValueRow(out, "Segment routes",
                formatCount(resolvedRouteCount), "route-map entries");
        appendValueRow(out, "Max scarce keys",
                formatCount(maxScarceIndexKeys),
                "ceil(max segment keys / chunk keys per page)");
        appendValueRow(out, "Chunk-store pages",
                formatCount(resolved.chunkStoreCache().pageLimit()),
                "configured page cache size");
        appendValueRow(out, "Chunk keys per page",
                formatCount(resolved.segment().chunkKeyLimit()),
                "keys stored in one chunk-store page");
        appendValueRow(out, "Page overhead", formatBytes(PAGE_OVERHEAD_BYTES),
                "fixed overhead per chunk-store page");
        appendValueRow(out, "Entry overhead",
                formatBytes(ENTRY_OVERHEAD_BYTES),
                "fixed overhead in key/value and key/position entries");
        appendValueRow(out, "Maintenance threads",
                formatCount(resolved.maintenance().indexThreads()),
                "used by maintenance memory estimate");
        appendValueRow(out, "Write-buffer keys",
                formatCount(resolved.writePath()
                        .indexBufferedWriteKeyLimit()),
                "reported, not included in total");
        appendTableFooter(out);
    }

    private static void appendNotes(final StringBuilder out) {
        out.append("Notes:").append(LINE_SEPARATOR);
        out.append("rough estimate only").append(LINE_SEPARATOR);
        out.append("not a JVM cap or measured allocation")
                .append(LINE_SEPARATOR);
        out.append("usage depends on JVM layout, GC, workload, and snapshots")
                .append(LINE_SEPARATOR);
        out.append("details: docs/operations/memory-estimate.md")
                .append(LINE_SEPARATOR);
    }

    private static void appendEstimateRow(final StringBuilder out,
            final String component, final OptionalLong estimateBytes,
            final String unknownReason, final String description) {
        appendValueRow(out, component, memoryText(estimateBytes),
                estimateDescription(estimateBytes, unknownReason,
                        description));
    }

    private static String estimateDescription(
            final OptionalLong estimateBytes, final String unknownReason,
            final String description) {
        if (estimateBytes.isEmpty() && !unknownReason.isBlank()) {
            return "reason: " + unknownReason + "; " + description;
        }
        return description;
    }

    private static void appendTableTitle(final StringBuilder out,
            final String title) {
        out.append(title).append(LINE_SEPARATOR);
        appendTableHeader(out);
    }

    private static void appendTableHeader(final StringBuilder out) {
        appendTableSeparator(out);
        appendTableLine(out, "Component", "Estimate", "Description");
        appendTableSeparator(out);
    }

    private static void appendTableFooter(final StringBuilder out) {
        appendTableSeparator(out);
    }

    private static void appendTableSeparator(final StringBuilder out) {
        out.append('+')
                .append("-".repeat(COMPONENT_WIDTH + 2))
                .append('+')
                .append("-".repeat(ESTIMATE_WIDTH + 2))
                .append('+')
                .append("-".repeat(DESCRIPTION_WIDTH + 2))
                .append('+')
                .append(LINE_SEPARATOR);
    }

    private static void appendValueRow(final StringBuilder out,
            final String component, final String estimate,
            final String description) {
        final List<String> componentLines = wrapCell(component,
                COMPONENT_WIDTH);
        final List<String> estimateLines = wrapCell(estimate, ESTIMATE_WIDTH);
        final List<String> descriptionLines = wrapCell(description,
                DESCRIPTION_WIDTH);
        final int rowCount = Math.max(componentLines.size(), Math.max(
                estimateLines.size(), descriptionLines.size()));
        for (int index = 0; index < rowCount; index++) {
            appendTableLine(out, cellLine(componentLines, index),
                    cellLine(estimateLines, index),
                    cellLine(descriptionLines, index));
        }
    }

    private static void appendTableLine(final StringBuilder out,
            final String component, final String estimate,
            final String description) {
        final String line = String.format(Locale.ROOT,
                "| %-" + COMPONENT_WIDTH + "s | %-" + ESTIMATE_WIDTH
                        + "s | %-" + DESCRIPTION_WIDTH + "s |",
                component, estimate, description);
        out.append(line).append(LINE_SEPARATOR);
    }

    private static String cellLine(final List<String> lines,
            final int index) {
        return index < lines.size() ? lines.get(index) : "";
    }

    private static List<String> wrapCell(final String value,
            final int width) {
        final String normalized = Vldtn.requireNonNull(value, "value").trim();
        if (normalized.isEmpty()) {
            return List.of("");
        }
        final List<String> lines = new ArrayList<>();
        String currentLine = "";
        for (final String word : normalized.split("\\s+")) {
            String remainingWord = word;
            while (remainingWord.length() > width) {
                if (!currentLine.isEmpty()) {
                    lines.add(currentLine);
                    currentLine = "";
                }
                lines.add(remainingWord.substring(0, width));
                remainingWord = remainingWord.substring(width);
            }
            if (currentLine.isEmpty()) {
                currentLine = remainingWord;
            } else if (currentLine.length() + 1
                    + remainingWord.length() <= width) {
                currentLine = currentLine + " " + remainingWord;
            } else {
                lines.add(currentLine);
                currentLine = remainingWord;
            }
        }
        if (!currentLine.isEmpty()) {
            lines.add(currentLine);
        }
        return lines;
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
