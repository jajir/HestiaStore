package org.hestiastore.index.senku.internal;

import java.util.List;
import java.util.Optional;
import java.util.function.BooleanSupplier;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkentryfile.KeyPageCodec;
import org.hestiastore.index.chunkentryfile.SingleChunkEntryWriterImpl;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.datatype.NullValue;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.datatype.TypeDescriptorNull;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.senku.SenkuMergeFunction;
import org.hestiastore.index.senku.SenkuMergeFunctions;

/**
 * Primitive-long-key sorted merge and run encoder for built-in long and null
 * values.
 *
 * <p>
 * One cursor and one cached primitive heap entry are retained per source, and
 * one bounded-growth page buffer is retained per active merge job. The heap
 * updates only its root after consuming a source's equal-key group. Distinct
 * records remain primitive from page decoding through output encoding; boxing
 * is limited to actual duplicate calls through the public generic
 * merge-function contract.
 * </p>
 */
final class SenkuLongMergeWriter<V> {

    private final SenkuStorageFormat format;
    private final KeyPageCodec<Long> keyCodec;
    private final List<SenkuMergeSource> sources;
    private final Directory directory;
    private final TypeDescriptorLong keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final SenkuMergeFunction<Long, V> mergeFunction;
    private final boolean primitiveLongValue;
    private final boolean longSet;
    private final int maxEncodedPageBytes;
    private final int maxKeysPerPage;
    private final long maxEntriesPerPart;
    private final DataBlockSize dataBlockSize;
    private final BooleanSupplier publicationAllowed;
    private final SenkuLongSourceCursor[] cursors;
    private final SenkuLongMergeHeap selector;

    private long mergedKey;
    private long logicalMergedKey;
    private boolean logicalMergedKeyAvailable;
    private long mergedLongValue;
    private V mergedValue;

    /**
     * Creates a primitive writer for one already-created output run.
     *
     * @param sources             exact sorted input ranges
     * @param directory           target run directory
     * @param keyTypeDescriptor   exact built-in long key descriptor
     * @param valueTypeDescriptor exact built-in long or null-value descriptor
     * @param mergeFunction       duplicate reducer
     * @param maxKeysPerPage      maximum entries in one page
     * @param maxEntriesPerPart   maximum entries in one physical part
     * @param dataBlockSize       chunk-store block size
     * @param publicationAllowed  publication fencing callback
     */
    SenkuLongMergeWriter(final List<SenkuMergeSource> sources,
            final Directory directory,
            final TypeDescriptorLong keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final SenkuMergeFunction<Long, V> mergeFunction,
            final int maxKeysPerPage, final long maxEntriesPerPart,
            final DataBlockSize dataBlockSize,
            final BooleanSupplier publicationAllowed) {
        this(sources, directory, keyTypeDescriptor, valueTypeDescriptor,
                mergeFunction, maxKeysPerPage, maxEntriesPerPart, dataBlockSize,
                publicationAllowed, SenkuStorageFormat.createDefault());
    }

    /** Creates this component with the index's immutable storage format. */
    SenkuLongMergeWriter(final List<SenkuMergeSource> sources,
            final Directory directory,
            final TypeDescriptorLong keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final SenkuMergeFunction<Long, V> mergeFunction,
            final int maxKeysPerPage, final long maxEntriesPerPart,
            final DataBlockSize dataBlockSize,
            final BooleanSupplier publicationAllowed,
            final SenkuStorageFormat format) {
        this.format = Vldtn.requireNonNull(format, "format");
        keyCodec = format.keyCodec();
        this.sources = List.copyOf(Vldtn.requireNonNull(sources, "sources"));
        cursors = new SenkuLongSourceCursor[this.sources.size()];
        selector = new SenkuLongMergeHeap(this.sources.size());
        this.directory = Vldtn.requireNonNull(directory, "directory");
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        primitiveLongValue = valueTypeDescriptor
                .getClass() == TypeDescriptorLong.class;
        Vldtn.requireTrue(primitiveLongValue
                || valueTypeDescriptor.getClass() == TypeDescriptorNull.class,
                "Primitive long merge requires Long or NullValue values");
        this.maxKeysPerPage = Vldtn.requireGreaterThanZero(maxKeysPerPage,
                "maxKeysPerPage");
        final int maximumBytesPerEntry = 2 + Long.BYTES
                + (primitiveLongValue ? Long.BYTES : 0);
        maxEncodedPageBytes = (int) Math.min(Integer.MAX_VALUE - 8L,
                (long) this.maxKeysPerPage * maximumBytesPerEntry);
        this.mergeFunction = Vldtn.requireNonNull(mergeFunction,
                "mergeFunction");
        longSet = !primitiveLongValue
                && mergeFunction == (Object) SenkuMergeFunctions.longSet();
        this.maxEntriesPerPart = Vldtn.requireGreaterThanZero(maxEntriesPerPart,
                "maxEntriesPerPart");
        this.dataBlockSize = Vldtn.requireNonNull(dataBlockSize,
                "dataBlockSize");
        this.publicationAllowed = Vldtn.requireNonNull(publicationAllowed,
                "publicationAllowed");
    }

    /**
     * Executes the merge, commits all parts, and publishes the manifest last.
     *
     * @return committed run manifest
     */
    SenkuRunManifest write() {
        final LargeFileWriterTx writer = new LargeFile(directory, dataBlockSize,
                maxEntriesPerPart, 0, format).openWriterTx();
        long recordCount = 0L;
        final SenkuLongKeySampler sampler = new SenkuLongKeySampler();
        try {
            openInputs();
            while (!selector.isEmpty()) {
                final SingleChunkEntryWriterImpl<Long, V> page = new SingleChunkEntryWriterImpl<>(
                        keyTypeDescriptor, valueTypeDescriptor,
                        maxEncodedPageBytes, format.keyCodec());
                int pageEntries = 0;
                while (pageEntries < maxKeysPerPage && mergeNext()) {
                    writeMerged(page);
                    if (sampler.selectNext()) {
                        sampler.addSelectedKey(logicalMergedKey());
                    }
                    pageEntries++;
                    recordCount = Math.incrementExact(recordCount);
                }
                writer.appendPage(page.closeSequence(), pageEntries);
            }
            final int partCount = writer.commit();
            final SenkuRunManifest manifest = new SenkuRunManifest(partCount,
                    recordCount, Optional.of(sampler.snapshot()));
            Vldtn.requireTrue(publicationAllowed.getAsBoolean(),
                    "Senku run publication is no longer allowed");
            SenkuMetadataCodec.publishRunManifest(directory, manifest);
            return manifest;
        } catch (Exception e) {
            writer.abort(e);
            closeInputs(e);
            if (e instanceof IndexException) {
                throw (IndexException) e;
            }
            throw new IndexException("Unable to write primitive-long run.", e);
        }
    }

    private void openInputs() {
        try {
            for (int ordinal = 0; ordinal < sources.size(); ordinal++) {
                final SenkuMergeSource source = sources.get(ordinal);
                final SenkuLongSourceCursor cursor = Vldtn
                        .requireNonNull(source, "source").openLongs(ordinal,
                                primitiveLongValue, format.keyCodec());
                cursors[ordinal] = cursor;
                if (cursor.hasCurrent()) {
                    Vldtn.requireTrue(
                            keyCodec.hasSameEncoding(cursor.keyCodec()),
                            "Merge source and target codec domains differ");
                    selector.add(cursor.encodedKey(), ordinal);
                } else {
                    cursors[ordinal] = null;
                }
            }
        } catch (Exception e) {
            closeInputs(e);
            throw e;
        }
    }

    private boolean mergeNext() {
        if (selector.isEmpty()) {
            return false;
        }
        try {
            SenkuLongSourceCursor active = cursors[selector.ordinal()];
            mergedKey = selector.key();
            logicalMergedKeyAvailable = false;
            if (primitiveLongValue) {
                mergedLongValue = active.value();
            } else {
                mergedValue = nullValue();
            }
            active.advance();
            while (active.hasCurrent() && active.encodedKey() == mergedKey) {
                mergeCurrent(active);
                active.advance();
            }
            refreshSelected(active);
            while (!selector.isEmpty() && selector.key() == mergedKey) {
                active = cursors[selector.ordinal()];
                do {
                    mergeCurrent(active);
                    active.advance();
                } while (active.hasCurrent()
                        && active.encodedKey() == mergedKey);
                refreshSelected(active);
            }
            return true;
        } catch (Exception e) {
            closeInputs(e);
            if (e instanceof IndexException) {
                throw (IndexException) e;
            }
            throw new IndexException("Unable to merge primitive-long inputs.",
                    e);
        }
    }

    private void mergeCurrent(final SenkuLongSourceCursor cursor) {
        if (longSet) {
            return;
        }
        if (primitiveLongValue) {
            mergedLongValue = ((Long) Vldtn.requireNonNull(mergeFunction.apply(
                    logicalMergedKey(), longValue(mergedLongValue),
                    longValue(cursor.value())), "mergedValue")).longValue();
            return;
        }
        mergedValue = Vldtn.requireNonNull(mergeFunction.apply(
                logicalMergedKey(), mergedValue, nullValue()), "mergedValue");
    }

    private void writeMerged(final SingleChunkEntryWriterImpl<Long, V> page) {
        if (primitiveLongValue) {
            page.putEncodedLongs(mergedKey, mergedLongValue, keyCodec);
            return;
        }
        page.putEncodedLongKey(mergedKey, mergedValue, keyCodec);
    }

    /** Decodes at most once for duplicate callbacks or selected sample keys. */
    private long logicalMergedKey() {
        if (!logicalMergedKeyAvailable) {
            logicalMergedKey = keyCodec.decodeLongKey(mergedKey);
            logicalMergedKeyAvailable = true;
        }
        return logicalMergedKey;
    }

    @SuppressWarnings("unchecked")
    private V longValue(final long value) {
        return (V) Long.valueOf(value);
    }

    @SuppressWarnings("unchecked")
    private V nullValue() {
        return (V) NullValue.NULL;
    }

    /**
     * Refreshes the root only after its complete equal-key group is consumed.
     */
    private void refreshSelected(final SenkuLongSourceCursor cursor) {
        if (cursor.hasCurrent()) {
            selector.replaceRoot(cursor.encodedKey());
        } else {
            cursors[selector.ordinal()] = null;
            selector.removeRoot();
        }
    }

    private void closeInputs(final Exception primary) {
        for (int ordinal = 0; ordinal < cursors.length; ordinal++) {
            final SenkuLongSourceCursor cursor = cursors[ordinal];
            cursors[ordinal] = null;
            if (cursor != null) {
                closeCursor(cursor, primary);
            }
        }
        selector.clear();
    }

    private static void closeCursor(final SenkuLongSourceCursor cursor,
            final Exception primary) {
        try {
            cursor.close();
        } catch (Exception cleanupFailure) {
            primary.addSuppressed(cleanupFailure);
        }
    }
}
