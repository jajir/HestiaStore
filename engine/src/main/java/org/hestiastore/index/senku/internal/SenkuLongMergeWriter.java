package org.hestiastore.index.senku.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.function.BooleanSupplier;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkentryfile.SingleChunkEntryWriterImpl;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.datatype.NullValue;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.datatype.TypeDescriptorNull;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.senku.SenkuMergeFunction;

/**
 * Primitive-long-key sorted merge and run encoder for built-in long and null
 * values.
 *
 * <p>
 * One cursor is retained per source and one bounded-growth page buffer is
 * retained per active merge job. Distinct records remain primitive from page
 * decoding through output encoding; boxing is limited to actual duplicate
 * calls through the public generic merge-function contract.
 * </p>
 */
final class SenkuLongMergeWriter<V> {

    private final List<SenkuMergeSource> sources;
    private final Directory directory;
    private final TypeDescriptorLong keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final SenkuMergeFunction<Long, V> mergeFunction;
    private final boolean primitiveLongValue;
    private final int maxEncodedPageBytes;
    private final int maxKeysPerPage;
    private final long maxEntriesPerPart;
    private final DataBlockSize dataBlockSize;
    private final BooleanSupplier publicationAllowed;
    private final PriorityQueue<SenkuLongSourceCursor> queue =
            new PriorityQueue<>(SenkuLongMergeWriter::compareCursors);

    private long mergedKey;
    private long mergedLongValue;
    private V mergedValue;

    /**
     * Creates a primitive writer for one already-created output run.
     *
     * @param sources exact sorted input ranges
     * @param directory target run directory
     * @param keyTypeDescriptor exact built-in long key descriptor
     * @param valueTypeDescriptor exact built-in long or null-value descriptor
     * @param mergeFunction duplicate reducer
     * @param maxKeysPerPage maximum entries in one page
     * @param maxEntriesPerPart maximum entries in one physical part
     * @param dataBlockSize chunk-store block size
     * @param publicationAllowed publication fencing callback
     */
    SenkuLongMergeWriter(final List<SenkuMergeSource> sources,
            final Directory directory,
            final TypeDescriptorLong keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final SenkuMergeFunction<Long, V> mergeFunction,
            final int maxKeysPerPage, final long maxEntriesPerPart,
            final DataBlockSize dataBlockSize,
            final BooleanSupplier publicationAllowed) {
        this.sources = List.copyOf(
                Vldtn.requireNonNull(sources, "sources"));
        this.directory = Vldtn.requireNonNull(directory, "directory");
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        primitiveLongValue = valueTypeDescriptor
                .getClass() == TypeDescriptorLong.class;
        Vldtn.requireTrue(primitiveLongValue || valueTypeDescriptor
                .getClass() == TypeDescriptorNull.class,
                "Primitive long merge requires Long or NullValue values");
        this.maxKeysPerPage = Vldtn.requireGreaterThanZero(maxKeysPerPage,
                "maxKeysPerPage");
        final int maximumBytesPerEntry = 2 + Long.BYTES
                + (primitiveLongValue ? Long.BYTES : 0);
        maxEncodedPageBytes = (int) Math.min(Integer.MAX_VALUE - 8L,
                (long) this.maxKeysPerPage * maximumBytesPerEntry);
        this.mergeFunction = Vldtn.requireNonNull(mergeFunction,
                "mergeFunction");
        this.maxEntriesPerPart = Vldtn.requireGreaterThanZero(
                maxEntriesPerPart, "maxEntriesPerPart");
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
                maxEntriesPerPart, 0).openWriterTx();
        long recordCount = 0L;
        try {
            openInputs();
            while (!queue.isEmpty()) {
                final SingleChunkEntryWriterImpl<Long, V> page =
                        new SingleChunkEntryWriterImpl<>(
                                keyTypeDescriptor, valueTypeDescriptor,
                                maxEncodedPageBytes);
                int pageEntries = 0;
                while (pageEntries < maxKeysPerPage && mergeNext()) {
                    writeMerged(page);
                    pageEntries++;
                    recordCount = Math.incrementExact(recordCount);
                }
                writer.appendPage(page.closeSequence(), pageEntries);
            }
            final int partCount = writer.commit();
            final SenkuRunManifest manifest = new SenkuRunManifest(partCount,
                    recordCount);
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
        final List<SenkuLongSourceCursor> opened = new ArrayList<>(
                sources.size());
        try {
            int ordinal = 0;
            for (final SenkuMergeSource source : sources) {
                final SenkuLongSourceCursor cursor = Vldtn
                        .requireNonNull(source, "source").openLongs(ordinal++,
                                primitiveLongValue);
                opened.add(cursor);
                if (cursor.hasCurrent()) {
                    queue.add(cursor);
                }
            }
        } catch (Exception e) {
            closeCursors(opened, e);
            throw e;
        }
    }

    private boolean mergeNext() {
        if (queue.isEmpty()) {
            return false;
        }
        SenkuLongSourceCursor active = null;
        try {
            active = queue.remove();
            mergedKey = active.key();
            if (primitiveLongValue) {
                mergedLongValue = active.value();
            } else {
                mergedValue = nullValue();
            }
            active.advance();
            while (active.hasCurrent() && active.key() == mergedKey) {
                mergeCurrent(active);
                active.advance();
            }
            if (active.hasCurrent()) {
                queue.add(active);
            }
            active = pollEqualKey();
            while (active != null) {
                do {
                    mergeCurrent(active);
                    active.advance();
                } while (active.hasCurrent() && active.key() == mergedKey);
                if (active.hasCurrent()) {
                    queue.add(active);
                }
                active = pollEqualKey();
            }
            return true;
        } catch (Exception e) {
            if (active != null) {
                closeCursor(active, e);
            }
            closeInputs(e);
            if (e instanceof IndexException) {
                throw (IndexException) e;
            }
            throw new IndexException("Unable to merge primitive-long inputs.",
                    e);
        }
    }

    private void mergeCurrent(final SenkuLongSourceCursor cursor) {
        if (primitiveLongValue) {
            mergedLongValue = ((Long) Vldtn.requireNonNull(
                    mergeFunction.apply(mergedKey, longValue(mergedLongValue),
                            longValue(cursor.value())),
                    "mergedValue")).longValue();
            return;
        }
        mergedValue = Vldtn.requireNonNull(mergeFunction.apply(mergedKey,
                mergedValue, nullValue()), "mergedValue");
    }

    private void writeMerged(final SingleChunkEntryWriterImpl<Long, V> page) {
        if (primitiveLongValue) {
            page.putLongs(mergedKey, mergedLongValue);
            return;
        }
        page.putLongKey(mergedKey, mergedValue);
    }

    @SuppressWarnings("unchecked")
    private V longValue(final long value) {
        return (V) Long.valueOf(value);
    }

    @SuppressWarnings("unchecked")
    private V nullValue() {
        return (V) NullValue.NULL;
    }

    private SenkuLongSourceCursor pollEqualKey() {
        final SenkuLongSourceCursor cursor = queue.peek();
        if (cursor == null || cursor.key() != mergedKey) {
            return null;
        }
        return queue.remove();
    }

    private void closeInputs(final Exception primary) {
        while (!queue.isEmpty()) {
            closeCursor(queue.remove(), primary);
        }
    }

    private static void closeCursors(
            final List<SenkuLongSourceCursor> cursors,
            final Exception primary) {
        for (final SenkuLongSourceCursor cursor : cursors) {
            closeCursor(cursor, primary);
        }
    }

    private static void closeCursor(final SenkuLongSourceCursor cursor,
            final Exception primary) {
        try {
            cursor.close();
        } catch (Exception cleanupFailure) {
            primary.addSuppressed(cleanupFailure);
        }
    }

    private static int compareCursors(final SenkuLongSourceCursor first,
            final SenkuLongSourceCursor second) {
        final int compared = Long.compare(first.key(), second.key());
        return compared == 0 ? Integer.compare(first.ordinal(), second.ordinal())
                : compared;
    }
}
