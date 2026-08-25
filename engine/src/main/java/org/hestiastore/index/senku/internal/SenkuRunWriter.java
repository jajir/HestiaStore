package org.hestiastore.index.senku.internal;

import java.util.function.BooleanSupplier;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkentryfile.SingleChunkEntryWriterImpl;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;

/**
 * Streams sorted entries into one committed sorted-run directory.
 */
final class SenkuRunWriter<K, V> {

    private final Directory directory;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final int maxKeysPerPage;
    private final long maxEntriesPerPart;
    private final DataBlockSize dataBlockSize;
    private final BooleanSupplier publicationAllowed;

    /**
     * Creates a writer for one already-created run directory.
     *
     * @param directory target run directory
     * @param keyTypeDescriptor key codec
     * @param valueTypeDescriptor value codec
     * @param maxKeysPerPage maximum entries in one encoded page
     * @param maxEntriesPerPart maximum entries in one physical part
     * @param dataBlockSize chunk-store block size
     */
    SenkuRunWriter(final Directory directory,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final int maxKeysPerPage, final long maxEntriesPerPart,
            final DataBlockSize dataBlockSize) {
        this(directory, keyTypeDescriptor, valueTypeDescriptor, maxKeysPerPage,
                maxEntriesPerPart, dataBlockSize, () -> true);
    }

    SenkuRunWriter(final Directory directory,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final int maxKeysPerPage, final long maxEntriesPerPart,
            final DataBlockSize dataBlockSize,
            final BooleanSupplier publicationAllowed) {
        this.directory = Vldtn.requireNonNull(directory, "directory");
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        this.maxKeysPerPage = Vldtn.requireGreaterThanZero(maxKeysPerPage,
                "maxKeysPerPage");
        this.maxEntriesPerPart = Vldtn.requireGreaterThanZero(
                maxEntriesPerPart, "maxEntriesPerPart");
        Vldtn.requireTrue(maxEntriesPerPart >= maxKeysPerPage,
                "maxEntriesPerPart must be greater than or equal to maxKeysPerPage");
        this.dataBlockSize = Vldtn.requireNonNull(dataBlockSize,
                "dataBlockSize");
        this.publicationAllowed = Vldtn.requireNonNull(publicationAllowed,
                "publicationAllowed");
    }

    /**
     * Writes all sorted entries and publishes the run manifest last.
     *
     * @param entries sorted input owned and closed by this method
     * @return published run manifest
     */
    SenkuRunManifest write(final EntryIterator<K, V> entries) {
        final EntryIterator<K, V> validatedEntries = Vldtn
                .requireNonNull(entries, "entries");
        final LargeFileWriterTx writer = new LargeFile(directory, dataBlockSize,
                maxEntriesPerPart, 0).openWriterTx();
        long recordCount = 0L;
        try {
            while (validatedEntries.hasNext()) {
                final SingleChunkEntryWriterImpl<K, V> page =
                        new SingleChunkEntryWriterImpl<>(keyTypeDescriptor,
                                valueTypeDescriptor);
                int pageEntries = 0;
                while (pageEntries < maxKeysPerPage
                        && validatedEntries.hasNext()) {
                    final Entry<K, V> entry = Vldtn
                            .requireNonNull(validatedEntries.next(), "entry");
                    page.put(entry);
                    pageEntries++;
                    recordCount = Math.incrementExact(recordCount);
                }
                writer.appendPage(page.closeSequence(), pageEntries);
            }
            final int partCount = writer.commit();
            closeInput(validatedEntries);
            final SenkuRunManifest manifest = new SenkuRunManifest(partCount,
                    recordCount);
            Vldtn.requireTrue(publicationAllowed.getAsBoolean(),
                    "Senku run publication is no longer allowed");
            SenkuMetadataCodec.publishRunManifest(directory, manifest);
            return manifest;
        } catch (Exception e) {
            writer.abort(e);
            closeInputAfterFailure(validatedEntries, e);
            if (e instanceof IndexException) {
                throw (IndexException) e;
            }
            throw new IndexException("Unable to write sorted run.", e);
        }
    }

    private static void closeInput(final EntryIterator<?, ?> entries) {
        if (!entries.wasClosed()) {
            entries.close();
        }
    }

    private static void closeInputAfterFailure(
            final EntryIterator<?, ?> entries, final Exception primary) {
        if (entries.wasClosed()) {
            return;
        }
        try {
            entries.close();
        } catch (Exception cleanupFailure) {
            primary.addSuppressed(cleanupFailure);
        }
    }
}
