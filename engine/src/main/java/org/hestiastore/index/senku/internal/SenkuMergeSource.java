package org.hestiastore.index.senku.internal;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;

/**
 * Immutable readable range used by one Senku merge job.
 */
final class SenkuMergeSource {

    private final LargeFile file;
    private final LargeFilePosition startPosition;
    private final long recordCount;
    private final boolean requireSourceEof;

    private SenkuMergeSource(final LargeFile file,
            final LargeFilePosition startPosition, final long recordCount,
            final boolean requireSourceEof) {
        this.file = Vldtn.requireNonNull(file, "file");
        this.startPosition = startPosition;
        this.recordCount = Vldtn.requireGreaterThanOrEqualToZero(recordCount,
                "recordCount");
        this.requireSourceEof = requireSourceEof;
    }

    /**
     * Creates a non-empty range inside a shared flush file.
     *
     * @param file validated flush large file
     * @param startPosition first page of the shard range
     * @param recordCount exact positive shard record count
     * @return immutable merge source
     */
    static SenkuMergeSource flush(final LargeFile file,
            final LargeFilePosition startPosition, final long recordCount) {
        Vldtn.requireGreaterThanZero(recordCount, "recordCount");
        return new SenkuMergeSource(file,
                Vldtn.requireNonNull(startPosition, "startPosition"),
                recordCount, false);
    }

    /**
     * Creates a complete sorted-run source, including an empty run.
     *
     * @param file validated run large file
     * @param recordCount exact run record count
     * @return immutable merge source
     */
    static SenkuMergeSource run(final LargeFile file,
            final long recordCount) {
        return new SenkuMergeSource(file, null, recordCount, true);
    }

    /**
     * Opens the exact source range.
     *
     * @param keyTypeDescriptor key codec
     * @param valueTypeDescriptor value codec
     * @return source iterator owned by the caller
     */
    <K, V> EntryIterator<K, V> open(
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor) {
        final LargeFileReader reader = startPosition == null ? file.openReader()
                : file.openReader(startPosition);
        return new SenkuSourceEntryIterator<>(reader, keyTypeDescriptor,
                valueTypeDescriptor, recordCount, requireSourceEof);
    }

    long recordCount() {
        return recordCount;
    }
}
