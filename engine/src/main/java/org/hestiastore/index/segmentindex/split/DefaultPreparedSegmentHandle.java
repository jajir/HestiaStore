package org.hestiastore.index.segmentindex.split;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.WriteTransaction;
import org.hestiastore.index.segment.SegmentId;

/**
 * Default prepared segment handle backed by a synchronous write transaction.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class DefaultPreparedSegmentHandle<K, V>
        implements PreparedSegmentHandle<K, V> {

    private final SegmentId segmentId;
    private final WriteTransaction<K, V> writerTx;
    private final EntryWriter<K, V> writer;
    private final SegmentMaterializationFileSystem fileSystem;
    private boolean committed;
    private boolean discarded;

    DefaultPreparedSegmentHandle(final SegmentId segmentId,
            final WriteTransaction<K, V> writerTx,
            final SegmentMaterializationFileSystem fileSystem) {
        this.segmentId = Vldtn.requireNonNull(segmentId, "segmentId");
        this.writerTx = Vldtn.requireNonNull(writerTx, "writerTx");
        this.fileSystem = Vldtn.requireNonNull(fileSystem, "fileSystem");
        this.writer = Vldtn.requireNonNull(writerTx.open(), "writer");
    }

    @Override
    public SegmentId segmentId() {
        return segmentId;
    }

    @Override
    public void write(final Entry<K, V> entry) {
        writer.write(Vldtn.requireNonNull(entry, "entry"));
    }

    @Override
    public void commit() {
        if (committed) {
            throw new IllegalStateException("Prepared segment committed");
        }
        if (discarded) {
            throw new IllegalStateException("Prepared segment discarded");
        }
        closeWriterIfNeeded();
        writerTx.commit();
        committed = true;
    }

    @Override
    public void discard() {
        if (discarded) {
            return;
        }
        closeWriterIfNeeded();
        fileSystem.deletePreparedSegment(segmentId);
        discarded = true;
    }

    @Override
    public void close() {
        if (discarded || committed) {
            return;
        }
        closeWriterIfNeeded();
    }

    private void closeWriterIfNeeded() {
        if (!writer.wasClosed()) {
            writer.close();
        }
    }
}
