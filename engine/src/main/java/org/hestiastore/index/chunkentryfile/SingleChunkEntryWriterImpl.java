package org.hestiastore.index.chunkentryfile;

import org.hestiastore.index.Entry;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.BytesAppender;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeWriter;
import org.hestiastore.index.sorteddatafile.DiffKeyWriter;

/**
 * Simplest Chunk writer that writes all data into memmory and than create from
 * it Chunk payload.
 */
public class SingleChunkEntryWriterImpl<K, V>
        implements SingleChunkEntryWriter<K, V> {

    private final BytesAppender appender = new BytesAppender();
    private final InMemoryFileWriter fileWriter = new InMemoryFileWriter(
            appender);
    private final TypeWriter<V> valueWriter;
    private final DiffKeyWriter<K> diffKeyWriter;
    private boolean closed = false;

    /**
     * Creates a new chunk writer.
     * 
     * @param keyTypeDescriptor   required key type descriptor
     * @param valueTypeDescriptor required value type descriptor
     */
    public SingleChunkEntryWriterImpl(final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor) {
        Vldtn.requireNonNull(keyTypeDescriptor, "keyTypeDescriptor");
        Vldtn.requireNonNull(valueTypeDescriptor, "valueTypeDescriptor");
        this.valueWriter = valueTypeDescriptor.getTypeWriter();
        this.diffKeyWriter = new DiffKeyWriter<>(
                keyTypeDescriptor.getTypeEncoder(),
                keyTypeDescriptor.getComparator());
    }

    @Override
    public void put(final Entry<K, V> entry) {
        Vldtn.requireNonNull(entry, "entry");
        if (closed) {
            throw new IllegalStateException("Chunk writer already closed");
        }
        // Write diff-encoded key directly into the chunk payload writer.
        diffKeyWriter.writeTo(fileWriter, entry.getKey());
        // Write value payload via type writer
        valueWriter.write(fileWriter, entry.getValue());
    }

    @Override
    public ByteSequence closeSequence() {
        if (!closed) {
            closed = true;
        }
        return appender.getBytes();
    }

}
