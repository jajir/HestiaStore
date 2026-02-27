package org.hestiastore.index.chunkentryfile;

import java.util.Optional;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIteratorWithCurrent;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.chunkstore.Chunk;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.MemFileReader;
import org.hestiastore.index.sorteddatafile.DiffKeyReader;
import org.hestiastore.index.unsorteddatafile.DataFileIterator;

/**
 * It allows to iterate over all entries stored in one chunk.
 */
public class SingleChunkEntryIterator<K, V>
        extends AbstractCloseableResource implements EntryIteratorWithCurrent<K, V> {

    private final EntryIteratorWithCurrent<K, V> iterator;

    /**
     * It creates an iterator over all entries stored in the given chunk.
     *
     * @param chunk               required chunk to iterate over
     * @param keyTypeDescriptor   required type descriptor of keys
     * @param valueTypeDescriptor required type descriptor of values
     */
    public SingleChunkEntryIterator(final Chunk chunk,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor) {
        this(Vldtn.requireNonNull(chunk, "chunk").getPayloadSequence(),
                keyTypeDescriptor, valueTypeDescriptor);
    }

    /**
     * It creates an iterator over all entries stored in the given payload.
     *
     * @param payload             required payload sequence to iterate over
     * @param keyTypeDescriptor   required type descriptor of keys
     * @param valueTypeDescriptor required type descriptor of values
     */
    public SingleChunkEntryIterator(final ByteSequence payload,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor) {
        Vldtn.requireNonNull(keyTypeDescriptor, "keyTypeDescriptor");
        Vldtn.requireNonNull(valueTypeDescriptor, "valueTypeDescriptor");
        final ByteSequence validatedPayload = Vldtn.requireNonNull(payload,
                "payload");

        // Fast path: iterate directly over chunk payload bytes without
        // constructing a Directory + SortedDataFile stack.
        final MemFileReader reader = new MemFileReader(validatedPayload);
        final DiffKeyReader<K> keyReader = new DiffKeyReader<>(
                keyTypeDescriptor.getTypeDecoder());
        this.iterator = new DataFileIterator<>(keyReader,
                valueTypeDescriptor.getTypeReader(), reader);
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public Entry<K, V> next() {
        return iterator.next();
    }

    @Override
    public Optional<Entry<K, V>> getCurrent() {
        return iterator.getCurrent();
    }

    @Override
    protected void doClose() {
        iterator.close();
    }

}
