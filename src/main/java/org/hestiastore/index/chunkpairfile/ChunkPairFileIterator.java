package org.hestiastore.index.chunkpairfile;

import java.util.NoSuchElementException;
import java.util.Optional;

import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIteratorWithCurrent;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.Chunk;
import org.hestiastore.index.chunkstore.ChunkPairIterator;
import org.hestiastore.index.chunkstore.ChunkStoreReader;
import org.hestiastore.index.datatype.TypeDescriptor;

public class ChunkPairFileIterator<K, V>
        implements PairIteratorWithCurrent<K, V> {

    private final ChunkStoreReader chunkStoreReader;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;

    private PairIteratorWithCurrent<K, V> iterator;

    public ChunkPairFileIterator(ChunkStoreReader chunkStoreReader,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor) {
        this.chunkStoreReader = Vldtn.requireNonNull(chunkStoreReader,
                "chunkStoreReader");
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        moveToNextChunk();
    }

    @Override
    public boolean hasNext() {
        return iterator != null && iterator.hasNext();
    }

    @Override
    public Pair<K, V> next() {
        if (iterator == null) {
            throw new NoSuchElementException("No more elements");
        }
        return iterator.next();
    }

    @Override
    public void close() {
        if (iterator != null) {
            iterator.close();
        }
        chunkStoreReader.close();
        iterator = null;
    }

    @Override
    public Optional<Pair<K, V>> getCurrent() {
        if (iterator == null) {
            return Optional.empty();
        }
        return iterator.getCurrent();
    }

    private void moveToNextChunk() {
        if (iterator == null || !iterator.hasNext()) {
            Chunk chunk = chunkStoreReader.read();
            if (chunk != null) {
                iterator = new ChunkPairIterator<>(chunk, keyTypeDescriptor,
                        valueTypeDescriptor);
            }
        }
    }

}
