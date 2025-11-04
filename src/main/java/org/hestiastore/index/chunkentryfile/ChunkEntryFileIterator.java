package org.hestiastore.index.chunkentryfile;

import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Function;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIteratorWithCurrent;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.Chunk;
import org.hestiastore.index.chunkstore.ChunkStoreReader;

/**
 * Iterator over entries stored in chunk entry file.
 * 
 * @author honza
 *
 * @param <K> key type
 * @param <V> value type
 */
public class ChunkEntryFileIterator<K, V>
        extends AbstractCloseableResource implements EntryIteratorWithCurrent<K, V> {

    private final ChunkStoreReader chunkStoreReader;
    private final Function<Chunk, EntryIteratorWithCurrent<K, V>> iteratorFactory;

    private EntryIteratorWithCurrent<K, V> iterator;

    /**
     * Constructor.
     * 
     * @param chunkStoreReader required reader of chunks
     * @param iteratorFactory  required factory of entry iterators for chunks
     */
    public ChunkEntryFileIterator(ChunkStoreReader chunkStoreReader,
            final Function<Chunk, EntryIteratorWithCurrent<K, V>> iteratorFactory) {
        this.chunkStoreReader = Vldtn.requireNonNull(chunkStoreReader,
                "chunkStoreReader");
        this.iteratorFactory = Vldtn.requireNonNull(iteratorFactory,
                "iteratorFactory");
        moveToNextChunk();
    }

    @Override
    public boolean hasNext() {
        if (iterator == null) {
            return false;
        }
        if (iterator.hasNext()) {
            return true;
        }

        // current iterator exhausted; attempt to move to next chunk
        moveToNextChunk();

        if (iterator == null) {
            return false;
        }
        return iterator.hasNext();
    }

    @Override
    public Entry<K, V> next() {
        if (iterator == null) {
            throw new NoSuchElementException("No more elements");
        }
        return iterator.next();
    }

    @Override
    public Optional<Entry<K, V>> getCurrent() {
        if (iterator == null) {
            return Optional.empty();
        }
        return iterator.getCurrent();
    }

    private void moveToNextChunk() {
        if (iterator == null || !iterator.hasNext()) {
            Chunk chunk = chunkStoreReader.read();
            if (chunk != null) {
                iterator = iteratorFactory.apply(chunk);
            } else {
                iterator = null;
            }
        }
    }

    @Override
    protected void doClose() {
        if (iterator != null) {
            iterator.close();
            iterator = null;
        }
        chunkStoreReader.close();
    }

}
