package org.hestiastore.index.chunkpairfile;

import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Function;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIteratorWithCurrent;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.Chunk;
import org.hestiastore.index.chunkstore.ChunkStoreReader;

/**
 * Iterator over pairs stored in chunk pair file.
 * 
 * @author honza
 *
 * @param <K> key type
 * @param <V> value type
 */
public class ChunkPairFileIterator<K, V>
        extends AbstractCloseableResource implements PairIteratorWithCurrent<K, V> {

    private final ChunkStoreReader chunkStoreReader;
    private final Function<Chunk, PairIteratorWithCurrent<K, V>> iteratorFactory;

    private PairIteratorWithCurrent<K, V> iterator;

    /**
     * Constructor.
     * 
     * @param chunkStoreReader required reader of chunks
     * @param iteratorFactory  required factory of pair iterators for chunks
     */
    public ChunkPairFileIterator(ChunkStoreReader chunkStoreReader,
            final Function<Chunk, PairIteratorWithCurrent<K, V>> iteratorFactory) {
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
    public Pair<K, V> next() {
        if (iterator == null) {
            throw new NoSuchElementException("No more elements");
        }
        return iterator.next();
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
