package org.hestiastore.index.chunkentryfile;

import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Function;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIteratorWithCurrent;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bytes.ByteSequence;
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
    private final Function<Chunk, EntryIteratorWithCurrent<K, V>> chunkIteratorFactory;
    private final Function<ByteSequence, EntryIteratorWithCurrent<K, V>> payloadIteratorFactory;

    private EntryIteratorWithCurrent<K, V> iterator;

    /**
     * Constructor.
     * 
     * @param chunkStoreReader required reader of chunks
     * @param iteratorFactory  required factory of entry iterators for chunks
     */
    public ChunkEntryFileIterator(ChunkStoreReader chunkStoreReader,
            final Function<Chunk, EntryIteratorWithCurrent<K, V>> iteratorFactory) {
        this(chunkStoreReader, Vldtn.requireNonNull(iteratorFactory,
                "iteratorFactory"), null);
    }

    /**
     * Creates an iterator that consumes chunk payload sequences directly,
     * bypassing temporary {@link Chunk} wrappers.
     *
     * @param chunkStoreReader       required reader of chunks
     * @param payloadIteratorFactory required factory of entry iterators for
     *                               payload sequences
     * @return configured chunk entry iterator
     */
    public static <K, V> ChunkEntryFileIterator<K, V> fromPayloads(
            final ChunkStoreReader chunkStoreReader,
            final Function<ByteSequence, EntryIteratorWithCurrent<K, V>> payloadIteratorFactory) {
        return new ChunkEntryFileIterator<>(chunkStoreReader, null,
                Vldtn.requireNonNull(payloadIteratorFactory,
                        "payloadIteratorFactory"));
    }

    private ChunkEntryFileIterator(final ChunkStoreReader chunkStoreReader,
            final Function<Chunk, EntryIteratorWithCurrent<K, V>> chunkIteratorFactory,
            final Function<ByteSequence, EntryIteratorWithCurrent<K, V>> payloadIteratorFactory) {
        this.chunkStoreReader = Vldtn.requireNonNull(chunkStoreReader,
                "chunkStoreReader");
        this.chunkIteratorFactory = chunkIteratorFactory;
        this.payloadIteratorFactory = payloadIteratorFactory;
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
            if (payloadIteratorFactory != null) {
                ByteSequence payload = chunkStoreReader.readPayloadSequence();
                if (payload != null) {
                    iterator = payloadIteratorFactory.apply(payload);
                } else {
                    iterator = null;
                }
                return;
            }
            Chunk chunk = chunkStoreReader.read();
            if (chunk == null) {
                iterator = null;
                return;
            }
            iterator = chunkIteratorFactory.apply(chunk);
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
