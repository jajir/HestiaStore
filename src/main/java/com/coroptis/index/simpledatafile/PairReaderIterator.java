package com.coroptis.index.simpledatafile;

import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.Pair;
import com.coroptis.index.CloseablePairReader;

public class PairReaderIterator<K, V>
        implements Iterator<Pair<K, V>>, CloseableResource {

    private final CloseablePairReader<K, V> pairFileReader;
    private Pair<K, V> pair;

    public PairReaderIterator(final CloseablePairReader<K, V> pairFileReader) {
        this.pairFileReader = Objects.requireNonNull(pairFileReader);
        tryReadNext();
    }

    public Optional<Pair<K, V>> readCurrent() {
        return Optional.ofNullable(pair);
    }

    @Override
    public boolean hasNext() {
        return pair != null;
    }

    @Override
    public Pair<K, V> next() {
        final Pair<K, V> out = pair;
        tryReadNext();
        return out;
    }

    private void tryReadNext() {
        pair = pairFileReader.read();
    }

    @Override
    public void close() {
        pairFileReader.close();
    }

}
