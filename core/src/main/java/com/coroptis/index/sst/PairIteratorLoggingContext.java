package com.coroptis.index.sst;

import java.util.Objects;

import org.slf4j.MDC;

import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;

public class PairIteratorLoggingContext<K, V> implements PairIterator<K, V> {

    private final PairIterator<K, V> pairIterator;
    private final IndexConf indexConf;

    PairIteratorLoggingContext(final PairIterator<K, V> pairIterator,
            final IndexConf indexConf) {
        this.pairIterator = Objects.requireNonNull(pairIterator,
                "Pair iterator cannot be null");
        this.indexConf = Objects.requireNonNull(indexConf,
                "Index configuration cannot be null");
    }

    @Override
    public boolean hasNext() {
        setContext();
        try {
            return pairIterator.hasNext();
        } finally {
            clearContext();
        }
    }

    @Override
    public Pair<K, V> next() {
        setContext();
        try {
            return pairIterator.next();
        } finally {
            clearContext();
        }
    }

    @Override
    public void close() {
        setContext();
        try {
            pairIterator.close();
        } finally {
            clearContext();
        }
    }

    private void setContext() {
        MDC.put("index.name", indexConf.getIndexName());
    }

    private void clearContext() {
        MDC.clear();
    }
}
