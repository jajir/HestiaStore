package org.hestiastore.index.sst;

import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIterator;
import org.hestiastore.index.Vldtn;
import org.slf4j.MDC;

public class PairIteratorLoggingContext<K, V> implements PairIterator<K, V> {

    private final PairIterator<K, V> pairIterator;
    private final IndexConfiguration<K, V> indexConf;

    PairIteratorLoggingContext(final PairIterator<K, V> pairIterator,
            final IndexConfiguration<K, V> indexConf) {
        this.pairIterator = Vldtn.requireNonNull(pairIterator, "pairIterator");
        this.indexConf = Vldtn.requireNonNull(indexConf, "indexConf");
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
        MDC.remove("index.name");
    }
}
