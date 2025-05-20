package com.coroptis.index.sst;

import java.util.Objects;
import java.util.stream.Stream;

import org.slf4j.MDC;

import com.coroptis.index.Pair;
import com.coroptis.index.log.LoggedKey;
import com.coroptis.index.unsorteddatafile.UnsortedDataFileStreamer;

public class IndexContextLoggingAdapter<K, V> implements Index<K, V> {

    private final IndexConfiguration indexConf;
    private final Index<K, V> index;

    IndexContextLoggingAdapter(final IndexConfiguration indexConf,
            final Index<K, V> index) {
        this.indexConf = Objects.requireNonNull(indexConf,
                "Index configuration cannot be null");
        this.index = Objects.requireNonNull(index, "Index cannot be null");
    }

    private void setContext() {
        MDC.put("index.name", indexConf.getIndexName());
    }

    private void clearContext() {
        MDC.clear();
    }

    @Override
    public void put(final K key, final V value) {
        setContext();
        try {
            index.put(key, value);
        } finally {
            clearContext();
        }
    }

    @Override
    public V get(final K key) {
        setContext();
        try {
            return index.get(key);
        } finally {
            clearContext();
        }
    }

    @Override
    public void delete(final K key) {
        setContext();
        try {
            index.delete(key);
        } finally {
            clearContext();
        }
    }

    @Override
    public void close() {
        setContext();
        try {
            index.close();
        } finally {
            clearContext();
        }
    }

    @Override
    public void compact() {
        setContext();
        try {
            index.compact();
        } finally {
            clearContext();
        }
    }

    @Override
    public void flush() {
        setContext();
        try {
            index.flush();
        } finally {
            clearContext();
        }
    }

    @Override
    public Stream<Pair<K, V>> getStream(final SegmentWindow segmentWindows) {
        setContext();
        try {
            return index.getStream(segmentWindows);
        } finally {
            clearContext();
        }
    }

    @Override
    public UnsortedDataFileStreamer<LoggedKey<K>, V> getLogStreamer() {
        setContext();
        try {
            return index.getLogStreamer();
        } finally {
            clearContext();
        }
    }

    @Override
    public void checkAndRepairConsistency() {
        setContext();
        try {
            index.checkAndRepairConsistency();
        } finally {
            clearContext();
        }
    }

    @Override
    public IndexConfiguration getConfiguration() {
        setContext();
        try {
            return index.getConfiguration();
        } finally {
            clearContext();
        }
    }

}
