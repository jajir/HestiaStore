package com.coroptis.index.sst;

import java.util.Objects;
import java.util.stream.Stream;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.Pair;
import com.coroptis.index.log.LoggedKey;
import com.coroptis.index.unsorteddatafile.UnsortedDataFileStreamer;

public interface Index<K, V> extends CloseableResource {

    public static <M, N> IndexBuilder<M, N> builder() {
        return new IndexBuilder<>();
    }

    void put(K key, V value);

    default void put(final Pair<K, V> pair) {
        Objects.requireNonNull(pair, "Pair cant be null");
        put(pair.getKey(), pair.getValue());
    }

    V get(K key);

    void delete(K key);

    void compact();

    void flush();

    /**
     * Went through all records. In fact read all index data. Doesn't use
     * indexes and caches in segments.
     * 
     * @return stream of all data.
     */
    Stream<Pair<K, V>> getStream();

    UnsortedDataFileStreamer<LoggedKey<K>, V> getLogStreamer();
}
