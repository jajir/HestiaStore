package com.coroptis.index.log;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.CloseableSpliterator;
import com.coroptis.index.Pair;
import com.coroptis.index.PairWriter;
import com.coroptis.index.unsorteddatafile.UnsortedDataFileStreamer;

public class LogEmptyImpl<K, V> implements Log<K, V> {

    @Override
    public LogWriter<K, V> openWriter() {
        return new LogWriter<K, V>() {

            @Override
            public void post(K key, V value) {
                // Intentionally do nothing
            }

            @Override
            public void delete(K key, V value) {
                // Intentionally do nothing
            }

            @Override
            public void close() {
                // Intentionally do nothing
            }
        };
    }

    @Override
    public UnsortedDataFileStreamer<LoggedKey<K>, V> openStreamer() {
        return new UnsortedDataFileStreamer<LoggedKey<K>, V>(null);
    }

}
