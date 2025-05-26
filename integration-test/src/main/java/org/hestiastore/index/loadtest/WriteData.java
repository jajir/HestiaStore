package org.hestiastore.index.loadtest;

import java.util.Objects;
import java.util.Random;

import org.hestiastore.index.benchmark.DataProvider;
import org.hestiastore.index.sst.Index;

public class WriteData {

    private final static String KEY_SUFFIX = "-bereke-prase-veverka-jede-ryhle";
    private final static Random RANDOM = new Random();
    private final static DataProvider DATA_PROVIDER = new DataProvider();
    private final Index<String, Long> index;

    public WriteData(final Index<String, Long> index) {
        this.index = Objects.requireNonNull(index, "index must not be null");
    }

    public void write(final long count) {
        for (long i = 0; i < count; i++) {
            final String key = DATA_PROVIDER.wrap(i) + KEY_SUFFIX;
            final Long value = RANDOM.nextLong();
            index.put(key, value);
        }
        index.flush();
        index.close();
    }

}
