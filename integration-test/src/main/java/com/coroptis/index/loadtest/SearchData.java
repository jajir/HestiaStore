package com.coroptis.index.loadtest;

import java.util.Objects;
import java.util.Random;

import com.coroptis.index.benchmark.DataProvider;
import com.hestiastore.index.sst.Index;

public class SearchData {
    private final static String KEY_SUFFIX = "-bereke-prase-veverka-jede-ryhle";
    private final static Random RANDOM = new Random();
    private final static DataProvider DATA_PROVIDER = new DataProvider();
    private final Index<String, Long> index;

    public SearchData(final Index<String, Long> index) {
        this.index = Objects.requireNonNull(index, "index must not be null");
    }

    public void search(final long count, final long maxKey) {
        for (long i = 0; i < count; i++) {
            final String key = DATA_PROVIDER.wrap(RANDOM.nextInt((int) maxKey))
                    + KEY_SUFFIX;
            index.get(key);
        }
    }

}
