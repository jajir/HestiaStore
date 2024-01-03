package com.coroptis.index.bloomfilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds basic bloom filter statistics, it allows analyze cache hit ratio:
 * 
 * <code><pre>
 * Bloom filter ratio (%) = [ key is not stored / number of calls] × 100
 * </pre></code>
 * 
 * 
 * @author honza
 *
 */
public class BloomFilterStats {

    private final Logger logger = LoggerFactory
            .getLogger(BloomFilterStats.class);

    private long keyIsNotStored = 0;

    private long bloomFilterCalls = 0;

    void increment(final boolean result) {
        bloomFilterCalls++;
        if (result) {
            keyIsNotStored++;
        }
    }

    public int getRatio() {
        if (bloomFilterCalls == 0) {
            return 0;
        }
        return (int) (keyIsNotStored / (float) bloomFilterCalls * 100);
    }

    void logStats() {
        logger.debug(
                "Bloom filter was called {} times and key was not stored in {} casses it's {}% ratio.",
                bloomFilterCalls, keyIsNotStored, getRatio());
    }

    long getKeyIsNotStored() {
        return keyIsNotStored;
    }

    void setKeyIsNotStored(long keyIsNotStored) {
        this.keyIsNotStored = keyIsNotStored;
    }

    long getBloomFilterCalls() {
        return bloomFilterCalls;
    }

    void setBloomFilterCalls(long bloomFilterCalls) {
        this.bloomFilterCalls = bloomFilterCalls;
    }

}
