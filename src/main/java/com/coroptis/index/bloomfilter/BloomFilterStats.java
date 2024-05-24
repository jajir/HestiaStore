package com.coroptis.index.bloomfilter;

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

    String getStatsString() {
        return String.format("Bloom filter was called %s times "
                + "and key was not stored in %s casses it's %s%% ratio.",
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
