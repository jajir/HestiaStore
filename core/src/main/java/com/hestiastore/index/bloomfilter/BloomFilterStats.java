package com.hestiastore.index.bloomfilter;

import com.hestiastore.index.F;

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

    private long falsePositive = 0;

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
        if (bloomFilterCalls == 0) {
            return "Bloom filter was not used.";
        }
        return String.format("Bloom filter was used %s times "
                + "and key was not stored in %s times, it's %s%% ratio. "
                + "Probability of false positive is %.3f%%",
                F.fmt(bloomFilterCalls), F.fmt(keyIsNotStored), getRatio(),
                getProbabilityOfFalsePositive());
    }

    long getKeyWasStored() {
        return bloomFilterCalls - keyIsNotStored;
    }

    float getProbabilityOfFalsePositive() {
        return falsePositive / ((float) getKeyWasStored()) * 100F;
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

    void incrementFalsePositive() {
        this.falsePositive++;
    }

}
