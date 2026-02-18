package org.hestiastore.index.bloomfilter;

import org.hestiastore.index.F;

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

    public double getPercentageOfFalseResponses() {
        if (bloomFilterCalls == 0) {
            return 0;
        }
        return (keyIsNotStored / (double) bloomFilterCalls * 100);
    }

    String getStatsString() {
        if (bloomFilterCalls == 0) {
            return "Bloom filter was not used.";
        }
        return String.format(
                "The Bloom filter was queried %s times "
                        + "and successfully avoided %s index accesses, "
                        + "achieving a %.3f%% effectiveness rate. "
                        + "It returned a positive result %s times, "
                        + "with an observed false positive rate of %.3f%%.",
                F.fmt(bloomFilterCalls), F.fmt(keyIsNotStored),
                getPercentageOfFalseResponses(),
                F.fmt(getCallsWithFalsePositive()),
                getProbabilityOfFalsePositive());
    }

    long getKeyWasStored() {
        return bloomFilterCalls - keyIsNotStored;
    }

    /**
     * Returns total number of Bloom filter requests.
     *
     * @return request count
     */
    public long getRequestCount() {
        return bloomFilterCalls;
    }

    /**
     * Returns number of requests refused by Bloom filter (definite miss).
     *
     * @return refused request count
     */
    public long getRefusedCount() {
        return keyIsNotStored;
    }

    /**
     * Returns number of positive Bloom responses (possible hit).
     *
     * @return positive response count
     */
    public long getPositiveCount() {
        return getKeyWasStored();
    }

    /**
     * Returns number of false-positive responses.
     *
     * @return false-positive count
     */
    public long getFalsePositiveCount() {
        return falsePositive;
    }

    double getProbabilityOfFalsePositive() {
        return falsePositive / ((double) getKeyWasStored()) * 100F;
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

    private long getCallsWithFalsePositive() {
        return bloomFilterCalls - keyIsNotStored;
    }

}
