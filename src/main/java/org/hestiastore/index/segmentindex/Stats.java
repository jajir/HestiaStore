package org.hestiastore.index.segmentindex;

import java.util.concurrent.atomic.LongAdder;

/**
 * Holds statistic informations about index utilization.
 * 
 * @author honza
 *
 */
public class Stats {
    private final LongAdder putCx = new LongAdder();
    private final LongAdder getCx = new LongAdder();
    private final LongAdder deleteCx = new LongAdder();

    Stats() {

    }

    void incPutCx() {
        putCx.increment();
    }

    void incGetCx() {
        getCx.increment();
    }

    void incDeleteCx() {
        deleteCx.increment();
    }

    /**
     * Returns the number of put operations recorded.
     *
     * @return put count
     */
    public long getPutCx() {
        return putCx.sum();
    }

    /**
     * Returns the number of get operations recorded.
     *
     * @return get count
     */
    public long getGetCx() {
        return getCx.sum();
    }

    /**
     * Returns the number of delete operations recorded.
     *
     * @return delete count
     */
    public long getDeleteCx() {
        return deleteCx.sum();
    }

}
