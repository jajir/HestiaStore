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

    public long getPutCx() {
        return putCx.sum();
    }

    public long getGetCx() {
        return getCx.sum();
    }

    public long getDeleteCx() {
        return deleteCx.sum();
    }

}
