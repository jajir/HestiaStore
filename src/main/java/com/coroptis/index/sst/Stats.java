package com.coroptis.index.sst;

/**
 * Holds statistic informations about index utilization.
 * 
 * @author honza
 *
 */
public class Stats {
    private long putCx = 0;
    private long getCx = 0;
    private long deleteCx = 0;

    Stats() {

    }

    void incPutCx() {
        putCx++;
    }

    void incGetCx() {
        getCx++;
    }

    void incDeleteCx() {
        deleteCx++;
    }

    public long getPutCx() {
        return putCx;
    }

    public long getGetCx() {
        return getCx;
    }

    public long getDeleteCx() {
        return deleteCx;
    }
    
}
