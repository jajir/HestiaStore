package com.coroptis.index.segment;

public interface SegmentCompacter<K, V> {

    void optionallyCompact();

    void forceCompact();

}
