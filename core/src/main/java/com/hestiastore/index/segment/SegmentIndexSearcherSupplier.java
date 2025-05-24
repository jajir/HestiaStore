package com.hestiastore.index.segment;

import java.util.function.Supplier;

public interface SegmentIndexSearcherSupplier<K, V>
        extends Supplier<SegmentIndexSearcher<K, V>> {

}
