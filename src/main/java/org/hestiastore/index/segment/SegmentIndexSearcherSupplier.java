package org.hestiastore.index.segment;

import java.util.function.Supplier;

//TODO remove it
@Deprecated
public interface SegmentIndexSearcherSupplier<K, V>
        extends Supplier<SegmentIndexSearcher<K, V>> {

}
