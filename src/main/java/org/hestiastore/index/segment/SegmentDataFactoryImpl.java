package org.hestiastore.index.segment;

import org.hestiastore.index.Vldtn;

/**
 * This factory is used for creating new instances of {@link SegmentData}.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class SegmentDataFactoryImpl<K, V> implements SegmentDataFactory<K, V> {

    private final SegmentDataSupplier<K, V> segmentDataSupplier;

    public SegmentDataFactoryImpl(
            final SegmentDataSupplier<K, V> segmentDataSupplier) {
        this.segmentDataSupplier = Vldtn.requireNonNull(segmentDataSupplier,
                "segmentDataSupplier");
    }

    @Override
    public SegmentData<K, V> getSegmentData() {
        return new SegmentDataLazyLoaded<>(segmentDataSupplier);
    }
}
