package org.hestiastore.index.segment;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;

/**
 * Creates cloned {@link Segment} instances anchored to the same storage
 * characteristics as the original segment.
 */
final class SegmentFactory<K, V> {

    private final Directory directory;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final SegmentConf templateConf;

    SegmentFactory(final Directory directory,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final SegmentConf templateConf) {
        this.directory = Vldtn.requireNonNull(directory, "directory");
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        this.templateConf = Vldtn.requireNonNull(templateConf,
                "segmentConf");
    }

    Segment<K, V> createSegment(final SegmentId segmentId) {
        return Segment.<K, V>builder()
                .withDirectory(directory)
                .withId(segmentId)
                .withKeyTypeDescriptor(keyTypeDescriptor)
                .withValueTypeDescriptor(valueTypeDescriptor)
                .withSegmentConf(new SegmentConf(templateConf))
                .build();
    }
}
