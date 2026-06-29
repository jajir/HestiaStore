package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.persistence.IndexConfigurationStore;

final class SegmentIndexSessionTestSupport {

    private SegmentIndexSessionTestSupport() {
    }

    @SuppressWarnings("java:S107")
    static <K, V> SegmentIndex<K, V> createStarted(
            final Directory directory,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final EffectiveIndexConfiguration<K, V> configuration) {
        Vldtn.requireNonNull(keyTypeDescriptor, "keyTypeDescriptor");
        Vldtn.requireNonNull(valueTypeDescriptor, "valueTypeDescriptor");
        new IndexConfigurationStore<K, V>(directory).save(configuration);
        return SegmentIndex.open(directory);
    }
}
