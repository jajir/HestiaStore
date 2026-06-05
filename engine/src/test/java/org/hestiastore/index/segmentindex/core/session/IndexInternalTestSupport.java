package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.persistence.IndexConfigurationStorage;
import org.hestiastore.index.segmentindex.core.bootstrap.SegmentIndexFactory;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;

final class IndexInternalTestSupport {

    private IndexInternalTestSupport() {
    }

    @SuppressWarnings("java:S107")
    static <K, V> IndexInternal<K, V> createStarted(
            final Directory directory,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final EffectiveIndexConfiguration<K, V> configuration,
            final ExecutorRegistry executorRegistry) {
        Vldtn.requireNonNull(keyTypeDescriptor, "keyTypeDescriptor");
        Vldtn.requireNonNull(valueTypeDescriptor, "valueTypeDescriptor");
        try {
            new IndexConfigurationStorage<K, V>(directory).save(configuration);
            return SegmentIndexFactory.openStored(directory);
        } finally {
            if (!executorRegistry.wasClosed()) {
                executorRegistry.close();
            }
        }
    }
}
