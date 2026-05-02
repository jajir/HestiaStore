package org.hestiastore.index.segmentindex.core.bootstrap;

import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.ResolvedIndexConfiguration;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.session.IndexInternalConcurrent;

/**
 * Internal bootstrap seam used by JUnit tests to inject opening-index failures
 * into startup rollback paths.
 * <p>
 * Production bootstrap uses the default method reference in
 * {@link SegmentIndexBootstrapTransaction}; this interface exists only to keep
 * those failure-path tests focused and deterministic.
 * </p>
 *
 * @param <K> key type
 * @param <V> value type
 */
@FunctionalInterface
interface OpeningIndexFactory<K, V> {

    IndexInternalConcurrent<K, V> create(Directory directory,
            TypeDescriptor<K> keyTypeDescriptor,
            TypeDescriptor<V> valueTypeDescriptor,
            IndexConfiguration<K, V> configuration,
            ResolvedIndexConfiguration<K, V> runtimeConfiguration,
            ExecutorRegistry executorRegistry);
}
