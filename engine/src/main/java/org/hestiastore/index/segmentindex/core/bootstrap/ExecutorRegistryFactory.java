package org.hestiastore.index.segmentindex.core.bootstrap;

import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;

/**
 * Internal bootstrap seam used by JUnit tests to inject executor-registry
 * failures into startup rollback paths.
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
interface ExecutorRegistryFactory<K, V> {

    ExecutorRegistry create(IndexConfiguration<K, V> configuration);
}
