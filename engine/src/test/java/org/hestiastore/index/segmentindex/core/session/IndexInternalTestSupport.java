package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
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
        final SegmentIndexSessionResources<K, V> sessionResources =
                new SegmentIndexSessionResources<>();
        try {
            sessionResources.acquireDirectoryLock(directory);
            sessionResources.createSessionInfrastructure();
            sessionResources.createRuntime(directory, keyTypeDescriptor,
                    valueTypeDescriptor, configuration, executorRegistry);
            final IndexInternal<K, V> index = sessionResources.createIndex(
                    configuration, keyTypeDescriptor);
            sessionResources.recoverFromWal();
            sessionResources.cleanupOrphanedSegmentDirectories();
            sessionResources.markReady();
            if (sessionResources.wasStaleLockRecovered()) {
                sessionResources.runStartupConsistencyCheck();
            }
            sessionResources.requestFullSplitScan();
            return index;
        } catch (final RuntimeException failure) {
            sessionResources.closeRuntimeAfterFailedInitialization();
            throw failure;
        }
    }
}
