package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistryFixture;
import org.hestiastore.index.segmentindex.metrics.Stats;
import org.hestiastore.index.segmentindex.core.storage.SegmentIndexCoreStorage;
import org.hestiastore.index.segmentindex.core.storage.SegmentIndexCoreStorageFactory;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

class SegmentIndexRuntimeServicesFactoryTest {

    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
    private final TypeDescriptorShortString tds = new TypeDescriptorShortString();

    private ExecutorRegistry executorRegistry;
    private SegmentIndexCoreStorage<Integer, String> coreStorage;
    private WalRuntime<Integer, String> walRuntime;

    @BeforeEach
    void setUp() {
        executorRegistry = ExecutorRegistryFixture.from(buildConf());
        coreStorage = new SegmentIndexCoreStorageFactory<>(newRequest(),
                new SegmentIndexRuntimeGraphBuilder.ResourceCreationObserver<>() {
                }).create();
        walRuntime = WalRuntime.open(new MemDirectory(), buildConf().wal(),
                tdi, tds);
    }

    @AfterEach
    void tearDown() {
        RuntimeException failure = null;
        if (walRuntime != null) {
            failure = closeIgnoringFailure(walRuntime::close, failure);
        }
        if (coreStorage != null) {
            failure = closeIgnoringFailure(coreStorage.segmentRegistry()::close,
                    failure);
            failure = closeIgnoringFailure(coreStorage.keyToSegmentMap()::close,
                    failure);
        }
        if (executorRegistry != null && !executorRegistry.wasClosed()) {
            failure = closeIgnoringFailure(executorRegistry::close, failure);
        }
        if (failure != null) {
            throw failure;
        }
    }

    @Test
    void createBuildsRuntimeCollaborators() {
        final SegmentIndexRuntimeInputs<Integer, String> request =
                newRequest();
        final SegmentTopologyRuntime<Integer, String> topologyRuntime =
                new SegmentTopologyRuntime<>(request, coreStorage);
        final SegmentIndexRuntimeServicesFactory<Integer, String> factory =
                new SegmentIndexRuntimeServicesFactory<>(request, coreStorage,
                        topologyRuntime);

        final SegmentIndexRuntimeServices<Integer, String> services =
                factory.create(walRuntime);

        assertNotNull(services.walCoordinator());
        assertNotNull(services.operationAccess());
        assertNotNull(services.maintenance());
        assertNotNull(services.metricsSnapshotSupplier());
        assertNotNull(services.runtimeMonitoring());
        assertNotNull(services.runtimeLimitApplier());
        assertNotNull(services.runtimeConfiguration());
    }

    private SegmentIndexRuntimeInputs<Integer, String> newRequest() {
        final IndexConfiguration<Integer, String> conf = buildConf();
        return new SegmentIndexRuntimeInputs<>(
                LoggerFactory.getLogger(getClass()), new MemDirectory(), tdi,
                tds, conf, conf.resolveRuntimeConfiguration(),
                executorRegistry, new Stats(), new AtomicLong(),
                new AtomicLong(), new AtomicLong(),
                () -> SegmentIndexState.READY, failure -> {
                });
    }

    private IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity.keyTypeDescriptor(tdi))
                .identity(identity -> identity.valueTypeDescriptor(tds))
                .identity(identity -> identity.name("segment-index-runtime-services-factory-test"))
                .logging(logging -> logging.contextEnabled(false))
                .segment(segment -> segment.cacheKeyLimit(10))
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(5))
                .writePath(writePath -> writePath.maintenanceWriteCacheKeyLimit(6))
                .segment(segment -> segment.chunkKeyLimit(2))
                .segment(segment -> segment.maxKeys(100))
                .segment(segment -> segment.cachedSegmentLimit(3))
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(1))
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024))
                .bloomFilter(bloomFilter -> bloomFilter.falsePositiveProbability(0.01D))
                .io(io -> io.diskBufferSizeBytes(1024))
                .filters(filters -> filters.encodingFilters(List.of(new ChunkFilterDoNothing())))
                .filters(filters -> filters.decodingFilters(List.of(new ChunkFilterDoNothing())))
                .build();
    }

    private RuntimeException closeIgnoringFailure(final Runnable action,
            final RuntimeException failure) {
        try {
            action.run();
            return failure;
        } catch (final RuntimeException e) {
            if (failure == null) {
                return e;
            }
            failure.addSuppressed(e);
            return failure;
        }
    }
}
