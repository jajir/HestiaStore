package org.hestiastore.index.segmentindex.core.storage;

import static org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfigurationTestSupport.effective;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.user.IndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.user.IndexWalConfiguration;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateView;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class WalRuntimeInitializationTest {

    @Mock
    private WalRuntime<Integer, String> walRuntime;

    @Mock
    private WalCheckpointDurableState durableState;

    @Mock
    private WalRuntimeFailureHandler failureHandler;

    private SegmentIndexStateView stateView;
    private AtomicLong lastAppliedWalLsn;

    @BeforeEach
    void setUp() {
        stateView = () -> SegmentIndexState.READY;
        lastAppliedWalLsn = new AtomicLong(0L);
    }

    @Test
    void constructorStoresInitializationCollaborators() {
        final EffectiveIndexConfiguration<Integer, String> configuration =
                effective(buildConf(IndexWalConfiguration.builder().build()));

        final WalRuntimeInitialization<Integer, String> initialization =
                new WalRuntimeInitialization<>(configuration, walRuntime,
                        durableState, stateView, failureHandler,
                        lastAppliedWalLsn);

        assertSame(configuration, initialization.configuration());
        assertSame(walRuntime, initialization.walRuntime());
        assertSame(durableState, initialization.durableState());
        assertSame(stateView, initialization.stateView());
        assertSame(failureHandler, initialization.failureHandler());
        assertSame(lastAppliedWalLsn, initialization.lastAppliedWalLsn());
    }

    @Test
    void constructorAllowsMissingWalRuntimeWhenWalIsDisabled() {
        final EffectiveIndexConfiguration<Integer, String> configuration =
                effective(buildConf(IndexWalConfiguration.EMPTY));

        final WalRuntimeInitialization<Integer, String> initialization =
                new WalRuntimeInitialization<>(configuration, null,
                        durableState, stateView, failureHandler,
                        lastAppliedWalLsn);

        assertSame(configuration, initialization.configuration());
    }

    @Test
    void constructorRejectsMissingWalRuntimeWhenWalIsEnabled() {
        final EffectiveIndexConfiguration<Integer, String> configuration =
                effective(buildConf(IndexWalConfiguration.builder().build()));

        assertThrows(IllegalArgumentException.class,
                () -> new WalRuntimeInitialization<>(configuration, null,
                        durableState, stateView, failureHandler,
                        lastAppliedWalLsn));
    }

    private IndexConfiguration<Integer, String> buildConf(
            final IndexWalConfiguration walConfiguration) {
        return IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity.keyTypeDescriptor(
                        new TypeDescriptorInteger()))
                .identity(identity -> identity.valueTypeDescriptor(
                        new TypeDescriptorShortString()))
                .identity(identity -> identity.name(
                        "wal-runtime-initialization-test"))
                .logging(logging -> logging.contextEnabled(false))
                .segment(segment -> segment.cacheKeyLimit(10))
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(5))
                .writePath(writePath -> writePath
                        .maintenanceWriteCacheKeyLimit(7))
                .writePath(writePath -> writePath.indexBufferedWriteKeyLimit(9))
                .segment(segment -> segment.chunkKeyLimit(2))
                .segment(segment -> segment.maxKeys(100))
                .writePath(writePath -> writePath.segmentSplitKeyThreshold(50))
                .segment(segment -> segment.cachedSegmentLimit(3))
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(1))
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024))
                .bloomFilter(bloomFilter -> bloomFilter
                        .falsePositiveProbability(0.01D))
                .io(io -> io.diskBufferSizeBytes(1024))
                .filters(filters -> filters.encodingFilters(
                        List.of(new ChunkFilterDoNothing())))
                .filters(filters -> filters.decodingFilters(
                        List.of(new ChunkFilterDoNothing())))
                .wal(wal -> wal.configuration(walConfiguration))
                .build();
    }
}
