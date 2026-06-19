package org.hestiastore.index.segmentindex.core.bootstrap;

import static org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfigurationTestSupport.effective;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.user.IndexConfiguration;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentIndexBootstrapStateTest {

    @Mock
    private ExecutorRegistry executorRegistry;

    @Mock
    private SegmentIndex<Integer, String> index;

    @Test
    void productGettersThrowBeforeMatchingSetterIsCalled() {
        final SegmentIndexBootstrapState<Integer, String> state =
                new SegmentIndexBootstrapState<>();

        assertThrows(IllegalStateException.class, state::getConfiguration);
        assertThrows(IllegalStateException.class,
                state::isConfigurationWriteRequired);
        assertThrows(IllegalStateException.class, state::getKeyTypeDescriptor);
        assertThrows(IllegalStateException.class,
                state::getValueTypeDescriptor);
        assertThrows(IllegalStateException.class, state::getExecutorRegistry);
        assertThrows(IllegalStateException.class, state::getIndex);
        assertFalse(state.hasExecutorRegistry());
    }

    @Test
    void settersStoreInitializedProducts() {
        final SegmentIndexBootstrapState<Integer, String> state =
                new SegmentIndexBootstrapState<>();
        final EffectiveIndexConfiguration<Integer, String> configuration =
                effective(buildConf());
        final TypeDescriptorInteger keyTypeDescriptor =
                new TypeDescriptorInteger();
        final TypeDescriptorShortString valueTypeDescriptor =
                new TypeDescriptorShortString();

        state.setConfiguration(configuration);
        state.setConfigurationWriteRequired(true);
        state.setKeyTypeDescriptor(keyTypeDescriptor);
        state.setValueTypeDescriptor(valueTypeDescriptor);
        state.setExecutorRegistry(executorRegistry);
        state.setIndex(index);

        assertSame(configuration, state.getConfiguration());
        assertTrue(state.isConfigurationWriteRequired());
        assertSame(keyTypeDescriptor, state.getKeyTypeDescriptor());
        assertSame(valueTypeDescriptor, state.getValueTypeDescriptor());
        assertSame(executorRegistry, state.getExecutorRegistry());
        assertSame(index, state.getIndex());
        assertTrue(state.hasExecutorRegistry());
    }

    @Test
    void settersRejectNullProducts() {
        final SegmentIndexBootstrapState<Integer, String> state =
                new SegmentIndexBootstrapState<>();

        assertThrows(IllegalArgumentException.class,
                () -> state.setConfiguration(null));
        assertThrows(IllegalArgumentException.class,
                () -> state.setKeyTypeDescriptor(null));
        assertThrows(IllegalArgumentException.class,
                () -> state.setValueTypeDescriptor(null));
        assertThrows(IllegalArgumentException.class,
                () -> state.setExecutorRegistry(null));
        assertThrows(IllegalArgumentException.class, () -> state.setIndex(null));
    }

    private static IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity.keyTypeDescriptor(
                        new TypeDescriptorInteger()))
                .identity(identity -> identity.valueTypeDescriptor(
                        new TypeDescriptorShortString()))
                .identity(identity -> identity.name(
                        "segment-index-bootstrap-state-test"))
                .logging(logging -> logging.contextEnabled(false))
                .segment(segment -> segment.cacheKeyLimit(10))
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(5))
                .writePath(writePath -> writePath
                        .maintenanceWriteCacheKeyLimit(6))
                .segment(segment -> segment.chunkKeyLimit(2))
                .segment(segment -> segment.maxKeys(100))
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
                .build();
    }
}
