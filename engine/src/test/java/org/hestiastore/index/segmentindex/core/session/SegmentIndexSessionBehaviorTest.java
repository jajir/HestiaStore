package org.hestiastore.index.segmentindex.core.session;

import static org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfigurationTestSupport.effective;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.configuration.api.IndexConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentIndexSessionBehaviorTest {

    private SegmentIndex<Integer, String> index;

    @BeforeEach
    void setUp() {
        final IndexConfiguration<Integer, String> conf = buildConf();
        index = SegmentIndexSessionTestSupport.createStarted(
                new MemDirectory(),
                new TypeDescriptorInteger(),
                new TypeDescriptorShortString(), effective(conf));
    }

    @AfterEach
    void tearDown() {
        if (index != null && !index.wasClosed()) {
            index.close();
        }
    }

    @Test
    void putGetAndDeleteRoundTrip() {
        index.put(1, "one");

        assertEquals("one", index.get(1));

        index.delete(1);
        assertNull(index.get(1));
    }

    @Test
    void closeReleasesDirectoryLock() {
        final MemDirectory directory = new MemDirectory();
        final IndexConfiguration<Integer, String> conf = buildConf();
        try (SegmentIndex<Integer, String> lockedIndex = SegmentIndexSessionTestSupport.createStarted(directory,
                        new TypeDescriptorInteger(),
                        new TypeDescriptorShortString(), effective(conf))) {
            assertTrue(directory.isFileExists(".lock"));
        }

        assertFalse(directory.isFileExists(".lock"));
    }

    @Test
    void failedOpenKeepsDirectoryLock() {
        final MemDirectory directory = spy(new MemDirectory());
        final IndexConfiguration<Integer, String> conf = buildConf();
        doThrow(new IllegalStateException("open failed")).when(directory)
                .openSubDirectory(anyString());

        assertThrows(RuntimeException.class,
                () -> SegmentIndexSessionTestSupport.createStarted(directory,
                        new TypeDescriptorInteger(),
                        new TypeDescriptorShortString(), effective(conf)));

        assertTrue(directory.isFileExists(".lock"));
    }

    private IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity.keyTypeDescriptor(new TypeDescriptorInteger()))
                .identity(identity -> identity.valueTypeDescriptor(new TypeDescriptorShortString()))
                .identity(identity -> identity.name("segment-index-impl-test"))
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

}
