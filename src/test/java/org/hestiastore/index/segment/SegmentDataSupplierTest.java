package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import java.util.List;

import org.hestiastore.index.bloomfilter.BloomFilter;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.hestiastore.index.scarceindex.ScarceSegmentIndex;
import org.junit.jupiter.api.Test;

class SegmentDataSupplierTest {

    @Test
    void supplierCreatesNewInstancesPerCall() {
        final SegmentFiles<Integer, String> files = new SegmentFiles<>(
                AsyncDirectoryAdapter.wrap(new MemDirectory()), SegmentId.of(1),
                new TypeDescriptorInteger(), new TypeDescriptorShortString(),
                1024, List.of(new ChunkFilterDoNothing()),
                List.of(new ChunkFilterDoNothing()));
        final SegmentConf conf = new SegmentConf(5, 6, 10, 2, 1, 1024, 0.01D,
                1024, List.of(new ChunkFilterDoNothing()),
                List.of(new ChunkFilterDoNothing()));
        final SegmentPropertiesManager properties = new SegmentPropertiesManager(
                files.getAsyncDirectory(), files.getId());
        final SegmentDataSupplier<Integer, String> supplier = new SegmentDataSupplier<>(
                files, conf, properties);

        final SegmentDeltaCache<Integer, String> deltaCache1 = supplier
                .getSegmentDeltaCache();
        final SegmentDeltaCache<Integer, String> deltaCache2 = supplier
                .getSegmentDeltaCache();

        assertNotNull(deltaCache1);
        assertNotNull(deltaCache2);
        assertNotSame(deltaCache1, deltaCache2);

        final BloomFilter<Integer> bloom1 = supplier.getBloomFilter();
        final BloomFilter<Integer> bloom2 = supplier.getBloomFilter();
        assertNotNull(bloom1);
        assertNotNull(bloom2);
        assertNotSame(bloom1, bloom2);
        bloom1.close();
        bloom2.close();

        final ScarceSegmentIndex<Integer> scarce1 = supplier.getScarceIndex();
        final ScarceSegmentIndex<Integer> scarce2 = supplier.getScarceIndex();
        assertNotNull(scarce1);
        assertNotNull(scarce2);
        assertNotSame(scarce1, scarce2);
        scarce1.close();
        scarce2.close();
    }
}
