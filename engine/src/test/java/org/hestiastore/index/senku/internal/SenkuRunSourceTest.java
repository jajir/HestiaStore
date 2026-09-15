package org.hestiastore.index.senku.internal;

import static org.hestiastore.index.senku.internal.LargeFileTestSupport.DATA_BLOCK_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class SenkuRunSourceTest {

    @Test
    void exposesIdentityAndCreatesLazyMergeSource() {
        final MemDirectory directory = new MemDirectory();
        final SenkuRunManifest manifest = new SenkuRunManifest(0, 0L);
        final SenkuRunSource source = new SenkuRunSource(directory, 1, 2, 3L,
                manifest);

        assertEquals(1, source.shardId());
        assertEquals(2, source.level());
        assertEquals(3L, source.runId());
        assertEquals(manifest, source.manifest());
        assertEquals(0L,
                source.mergeSource(DATA_BLOCK_SIZE, 1L).recordCount());
    }

    @Test
    void openingMergeSourceValidatesPhysicalParts() {
        final MemDirectory directory = new MemDirectory();
        directory.touch(SenkuFileNames.partFile(0));
        final SenkuRunSource source = new SenkuRunSource(directory, 0, 0, 0L,
                new SenkuRunManifest(0, 0L));

        assertThrows(IndexException.class,
                () -> source.mergeSource(DATA_BLOCK_SIZE, 1L).open(
                        new TypeDescriptorInteger(), new TypeDescriptorLong()));
    }

    @Test
    void constructorRejectsInvalidValues() {
        final MemDirectory directory = new MemDirectory();
        final SenkuRunManifest manifest = new SenkuRunManifest(0, 0L);

        assertThrows(IllegalArgumentException.class,
                () -> new SenkuRunSource(null, 0, 0, 0L, manifest));
        assertThrows(IllegalArgumentException.class,
                () -> new SenkuRunSource(directory, -1, 0, 0L, manifest));
        assertThrows(IllegalArgumentException.class,
                () -> new SenkuRunSource(directory, 0, -1, 0L, manifest));
        assertThrows(IllegalArgumentException.class,
                () -> new SenkuRunSource(directory, 0, 0, -1L, manifest));
        assertThrows(IllegalArgumentException.class,
                () -> new SenkuRunSource(directory, 0, 0, 0L, null));
    }
}
