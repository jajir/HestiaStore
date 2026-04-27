package org.hestiastore.index.segmentindex.wal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;

import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

class WalMetadataCatalogTest {

    @Test
    void loadRecoveryCatalogPromotesValidCheckpointTempFile() {
        final WalStorageMem storage = new WalStorageMem(new MemDirectory());
        final WalMetadataCatalog catalog = new WalMetadataCatalog(storage,
                LoggerFactory.getLogger(WalMetadataCatalogTest.class));

        catalog.ensureFormatMarker();
        final byte[] checkpoint = "5".getBytes(StandardCharsets.US_ASCII);
        storage.overwrite("checkpoint.meta.tmp", checkpoint, 0,
                checkpoint.length);

        final WalCatalogView view = catalog.loadRecoveryCatalog();

        assertEquals(5L, view.checkpointLsn());
        assertTrue(storage.exists("checkpoint.meta"));
        assertFalse(storage.exists("checkpoint.meta.tmp"));
    }

    @Test
    void ensureFormatMarkerCreatesCanonicalMetadata() {
        final WalStorageMem storage = new WalStorageMem(new MemDirectory());
        final WalMetadataCatalog catalog = new WalMetadataCatalog(storage,
                LoggerFactory.getLogger(WalMetadataCatalogTest.class));

        catalog.ensureFormatMarker();

        assertTrue(storage.exists("format.meta"));
    }
}
