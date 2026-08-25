package org.hestiastore.index.senku.internal;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import java.util.stream.Collectors;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileWriter;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SenkuMetadataCodecTest {

    private MemDirectory directory;

    @BeforeEach
    void setUp() {
        directory = new MemDirectory();
    }

    @Test
    void flushManifest_roundTripsAndPublishesCommittedNameLast() {
        SenkuMetadataCodec.publishFlushManifest(directory, 2);

        assertEquals(2, SenkuMetadataCodec.readFlushPartCount(directory));
        assertEquals(Set.of(SenkuFileNames.MANIFEST_FILE), fileNames(directory));
        assertFalse(directory.isFileExists("manifest.properties.tmp"));
    }

    @Test
    void runManifest_roundTripsEmptyAndNonEmptyCounts() {
        SenkuMetadataCodec.publishRunManifest(directory,
                new SenkuRunManifest(2_000, 125_026L));

        final SenkuRunManifest actual = SenkuMetadataCodec
                .readRunManifest(directory);
        assertEquals(2_000, actual.partCount());
        assertEquals(125_026L, actual.recordCount());

        final MemDirectory emptyDirectory = new MemDirectory();
        SenkuMetadataCodec.publishRunManifest(emptyDirectory,
                new SenkuRunManifest(0, 0L));
        final SenkuRunManifest empty = SenkuMetadataCodec
                .readRunManifest(emptyDirectory);
        assertEquals(0, empty.partCount());
        assertEquals(0L, empty.recordCount());
    }

    @Test
    void readyManifest_roundTripsShardCount() {
        SenkuMetadataCodec.publishReady(directory, 128);

        assertEquals(128,
                SenkuMetadataCodec.readReadyShardCount(directory));
        assertEquals(Set.of(SenkuFileNames.READY_FILE), fileNames(directory));
    }

    @Test
    void readers_rejectMissingMetadata() {
        assertThrows(IndexException.class,
                () -> SenkuMetadataCodec.readFlushPartCount(directory));
        assertThrows(IndexException.class,
                () -> SenkuMetadataCodec.readRunManifest(directory));
        assertThrows(IndexException.class,
                () -> SenkuMetadataCodec.readReadyShardCount(directory));
    }

    @Test
    void readers_rejectMissingAndExtraProperties() {
        writeProperties(directory, SenkuFileNames.MANIFEST_FILE,
                "recordCount=1\n");
        assertThrows(IndexException.class,
                () -> SenkuMetadataCodec.readFlushPartCount(directory));

        final MemDirectory extraDirectory = new MemDirectory();
        writeProperties(extraDirectory, SenkuFileNames.MANIFEST_FILE,
                "partCount=1\nextra=2\n");
        assertThrows(IndexException.class,
                () -> SenkuMetadataCodec.readFlushPartCount(extraDirectory));
    }

    @Test
    void readers_rejectMalformedAndNonCanonicalNumbers() {
        writeProperties(directory, SenkuFileNames.MANIFEST_FILE,
                "partCount=abc\n");
        assertThrows(IndexException.class,
                () -> SenkuMetadataCodec.readFlushPartCount(directory));

        final MemDirectory leadingZeroDirectory = new MemDirectory();
        writeProperties(leadingZeroDirectory, SenkuFileNames.MANIFEST_FILE,
                "partCount=01\n");
        assertThrows(IndexException.class, () -> SenkuMetadataCodec
                .readFlushPartCount(leadingZeroDirectory));
    }

    @Test
    void readers_rejectInvalidCountRelationships() {
        writeProperties(directory, SenkuFileNames.MANIFEST_FILE,
                "partCount=0\n");
        assertThrows(IndexException.class,
                () -> SenkuMetadataCodec.readFlushPartCount(directory));

        final MemDirectory runDirectory = new MemDirectory();
        writeProperties(runDirectory, SenkuFileNames.MANIFEST_FILE,
                "partCount=0\nrecordCount=1\n");
        assertThrows(IndexException.class,
                () -> SenkuMetadataCodec.readRunManifest(runDirectory));

        final MemDirectory readyDirectory = new MemDirectory();
        writeProperties(readyDirectory, SenkuFileNames.READY_FILE,
                "shardCount=0\n");
        assertThrows(IndexException.class,
                () -> SenkuMetadataCodec.readReadyShardCount(readyDirectory));
    }

    @Test
    void publishers_rejectExistingCommittedOrTemporaryMetadata() {
        directory.touch(SenkuFileNames.MANIFEST_FILE);
        assertThrows(IndexException.class,
                () -> SenkuMetadataCodec.publishFlushManifest(directory, 1));

        final MemDirectory temporaryDirectory = new MemDirectory();
        temporaryDirectory.touch("manifest.properties.tmp");
        assertThrows(IndexException.class, () -> SenkuMetadataCodec
                .publishFlushManifest(temporaryDirectory, 1));
    }

    @Test
    void publisherLeavesTemporaryFileWhenRenameFails() {
        final MemDirectory failingDirectory = new MemDirectory() {
            @Override
            public void renameFile(final String currentFileName,
                    final String newFileName) {
                throw new IndexException("rename failed");
            }
        };

        assertThrows(IndexException.class,
                () -> SenkuMetadataCodec.publishReady(failingDirectory, 1));
        assertTrue(failingDirectory.isFileExists("ready.properties.tmp"));
        assertFalse(failingDirectory.isFileExists("ready.properties"));
    }

    private static Set<String> fileNames(final Directory source) {
        return source.getFileNames().collect(Collectors.toSet());
    }

    private static void writeProperties(final Directory target,
            final String fileName, final String content) {
        try (FileWriter writer = target.getFileWriter(fileName)) {
            writer.write(content.getBytes(ISO_8859_1));
        }
    }
}
