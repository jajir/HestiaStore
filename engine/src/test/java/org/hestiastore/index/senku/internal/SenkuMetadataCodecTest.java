package org.hestiastore.index.senku.internal;

import org.hestiastore.index.chunkentryfile.KeyPageCodecs;
import org.hestiastore.index.chunkstore.Compression;
import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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

    private static final String RANK_FORMAT = "formatVersion=2\nkeyPageCodec=4\n"
            + "compression=zstd\ncompressionLevel=3\nfixedWeightBitCount=7\n"
            + "fixedWeightSetBitCount=3\nfixedWeightParityMasks=3,12\n"
            + "fixedWeightParitySyndrome=1\n";

    @Test
    void rankFormatRoundTripsAllDomainParametersAndEmptyMaskList() {
        final var codec = KeyPageCodecs.longFixedWeightDeltaVarint(7, 3,
                new long[] { 3, 12 }, 1);
        SenkuMetadataCodec.publishStorageFormat(directory,
                new SenkuStorageFormat(codec, Compression.zstd(3)));
        final var actual = SenkuMetadataCodec.readStorageFormat(directory)
                .keyCodec();
        assertEquals(4, actual.getId());
        assertEquals(7, actual.getFixedWeightBitCount());
        assertEquals(3, actual.getFixedWeightSetBitCount());
        assertArrayEquals(new long[] { 3, 12 },
                actual.getFixedWeightParityMasks());
        assertEquals(1, actual.getFixedWeightParitySyndrome());

        final var emptyMasks = new MemDirectory();
        SenkuMetadataCodec.publishStorageFormat(emptyMasks,
                new SenkuStorageFormat(KeyPageCodecs.longFixedWeightDeltaVarint(
                        63, 0, new long[0], 0), Compression.none()));
        assertArrayEquals(new long[0],
                SenkuMetadataCodec.readStorageFormat(emptyMasks).keyCodec()
                        .getFixedWeightParityMasks());
    }

    @Test
    void rankMetadataRejectsMissingMalformedMismatchedAndEmptyDomains() {
        for (final String fields : new String[] {
                RANK_FORMAT.replace("fixedWeightBitCount=7\n", ""),
                RANK_FORMAT.replace("fixedWeightParityMasks=3,12\n", ""),
                RANK_FORMAT.replace("fixedWeightParitySyndrome=1\n", ""),
                RANK_FORMAT.replace("fixedWeightSetBitCount=3\n", ""),
                RANK_FORMAT.replace("keyPageCodec=4", "keyPageCodec=3"),
                RANK_FORMAT.replace("keyPageCodec=4", "keyPageCodec=99"),
                RANK_FORMAT.replace("fixedWeightBitCount=7",
                        "fixedWeightBitCount=64"),
                RANK_FORMAT.replace("fixedWeightSetBitCount=3",
                        "fixedWeightSetBitCount=8"),
                RANK_FORMAT.replace("fixedWeightSetBitCount=3",
                        "fixedWeightSetBitCount=03"),
                RANK_FORMAT.replace("fixedWeightParityMasks=3,12",
                        "fixedWeightParityMasks=3,"),
                RANK_FORMAT.replace("fixedWeightParityMasks=3,12",
                        "fixedWeightParityMasks=03,12"),
                RANK_FORMAT.replace("fixedWeightParityMasks=3,12",
                        "fixedWeightParityMasks=\\uZZZZ"),
                RANK_FORMAT.replace("fixedWeightParityMasks=3,12",
                        "fixedWeightParityMasks=-1,12"),
                RANK_FORMAT.replace("fixedWeightParityMasks=3,12",
                        "fixedWeightParityMasks=128,12"),
                RANK_FORMAT.replace("fixedWeightParityMasks=3,12",
                        "fixedWeightParityMasks=0"),
                RANK_FORMAT.replace("fixedWeightParityMasks=3,12",
                        "fixedWeightParityMasks=0,0,0,0,0,0,0,0,0"),
                RANK_FORMAT.replace("fixedWeightParitySyndrome=1",
                        "fixedWeightParitySyndrome=4"),
                RANK_FORMAT + "extra=1\n" }) {
            final var invalid = new MemDirectory();
            writeProperties(invalid, SenkuFileNames.FORMAT_FILE, fields);
            assertThrows(IndexException.class,
                    () -> SenkuMetadataCodec.readStorageFormat(invalid));
        }
        final var missing = new MemDirectory();
        writeProperties(missing, SenkuFileNames.FORMAT_FILE,
                "formatVersion=2\nkeyPageCodec=4\ncompression=zstd\ncompressionLevel=3\n");
        assertThrows(IndexException.class,
                () -> SenkuMetadataCodec.readStorageFormat(missing));
    }

    private MemDirectory directory;

    @BeforeEach
    void setUp() {
        directory = new MemDirectory();
    }

    @Test
    void flushManifest_roundTripsAndPublishesCommittedNameLast() {
        SenkuMetadataCodec.publishFlushManifest(directory, 2);

        assertEquals(2, SenkuMetadataCodec.readFlushPartCount(directory));
        assertEquals(Set.of(SenkuFileNames.MANIFEST_FILE),
                fileNames(directory));
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

        assertEquals(128, SenkuMetadataCodec.readReadyShardCount(directory));
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

    @Test
    void storageFormatRoundTripsAndRejectsUnknownAndNonCanonicalFields() {
        final var format = new SenkuStorageFormat(
                KeyPageCodecs.longDeltaVarint(), Compression.zstd(3));
        SenkuMetadataCodec.publishStorageFormat(directory, format);
        final var read = SenkuMetadataCodec.readStorageFormat(directory);
        assertEquals(3, read.keyCodec().getId());
        assertEquals("zstd", read.compression().getId());
        assertThrows(IndexException.class, () -> SenkuMetadataCodec
                .publishStorageFormat(directory, format));
        for (String fields : new String[] {
                "formatVersion=1\nkeyPageCodec=3\ncompression=zstd\ncompressionLevel=3\n",
                "formatVersion=2\nkeyPageCodec=99\ncompression=zstd\ncompressionLevel=3\n",
                "formatVersion=2\nkeyPageCodec=3\ncompression=snappy\ncompressionLevel=3\n",
                "formatVersion=2\nkeyPageCodec=3\ncompression=zstd\ncompressionLevel=03\n",
                "formatVersion=2\nkeyPageCodec=3\ncompression=zstd\ncompressionLevel=23\n",
                "formatVersion=2\nkeyPageCodec=3\ncompression=zstd\ncompressionLevel=3\nextra=1\n" }) {
            final var invalid = new MemDirectory();
            writeProperties(invalid, SenkuFileNames.FORMAT_FILE, fields);
            assertThrows(IndexException.class,
                    () -> SenkuMetadataCodec.readStorageFormat(invalid));
        }
    }

}
