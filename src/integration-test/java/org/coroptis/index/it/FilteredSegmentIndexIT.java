package org.coroptis.index.it;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import org.hestiastore.index.chunkstore.ChunkFilterCrc32Validation;
import org.hestiastore.index.chunkstore.ChunkFilterCrc32Writing;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberValidation;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberWriting;
import org.hestiastore.index.chunkstore.ChunkFilterSnappyCompress;
import org.hestiastore.index.chunkstore.ChunkFilterSnappyDecompress;
import org.hestiastore.index.chunkstore.ChunkFilterXorDecrypt;
import org.hestiastore.index.chunkstore.ChunkFilterXorEncrypt;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.FileReader;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class FilteredSegmentIndexIT {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(FilteredSegmentIndexIT.class);

    @Test
    void test_index_with_snappy_and_xor_filters() {
        final Directory directory = new MemDirectory();
        final IndexConfiguration<String, String> createConf = IndexConfiguration
                .<String, String>builder()//
                .withKeyClass(String.class)//
                .withValueClass(String.class)//
                .withName("test_index_filtered")//
                .addEncodingFilter(new ChunkFilterMagicNumberWriting())//
                .addEncodingFilter(new ChunkFilterCrc32Writing())//
                .addEncodingFilter(new ChunkFilterSnappyCompress())//
                .addEncodingFilter(new ChunkFilterXorEncrypt())//
                .addDecodingFilter(new ChunkFilterXorDecrypt())//
                .addDecodingFilter(new ChunkFilterSnappyDecompress())//
                .addDecodingFilter(new ChunkFilterCrc32Validation())//
                .addDecodingFilter(new ChunkFilterMagicNumberValidation())//
                .build();

        final IndexConfiguration<String, String> openConf = IndexConfiguration
                .<String, String>builder()//
                .withKeyClass(String.class)//
                .withValueClass(String.class)//
                .withName("test_index_filtered")//
                .build();

        final Map<String, String> entries = new LinkedHashMap<>();
        entries.put("alpha", "first value");
        entries.put("beta", "second value");
        entries.put("gamma", "third value");

        try (SegmentIndex<String, String> index = SegmentIndex.create(directory,
                createConf)) {
            entries.forEach(index::put);
            index.flush();
            index.compact();
            LOGGER.info("Created index with configuration: {}", createConf);
            logPropertiesFile(directory, "Created index properties file");
        }

        try (SegmentIndex<String, String> index = SegmentIndex.open(directory, openConf)) {
            entries.forEach((key, expectedValue) -> assertEquals(expectedValue,
                    index.get(key)));
            LOGGER.info("Opened index with configuration: {}", openConf);
            logPropertiesFile(directory, "Opened index properties file");
            LOGGER.info("Files in directory after reopen: {}",
                    directory.getFileNames().sorted().toList());
        }
    }

    private void logPropertiesFile(final Directory directory,
            final String prefix) {
        directory.getFileNames()//
                .filter(name -> name.endsWith(".properties"))//
                .findFirst()
                .ifPresent(file -> LOGGER.info("{}: {}", prefix, file));

        directory.getFileNames()//
                .filter(name -> name.equals("index-configuration.properties"))//
                .findFirst()//
                .ifPresent(name -> {
                    final FileReader reader = directory.getFileReader(name);
                    try (reader) {
                        final ByteArrayOutputStream out = new ByteArrayOutputStream();
                        final byte[] buffer = new byte[1024];
                        int read;
                        while ((read = reader.read(buffer)) != -1) {
                            if (read > 0) {
                                out.write(buffer, 0, read);
                            }
                        }
                        final String content = new String(out.toByteArray(),
                                StandardCharsets.UTF_8);
                        LOGGER.info(
                                "index-configuration.properties content:\n{}",
                                content);
                    } catch (Exception ex) {
                        LOGGER.warn("Unable to read {}", name, ex);
                    }
                });
    }
}
