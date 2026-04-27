package org.hestiastore.index.it;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.chunkstore.ChunkFilterAesGcmDecrypt;
import org.hestiastore.index.chunkstore.ChunkFilterAesGcmEncrypt;
import org.hestiastore.index.chunkstore.ChunkFilterCrc32Validation;
import org.hestiastore.index.chunkstore.ChunkFilterCrc32Writing;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberValidation;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberWriting;
import org.hestiastore.index.chunkstore.ChunkFilterProvider;
import org.hestiastore.index.chunkstore.ChunkFilterProviderRegistry;
import org.hestiastore.index.chunkstore.ChunkFilterSpec;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileReader;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.junit.jupiter.api.Test;

class AesGcmFilteredSegmentIndexIT {

    private static final String PROVIDER_ID = "aes-gcm";
    private static final String KEY_REF = "orders-main";
    private static final SecretKey TEST_KEY = new SecretKeySpec(
            new byte[] { 9, 1, 8, 2, 7, 3, 6, 4, 5, 0, 5, 4, 6, 3, 7, 2 },
            "AES");

    @Test
    void createAndReopenIndexWithAesGcmChunkFilterProvider() {
        final Directory directory = new MemDirectory();
        final ChunkFilterSpec aesSpec = ChunkFilterSpec.ofProvider(PROVIDER_ID)
                .withParameter("keyRef", KEY_REF);
        final ChunkFilterProviderRegistry registry = ChunkFilterProviderRegistry
                .defaultRegistry()
                .withProvider(new AesGcmChunkFilterProvider(TEST_KEY));
        final IndexConfiguration<String, String> createConf = IndexConfiguration
                .<String, String>builder().withKeyClass(String.class)
                .withValueClass(String.class).withName("orders_encrypted")
                .addEncodingFilter(ChunkFilterCrc32Writing.class)
                .addEncodingFilter(ChunkFilterMagicNumberWriting.class)
                .addEncodingFilter(registry.createEncodingSupplier(aesSpec),
                        aesSpec)
                .addDecodingFilter(ChunkFilterMagicNumberValidation.class)
                .addDecodingFilter(registry.createDecodingSupplier(aesSpec),
                        aesSpec)
                .addDecodingFilter(ChunkFilterCrc32Validation.class).build();

        final Map<String, String> entries = new LinkedHashMap<>();
        entries.put("alpha", "first value");
        entries.put("beta", "second value");
        entries.put("gamma", "third value");

        try (SegmentIndex<String, String> index = SegmentIndex.create(directory,
                createConf, registry)) {
            entries.forEach(index::put);
            index.flush();
            index.compact();
        }

        final Properties manifest = readRequiredProperties(directory,
                "manifest.txt");
        assertEquals("p=crc32,p=magic-number,p=aes-gcm|keyRef=orders-main",
                manifest.getProperty("encodingChunkFilters"));
        assertEquals("p=magic-number,p=aes-gcm|keyRef=orders-main,p=crc32",
                manifest.getProperty("decodingChunkFilters"));

        try (SegmentIndex<String, String> index = SegmentIndex.open(directory,
                registry)) {
            entries.forEach((key, expectedValue) -> assertEquals(expectedValue, index.get(key)));
        }
    }

    private static Properties readRequiredProperties(final Directory directory,
            final String fileName) {
        try (FileReader reader = directory.getFileReader(fileName)) {
            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            final byte[] buffer = new byte[1024];
            int read = reader.read(buffer);
            while (read != -1) {
                if (read > 0) {
                    out.write(buffer, 0, read);
                }
                read = reader.read(buffer);
            }
            final Properties properties = new Properties();
            properties.load(
                    new StringReader(out.toString(StandardCharsets.UTF_8)));
            return properties;
        } catch (Exception ex) {
            throw new IllegalStateException(
                    "Unable to read required file '" + fileName + "'", ex);
        }
    }

    private static final class AesGcmChunkFilterProvider
            implements ChunkFilterProvider {

        private final SecretKey key;

        private AesGcmChunkFilterProvider(final SecretKey key) {
            this.key = key;
        }

        @Override
        public String getProviderId() {
            return PROVIDER_ID;
        }

        @Override
        public Supplier<? extends ChunkFilter> createEncodingSupplier(
                final ChunkFilterSpec spec) {
            assertKeyRef(spec);
            return () -> new ChunkFilterAesGcmEncrypt(key);
        }

        @Override
        public Supplier<? extends ChunkFilter> createDecodingSupplier(
                final ChunkFilterSpec spec) {
            assertKeyRef(spec);
            return () -> new ChunkFilterAesGcmDecrypt(key);
        }

        private void assertKeyRef(final ChunkFilterSpec spec) {
            assertEquals(KEY_REF, spec.getRequiredParameter("keyRef"));
        }
    }
}
