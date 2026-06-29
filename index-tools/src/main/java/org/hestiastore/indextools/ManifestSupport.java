package org.hestiastore.indextools;

import java.io.IOException;
import java.nio.file.Path;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

final class ManifestSupport {

    static final String MANIFEST_FILE_NAME = "manifest.json";
    static final String CONFIG_FILE_NAME = "source-config.json";
    static final String CHECKSUMS_FILE_NAME = "checksums.txt";
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .enable(SerializationFeature.INDENT_OUTPUT);
    private static final ObjectMapper LINE_MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private ManifestSupport() {
    }

    static ObjectMapper mapper() {
        return MAPPER;
    }

    static ObjectMapper lineMapper() {
        return LINE_MAPPER;
    }

    static <T> T readJson(final Path path, final Class<T> type)
            throws IOException {
        return MAPPER.readValue(path.toFile(), type);
    }

    static void writeJson(final Path path, final Object value)
            throws IOException {
        MAPPER.writeValue(path.toFile(), value);
    }
}
