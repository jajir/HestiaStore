package org.hestiastore.index.senku.internal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileReader;
import org.hestiastore.index.properties.PropertyMutationSession;
import org.hestiastore.index.properties.PropertyStoreImpl;
import org.hestiastore.index.properties.PropertyWriter;

/**
 * Reads, validates, and publishes the first Senku property formats.
 */
final class SenkuMetadataCodec {

    private static final String PART_COUNT = "partCount";
    private static final String RECORD_COUNT = "recordCount";
    private static final String SHARD_COUNT = "shardCount";
    private static final Set<String> FLUSH_KEYS = Set.of(PART_COUNT);
    private static final Set<String> RUN_KEYS = Set.of(PART_COUNT, RECORD_COUNT);
    private static final Set<String> READY_KEYS = Set.of(SHARD_COUNT);

    private SenkuMetadataCodec() {
        // Static metadata utility.
    }

    /**
     * Publishes a committed non-empty flush manifest.
     *
     * @param directory flush directory
     * @param partCount positive physical part count
     */
    static void publishFlushManifest(final Directory directory,
            final int partCount) {
        final int validatedPartCount = Vldtn.requireGreaterThanZero(partCount,
                PART_COUNT);
        writeAndPublish(directory, SenkuFileNames.MANIFEST_FILE,
                writer -> writer.setString(PART_COUNT,
                        Integer.toString(validatedPartCount)));
    }

    /**
     * Reads an exact committed flush manifest.
     *
     * @param directory flush directory
     * @return positive physical part count
     */
    static int readFlushPartCount(final Directory directory) {
        final Properties properties = readExact(directory,
                SenkuFileNames.MANIFEST_FILE, FLUSH_KEYS);
        final int partCount = parseInt(properties, PART_COUNT);
        if (partCount <= 0) {
            throw invalidMetadata("Flush partCount must be positive.");
        }
        return partCount;
    }

    /**
     * Publishes a committed sorted-run manifest.
     *
     * @param directory run directory
     * @param manifest  validated run counts
     */
    static void publishRunManifest(final Directory directory,
            final SenkuRunManifest manifest) {
        final SenkuRunManifest validated = Vldtn.requireNonNull(manifest,
                "manifest");
        writeAndPublish(directory, SenkuFileNames.MANIFEST_FILE, writer -> {
            writer.setString(PART_COUNT,
                    Integer.toString(validated.partCount()));
            writer.setString(RECORD_COUNT,
                    Long.toString(validated.recordCount()));
        });
    }

    /**
     * Reads an exact committed sorted-run manifest.
     *
     * @param directory run directory
     * @return validated run counts
     */
    static SenkuRunManifest readRunManifest(final Directory directory) {
        final Properties properties = readExact(directory,
                SenkuFileNames.MANIFEST_FILE, RUN_KEYS);
        try {
            return new SenkuRunManifest(parseInt(properties, PART_COUNT),
                    parseLong(properties, RECORD_COUNT));
        } catch (IllegalArgumentException e) {
            throw new IndexException("Invalid sorted-run manifest.", e);
        }
    }

    /**
     * Publishes the root ready marker.
     *
     * @param directory  index root
     * @param shardCount positive configured shard count
     */
    static void publishReady(final Directory directory, final int shardCount) {
        final int validatedShardCount = Vldtn.requireGreaterThanZero(shardCount,
                SHARD_COUNT);
        writeAndPublish(directory, SenkuFileNames.READY_FILE,
                writer -> writer.setString(SHARD_COUNT,
                        Integer.toString(validatedShardCount)));
    }

    /**
     * Reads the exact root ready marker.
     *
     * @param directory index root
     * @return positive configured shard count
     */
    static int readReadyShardCount(final Directory directory) {
        final Properties properties = readExact(directory,
                SenkuFileNames.READY_FILE, READY_KEYS);
        final int shardCount = parseInt(properties, SHARD_COUNT);
        if (shardCount <= 0) {
            throw invalidMetadata("Ready shardCount must be positive.");
        }
        return shardCount;
    }

    private static void writeAndPublish(final Directory directory,
            final String committedName,
            final Consumer<PropertyWriter> values) {
        final Directory validatedDirectory = Vldtn.requireNonNull(directory,
                "directory");
        final String temporaryName = SenkuFileNames.temporary(committedName);
        requireAbsent(validatedDirectory, committedName);
        requireAbsent(validatedDirectory, temporaryName);
        final PropertyStoreImpl store = PropertyStoreImpl.fromDirectory(
                validatedDirectory, temporaryName, false);
        try (PropertyMutationSession session = store.openMutationSession()) {
            values.accept(session.writer());
        }
        validatedDirectory.renameFile(temporaryName, committedName);
    }

    private static Properties readExact(final Directory directory,
            final String fileName, final Set<String> expectedKeys) {
        final Directory validatedDirectory = Vldtn.requireNonNull(directory,
                "directory");
        if (!validatedDirectory.isFileExists(fileName)) {
            throw invalidMetadata("Required metadata file '" + fileName
                    + "' is missing.");
        }
        final Properties properties = new Properties();
        try {
            properties.load(new ByteArrayInputStream(
                    readEntireFile(validatedDirectory, fileName)));
        } catch (IOException e) {
            throw new IndexException(
                    "Unable to parse metadata file '" + fileName + "'.", e);
        }
        if (!properties.stringPropertyNames().equals(expectedKeys)) {
            throw invalidMetadata("Metadata file '" + fileName
                    + "' must contain exactly " + expectedKeys + ".");
        }
        return properties;
    }

    private static byte[] readEntireFile(final Directory directory,
            final String fileName) {
        try (FileReader reader = directory.getFileReader(fileName)) {
            final ByteArrayOutputStream output = new ByteArrayOutputStream();
            final byte[] buffer = new byte[256];
            int count = reader.read(buffer);
            while (count != -1) {
                output.write(buffer, 0, count);
                count = reader.read(buffer);
            }
            return output.toByteArray();
        }
    }

    private static int parseInt(final Properties properties,
            final String propertyName) {
        final String encoded = properties.getProperty(propertyName);
        try {
            final int parsed = Integer.parseInt(encoded);
            if (!Integer.toString(parsed).equals(encoded)) {
                throw new NumberFormatException("Non-canonical integer");
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IndexException(
                    "Metadata property '" + propertyName
                            + "' must be a canonical integer.",
                    e);
        }
    }

    private static long parseLong(final Properties properties,
            final String propertyName) {
        final String encoded = properties.getProperty(propertyName);
        try {
            final long parsed = Long.parseLong(encoded);
            if (!Long.toString(parsed).equals(encoded)) {
                throw new NumberFormatException("Non-canonical long");
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IndexException(
                    "Metadata property '" + propertyName
                            + "' must be a canonical long.",
                    e);
        }
    }

    private static void requireAbsent(final Directory directory,
            final String fileName) {
        if (directory.isFileExists(fileName)) {
            throw new IndexException(
                    "Metadata file '" + fileName + "' already exists.");
        }
    }

    private static IndexException invalidMetadata(final String message) {
        return new IndexException(message);
    }
}
