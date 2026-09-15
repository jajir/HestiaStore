package org.hestiastore.index.senku.internal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.Set;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkentryfile.KeyPageCodecs;
import org.hestiastore.index.chunkentryfile.KeyPageCodec;
import org.hestiastore.index.chunkstore.Compression;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileReader;
import org.hestiastore.index.properties.PropertyMutationSession;
import org.hestiastore.index.properties.PropertyStoreImpl;
import org.hestiastore.index.properties.PropertyWriter;
import org.hestiastore.index.senku.SenkuLongKeySummary;

/**
 * Reads, validates, and publishes the first Senku property formats.
 */
final class SenkuMetadataCodec {

    private static final String PART_COUNT = "partCount";
    private static final String RECORD_COUNT = "recordCount";
    private static final String SUMMARY_VERSION = "longSummaryVersion";
    private static final String SUMMARY_KEYS = "longSummaryKeys";
    private static final String SUMMARY_WEIGHTS = "longSummaryWeights";
    private static final String SHARD_COUNT = "shardCount";
    private static final String FORMAT_VERSION = "formatVersion";
    private static final String KEY_CODEC = "keyPageCodec";
    private static final String COMPRESSION = "compression";
    private static final String COMPRESSION_LEVEL = "compressionLevel";
    private static final String FIXED_WEIGHT_BIT_COUNT = "fixedWeightBitCount";
    private static final String FIXED_WEIGHT_SET_BIT_COUNT = "fixedWeightSetBitCount";
    private static final String FIXED_WEIGHT_PARITY_MASKS = "fixedWeightParityMasks";
    private static final String FIXED_WEIGHT_PARITY_SYNDROME = "fixedWeightParitySyndrome";
    private static final Set<String> FORMAT_KEYS = Set.of(FORMAT_VERSION,
            KEY_CODEC, COMPRESSION, COMPRESSION_LEVEL);
    private static final Set<String> FIXED_WEIGHT_FORMAT_KEYS = Set.of(
            FORMAT_VERSION, KEY_CODEC, COMPRESSION, COMPRESSION_LEVEL,
            FIXED_WEIGHT_BIT_COUNT, FIXED_WEIGHT_SET_BIT_COUNT,
            FIXED_WEIGHT_PARITY_MASKS, FIXED_WEIGHT_PARITY_SYNDROME);
    private static final Set<String> FLUSH_KEYS = Set.of(PART_COUNT);
    private static final Set<String> RUN_KEYS = Set.of(PART_COUNT,
            RECORD_COUNT);
    private static final Set<String> SUMMARIZED_RUN_KEYS = Set.of(PART_COUNT,
            RECORD_COUNT, SUMMARY_VERSION, SUMMARY_KEYS, SUMMARY_WEIGHTS);
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
            validated.longKeySummary().ifPresent(summary -> {
                writer.setString(SUMMARY_VERSION, "1");
                writer.setString(SUMMARY_KEYS,
                        encodeSummaryArray(summary.keys()));
                writer.setString(SUMMARY_WEIGHTS,
                        encodeSummaryArray(summary.weights()));
            });
        });
    }

    /**
     * Reads an exact committed sorted-run manifest.
     *
     * @param directory run directory
     * @return validated run counts
     */
    static SenkuRunManifest readRunManifest(final Directory directory) {
        final Properties properties = readProperties(directory,
                SenkuFileNames.MANIFEST_FILE);
        final boolean summarized = properties.containsKey(SUMMARY_VERSION);
        requireExactKeys(properties, SenkuFileNames.MANIFEST_FILE,
                summarized ? SUMMARIZED_RUN_KEYS : RUN_KEYS);
        try {
            final long count = parseLong(properties, RECORD_COUNT);
            final Optional<SenkuLongKeySummary> summary;
            if (summarized) {
                if (parseInt(properties, SUMMARY_VERSION) != 1) {
                    throw invalidMetadata("Unsupported long summary version.");
                }
                summary = Optional.of(SenkuLongKeySummary.of(count,
                        parseSummaryArray(properties, SUMMARY_KEYS),
                        parseSummaryArray(properties, SUMMARY_WEIGHTS)));
            } else {
                summary = Optional.empty();
            }
            return new SenkuRunManifest(parseInt(properties, PART_COUNT), count,
                    summary);
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
        writeAndPublish(directory, SenkuFileNames.READY_FILE, writer -> writer
                .setString(SHARD_COUNT, Integer.toString(validatedShardCount)));
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

    /** Publishes immutable format metadata before any pages are written. */
    static void publishStorageFormat(final Directory directory,
            final SenkuStorageFormat format) {
        writeAndPublish(directory, SenkuFileNames.FORMAT_FILE, writer -> {
            writer.setString(FORMAT_VERSION, "2");
            writer.setString(KEY_CODEC,
                    Integer.toString(format.keyCodec().getId()));
            writer.setString(COMPRESSION, format.compression().getId());
            writer.setString(COMPRESSION_LEVEL,
                    Integer.toString(format.compression().getLevel()));
            final KeyPageCodec<?> codec = format.keyCodec();
            if (codec.isLongFixedWeightDeltaVarint()) {
                writer.setString(FIXED_WEIGHT_BIT_COUNT,
                        Integer.toString(codec.getFixedWeightBitCount()));
                writer.setString(FIXED_WEIGHT_SET_BIT_COUNT,
                        Integer.toString(codec.getFixedWeightSetBitCount()));
                writer.setString(FIXED_WEIGHT_PARITY_MASKS,
                        Arrays.stream(codec.getFixedWeightParityMasks())
                                .mapToObj(Long::toString)
                                .collect(Collectors.joining(",")));
                writer.setString(FIXED_WEIGHT_PARITY_SYNDROME,
                        Integer.toString(codec.getFixedWeightParitySyndrome()));
            }
        });
    }

    /**
     * Resolves exact metadata; indexes without the new format must be rebuilt.
     */
    static SenkuStorageFormat readStorageFormat(final Directory directory) {
        final Properties properties = readProperties(directory,
                SenkuFileNames.FORMAT_FILE);
        final boolean fixedWeight = properties
                .containsKey(FIXED_WEIGHT_BIT_COUNT);
        requireExactKeys(properties, SenkuFileNames.FORMAT_FILE,
                fixedWeight ? FIXED_WEIGHT_FORMAT_KEYS : FORMAT_KEYS);
        if (parseInt(properties, FORMAT_VERSION) != 2) {
            throw invalidMetadata(
                    "Unsupported Senku storage format; rebuild the index.");
        }
        return new SenkuStorageFormat(readKeyCodec(properties, fixedWeight),
                Compression.fromId(properties.getProperty(COMPRESSION),
                        parseInt(properties, COMPRESSION_LEVEL)));
    }

    private static KeyPageCodec<?> readKeyCodec(final Properties properties,
            final boolean fixedWeight) {
        final int codecId = parseInt(properties, KEY_CODEC);
        if (!fixedWeight) {
            return KeyPageCodecs.fromId(codecId);
        }
        final String encodedMasks = properties
                .getProperty(FIXED_WEIGHT_PARITY_MASKS);
        final String[] maskValues = encodedMasks.isEmpty() ? new String[0]
                : encodedMasks.split(",", -1);
        if (maskValues.length > 8) {
            throw invalidMetadata(
                    "Fixed-weight format has too many parity masks.");
        }
        final long[] masks = new long[maskValues.length];
        for (int i = 0; i < masks.length; i++) {
            masks[i] = parseCanonicalLong(maskValues[i],
                    FIXED_WEIGHT_PARITY_MASKS);
        }
        try {
            final KeyPageCodec<Long> codec = KeyPageCodecs
                    .longFixedWeightDeltaVarint(
                            parseInt(properties, FIXED_WEIGHT_BIT_COUNT),
                            parseInt(properties, FIXED_WEIGHT_SET_BIT_COUNT),
                            masks,
                            parseInt(properties, FIXED_WEIGHT_PARITY_SYNDROME));
            if (codec.getId() != codecId) {
                throw invalidMetadata(
                        "Key codec does not match fixed-weight parameters.");
            }
            return codec;
        } catch (IllegalArgumentException exception) {
            throw new IndexException("Invalid fixed-weight codec metadata.",
                    exception);
        }
    }

    private static void writeAndPublish(final Directory directory,
            final String committedName, final Consumer<PropertyWriter> values) {
        final Directory validatedDirectory = Vldtn.requireNonNull(directory,
                "directory");
        final String temporaryName = SenkuFileNames.temporary(committedName);
        requireAbsent(validatedDirectory, committedName);
        requireAbsent(validatedDirectory, temporaryName);
        final PropertyStoreImpl store = PropertyStoreImpl
                .fromDirectory(validatedDirectory, temporaryName, false);
        try (PropertyMutationSession session = store.openMutationSession()) {
            values.accept(session.writer());
        }
        validatedDirectory.renameFile(temporaryName, committedName);
    }

    private static Properties readExact(final Directory directory,
            final String fileName, final Set<String> expectedKeys) {
        final Properties properties = readProperties(directory, fileName);
        requireExactKeys(properties, fileName, expectedKeys);
        return properties;
    }

    private static Properties readProperties(final Directory directory,
            final String fileName) {
        final Directory validatedDirectory = Vldtn.requireNonNull(directory,
                "directory");
        if (!validatedDirectory.isFileExists(fileName)) {
            throw invalidMetadata(
                    "Required metadata file '" + fileName + "' is missing.");
        }
        final Properties properties = new Properties();
        try {
            properties.load(new ByteArrayInputStream(
                    readEntireFile(validatedDirectory, fileName)));
        } catch (IOException | IllegalArgumentException e) {
            throw new IndexException(
                    "Unable to parse metadata file '" + fileName + "'.", e);
        }
        return properties;
    }

    private static void requireExactKeys(final Properties properties,
            final String fileName, final Set<String> expectedKeys) {
        if (!properties.stringPropertyNames().equals(expectedKeys)) {
            throw invalidMetadata("Metadata file '" + fileName
                    + "' must contain exactly " + expectedKeys + ".");
        }
    }

    private static byte[] readEntireFile(final Directory directory,
            final String fileName) {
        try (FileReader reader = directory.getFileReader(fileName)) {
            final ByteArrayOutputStream output = new ByteArrayOutputStream();
            final byte[] buffer = new byte[256];
            int count = reader.read(buffer);
            while (count != -1) {
                if (output.size() + count > 262_144) {
                    throw invalidMetadata(
                            "Senku metadata exceeds its bounded size.");
                }
                output.write(buffer, 0, count);
                count = reader.read(buffer);
            }
            return output.toByteArray();
        }
    }

    private static String encodeSummaryArray(final long[] values) {
        return Arrays.stream(values).mapToObj(Long::toString)
                .collect(Collectors.joining(","));
    }

    private static long[] parseSummaryArray(final Properties properties,
            final String name) {
        final String value = properties.getProperty(name);
        final String[] fields = value.isEmpty() ? new String[0]
                : value.split(",", SenkuLongKeySummary.MAX_SAMPLES + 1);
        if (fields.length > SenkuLongKeySummary.MAX_SAMPLES) {
            throw invalidMetadata("Too many long summary representatives.");
        }
        final long[] decoded = new long[fields.length];
        for (int index = 0; index < fields.length; index++) {
            decoded[index] = parseCanonicalLong(fields[index], name);
        }
        return decoded;
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
            throw new IndexException("Metadata property '" + propertyName
                    + "' must be a canonical integer.", e);
        }
    }

    private static long parseLong(final Properties properties,
            final String propertyName) {
        return parseCanonicalLong(properties.getProperty(propertyName),
                propertyName);
    }

    private static long parseCanonicalLong(final String encoded,
            final String propertyName) {
        try {
            final long parsed = Long.parseLong(encoded);
            if (!Long.toString(parsed).equals(encoded)) {
                throw new NumberFormatException("Non-canonical long");
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IndexException("Metadata property '" + propertyName
                    + "' must be a canonical long.", e);
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
