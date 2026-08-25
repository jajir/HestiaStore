package org.hestiastore.index.senku.internal;

import java.util.Locale;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;

/**
 * Canonical Senku directory and file names.
 */
final class SenkuFileNames {

    static final String LOCK_FILE = ".lock";
    static final String FLUSH_DIRECTORY = "flush";
    static final String MANIFEST_FILE = "manifest.properties";
    static final String READY_FILE = "ready.properties";
    static final String SHARD_INDEX_FILE = "shard-index.dat";
    static final String TEMP_SUFFIX = ".tmp";

    private static final String FLUSH_PREFIX = "flush-";
    private static final String SHARD_PREFIX = "shard-";
    private static final String LEVEL_PREFIX = "level-";
    private static final String RUN_PREFIX = "run-";
    private static final String PART_PREFIX = "part-";
    private static final String CHUNK_SUFFIX = ".chunk";
    private static final int MAX_PART_NUMBER = Integer.MAX_VALUE - 1;

    private SenkuFileNames() {
        // Static naming utility.
    }

    static String flushDirectory(final long id) {
        return formatLong(FLUSH_PREFIX, id, "flushId", "");
    }

    static long parseFlushDirectory(final String name) {
        return parseLong(name, FLUSH_PREFIX, "", "flush directory");
    }

    static String shardDirectory(final int id) {
        return formatInt(SHARD_PREFIX, id, Integer.MAX_VALUE, "shardId", "");
    }

    static int parseShardDirectory(final String name) {
        return parseInt(name, SHARD_PREFIX, "", Integer.MAX_VALUE,
                "shard directory");
    }

    static String levelDirectory(final int id) {
        return formatInt(LEVEL_PREFIX, id, Integer.MAX_VALUE, "level", "");
    }

    static int parseLevelDirectory(final String name) {
        return parseInt(name, LEVEL_PREFIX, "", Integer.MAX_VALUE,
                "level directory");
    }

    static String runDirectory(final long id) {
        return formatLong(RUN_PREFIX, id, "runId", "");
    }

    static long parseRunDirectory(final String name) {
        return parseLong(name, RUN_PREFIX, "", "run directory");
    }

    static String partFile(final int id) {
        return formatInt(PART_PREFIX, id, MAX_PART_NUMBER, "partNumber",
                CHUNK_SUFFIX);
    }

    static int parsePartFile(final String name) {
        return parseInt(name, PART_PREFIX, CHUNK_SUFFIX, MAX_PART_NUMBER,
                "part file");
    }

    static boolean isPartFile(final String name) {
        final String validated = Vldtn.requireNonNull(name, "name");
        return validated.startsWith(PART_PREFIX)
                && validated.endsWith(CHUNK_SUFFIX);
    }

    static String temporary(final String committedName) {
        return Vldtn.requireNonNull(committedName, "committedName")
                + TEMP_SUFFIX;
    }

    private static String formatInt(final String prefix, final int value,
            final int maximum, final String propertyName, final String suffix) {
        final int validated = Vldtn.requireBetween(value, 0, maximum,
                propertyName);
        return String.format(Locale.ROOT, "%s%05d%s", prefix, validated,
                suffix);
    }

    private static String formatLong(final String prefix, final long value,
            final String propertyName, final String suffix) {
        final long validated = Vldtn.requireGreaterThanOrEqualToZero(value,
                propertyName);
        return String.format(Locale.ROOT, "%s%05d%s", prefix, validated,
                suffix);
    }

    private static int parseInt(final String name, final String prefix,
            final String suffix, final int maximum, final String description) {
        final long parsed = parseLong(name, prefix, suffix, description);
        if (parsed > maximum) {
            throw invalidName(name, description);
        }
        return (int) parsed;
    }

    private static long parseLong(final String name, final String prefix,
            final String suffix, final String description) {
        final String validatedName = Vldtn.requireNonNull(name, "name");
        if (!validatedName.startsWith(prefix)
                || !validatedName.endsWith(suffix)) {
            throw invalidName(validatedName, description);
        }
        final int suffixStart = validatedName.length() - suffix.length();
        final String numeric = validatedName.substring(prefix.length(),
                suffixStart);
        final long parsed;
        try {
            parsed = Long.parseLong(numeric);
        } catch (NumberFormatException e) {
            throw new IndexException(
                    "Invalid canonical " + description + " name '"
                            + validatedName + "'.",
                    e);
        }
        final String canonical;
        try {
            canonical = formatLong(prefix, parsed, description, suffix);
        } catch (IllegalArgumentException e) {
            throw new IndexException(
                    "Invalid canonical " + description + " name '"
                            + validatedName + "'.",
                    e);
        }
        if (!validatedName.equals(canonical)) {
            throw invalidName(validatedName, description);
        }
        return parsed;
    }

    private static IndexException invalidName(final String name,
            final String description) {
        return new IndexException("Invalid canonical " + description
                + " name '" + name + "'.");
    }
}
