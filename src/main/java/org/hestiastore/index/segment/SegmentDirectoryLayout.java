package org.hestiastore.index.segment;

import org.hestiastore.index.FileNameUtil;
import org.hestiastore.index.Vldtn;

/**
 * Provides file naming helpers for segment-related files.
 */
public final class SegmentDirectoryLayout {

    private static final String INDEX_FILE_NAME_EXTENSION = ".index";
    private static final String SCARCE_FILE_NAME_EXTENSION = ".scarce";
    private static final String BLOOM_FILTER_FILE_NAME_EXTENSION = ".bloom-filter";
    private static final String PROPERTIES_FILE_NAME_EXTENSION = ".properties";
    private static final String LOCK_FILE_NAME_EXTENSION = ".lock";
    private static final String ACTIVE_POINTER_FILE_NAME_EXTENSION = ".active";
    private static final String DELTA_FILE_NAME_MIDDLE = "-delta-";
    private static final String CACHE_FILE_NAME_EXTENSION = ".cache";
    private static final int DELTA_ID_PAD_LENGTH = 3;
    private static final String VERSION_FILE_NAME_MARKER = "-v";
    private static final String VERSION_DIRECTORY_PREFIX = "v";

    private final SegmentId segmentId;

    /**
     * Creates a naming helper for a segment.
     *
     * @param segmentId segment identifier
     */
    public SegmentDirectoryLayout(final SegmentId segmentId) {
        this.segmentId = Vldtn.requireNonNull(segmentId, "segmentId");
    }

    /**
     * Returns the segment identifier tied to this layout.
     *
     * @return segment id
     */
    public SegmentId getSegmentId() {
        return segmentId;
    }

    /**
     * Returns the main index file name.
     *
     * @return index file name
     */
    public String getIndexFileName() {
        return getIndexFileName(0);
    }

    /**
     * Returns the main index file name for the given version.
     *
     * @param version active version (0 means legacy, unversioned file)
     * @return index file name
     */
    public String getIndexFileName(final long version) {
        return buildVersionedName(version, INDEX_FILE_NAME_EXTENSION);
    }

    /**
     * Returns the scarce index file name.
     *
     * @return scarce index file name
     */
    public String getScarceFileName() {
        return getScarceFileName(0);
    }

    /**
     * Returns the scarce index file name for the given version.
     *
     * @param version active version (0 means legacy, unversioned file)
     * @return scarce index file name
     */
    public String getScarceFileName(final long version) {
        return buildVersionedName(version, SCARCE_FILE_NAME_EXTENSION);
    }

    /**
     * Returns the bloom filter file name.
     *
     * @return bloom filter file name
     */
    public String getBloomFilterFileName() {
        return getBloomFilterFileName(0);
    }

    /**
     * Returns the bloom filter file name for the given version.
     *
     * @param version active version (0 means legacy, unversioned file)
     * @return bloom filter file name
     */
    public String getBloomFilterFileName(final long version) {
        return buildVersionedName(version, BLOOM_FILTER_FILE_NAME_EXTENSION);
    }

    /**
     * Returns the properties file name.
     *
     * @return properties file name
     */
    public String getPropertiesFileName() {
        return segmentId.getName() + PROPERTIES_FILE_NAME_EXTENSION;
    }

    /**
     * Returns the segment lock file name.
     *
     * @return lock file name
     */
    public String getLockFileName() {
        return segmentId.getName() + LOCK_FILE_NAME_EXTENSION;
    }

    /**
     * Returns the pointer file name that stores the active segment directory.
     *
     * @return active pointer file name
     */
    public String getActivePointerFileName() {
        return segmentId.getName() + ACTIVE_POINTER_FILE_NAME_EXTENSION;
    }

    /**
     * Returns the delta cache file name for a numeric delta id.
     *
     * @param deltaFileId numeric delta id
     * @return delta cache file name
     */
    public String getDeltaCacheFileName(final int deltaFileId) {
        return getDeltaCacheFileName(0, deltaFileId);
    }

    /**
     * Returns the delta cache file name for a numeric delta id and version.
     *
     * @param version active version (0 means legacy, unversioned file)
     * @param deltaFileId numeric delta id
     * @return delta cache file name
     */
    public String getDeltaCacheFileName(final long version,
            final int deltaFileId) {
        final String rawId = String.valueOf(deltaFileId);
        final String paddedId = rawId.length() > DELTA_ID_PAD_LENGTH ? rawId
                : FileNameUtil.getPaddedId(deltaFileId, DELTA_ID_PAD_LENGTH);
        return buildVersionedPrefix(version) + DELTA_FILE_NAME_MIDDLE + paddedId
                + CACHE_FILE_NAME_EXTENSION;
    }

    /**
     * Returns the delta cache prefix for the given version.
     *
     * @param version active version (0 means legacy, unversioned file)
     * @return prefix including the delta separator
     */
    public String getDeltaCachePrefix(final long version) {
        return buildVersionedPrefix(version) + DELTA_FILE_NAME_MIDDLE;
    }

    /**
     * Parses the version from an index file name.
     *
     * @param fileName file name to parse
     * @return parsed version, 0 for legacy names, or -1 when invalid
     */
    public long parseVersionFromIndexFileName(final String fileName) {
        if (fileName == null || fileName.isBlank()) {
            return -1;
        }
        if (fileName.equals(getIndexFileName(0))) {
            return 0;
        }
        final String prefix = segmentId.getName() + VERSION_FILE_NAME_MARKER;
        if (!fileName.startsWith(prefix)
                || !fileName.endsWith(INDEX_FILE_NAME_EXTENSION)) {
            return -1;
        }
        final int versionStart = prefix.length();
        final int versionEnd = fileName.length()
                - INDEX_FILE_NAME_EXTENSION.length();
        if (versionEnd <= versionStart) {
            return -1;
        }
        final String rawVersion = fileName.substring(versionStart, versionEnd);
        try {
            return Long.parseLong(rawVersion);
        } catch (final NumberFormatException e) {
            return -1;
        }
    }

    /**
     * Builds a versioned directory name like {@code v2}.
     *
     * @param version positive version number
     * @return versioned directory name
     */
    public static String getVersionDirectoryName(final long version) {
        if (version <= 0) {
            throw new IllegalArgumentException(String.format(
                    "Version '%s' must be greater than 0", version));
        }
        return VERSION_DIRECTORY_PREFIX + version;
    }

    /**
     * Parses a versioned directory name like {@code v2}.
     *
     * @param directoryName directory name to parse
     * @return parsed version or {@code -1} when invalid
     */
    public static long parseVersionDirectoryName(
            final String directoryName) {
        if (directoryName == null || directoryName.isBlank()) {
            return -1;
        }
        if (!directoryName.startsWith(VERSION_DIRECTORY_PREFIX)) {
            return -1;
        }
        final String raw = directoryName
                .substring(VERSION_DIRECTORY_PREFIX.length());
        if (raw.isBlank()) {
            return -1;
        }
        try {
            return Long.parseLong(raw);
        } catch (final NumberFormatException e) {
            return -1;
        }
    }

    private String buildVersionedName(final long version,
            final String extension) {
        return buildVersionedPrefix(version) + extension;
    }

    private String buildVersionedPrefix(final long version) {
        if (version <= 0) {
            return segmentId.getName();
        }
        return segmentId.getName() + VERSION_FILE_NAME_MARKER + version;
    }
}
