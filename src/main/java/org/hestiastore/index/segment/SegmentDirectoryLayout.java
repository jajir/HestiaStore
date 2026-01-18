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
    private static final String DELTA_FILE_NAME_MIDDLE = "-delta-";
    private static final String CACHE_FILE_NAME_EXTENSION = ".cache";
    private static final int DELTA_ID_PAD_LENGTH = 3;

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
     * Returns the main index file name.
     *
     * @return index file name
     */
    public String getIndexFileName() {
        return segmentId.getName() + INDEX_FILE_NAME_EXTENSION;
    }

    /**
     * Returns the scarce index file name.
     *
     * @return scarce index file name
     */
    public String getScarceFileName() {
        return segmentId.getName() + SCARCE_FILE_NAME_EXTENSION;
    }

    /**
     * Returns the bloom filter file name.
     *
     * @return bloom filter file name
     */
    public String getBloomFilterFileName() {
        return segmentId.getName() + BLOOM_FILTER_FILE_NAME_EXTENSION;
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
     * Returns the delta cache file name for a numeric delta id.
     *
     * @param deltaFileId numeric delta id
     * @return delta cache file name
     */
    public String getDeltaCacheFileName(final int deltaFileId) {
        final String rawId = String.valueOf(deltaFileId);
        final String paddedId = rawId.length() > DELTA_ID_PAD_LENGTH ? rawId
                : FileNameUtil.getPaddedId(deltaFileId, DELTA_ID_PAD_LENGTH);
        return segmentId.getName() + DELTA_FILE_NAME_MIDDLE + paddedId
                + CACHE_FILE_NAME_EXTENSION;
    }
}
