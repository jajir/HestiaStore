package org.hestiastore.index.chunkstore;

import org.hestiastore.index.IndexException;

/** Immutable chunk compression setting; independent of key page encoding. */
public final class Compression {
    private final int level;

    private Compression(final int level) {
        this.level = level;
    }

    /** @return uncompressed chunk payloads */
    public static Compression none() {
        return new Compression(0);
    }

    /**
     * @param level Zstd compression level 1 through 22
     * @return Zstd setting
     */
    public static Compression zstd(final int level) {
        if (level < 1 || level > 22) {
            throw new IndexException("Zstd level must be between 1 and 22.");
        }
        return new Compression(level);
    }

    /** @return stable codec name for index metadata */
    public String getId() {
        return level == 0 ? "none" : "zstd";
    }

    /** @return Zstd level, or zero for no compression */
    public int getLevel() {
        return level;
    }

    /**
     * Resolves exact persisted settings, rejecting unknown codecs.
     *
     * @param id    persisted name
     * @param level persisted level
     * @return compression setting
     */
    public static Compression fromId(final String id, final int level) {
        if ("none".equals(id) && level == 0) {
            return none();
        }
        if ("zstd".equals(id)) {
            return zstd(level);
        }
        throw new IndexException(
                "Unsupported compression setting: " + id + "/" + level);
    }
}
