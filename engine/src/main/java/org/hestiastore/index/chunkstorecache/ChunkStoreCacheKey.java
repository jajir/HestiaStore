package org.hestiastore.index.chunkstorecache;

import java.util.Objects;

import org.hestiastore.index.Vldtn;

/**
 * Versioned key for one parsed persisted chunk page.
 */
final class ChunkStoreCacheKey {

    private final String ownerId;
    private final long activeVersion;
    private final long chunkPosition;

    private ChunkStoreCacheKey(final String ownerId, final long activeVersion,
            final long chunkPosition) {
        this.ownerId = Vldtn.requireNotBlank(ownerId, "ownerId");
        this.activeVersion = Vldtn.requireGreaterThanOrEqualToZero(
                activeVersion, "activeVersion");
        this.chunkPosition = Vldtn.requireGreaterThanOrEqualToZero(
                chunkPosition, "chunkPosition");
    }

    /**
     * Creates a cache key.
     *
     * @param ownerId segment owner id
     * @param activeVersion active persisted version
     * @param chunkPosition chunk start position
     * @return cache key
     */
    static ChunkStoreCacheKey of(final String ownerId,
            final long activeVersion, final long chunkPosition) {
        return new ChunkStoreCacheKey(ownerId, activeVersion, chunkPosition);
    }

    String ownerId() {
        return ownerId;
    }

    long activeVersion() {
        return activeVersion;
    }

    long chunkPosition() {
        return chunkPosition;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ownerId, activeVersion, chunkPosition);
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final ChunkStoreCacheKey other = (ChunkStoreCacheKey) obj;
        return activeVersion == other.activeVersion
                && chunkPosition == other.chunkPosition
                && ownerId.equals(other.ownerId);
    }

    @Override
    public String toString() {
        return "ChunkStoreCacheKey[ownerId=" + ownerId + ",activeVersion="
                + activeVersion + ",chunkPosition=" + chunkPosition + "]";
    }
}
