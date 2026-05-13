package org.hestiastore.index.chunkstorecache;

/**
 * Loads one parsed chunk page from the backing chunk store.
 *
 * @param <K> key type
 * @param <V> value type
 */
@FunctionalInterface
public interface ChunkPageLoader<K, V> {

    /**
     * Loads and parses the page.
     *
     * @return parsed page
     */
    ParsedChunkPage<K, V> load();
}
