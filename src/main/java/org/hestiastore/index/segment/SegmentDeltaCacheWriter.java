package org.hestiastore.index.segment;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.cache.UniqueCache;
import org.hestiastore.index.chunkentryfile.ChunkEntryFileWriter;
import org.hestiastore.index.chunkentryfile.ChunkEntryFileWriterTx;

/**
 * Class collect unsorted data, sort them and finally write them into SST delta
 * file.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
final class SegmentDeltaCacheWriter<K, V>
        extends AbstractCloseableResource implements EntryWriter<K, V> {

    /**
     * Cache will contains data written into this delta file.
     */
    private final UniqueCache<K, V> uniqueCache;

    private final SegmentPropertiesManager segmentPropertiesManager;
    private final SegmentFiles<K, V> segmentFiles;
    private final SegmentResources<K, V> segmentCacheDataProvider;
    private final int maxNumberOfKeysInChunk;

    /**
     * How many keys was added to delta cache.
     * 
     * Consider using this number, it could be higher. Because of this delta
     * file will contains update command or tombstones.
     */
    private int cx = 0;

    /**
     * Creates a writer that aggregates updates into a delta cache file.
     *
     * @param segmentFiles                       required segment files accessor
     * @param segmentPropertiesManager           required properties manager for
     *                                           stats and file names
     * @param segmentCacheDataProvider           required data provider to
     *                                           update in-memory cache when
     *                                           loaded
     * @param maxNumberOfKeysInSegmentWriteCache expected upper bound of keys
     *                                           collected in this delta file;
     *                                           must be greater than 0
     * @param maxNumberOfKeysInChunk            number of entries stored in a
     *                                           single chunk; must be greater
     *                                           than 0
     * @throws IllegalArgumentException when any argument is invalid or the
     *                                  provided max is not greater than 0
     */
    public SegmentDeltaCacheWriter(final SegmentFiles<K, V> segmentFiles,
            final SegmentPropertiesManager segmentPropertiesManager,
            final SegmentResources<K, V> segmentCacheDataProvider,
            final int maxNumberOfKeysInSegmentWriteCache,
            final int maxNumberOfKeysInChunk) {
        this.segmentPropertiesManager = Vldtn.requireNonNull(
                segmentPropertiesManager, "segmentPropertiesManager");
        this.segmentFiles = Vldtn.requireNonNull(segmentFiles, "segmentFiles");
        Vldtn.requireGreaterThanZero(maxNumberOfKeysInSegmentWriteCache,
                "maxNumberOfKeysInSegmentWriteCache");
        Vldtn.requireGreaterThanZero(maxNumberOfKeysInChunk,
                "maxNumberOfKeysInChunk");
        this.uniqueCache = UniqueCache.<K, V>builder()
                .withKeyComparator(
                        segmentFiles.getKeyTypeDescriptor().getComparator())
                .withInitialCapacity(maxNumberOfKeysInSegmentWriteCache)
                .buildEmpty();
        this.segmentCacheDataProvider = Vldtn.requireNonNull(
                segmentCacheDataProvider, "segmentCacheDataProvider");
        this.maxNumberOfKeysInChunk = maxNumberOfKeysInChunk;
    }

    /**
     * Returns the number of keys written into this delta cache writer.
     *
     * @return number of keys written
     */
    public int getNumberOfKeys() {
        return cx;
    }

    /**
     * Finally writes data to segment delta file and update numbed of keys in
     * delta cache.
     */
    @Override
    protected void doClose() {
        if (uniqueCache.isEmpty()) {
            return;
        }

        // store cache
        final String deltaFileName = segmentPropertiesManager
                .getAndIncreaseDeltaFileName();
        final ChunkEntryFileWriterTx<K, V> writerTx = segmentFiles
                .getDeltaCacheChunkEntryFile(deltaFileName).openWriterTx();
        try (ChunkEntryFileWriter<K, V> writer = writerTx.openWriter()) {
            int entriesInChunk = 0;
            for (final Entry<K, V> entry : uniqueCache.getAsSortedList()) {
                writer.write(entry);
                entriesInChunk++;
                if (entriesInChunk >= maxNumberOfKeysInChunk) {
                    writer.flush();
                    entriesInChunk = 0;
                }
            }
            if (entriesInChunk > 0) {
                writer.flush();
            }
        }
        writerTx.commit();
        // increase number of keys in cache
        final int keysInCache = uniqueCache.size();
        segmentPropertiesManager.increaseNumberOfKeysInDeltaCache(keysInCache);

        uniqueCache.clear();
    }

    /**
     * Adds an entry to the buffered delta cache and the in-memory cache view.
     *
     * @param entry entry to write
     */
    @Override
    public void write(Entry<K, V> entry) {
        uniqueCache.put(entry);
        cx++;
        segmentCacheDataProvider.getSegmentDeltaCache().put(entry);
    }

}
