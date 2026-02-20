package org.hestiastore.index.segment;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.properties.IndexPropertiesSchema;
import org.hestiastore.index.properties.PropertyMutationSession;
import org.hestiastore.index.properties.PropertyStore;
import org.hestiastore.index.properties.PropertyStoreimpl;
import org.hestiastore.index.properties.PropertyView;
import org.hestiastore.index.properties.PropertyWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages segment metadata stored in the properties file.
 */
public class SegmentPropertiesManager {

    private static final Logger logger = LoggerFactory
            .getLogger(SegmentPropertiesManager.class);

    private static final String NUMBER_OF_KEYS_IN_DELTA_CACHE = IndexPropertiesSchema.SegmentKeys.NUMBER_OF_KEYS_IN_DELTA_CACHE;
    private static final String NUMBER_OF_KEYS_IN_MAIN_INDEX = IndexPropertiesSchema.SegmentKeys.NUMBER_OF_KEYS_IN_MAIN_INDEX;
    private static final String NUMBER_OF_KEYS_IN_SCARCE_INDEX = IndexPropertiesSchema.SegmentKeys.NUMBER_OF_KEYS_IN_SCARCE_INDEX;
    private static final String NUMBER_OF_SEGMENT_CACHE_DELTA_FILES = IndexPropertiesSchema.SegmentKeys.NUMBER_OF_SEGMENT_CACHE_DELTA_FILES;
    private static final String SEGMENT_VERSION = IndexPropertiesSchema.SegmentKeys.SEGMENT_VERSION;
    private final SegmentId id;
    private final SegmentDirectoryLayout layout;
    private volatile PropertyStore propertyStore;
    private final Object propertyLock = new Object();

    /**
     * Creates a manager for the given segment properties file.
     *
     * @param directoryFacade directory for property storage
     * @param id segment identifier
     */
    public SegmentPropertiesManager(final Directory directoryFacade,
            final SegmentId id) {
        Vldtn.requireNonNull(directoryFacade, "directoryFacade");
        this.id = Vldtn.requireNonNull(id, "segmentId");
        this.layout = new SegmentDirectoryLayout(id);
        this.propertyStore = createStore(directoryFacade);
        if (logger.isDebugEnabled()) {
            logger.debug(
                    "SegmentPropertiesManager created: segment='{}' file='{}' thread='{}' manager='{}'",
                    this.id.getName(), getPropertiesFilename(),
                    Thread.currentThread().getName(),
                    Integer.toHexString(System.identityHashCode(this)));
        }
    }

    private PropertyStore createStore(final Directory directoryFacade) {
        final PropertyStore store = PropertyStoreimpl.fromDirectory(
                directoryFacade, getPropertiesFilename(), false);
        if (directoryFacade.isFileExists(getPropertiesFilename())) {
            IndexPropertiesSchema.SEGMENT_SCHEMA.ensure(store);
        }
        return store;
    }

    /**
     * Returns the properties file name for this segment.
     *
     * @return properties file name
     */
    private String getPropertiesFilename() {
        return layout.getPropertiesFileName();
    }

    /**
     * Returns a snapshot of segment key statistics from persisted properties.
     *
     * @return segment statistics snapshot
     */
    public SegmentStats getSegmentStats() {
        final PropertyView view = propertyStore.snapshot();
        return new SegmentStats(
                view.getLong(NUMBER_OF_KEYS_IN_DELTA_CACHE),
                view.getLong(NUMBER_OF_KEYS_IN_MAIN_INDEX),
                view.getLong(NUMBER_OF_KEYS_IN_SCARCE_INDEX));
    }

    /**
     * Returns the active on-disk version for this segment.
     *
     * @return active version (0 indicates legacy unversioned files)
     */
    public long getVersion() {
        return propertyStore.snapshot().getLong(SEGMENT_VERSION);
    }

    /**
     * Sets the active on-disk version for this segment.
     *
     * @param version active version (0 indicates legacy unversioned files)
     */
    public void setVersion(final long version) {
        synchronized (propertyLock) {
            updateTransaction("setVersion",
                    writer -> writer.setLong(SEGMENT_VERSION, version));
        }
    }

    /**
     * Resets the delta file name counter to zero.
     */
    public void clearCacheDeltaFileNamesCouter() {
        synchronized (propertyLock) {
            updateTransaction("clearCacheDeltaFileNamesCounter",
                    writer -> writer.setInt(
                    NUMBER_OF_SEGMENT_CACHE_DELTA_FILES, 0));
        }
    }

    /**
     * Returns the next delta file name and increments the counter.
     *
     * @return new delta file name
     */
    public String getAndIncreaseDeltaFileName() {
        synchronized (propertyLock) {
            final PropertyView view = propertyStore.snapshot();
            final int counter = view
                    .getInt(NUMBER_OF_SEGMENT_CACHE_DELTA_FILES);
            final long version = view.getLong(SEGMENT_VERSION);
            updateTransaction("getAndIncreaseDeltaFileName",
                    writer -> writer.setInt(
                    NUMBER_OF_SEGMENT_CACHE_DELTA_FILES, counter + 1));
            return getDeltaString(version, counter);
        }
    }

    /**
     * Returns the next delta file name without incrementing the counter.
     *
     * @return next delta file name
     */
    public String getNextDeltaFileName() {
        synchronized (propertyLock) {
            final PropertyView view = propertyStore.snapshot();
            final int counter = view
                    .getInt(NUMBER_OF_SEGMENT_CACHE_DELTA_FILES);
            final long version = view.getLong(SEGMENT_VERSION);
            return getDeltaString(version, counter);
        }
    }

    /**
     * Increments the delta file name counter.
     */
    public void incrementDeltaFileNameCounter() {
        synchronized (propertyLock) {
            final int counter = propertyStore.snapshot()
                    .getInt(NUMBER_OF_SEGMENT_CACHE_DELTA_FILES);
            updateTransaction("incrementDeltaFileNameCounter",
                    writer -> writer.setInt(
                    NUMBER_OF_SEGMENT_CACHE_DELTA_FILES, counter + 1));
        }
    }

    /**
     * Builds a delta cache file name for the given numeric id.
     *
     * @param segmentCacheDeltaFileId delta file numeric id
     * @return delta file name
     */
    private String getDeltaString(final long version,
            final int segmentCacheDeltaFileId) {
        final long resolvedVersion = Math.max(1, version);
        return layout.getDeltaCacheFileName(resolvedVersion,
                segmentCacheDeltaFileId);
    }

    /**
     * Prepare cache delta file names. File names are ascending ordered.
     * 
     * @return return sorted cache delta filenames.
     */
    public List<String> getCacheDeltaFileNames() {
        final List<String> out = new ArrayList<>();
        final PropertyView view = propertyStore.snapshot();
        final int lastOne = view.getInt(NUMBER_OF_SEGMENT_CACHE_DELTA_FILES);
        final long version = view.getLong(SEGMENT_VERSION);
        for (int i = 0; i < lastOne; i++) {
            out.add(getDeltaString(version, i));
        }
        return out;
    }

    /**
     * Prepare cache delta file names for the given version.
     *
     * @param version version to use for naming
     * @return sorted cache delta filenames
     */
    public List<String> getCacheDeltaFileNames(final long version) {
        final List<String> out = new ArrayList<>();
        final int lastOne = propertyStore.snapshot()
                .getInt(NUMBER_OF_SEGMENT_CACHE_DELTA_FILES);
        for (int i = 0; i < lastOne; i++) {
            out.add(getDeltaString(version, i));
        }
        return out;
    }

    /**
     * Returns the number of delta files recorded in properties.
     *
     * @return delta file count
     */
    public int getDeltaFileCount() {
        return propertyStore.snapshot()
                .getInt(NUMBER_OF_SEGMENT_CACHE_DELTA_FILES);
    }

    /**
     * Sets the number of delta files recorded in properties.
     *
     * @param count number of delta files
     */
    public void setDeltaFileCount(final int count) {
        synchronized (propertyLock) {
            updateTransaction("setDeltaFileCount",
                    writer -> writer.setInt(
                            NUMBER_OF_SEGMENT_CACHE_DELTA_FILES, count));
        }
    }

    /**
     * Sets the number of keys stored in the delta cache.
     *
     * @param numberOfKeysInCache number of keys in cache
     */
    public void setNumberOfKeysInCache(final long numberOfKeysInCache) {
        synchronized (propertyLock) {
            updateTransaction("setNumberOfKeysInCache",
                    writer -> writer.setLong(NUMBER_OF_KEYS_IN_DELTA_CACHE,
                            numberOfKeysInCache));
        }
    }

    /**
     * Increases the number of keys in the delta cache by the given amount.
     *
     * @param howMuchKeys number of keys to add
     */
    public void increaseNumberOfKeysInDeltaCache(final int howMuchKeys) {
        if (howMuchKeys < 0) {
            throw new IllegalArgumentException(String.format(
                    "Unable to increase numebr of keys in cache about value '%s'",
                    howMuchKeys));
        }
        synchronized (propertyLock) {
            final long current = propertyStore.snapshot()
                    .getLong(NUMBER_OF_KEYS_IN_DELTA_CACHE);
            updateTransaction("increaseNumberOfKeysInDeltaCache",
                    writer -> writer.setLong(NUMBER_OF_KEYS_IN_DELTA_CACHE,
                            current + howMuchKeys));
        }
    }

    /**
     * Increments the number of keys in the delta cache by one.
     */
    public void incrementNumberOfKeysInCache() {
        increaseNumberOfKeysInDeltaCache(1);
    }

    /**
     * Sets the number of keys stored in the main index.
     *
     * @param numberOfKeysInIndex number of keys in the index
     */
    public void setNumberOfKeysInIndex(final long numberOfKeysInIndex) {
        synchronized (propertyLock) {
            updateTransaction("setNumberOfKeysInIndex",
                    writer -> writer.setLong(NUMBER_OF_KEYS_IN_MAIN_INDEX,
                            numberOfKeysInIndex));
        }
    }

    /**
     * Sets the number of keys stored in the scarce index.
     *
     * @param numberOfKeysInScarceIndex number of scarce index keys
     */
    public void setNumberOfKeysInScarceIndex(
            final long numberOfKeysInScarceIndex) {
        synchronized (propertyLock) {
            updateTransaction("setNumberOfKeysInScarceIndex",
                    writer -> writer.setLong(NUMBER_OF_KEYS_IN_SCARCE_INDEX,
                            numberOfKeysInScarceIndex));
        }
    }

    /**
     * Stores all key counters in one transaction to avoid repeated metadata
     * writes.
     *
     * @param numberOfKeysInCache       number of keys in delta cache
     * @param numberOfKeysInIndex       number of keys in main index
     * @param numberOfKeysInScarceIndex number of keys in scarce index
     */
    public void setKeyCounters(final long numberOfKeysInCache,
            final long numberOfKeysInIndex,
            final long numberOfKeysInScarceIndex) {
        synchronized (propertyLock) {
            updateTransaction("setKeyCounters", writer -> {
                writer.setLong(NUMBER_OF_KEYS_IN_DELTA_CACHE,
                        numberOfKeysInCache);
                writer.setLong(NUMBER_OF_KEYS_IN_MAIN_INDEX,
                        numberOfKeysInIndex);
                writer.setLong(NUMBER_OF_KEYS_IN_SCARCE_INDEX,
                        numberOfKeysInScarceIndex);
            });
        }
    }

    /**
     * Returns the number of keys stored in the delta cache.
     *
     * @return delta cache key count
     */
    public long getNumberOfKeysInDeltaCache() {
        return getSegmentStats().getNumberOfKeysInDeltaCache();
    }

    /**
     * Runs a property update transaction under the segment lock.
     *
     * @param updater callback that mutates the property writer
     */
    private void updateTransaction(final String reason,
            final Consumer<PropertyWriter> updater) {
        Vldtn.requireNonNull(reason, "reason");
        IndexPropertiesSchema.SEGMENT_SCHEMA.ensure(propertyStore);
        boolean changed = false;
        try (PropertyMutationSession session = propertyStore
                .openMutationSession()) {
            updater.accept(session.writer());
            changed = session.hasChanges();
        }
        if (changed && logger.isDebugEnabled()) {
            logger.debug(
                    "Segment properties write end: segment='{}' file='{}' reason='{}' thread='{}' manager='{}'",
                    id.getName(), getPropertiesFilename(),
                    reason,
                    Thread.currentThread().getName(),
                    Integer.toHexString(System.identityHashCode(this)));
        }
    }

    PropertyStore getPropertyStore() {
        return propertyStore;
    }

    void switchToStore(final PropertyStore store) {
        Vldtn.requireNonNull(store, "store");
        synchronized (propertyLock) {
            this.propertyStore = store;
        }
    }

    /**
     * Switches the properties manager to a new directory.
     *
     * @param directoryFacade directory containing the properties file
     */
    void switchDirectory(final Directory directoryFacade) {
        Vldtn.requireNonNull(directoryFacade, "directoryFacade");
        switchToStore(createStore(directoryFacade));
    }

}
