package org.hestiastore.index.segment;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.hestiastore.index.FileNameUtil;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.properties.IndexPropertiesSchema;
import org.hestiastore.index.properties.PropertyStore;
import org.hestiastore.index.properties.PropertyStoreimpl;
import org.hestiastore.index.properties.PropertyTransaction;
import org.hestiastore.index.properties.PropertyView;
import org.hestiastore.index.properties.PropertyWriter;

/**
 * Manages segment metadata stored in the properties file.
 */
public class SegmentPropertiesManager {

    private static final String NUMBER_OF_KEYS_IN_DELTA_CACHE = IndexPropertiesSchema.SegmentKeys.NUMBER_OF_KEYS_IN_DELTA_CACHE;
    private static final String NUMBER_OF_KEYS_IN_MAIN_INDEX = IndexPropertiesSchema.SegmentKeys.NUMBER_OF_KEYS_IN_MAIN_INDEX;
    private static final String NUMBER_OF_KEYS_IN_SCARCE_INDEX = IndexPropertiesSchema.SegmentKeys.NUMBER_OF_KEYS_IN_SCARCE_INDEX;
    private static final String NUMBER_OF_SEGMENT_CACHE_DELTA_FILES = IndexPropertiesSchema.SegmentKeys.NUMBER_OF_SEGMENT_CACHE_DELTA_FILES;
    private static final String SEGMENT_STATE = IndexPropertiesSchema.SegmentKeys.SEGMENT_STATE;
    private static final String SEGMENT_VERSION = IndexPropertiesSchema.SegmentKeys.SEGMENT_VERSION;
    private static final String PROPERTIES_FILENAME_EXTENSION = ".properties";

    private final SegmentId id;
    private volatile PropertyStore propertyStore;
    private final Object propertyLock = new Object();

    /**
     * States used for directory-level segment publishing.
     */
    public enum SegmentDataState {
        ACTIVE, PREPARED
    }

    /**
     * Creates a manager for the given segment properties file.
     *
     * @param directoryFacade async directory for property storage
     * @param id segment identifier
     */
    public SegmentPropertiesManager(final AsyncDirectory directoryFacade,
            final SegmentId id) {
        Vldtn.requireNonNull(directoryFacade, "directoryFacade");
        this.id = Vldtn.requireNonNull(id, "segmentId");
        this.propertyStore = createStore(directoryFacade);
    }

    private PropertyStore createStore(final AsyncDirectory directoryFacade) {
        final PropertyStore store = PropertyStoreimpl.fromAsyncDirectory(
                directoryFacade, getPropertiesFilename(), false);
        if (directoryFacade.isFileExistsAsync(getPropertiesFilename())
                .toCompletableFuture().join()) {
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
        return id.getName() + PROPERTIES_FILENAME_EXTENSION;
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
     * Returns the directory-level state of the segment.
     *
     * @return segment data state
     */
    public SegmentDataState getState() {
        final String state = propertyStore.snapshot().getString(SEGMENT_STATE);
        if (state == null || state.isBlank()) {
            return SegmentDataState.ACTIVE;
        }
        try {
            return SegmentDataState.valueOf(state);
        } catch (final IllegalArgumentException e) {
            return SegmentDataState.ACTIVE;
        }
    }

    /**
     * Sets the directory-level state of the segment.
     *
     * @param state state to store
     */
    public void setState(final SegmentDataState state) {
        Vldtn.requireNonNull(state, "state");
        synchronized (propertyLock) {
            updateTransaction(writer -> writer.setString(SEGMENT_STATE,
                    state.name()));
        }
    }

    /**
     * Returns the version identifier stored with this segment.
     *
     * @return segment version
     */
    public long getVersion() {
        return propertyStore.snapshot().getLong(SEGMENT_VERSION);
    }

    /**
     * Sets the version identifier for this segment.
     *
     * @param version segment version
     */
    public void setVersion(final long version) {
        synchronized (propertyLock) {
            updateTransaction(
                    writer -> writer.setLong(SEGMENT_VERSION, version));
        }
    }

    /**
     * Resets the delta file name counter to zero.
     */
    public void clearCacheDeltaFileNamesCouter() {
        synchronized (propertyLock) {
            updateTransaction(writer -> writer.setInt(
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
            final int counter = propertyStore.snapshot()
                    .getInt(NUMBER_OF_SEGMENT_CACHE_DELTA_FILES);
            updateTransaction(writer -> writer.setInt(
                    NUMBER_OF_SEGMENT_CACHE_DELTA_FILES, counter + 1));
            return getDeltaString(counter);
        }
    }

    /**
     * Returns the next delta file name without incrementing the counter.
     *
     * @return next delta file name
     */
    public String getNextDeltaFileName() {
        synchronized (propertyLock) {
            final int counter = propertyStore.snapshot()
                    .getInt(NUMBER_OF_SEGMENT_CACHE_DELTA_FILES);
            return getDeltaString(counter);
        }
    }

    /**
     * Increments the delta file name counter.
     */
    public void incrementDeltaFileNameCounter() {
        synchronized (propertyLock) {
            final int counter = propertyStore.snapshot()
                    .getInt(NUMBER_OF_SEGMENT_CACHE_DELTA_FILES);
            updateTransaction(writer -> writer.setInt(
                    NUMBER_OF_SEGMENT_CACHE_DELTA_FILES, counter + 1));
        }
    }

    /**
     * Builds a delta cache file name for the given numeric id.
     *
     * @param segmentCacheDeltaFileId delta file numeric id
     * @return delta file name
     */
    private String getDeltaString(final int segmentCacheDeltaFileId) {
        final String rawId = String.valueOf(segmentCacheDeltaFileId);
        final String paddedId = rawId.length() > 3 ? rawId
                : FileNameUtil.getPaddedId(segmentCacheDeltaFileId, 3);
        return id.getName() + "-delta-" + paddedId
                + SegmentFiles.CACHE_FILE_NAME_EXTENSION;
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
        for (int i = 0; i < lastOne; i++) {
            out.add(getDeltaString(i));
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
     * Sets the number of keys stored in the delta cache.
     *
     * @param numberOfKeysInCache number of keys in cache
     */
    public void setNumberOfKeysInCache(final long numberOfKeysInCache) {
        synchronized (propertyLock) {
            updateTransaction(writer -> writer.setLong(
                    NUMBER_OF_KEYS_IN_DELTA_CACHE, numberOfKeysInCache));
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
            updateTransaction(
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
            updateTransaction(writer -> writer.setLong(
                    NUMBER_OF_KEYS_IN_MAIN_INDEX, numberOfKeysInIndex));
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
            updateTransaction(writer -> writer.setLong(
                    NUMBER_OF_KEYS_IN_SCARCE_INDEX, numberOfKeysInScarceIndex));
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
    private void updateTransaction(final Consumer<PropertyWriter> updater) {
        IndexPropertiesSchema.SEGMENT_SCHEMA.ensure(propertyStore);
        final PropertyTransaction tx = propertyStore.beginTransaction();
        final PropertyWriter writer = tx.openPropertyWriter();
        updater.accept(writer);
        tx.close();
    }

    /**
     * Switches the properties manager to a new directory.
     *
     * @param directoryFacade directory containing the properties file
     */
    void switchDirectory(final AsyncDirectory directoryFacade) {
        Vldtn.requireNonNull(directoryFacade, "directoryFacade");
        synchronized (propertyLock) {
            this.propertyStore = createStore(directoryFacade);
        }
    }

}
