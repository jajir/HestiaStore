package org.hestiastore.index.segment;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.properties.IndexPropertiesSchema;
import org.hestiastore.index.properties.PropertyMutationSession;
import org.hestiastore.index.properties.PropertyStore;
import org.hestiastore.index.properties.PropertyStoreImpl;
import org.hestiastore.index.properties.PropertyView;
import org.hestiastore.index.properties.PropertyWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

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
    private static final String INDEX_NAME_MDC_KEY = "index.name";
    private final SegmentId id;
    private final SegmentDirectoryLayout layout;
    private final String loggingContextIndexName;
    private volatile PropertyStore propertyStore;
    private final Object propertyLock = new Object();

    /**
     * Creates a manager for the given segment properties file.
     *
     * @param directoryFacade directory for property storage
     * @param id              segment identifier
     */
    public SegmentPropertiesManager(final Directory directoryFacade,
            final SegmentId id) {
        this(directoryFacade, id, null);
    }

    /**
     * Creates a manager for the given segment properties file.
     *
     * @param directoryFacade         directory for property storage
     * @param id                      segment identifier
     * @param loggingContextIndexName optional index name used for MDC logging
     *                                context
     */
    public SegmentPropertiesManager(final Directory directoryFacade,
            final SegmentId id, final String loggingContextIndexName) {
        Vldtn.requireNonNull(directoryFacade, "directoryFacade");
        this.id = Vldtn.requireNonNull(id, "segmentId");
        this.layout = new SegmentDirectoryLayout(id);
        this.loggingContextIndexName = normalizeIndexNameForMdc(
                loggingContextIndexName);
        this.propertyStore = createStore(directoryFacade);
    }

    private static String normalizeIndexNameForMdc(final String indexName) {
        if (indexName == null) {
            return null;
        }
        final String normalized = indexName.trim();
        return normalized.isEmpty() ? null : normalized;
    }

    private PropertyStore createStore(final Directory directoryFacade) {
        final PropertyStore store = PropertyStoreImpl
                .fromDirectory(directoryFacade, getPropertiesFilename(), false);
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
        return new SegmentStats(view.getLong(NUMBER_OF_KEYS_IN_DELTA_CACHE),
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
     * Starts a staged metadata transaction for this segment.
     *
     * @return opened segment-properties transaction
     */
    public SegmentPropertiesManagerTx startTx() {
        return new SegmentPropertiesManagerTx(this);
    }

    /**
     * Resets the delta file name counter to zero.
     */
    public void clearCacheDeltaFileNamesCouter() {
        synchronized (propertyLock) {
            updateTransaction("clearCacheDeltaFileNamesCounter",
                    writer -> writer.setInt(NUMBER_OF_SEGMENT_CACHE_DELTA_FILES,
                            0));
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
            updateTransaction("incrementDeltaFileNameCounter", writer -> writer
                    .setInt(NUMBER_OF_SEGMENT_CACHE_DELTA_FILES, counter + 1));
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
     * Increments the number of keys in the delta cache by one.
     */
    public void incrementNumberOfKeysInCache() {
        synchronized (propertyLock) {
            final long current = propertyStore.snapshot()
                    .getLong(NUMBER_OF_KEYS_IN_DELTA_CACHE);
            updateTransaction("incrementNumberOfKeysInCache", writer -> writer
                    .setLong(NUMBER_OF_KEYS_IN_DELTA_CACHE, current + 1));
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
        final String previousIndexName = MDC.get(INDEX_NAME_MDC_KEY);
        final boolean contextApplied = loggingContextIndexName != null;
        if (contextApplied) {
            MDC.put(INDEX_NAME_MDC_KEY, loggingContextIndexName);
        }
        try {
            IndexPropertiesSchema.SEGMENT_SCHEMA.ensure(propertyStore);
            boolean changed = false;
            try (PropertyMutationSession session = propertyStore
                    .openMutationSession()) {
                updater.accept(session.writer());
                changed = session.hasChanges();
            }
            if (changed && logger.isDebugEnabled()) {
                logger.debug(
                        "Segment properties were written: "
                                + "segment='{}' file='{}' reason='{}' "
                                + "thread='{}' manager='{}'",
                        id.getName(), getPropertiesFilename(), reason,
                        Thread.currentThread().getName(),
                        Integer.toHexString(System.identityHashCode(this)));
            }
        } finally {
            if (contextApplied) {
                restorePreviousIndexName(previousIndexName);
            }
        }
    }

    private static void restorePreviousIndexName(
            final String previousIndexName) {
        if (previousIndexName == null) {
            MDC.remove(INDEX_NAME_MDC_KEY);
            return;
        }
        MDC.put(INDEX_NAME_MDC_KEY, previousIndexName);
    }

    void commitTx(final String reason, final Consumer<PropertyWriter> updater) {
        Vldtn.requireNonNull(updater, "updater");
        synchronized (propertyLock) {
            updateTransaction(reason, updater);
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
