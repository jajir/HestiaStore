package org.hestiastore.index.segment;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.hestiastore.index.FileNameUtil;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.properties.PropertyStore;
import org.hestiastore.index.properties.PropertyStoreimpl;
import org.hestiastore.index.properties.PropertyTransaction;
import org.hestiastore.index.properties.PropertyView;
import org.hestiastore.index.properties.PropertyWriter;

/**
 * 
 * @author honza
 *
 */
public class SegmentPropertiesManager {

    private static final String NUMBER_OF_KEYS_IN_DELTA_CACHE = "numberOfKeysInDeltaCache";
    private static final String NUMBER_OF_KEYS_IN_MAIN_INDEX = "numberOfKeysInMainIndex";
    private static final String NUMBER_OF_KEYS_IN_SCARCE_INDEX = "numberOfKeysInScarceIndex";
    private static final String NUMBER_OF_SEGMENT_CACHE_DELTA_FILES = "numberOfSegmentDeltaFiles";
    private static final String PROPERTIES_FILENAME_EXTENSION = ".properties";

    private final SegmentId id;
    private final PropertyStore propertyStore;

    public SegmentPropertiesManager(final AsyncDirectory directoryFacade,
            final SegmentId id) {
        Vldtn.requireNonNull(directoryFacade, "directoryFacade");
        this.id = Vldtn.requireNonNull(id, "segmentId");
        this.propertyStore = PropertyStoreimpl.fromAsyncDirectory(
                directoryFacade, getPropertiesFilename(), false);
    }

    private String getPropertiesFilename() {
        return id.getName() + PROPERTIES_FILENAME_EXTENSION;
    }

    public SegmentStats getSegmentStats() {
        final PropertyView view = propertyStore.snapshot();
        return new SegmentStats(
                view.getLong(NUMBER_OF_KEYS_IN_DELTA_CACHE),
                view.getLong(NUMBER_OF_KEYS_IN_MAIN_INDEX),
                view.getLong(NUMBER_OF_KEYS_IN_SCARCE_INDEX));
    }

    public void clearCacheDeltaFileNamesCouter() {
        updateTransaction(writer -> writer.setInt(
                NUMBER_OF_SEGMENT_CACHE_DELTA_FILES, 0));
    }

    public String getAndIncreaseDeltaFileName() {
        final String nextName = getNextDeltaFileName();
        incrementDeltaFileNameCounter();
        return nextName;
    }

    public String getNextDeltaFileName() {
        final int counter = propertyStore.snapshot()
                .getInt(NUMBER_OF_SEGMENT_CACHE_DELTA_FILES);
        return getDeltaString(counter);
    }

    public void incrementDeltaFileNameCounter() {
        final int counter = propertyStore.snapshot()
                .getInt(NUMBER_OF_SEGMENT_CACHE_DELTA_FILES);
        updateTransaction(writer -> writer.setInt(
                NUMBER_OF_SEGMENT_CACHE_DELTA_FILES, counter + 1));
    }

    private String getDeltaString(final int segmentCacheDeltaFileId) {
        return id.getName() + "-delta-"
                + FileNameUtil.getPaddedId(segmentCacheDeltaFileId, 3)
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

    public void setNumberOfKeysInCache(final long numberOfKeysInCache) {
        updateTransaction(writer -> writer.setLong(
                NUMBER_OF_KEYS_IN_DELTA_CACHE, numberOfKeysInCache));
    }

    public void increaseNumberOfKeysInDeltaCache(final int howMuchKeys) {
        if (howMuchKeys < 0) {
            throw new IllegalArgumentException(String.format(
                    "Unable to increase numebr of keys in cache about value '%s'",
                    howMuchKeys));
        }
        final long current = propertyStore.snapshot()
                .getLong(NUMBER_OF_KEYS_IN_DELTA_CACHE);
        updateTransaction(
                writer -> writer.setLong(NUMBER_OF_KEYS_IN_DELTA_CACHE,
                        current + howMuchKeys));
    }

    public void incrementNumberOfKeysInCache() {
        increaseNumberOfKeysInDeltaCache(1);
    }

    public void setNumberOfKeysInIndex(final long numberOfKeysInIndex) {
        updateTransaction(writer -> writer.setLong(
                NUMBER_OF_KEYS_IN_MAIN_INDEX, numberOfKeysInIndex));
    }

    public void setNumberOfKeysInScarceIndex(
            final long numberOfKeysInScarceIndex) {
        updateTransaction(writer -> writer.setLong(
                NUMBER_OF_KEYS_IN_SCARCE_INDEX, numberOfKeysInScarceIndex));
    }

    public long getNumberOfKeysInDeltaCache() {
        return getSegmentStats().getNumberOfKeysInDeltaCache();
    }

    private void updateTransaction(final Consumer<PropertyWriter> updater) {
        final PropertyTransaction tx = propertyStore.beginTransaction();
        final PropertyWriter writer = tx.openPropertyWriter();
        updater.accept(writer);
        tx.close();
    }

}
