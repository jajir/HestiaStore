package com.coroptis.index.segment;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.Props;

/**
 * 
 * @author honza
 *
 */
public class SegmentPropertiesManager {

    private final static String NUMBER_OF_KEYS_IN_CACHE = "numberOfKeysInCache";
    private final static String NUMBER_OF_KEYS_IN_INDEX = "numberOfKeysInIndex";
    private final static String NUMBER_OF_KEYS_IN_SCARCE_INDEX = "numberOfKeysInScarceIndex";
    private final static String NUMBER_OF_SEGMENT_CACHE_DELTA_FILES = "numberOfSegmentDeltaFiles";
    private final static String PROPERTIES_FILENAME_EXTENSION = ".properties";

    private final SegmentId id;
    private final Props props;

    public SegmentPropertiesManager(final Directory directory,
            final SegmentId id) {
        Objects.requireNonNull(directory);
        this.id = Objects.requireNonNull(id);
        this.props = new Props(directory, getPropertiesFilename());
    }

    private String getPropertiesFilename() {
        return id.getName() + PROPERTIES_FILENAME_EXTENSION;
    }

    public SegmentStats getSegmentStats() {
        return new SegmentStats(props.getLong(NUMBER_OF_KEYS_IN_CACHE),
                props.getLong(NUMBER_OF_KEYS_IN_INDEX),
                props.getLong(NUMBER_OF_KEYS_IN_SCARCE_INDEX));
    }

    public void clearCacheDeltaFileNamesCouter() {
        props.setInt(NUMBER_OF_SEGMENT_CACHE_DELTA_FILES, 0);
        props.writeData();
    }

    public String getAndIncreaseDeltaFileName() {
        int lastOne = props.getInt(NUMBER_OF_SEGMENT_CACHE_DELTA_FILES);
        props.setInt(NUMBER_OF_SEGMENT_CACHE_DELTA_FILES, lastOne + 1);
        props.writeData();
        return getDeltaString(lastOne);
    }

    private String getDeltaString(final int segmentCacheDeltaFileId) {
        String name = String.valueOf(segmentCacheDeltaFileId);
        while (name.length() < 3) {
            name = "0" + name;
        }
        return id.getName() + "-delta-" + name
                + SegmentFiles.CACHE_FILE_NAME_EXTENSION;
    }

    //TODO add sorting from 0 to n.
    public List<String> getCacheDeltaFileNames() {
        final List<String> out = new ArrayList<>();
        int lastOne = props.getInt(NUMBER_OF_SEGMENT_CACHE_DELTA_FILES);
        for (int i = 0; i < lastOne; i++) {
            out.add(getDeltaString(i));
        }
        return out;
    }

    public void setNumberOfKeysInCache(final long numberOfKeysInCache) {
        props.setLong(NUMBER_OF_KEYS_IN_CACHE, numberOfKeysInCache);
    }

    public void incrementNumberOfKeysInCache() {
        props.setLong(NUMBER_OF_KEYS_IN_CACHE,
                props.getLong(NUMBER_OF_KEYS_IN_CACHE) + 1);
    }

    public void setNumberOfKeysInIndex(final long numberOfKeysInIndex) {
        props.setLong(NUMBER_OF_KEYS_IN_INDEX, numberOfKeysInIndex);
    }

    public void setNumberOfKeysInScarceIndex(
            final long numberOfKeysInScarceIndex) {
        props.setLong(NUMBER_OF_KEYS_IN_SCARCE_INDEX,
                numberOfKeysInScarceIndex);
    }

    public void flush() {
        props.writeData();
    }

}
