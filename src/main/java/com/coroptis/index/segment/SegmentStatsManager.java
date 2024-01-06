package com.coroptis.index.segment;

import java.util.Objects;

import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.Props;

/**
 * 
 * @author honza
 *
 */
public class SegmentStatsManager {

    private final static String NUMBER_OF_KEYS_IN_CACHE = "numberOfKeysInCache";
    private final static String NUMBER_OF_KEYS_IN_INDEX = "numberOfKeysInIndex";
    private final static String NUMBER_OF_KEYS_IN_SCARCE_INDEX = "numberOfKeysInScarceIndex";
    private final static String PROPERTIES_FILENAME_EXTENSION = ".properties";

    private final SegmentId id;
    private final Props props;

    public SegmentStatsManager(final Directory directory, final SegmentId id) {
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
