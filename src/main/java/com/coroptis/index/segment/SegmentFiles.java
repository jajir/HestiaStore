package com.coroptis.index.segment;

import java.util.Objects;

import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.scarceindex.ScarceIndex;
import com.coroptis.index.sstfile.SstFile;

/**
 * Allows to easily access segment files.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class SegmentFiles<K, V> {

    private final static String INDEX_FILE_NAME_EXTENSION = ".index";
    private final static String SCARCE_FILE_NAME_EXTENSION = ".scarce";
    private final static String CACHE_FILE_NAME_EXTENSION = ".cache";
    private final static String TEMP_FILE_NAME_EXTENSION = ".tmp";
    private final static String BOOM_FILTER_FILE_NAME_EXTENSION = ".bloom-filter";

    private final Directory directory;
    private final SegmentId id;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;

    public SegmentFiles(final Directory directory, final SegmentId id,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor) {
        this.directory = Objects.requireNonNull(directory);
        this.id = Objects.requireNonNull(id);
        this.keyTypeDescriptor = Objects.requireNonNull(keyTypeDescriptor);
        this.valueTypeDescriptor = Objects.requireNonNull(valueTypeDescriptor);
    }

    String getCacheFileName() {
        return id.getName() + CACHE_FILE_NAME_EXTENSION;
    }

    String getTempIndexFileName() {
        return id.getName() + TEMP_FILE_NAME_EXTENSION
                + INDEX_FILE_NAME_EXTENSION;
    }

    String getTempScarceFileName() {
        return id.getName() + TEMP_FILE_NAME_EXTENSION
                + SCARCE_FILE_NAME_EXTENSION;
    }

    String getScarceFileName() {
        return id.getName() + SCARCE_FILE_NAME_EXTENSION;
    }

    String getBloomFilterFileName() {
        return id.getName() + BOOM_FILTER_FILE_NAME_EXTENSION;
    }

    String getIndexFileName() {
        return id.getName() + INDEX_FILE_NAME_EXTENSION;
    }

    SstFile<K, V> getCacheSstFile() {
        return new SstFile<>(directory, getCacheFileName(),
                valueTypeDescriptor.getTypeWriter(),
                valueTypeDescriptor.getTypeReader(),
                keyTypeDescriptor.getComparator(),
                keyTypeDescriptor.getConvertorFromBytes(),
                keyTypeDescriptor.getConvertorToBytes());
    }

    SstFile<K, V> getIndexSstFile() {
        return new SstFile<>(directory, getIndexFileName(),
                valueTypeDescriptor.getTypeWriter(),
                valueTypeDescriptor.getTypeReader(),
                keyTypeDescriptor.getComparator(),
                keyTypeDescriptor.getConvertorFromBytes(),
                keyTypeDescriptor.getConvertorToBytes());
    }

    SstFile<K, V> getTempIndexFile() {
        return new SstFile<>(directory, getTempIndexFileName(),
                valueTypeDescriptor.getTypeWriter(),
                valueTypeDescriptor.getTypeReader(),
                keyTypeDescriptor.getComparator(),
                keyTypeDescriptor.getConvertorFromBytes(),
                keyTypeDescriptor.getConvertorToBytes());
    }

    ScarceIndex<K> getTempScarceIndex() {
        return ScarceIndex.<K>builder()//
                .withDirectory(getDirectory())//
                .withFileName(getTempScarceFileName())//
                .withKeyTypeDescriptor(getKeyTypeDescriptor())//
                .build();
    }

    Directory getDirectory() {
        return directory;
    }

    SegmentId getId() {
        return id;
    }

    TypeDescriptor<K> getKeyTypeDescriptor() {
        return keyTypeDescriptor;
    }

    TypeDescriptor<V> getValueTypeDescriptor() {
        return valueTypeDescriptor;
    }
}
