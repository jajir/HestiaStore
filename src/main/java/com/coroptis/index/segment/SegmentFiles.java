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
    final static String CACHE_FILE_NAME_EXTENSION = ".cache";
    private final static String TEMP_FILE_NAME_EXTENSION = ".tmp";
    private final static String BOOM_FILTER_FILE_NAME_EXTENSION = ".bloom-filter";

    private final Directory directory;
    private final SegmentId id;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final int indexBufferSize;

    public SegmentFiles(final Directory directory, final SegmentId id,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final int indexBufferSize) {
        this.directory = Objects.requireNonNull(directory);
        this.id = Objects.requireNonNull(id);
        this.keyTypeDescriptor = Objects.requireNonNull(keyTypeDescriptor);
        this.valueTypeDescriptor = Objects.requireNonNull(valueTypeDescriptor);
        this.indexBufferSize = indexBufferSize;
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
        return SstFile.<K, V>builder() //
                .withDirectory(directory) //
                .withFileName(getCacheFileName())//
                .withKeyComparator(keyTypeDescriptor.getComparator()) //
                .withKeyConvertorFromBytes(
                        keyTypeDescriptor.getConvertorFromBytes())//
                .withKeyConvertorToBytes(
                        keyTypeDescriptor.getConvertorToBytes()) //
                .withValueReader(valueTypeDescriptor.getTypeReader())//
                .withValueWriter(valueTypeDescriptor.getTypeWriter())//
                .build();
    }

    SstFile<K, V> getCacheSstFile(final String fileName) {
        return SstFile.<K, V>builder() //
                .withDirectory(directory) //
                .withFileName(fileName)//
                .withKeyComparator(keyTypeDescriptor.getComparator()) //
                .withKeyConvertorFromBytes(
                        keyTypeDescriptor.getConvertorFromBytes())//
                .withKeyConvertorToBytes(
                        keyTypeDescriptor.getConvertorToBytes()) //
                .withValueReader(valueTypeDescriptor.getTypeReader())//
                .withValueWriter(valueTypeDescriptor.getTypeWriter())//
                .build();
    }

    SstFile<K, V> getIndexSstFile() {
        return SstFile.<K, V>builder() //
                .withDirectory(directory) //
                .withFileName(getIndexFileName())//
                .withKeyComparator(keyTypeDescriptor.getComparator()) //
                .withKeyConvertorFromBytes(
                        keyTypeDescriptor.getConvertorFromBytes())//
                .withKeyConvertorToBytes(
                        keyTypeDescriptor.getConvertorToBytes()) //
                .withValueReader(valueTypeDescriptor.getTypeReader())//
                .withValueWriter(valueTypeDescriptor.getTypeWriter())//
                .build();
    }

    SstFile<K, V> getIndexSstFileForIteration() {
        return SstFile.<K, V>builder() //
                .withDirectory(directory) //
                .withFileName(getIndexFileName())//
                .withKeyComparator(keyTypeDescriptor.getComparator()) //
                .withKeyConvertorFromBytes(
                        keyTypeDescriptor.getConvertorFromBytes())//
                .withKeyConvertorToBytes(
                        keyTypeDescriptor.getConvertorToBytes()) //
                .withValueReader(valueTypeDescriptor.getTypeReader())//
                .withValueWriter(valueTypeDescriptor.getTypeWriter())//
                .withFileReadingBufferSize(indexBufferSize)//
                .build();
    }

    SstFile<K, V> getTempIndexFile() {
        return SstFile.<K, V>builder() //
                .withDirectory(directory) //
                .withFileName(getTempIndexFileName())//
                .withKeyComparator(keyTypeDescriptor.getComparator()) //
                .withKeyConvertorFromBytes(
                        keyTypeDescriptor.getConvertorFromBytes())//
                .withKeyConvertorToBytes(
                        keyTypeDescriptor.getConvertorToBytes()) //
                .withValueReader(valueTypeDescriptor.getTypeReader())//
                .withValueWriter(valueTypeDescriptor.getTypeWriter())//
                .build();
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

    void deleteFile(final String fileName) {
        if (!directory.deleteFile(fileName)) {
            throw new IllegalStateException(String.format(
                    "Unable to delete file '%s' in directory '%s'", fileName,
                    directory));
        }
    }

    void optionallyDeleteFile(final String fileName) {
        directory.deleteFile(fileName);
    }

}
