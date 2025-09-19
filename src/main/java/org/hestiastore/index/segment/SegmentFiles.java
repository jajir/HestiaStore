package org.hestiastore.index.segment;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkpairfile.ChunkPairFile;
import org.hestiastore.index.chunkstore.ChunkStoreFile;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.scarceindex.ScarceIndex;
import org.hestiastore.index.sorteddatafile.SortedDataFile;

/**
 * Allows to easily access segment files.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public final class SegmentFiles<K, V> {

    private static final String INDEX_FILE_NAME_EXTENSION = ".index";
    private static final String SCARCE_FILE_NAME_EXTENSION = ".scarce";
    static final String CACHE_FILE_NAME_EXTENSION = ".cache";
    private static final String TEMP_FILE_NAME_EXTENSION = ".tmp";
    private static final String BOOM_FILTER_FILE_NAME_EXTENSION = ".bloom-filter";
    private static final String PROPERTIES_FILENAME_EXTENSION = ".properties";

    private final Directory directory;
    private final SegmentId id;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final int diskIoBufferSize;

    public SegmentFiles(final Directory directory, final SegmentId id,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final int diskIoBufferSize) {
        this.directory = Vldtn.requireNonNull(directory, "directory");
        this.id = Vldtn.requireNonNull(id, "segmentId");
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        this.diskIoBufferSize = diskIoBufferSize;
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

    String getPropertiesFilename() {
        return id.getName() + PROPERTIES_FILENAME_EXTENSION;
    }

    SortedDataFile<K, V> getCacheSstFile() {
        return SortedDataFile.<K, V>builder() //
                .withDirectory(directory) //
                .withFileName(getCacheFileName())//
                .withKeyTypeDescriptor(keyTypeDescriptor) //
                .withValueTypeDescriptor(valueTypeDescriptor) //
                .withDiskIoBufferSize(diskIoBufferSize)//
                .build();
    }

    SortedDataFile<K, V> getCacheSstFile(final String fileName) {
        return SortedDataFile.<K, V>builder() //
                .withDirectory(directory) //
                .withFileName(fileName)//
                .withKeyTypeDescriptor(keyTypeDescriptor) //
                .withValueTypeDescriptor(valueTypeDescriptor) //
                .withDiskIoBufferSize(diskIoBufferSize)//
                .build();
    }

    SortedDataFile<K, V> getIndexSstFileForIteration() {
        return SortedDataFile.<K, V>builder() //
                .withDirectory(directory) //
                .withFileName(getIndexFileName())//
                .withKeyTypeDescriptor(keyTypeDescriptor) //
                .withValueTypeDescriptor(valueTypeDescriptor) //
                .withDiskIoBufferSize(diskIoBufferSize)//
                .build();
    }

    @Deprecated
    SortedDataFile<K, V> getTempIndexFile() {
        return SortedDataFile.<K, V>builder() //
                .withDirectory(directory) //
                .withFileName(getTempIndexFileName())//
                .withKeyTypeDescriptor(keyTypeDescriptor) //
                .withValueTypeDescriptor(valueTypeDescriptor) //
                .withDiskIoBufferSize(diskIoBufferSize)//
                .build();
    }

    ScarceIndex<K> getTempScarceIndex() {
        return ScarceIndex.<K>builder()//
                .withDirectory(getDirectory())//
                .withFileName(getTempScarceFileName())//
                .withKeyTypeDescriptor(getKeyTypeDescriptor())//
                .withDiskIoBufferSize(diskIoBufferSize) //
                .build();
    }

    ChunkPairFile<K, V> getIndexFile() {
        final ChunkStoreFile chunkStoreFile = new ChunkStoreFile(getDirectory(),
                getIndexFileName(),
                DataBlockSize.ofDataBlockSize(diskIoBufferSize));
        return new ChunkPairFile<>(chunkStoreFile, keyTypeDescriptor,
                valueTypeDescriptor,
                DataBlockSize.ofDataBlockSize(diskIoBufferSize));
    }

    Directory getDirectory() {
        return directory;
    }

    SegmentId getId() {
        return id;
    }

    public String getSegmentIdName() {
        return id.getName();
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
