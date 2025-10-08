package org.hestiastore.index.segment;

import java.util.List;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkpairfile.ChunkPairFile;
import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.chunkstore.ChunkStoreFile;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.scarceindex.ScarceIndex;
import org.hestiastore.index.sorteddatafile.SortedDataFile;

/**
 * Accessor and factory for all files that belong to a single segment
 * (cache, index, scarce index, bloom filter, properties).
 *
 * <p>Provides file names, typed file handles and common configuration used
 * across these files.</p>
 *
 * @param <K> key type stored in the segment
 * @param <V> value type stored in the segment
 */
public final class SegmentFiles<K, V> {

    private static final String INDEX_FILE_NAME_EXTENSION = ".index";
    private static final String SCARCE_FILE_NAME_EXTENSION = ".scarce";
    static final String CACHE_FILE_NAME_EXTENSION = ".cache";
    private static final String BOOM_FILTER_FILE_NAME_EXTENSION = ".bloom-filter";
    private static final String PROPERTIES_FILENAME_EXTENSION = ".properties";

    private final Directory directory;
    private final SegmentId id;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final int diskIoBufferSize;
    private final List<ChunkFilter> encodingChunkFilters;
    private final List<ChunkFilter> decodingChunkFilters;

    /**
     * Create accessor for segment files.
     *
     * @param directory directory implementation used for I/O
     * @param id unique segment identifier
     * @param keyTypeDescriptor descriptor for key serialization and comparison
     * @param valueTypeDescriptor descriptor for value serialization
     * @param diskIoBufferSize buffer size in bytes for on-disk operations
     * @param encodingChunkFilters filters applied when writing chunks
     * @param decodingChunkFilters filters applied when reading chunks
     */
    public SegmentFiles(final Directory directory, final SegmentId id,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final int diskIoBufferSize,
            final List<ChunkFilter> encodingChunkFilters,
            final List<ChunkFilter> decodingChunkFilters) {
        this.directory = Vldtn.requireNonNull(directory, "directory");
        this.id = Vldtn.requireNonNull(id, "segmentId");
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        this.diskIoBufferSize = diskIoBufferSize;
        this.encodingChunkFilters = List.copyOf(Vldtn
                .requireNotEmpty(encodingChunkFilters, "encodingChunkFilters"));
        this.decodingChunkFilters = List.copyOf(Vldtn
                .requireNotEmpty(decodingChunkFilters, "decodingChunkFilters"));
    }

    /**
     * File name for the main cache data file of this segment.
     *
     * @return cache file name
     */
    String getCacheFileName() {
        return id.getName() + CACHE_FILE_NAME_EXTENSION;
    }

    /**
     * File name for the scarce index file.
     *
     * @return scarce index file name
     */
    String getScarceFileName() {
        return id.getName() + SCARCE_FILE_NAME_EXTENSION;
    }

    /**
     * File name for the Bloom filter file.
     *
     * @return bloom filter file name
     */
    String getBloomFilterFileName() {
        return id.getName() + BOOM_FILTER_FILE_NAME_EXTENSION;
    }

    /**
     * File name for the main index file.
     *
     * @return index file name
     */
    String getIndexFileName() {
        return id.getName() + INDEX_FILE_NAME_EXTENSION;
    }

    /**
     * File name for the segment properties file.
     *
     * @return properties file name
     */
    String getPropertiesFilename() {
        return id.getName() + PROPERTIES_FILENAME_EXTENSION;
    }

    /**
     * Open a typed handle for the main cache data file.
     *
     * @return sorted data file for this segment's cache
     */
    SortedDataFile<K, V> getCacheDataFile() {
        return SortedDataFile.<K, V>builder() //
                .withDirectory(directory) //
                .withFileName(getCacheFileName())//
                .withKeyTypeDescriptor(keyTypeDescriptor) //
                .withValueTypeDescriptor(valueTypeDescriptor) //
                .withDiskIoBufferSize(diskIoBufferSize)//
                .build();
    }

    /**
     * Open a typed handle for a delta cache data file by name.
     *
     * @param fileName target file name
     * @return sorted data file handle
     */
    SortedDataFile<K, V> getDeltaCacheSortedDataFile(final String fileName) {
        return SortedDataFile.<K, V>builder() //
                .withDirectory(directory) //
                .withFileName(fileName)//
                .withKeyTypeDescriptor(keyTypeDescriptor) //
                .withValueTypeDescriptor(valueTypeDescriptor) //
                .withDiskIoBufferSize(diskIoBufferSize)//
                .build();
    }

    /**
     * Open a handle for the scarce index of this segment.
     *
     * @return scarce index instance
     */
    ScarceIndex<K> getScarceIndex() {
        return ScarceIndex.<K>builder()//
                .withDirectory(getDirectory())//
                .withFileName(getScarceFileName())//
                .withKeyTypeDescriptor(getKeyTypeDescriptor())//
                .withDiskIoBufferSize(diskIoBufferSize) //
                .build();
    }

    /**
     * Open a handle for the main segment index file.
     *
     * @return chunk-pair file for the index
     */
    ChunkPairFile<K, V> getIndexFile() {
        final ChunkStoreFile chunkStoreFile = new ChunkStoreFile(getDirectory(),
                getIndexFileName(),
                DataBlockSize.ofDataBlockSize(diskIoBufferSize),
                encodingChunkFilters, decodingChunkFilters);
        return new ChunkPairFile<>(chunkStoreFile, keyTypeDescriptor,
                valueTypeDescriptor,
                DataBlockSize.ofDataBlockSize(diskIoBufferSize));
    }

    /**
     * Directory used by this segment.
     *
     * @return directory
     */
    Directory getDirectory() {
        return directory;
    }

    /**
     * Segment identifier.
     *
     * @return segment id
     */
    SegmentId getId() {
        return id;
    }

    /**
     * Segment identifier in string form.
     *
     * @return segment id name
     */
    public String getSegmentIdName() {
        return id.getName();
    }

    /**
     * Type descriptor for keys.
     *
     * @return key type descriptor
     */
    TypeDescriptor<K> getKeyTypeDescriptor() {
        return keyTypeDescriptor;
    }

    /**
     * Type descriptor for values.
     *
     * @return value type descriptor
     */
    TypeDescriptor<V> getValueTypeDescriptor() {
        return valueTypeDescriptor;
    }

    /**
     * Chunk filters used for encoding (write path).
     *
     * @return immutable list of encoding filters
     */
    List<ChunkFilter> getEncodingChunkFilters() {
        return encodingChunkFilters;
    }

    /**
     * Chunk filters used for decoding (read path).
     *
     * @return immutable list of decoding filters
     */
    List<ChunkFilter> getDecodingChunkFilters() {
        return decodingChunkFilters;
    }

    /**
     * Delete a file in the segment directory and fail if deletion was not
     * successful.
     *
     * @param fileName file to delete
     * @throws IllegalStateException if the file could not be deleted
     */
    void deleteFile(final String fileName) {
        if (!directory.deleteFile(fileName)) {
            throw new IllegalStateException(String.format(
                    "Unable to delete file '%s' in directory '%s'", fileName,
                    directory));
        }
    }

    /**
     * Attempt to delete a file in the segment directory, ignoring result.
     *
     * @param fileName file to delete if present
     */
    void optionallyDeleteFile(final String fileName) {
        directory.deleteFile(fileName);
    }

}
