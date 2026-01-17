package org.hestiastore.index.segment;

import java.util.List;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkentryfile.ChunkEntryFile;
import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.chunkstore.ChunkStoreFile;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.scarceindex.ScarceSegmentIndex;
import org.hestiastore.index.sorteddatafile.SortedDataFile;

/**
 * Accessor and factory for all files that belong to a single segment (delta
 * cache files, index, scarce index, bloom filter, properties).
 *
 * <p>
 * Provides file names, typed file handles and common configuration used across
 * these files.
 * </p>
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

    private final AsyncDirectory directoryFacade;
    private final SegmentId id;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final int diskIoBufferSize;
    private final List<ChunkFilter> encodingChunkFilters;
    private final List<ChunkFilter> decodingChunkFilters;

    /**
     * Create accessor for segment files.
     *
     * @param directoryFacade      directory facade used for I/O
     * @param id                   unique segment identifier
     * @param keyTypeDescriptor    descriptor for key serialization and
     *                             comparison
     * @param valueTypeDescriptor  descriptor for value serialization
     * @param diskIoBufferSize     buffer size in bytes for on-disk operations
     * @param encodingChunkFilters filters applied when writing chunks
     * @param decodingChunkFilters filters applied when reading chunks
     */
    public SegmentFiles(final AsyncDirectory directoryFacade,
            final SegmentId id, final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final int diskIoBufferSize,
            final List<ChunkFilter> encodingChunkFilters,
            final List<ChunkFilter> decodingChunkFilters) {
        this.directoryFacade = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
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
     * Open a typed handle for a delta cache data file by name.
     *
     * @param fileName target file name
     * @return sorted data file handle
     */
    SortedDataFile<K, V> getDeltaCacheSortedDataFile(final String fileName) {
        return SortedDataFile.fromAsyncDirectory(directoryFacade, fileName,
                keyTypeDescriptor, valueTypeDescriptor, diskIoBufferSize);
    }

    /**
     * Open a chunk-entry handle for a delta cache data file by name.
     *
     * @param fileName target file name
     * @return chunk-entry file handle
     */
    ChunkEntryFile<K, V> getDeltaCacheChunkEntryFile(final String fileName) {
        final ChunkStoreFile chunkStoreFile = new ChunkStoreFile(
                directoryFacade, fileName,
                DataBlockSize.ofDataBlockSize(diskIoBufferSize),
                encodingChunkFilters, decodingChunkFilters);
        return new ChunkEntryFile<>(chunkStoreFile, keyTypeDescriptor,
                valueTypeDescriptor,
                DataBlockSize.ofDataBlockSize(diskIoBufferSize));
    }

    /**
     * Open a handle for the scarce index of this segment.
     *
     * @return scarce index instance
     */
    ScarceSegmentIndex<K> getScarceIndex() {
        return ScarceSegmentIndex.<K>builder()//
                .withAsyncDirectory(directoryFacade)//
                .withFileName(getScarceFileName())//
                .withKeyTypeDescriptor(getKeyTypeDescriptor())//
                .withDiskIoBufferSize(diskIoBufferSize) //
                .build();
    }

    /**
     * Open a handle for the main segment index file.
     *
     * @return chunk-entry file for the index
     */
    ChunkEntryFile<K, V> getIndexFile() {
        final ChunkStoreFile chunkStoreFile = new ChunkStoreFile(
                directoryFacade, getIndexFileName(),
                DataBlockSize.ofDataBlockSize(diskIoBufferSize),
                encodingChunkFilters, decodingChunkFilters);
        return new ChunkEntryFile<>(chunkStoreFile, keyTypeDescriptor,
                valueTypeDescriptor,
                DataBlockSize.ofDataBlockSize(diskIoBufferSize));
    }

    /**
     * Returns the async directory backing this segment.
     *
     * @return async directory facade
     */
    AsyncDirectory getAsyncDirectory() {
        return directoryFacade;
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
        if (!directoryFacade.deleteFileAsync(fileName).toCompletableFuture()
                .join()) {
            throw new IllegalStateException(String.format(
                    "Unable to delete file '%s' in directory '%s'", fileName,
                    directoryFacade));
        }
    }

    /**
     * Attempt to delete a file in the segment directory, ignoring result.
     *
     * @param fileName file to delete if present
     */
    void optionallyDeleteFile(final String fileName) {
        directoryFacade.deleteFileAsync(fileName).toCompletableFuture().join();
    }

    /**
     * Removes all files that belong to this segment, including delta cache
     * files listed in the provided properties manager.
     *
     * @param segmentPropertiesManager properties manager for this segment
     */
    public void deleteAllFiles(
            final SegmentPropertiesManager segmentPropertiesManager) {
        Vldtn.requireNonNull(segmentPropertiesManager,
                "segmentPropertiesManager");
        segmentPropertiesManager.getCacheDeltaFileNames()
                .forEach(this::optionallyDeleteFile);
        optionallyDeleteFile(getIndexFileName());
        optionallyDeleteFile(getScarceFileName());
        optionallyDeleteFile(getBloomFilterFileName());
        optionallyDeleteFile(getPropertiesFilename());
    }

}
