package org.hestiastore.index.segment;

import java.util.List;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkentryfile.ChunkEntryFile;
import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.chunkstore.ChunkStoreFile;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.directory.FileLock;
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

    static final String CACHE_FILE_NAME_EXTENSION = ".cache";

    private final AsyncDirectory rootDirectory;
    private final boolean segmentRootDirectoryEnabled;
    private volatile AsyncDirectory directoryFacade;
    private volatile String activeDirectoryName;
    private final SegmentId id;
    private final SegmentDirectoryLayout layout;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final int diskIoBufferSize;
    private final List<ChunkFilter> encodingChunkFilters;
    private final List<ChunkFilter> decodingChunkFilters;

    /**
     * Create accessor for segment files stored in a flat directory layout.
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
        this(directoryFacade, directoryFacade, new SegmentDirectoryLayout(id),
                SegmentDirectoryLayout.ROOT_DIRECTORY_NAME, false,
                keyTypeDescriptor, valueTypeDescriptor, diskIoBufferSize,
                encodingChunkFilters, decodingChunkFilters);
    }

    /**
     * Create accessor for segment files rooted at a segment directory.
     *
     * @param segmentRootDirectory directory for the segment files
     * @param layout               segment file naming layout
     * @param keyTypeDescriptor    descriptor for key serialization and
     *                             comparison
     * @param valueTypeDescriptor  descriptor for value serialization
     * @param diskIoBufferSize     buffer size in bytes for on-disk operations
     * @param encodingChunkFilters filters applied when writing chunks
     * @param decodingChunkFilters filters applied when reading chunks
     */
    public SegmentFiles(final AsyncDirectory segmentRootDirectory,
            final SegmentDirectoryLayout layout,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final int diskIoBufferSize,
            final List<ChunkFilter> encodingChunkFilters,
            final List<ChunkFilter> decodingChunkFilters) {
        this(segmentRootDirectory, segmentRootDirectory, layout,
                SegmentDirectoryLayout.ROOT_DIRECTORY_NAME, true,
                keyTypeDescriptor, valueTypeDescriptor, diskIoBufferSize,
                encodingChunkFilters, decodingChunkFilters);
    }

    /**
     * Create accessor for segment files with explicit root and active
     * directories.
     *
     * @param rootDirectory          segment root directory
     * @param activeDirectory        active directory holding files
     * @param layout                 segment file naming layout
     * @param activeDirectoryName    active directory name in the root
     * @param keyTypeDescriptor      descriptor for key serialization and
     *                               comparison
     * @param valueTypeDescriptor    descriptor for value serialization
     * @param diskIoBufferSize       buffer size in bytes for on-disk operations
     * @param encodingChunkFilters   filters applied when writing chunks
     * @param decodingChunkFilters   filters applied when reading chunks
     */
    public SegmentFiles(final AsyncDirectory rootDirectory,
            final AsyncDirectory activeDirectory,
            final SegmentDirectoryLayout layout,
            final String activeDirectoryName,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final int diskIoBufferSize,
            final List<ChunkFilter> encodingChunkFilters,
            final List<ChunkFilter> decodingChunkFilters) {
        this(rootDirectory, activeDirectory, layout,
                Vldtn.requireNonNull(activeDirectoryName,
                        "activeDirectoryName"),
                true, keyTypeDescriptor, valueTypeDescriptor, diskIoBufferSize,
                encodingChunkFilters, decodingChunkFilters);
    }

    private SegmentFiles(final AsyncDirectory rootDirectory,
            final AsyncDirectory activeDirectory,
            final SegmentDirectoryLayout layout,
            final String activeDirectoryName,
            final boolean segmentRootDirectoryEnabled,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final int diskIoBufferSize,
            final List<ChunkFilter> encodingChunkFilters,
            final List<ChunkFilter> decodingChunkFilters) {
        this.rootDirectory = Vldtn.requireNonNull(rootDirectory,
                "rootDirectory");
        this.directoryFacade = Vldtn.requireNonNull(activeDirectory,
                "activeDirectory");
        this.activeDirectoryName = Vldtn.requireNonNull(activeDirectoryName,
                "activeDirectoryName");
        this.segmentRootDirectoryEnabled = segmentRootDirectoryEnabled;
        this.layout = Vldtn.requireNonNull(layout, "segmentLayout");
        this.id = Vldtn.requireNonNull(layout.getSegmentId(), "segmentId");
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
        return layout.getScarceFileName();
    }

    /**
     * File name for the Bloom filter file.
     *
     * @return bloom filter file name
     */
    String getBloomFilterFileName() {
        return layout.getBloomFilterFileName();
    }

    /**
     * File name for the main index file.
     *
     * @return index file name
     */
    String getIndexFileName() {
        return layout.getIndexFileName();
    }

    /**
     * File name for the segment properties file.
     *
     * @return properties file name
     */
    String getPropertiesFilename() {
        return layout.getPropertiesFileName();
    }

    /**
     * Acquire the per-segment lock file, failing fast when already locked.
     *
     * Stale locks must be removed manually.
     *
     * @return locked file lock handle
     */
    FileLock acquireLock() {
        final String lockFileName = layout.getLockFileName();
        final FileLock fileLock = directoryFacade.getLockAsync(lockFileName)
                .toCompletableFuture().join();
        if (fileLock.isLocked()) {
            throw new IllegalStateException(
                    lockHeldMessage(lockFileName));
        }
        try {
            fileLock.lock();
        } catch (final IllegalStateException e) {
            throw new IllegalStateException(lockHeldMessage(lockFileName), e);
        }
        return fileLock;
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

    AsyncDirectory getRootDirectory() {
        return rootDirectory;
    }

    boolean isSegmentRootDirectoryEnabled() {
        return segmentRootDirectoryEnabled;
    }

    String getActiveDirectoryName() {
        return activeDirectoryName;
    }

    void switchActiveDirectory(final String directoryName,
            final AsyncDirectory directoryFacade) {
        Vldtn.requireNonNull(directoryName, "directoryName");
        Vldtn.requireNonNull(directoryFacade, "directoryFacade");
        if (!segmentRootDirectoryEnabled) {
            throw new IllegalStateException(
                    "Segment root directory switching is disabled.");
        }
        this.directoryFacade = directoryFacade;
        this.activeDirectoryName = directoryName;
    }

    SegmentFiles<K, V> copyWithDirectory(final String directoryName,
            final AsyncDirectory directoryFacade) {
        return new SegmentFiles<>(rootDirectory, directoryFacade, layout,
                directoryName, segmentRootDirectoryEnabled, keyTypeDescriptor,
                valueTypeDescriptor, diskIoBufferSize, encodingChunkFilters,
                decodingChunkFilters);
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
        optionallyDeleteFile(layout.getLockFileName());
    }

    private String lockHeldMessage(final String lockFileName) {
        return String.format(
                "Segment '%s' is already locked. Delete '%s' to recover.",
                getSegmentIdName(), lockFileName);
    }

}
