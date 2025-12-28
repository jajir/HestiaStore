package org.hestiastore.index.segment;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.DirectoryFacade;

/**
 * Provides a utility method to rename all files associated with a segment from
 * one SegmentFiles instance to another.
 */
public class SegmentFilesRenamer {

    /**
     * Renames all files from the 'from' SegmentFiles to the 'to' SegmentFiles.
     * This includes index, scarce index, bloom filter, properties, and cache
     * files.
     *
     * @param from the source SegmentFiles
     * @param to   the target SegmentFiles
     * @param <K>  the key type
     * @param <V>  the value type
     */
    public <K, V> void renameFiles(final SegmentFiles<K, V> from,
            final SegmentFiles<K, V> to) {
        Vldtn.requireNonNull(from, "from");
        Vldtn.requireNonNull(to, "to");
        final DirectoryFacade dirFacade = from.getDirectoryFacade();
        dirFacade.renameFileAsync(from.getIndexFileName(), to.getIndexFileName())
                .toCompletableFuture().join();
        dirFacade.renameFileAsync(from.getScarceFileName(),
                to.getScarceFileName()).toCompletableFuture().join();
        dirFacade.renameFileAsync(from.getBloomFilterFileName(),
                to.getBloomFilterFileName()).toCompletableFuture().join();
        dirFacade.renameFileAsync(from.getPropertiesFilename(),
                to.getPropertiesFilename()).toCompletableFuture().join();
        dirFacade.renameFileAsync(from.getCacheFileName(),
                to.getCacheFileName()).toCompletableFuture().join();
    }
}
