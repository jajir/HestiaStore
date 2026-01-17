package org.hestiastore.index.segment;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.IndexException;

/**
 * Provides a utility method to rename all files associated with a segment from
 * one SegmentFiles instance to another.
 */
public class SegmentFilesRenamer {

    /**
     * Renames all files from the 'from' SegmentFiles to the 'to' SegmentFiles.
     * This includes index, scarce index, bloom filter, properties, and delta
     * cache files.
     *
     * @param from           the source SegmentFiles
     * @param to             the target SegmentFiles
     * @param fromProperties properties manager for the source segment
     * @param <K>            the key type
     * @param <V>            the value type
     */
    public <K, V> void renameFiles(final SegmentFiles<K, V> from,
            final SegmentFiles<K, V> to,
            final SegmentPropertiesManager fromProperties) {
        Vldtn.requireNonNull(from, "from");
        Vldtn.requireNonNull(to, "to");
        Vldtn.requireNonNull(fromProperties, "fromProperties");
        final AsyncDirectory dirFacade = from.getAsyncDirectory();
        final String fromSegmentIdName = from.getSegmentIdName();
        final String toSegmentIdName = to.getSegmentIdName();
        final String fromPrefix = fromSegmentIdName + "-delta-";
        final String toPrefix = toSegmentIdName + "-delta-";
        fromProperties.getCacheDeltaFileNames().forEach(fileName -> {
            final String targetFileName = renameDeltaFileName(fromPrefix,
                    toPrefix, fileName, fromSegmentIdName);
            dirFacade.renameFileAsync(fileName, targetFileName)
                    .toCompletableFuture().join();
        });
        dirFacade
                .renameFileAsync(from.getIndexFileName(), to.getIndexFileName())
                .toCompletableFuture().join();
        dirFacade.renameFileAsync(from.getScarceFileName(),
                to.getScarceFileName()).toCompletableFuture().join();
        dirFacade
                .renameFileAsync(from.getBloomFilterFileName(),
                        to.getBloomFilterFileName())
                .toCompletableFuture().join();
        dirFacade
                .renameFileAsync(from.getPropertiesFilename(),
                        to.getPropertiesFilename())
                .toCompletableFuture().join();
    }

    /**
     * Rewrites a delta file name from the source prefix to the target prefix.
     *
     * @param fromPrefix expected source prefix
     * @param toPrefix target prefix to apply
     * @param fileName current delta file name
     * @param fromSegmentIdName source segment identifier for validation
     * @return renamed delta file name
     */
    private String renameDeltaFileName(final String fromPrefix,
            final String toPrefix, final String fileName,
            final String fromSegmentIdName) {
        if (!fileName.startsWith(fromPrefix)) {
            throw new IndexException("Delta cache file '" + fileName
                    + "' does not belong to segment '" + fromSegmentIdName
                    + "'. Expected prefix '" + fromPrefix + "'.");
        }
        return toPrefix + fileName.substring(fromPrefix.length());
    }
}
