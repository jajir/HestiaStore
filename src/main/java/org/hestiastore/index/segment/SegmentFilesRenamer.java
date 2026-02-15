package org.hestiastore.index.segment;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a utility method to rename all files associated with a segment from
 * one SegmentFiles instance to another.
 */
public class SegmentFilesRenamer {

    private static final Logger logger = LoggerFactory
            .getLogger(SegmentFilesRenamer.class);

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
        fromProperties.getCacheDeltaFileNames().forEach(fileName -> {
            final String targetFileName = renameSegmentFileName(
                    fromSegmentIdName, toSegmentIdName, fileName);
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
        if (logger.isDebugEnabled()) {
            logger.debug(
                    "Segment properties rename: from='{}' to='{}' thread='{}'",
                    from.getPropertiesFilename(), to.getPropertiesFilename(),
                    Thread.currentThread().getName());
        }
        dirFacade
                .renameFileAsync(from.getPropertiesFilename(),
                        to.getPropertiesFilename())
                .toCompletableFuture().join();
    }

    /**
     * Rewrites a delta file name from the source prefix to the target prefix
     * when the source prefix is present.
     *
     * @param fromSegmentIdName expected source prefix
     * @param toSegmentIdName target prefix to apply
     * @param fileName current delta file name
     * @return renamed delta file name
     */
    private String renameSegmentFileName(final String fromSegmentIdName,
            final String toSegmentIdName, final String fileName) {
        if (!fileName.startsWith(fromSegmentIdName)) {
            return fileName;
        }
        return toSegmentIdName + fileName.substring(fromSegmentIdName.length());
    }
}
