package org.hestiastore.index.segment;

import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.Vldtn;

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
        Directory dir = from.getDirectory();
        dir.renameFile(from.getIndexFileName(), to.getIndexFileName());
        dir.renameFile(from.getScarceFileName(), to.getScarceFileName());
        dir.renameFile(from.getBloomFilterFileName(),
                to.getBloomFilterFileName());
        dir.renameFile(from.getPropertiesFilename(),
                to.getPropertiesFilename());
        dir.renameFile(from.getCacheFileName(), to.getCacheFileName());
    }
}
