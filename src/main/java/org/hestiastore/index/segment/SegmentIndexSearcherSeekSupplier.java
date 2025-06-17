package org.hestiastore.index.segment;

import org.hestiastore.index.Vldtn;

public class SegmentIndexSearcherSeekSupplier<K, V>
        implements SegmentIndexSearcherSupplier<K, V> {

    private final SegmentFiles<K, V> segmentFiles;
    private final SegmentConf segmentConf;

    SegmentIndexSearcherSeekSupplier(final SegmentFiles<K, V> segmentFiles,
            final SegmentConf segmentConf) {
        this.segmentFiles = Vldtn.requireNonNull(segmentFiles, "segmentFiles");
        this.segmentConf = Vldtn.requireNonNull(segmentConf, "segmentConf");
    }

    @Override
    public SegmentIndexSearcher<K, V> get() {
        return new SegmentIndexSearcherSeek<>(segmentFiles.getIndexSstFile(),
                segmentConf.getMaxNumberOfKeysInIndexPage(),
                segmentFiles.getKeyTypeDescriptor().getComparator());
    }

}
