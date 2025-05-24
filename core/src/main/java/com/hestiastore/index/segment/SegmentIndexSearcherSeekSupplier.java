package com.hestiastore.index.segment;

import java.util.Objects;

public class SegmentIndexSearcherSeekSupplier<K, V>
        implements SegmentIndexSearcherSupplier<K, V> {

    private final SegmentFiles<K, V> segmentFiles;
    private final SegmentConf segmentConf;

    SegmentIndexSearcherSeekSupplier(final SegmentFiles<K, V> segmentFiles,
            final SegmentConf segmentConf) {
        this.segmentFiles = Objects.requireNonNull(segmentFiles);
        this.segmentConf = Objects.requireNonNull(segmentConf);
    }

    @Override
    public SegmentIndexSearcher<K, V> get() {
        return new SegmentIndexSearcherSeek<>(segmentFiles.getIndexSstFile(),
                segmentConf.getMaxNumberOfKeysInIndexPage(),
                segmentFiles.getKeyTypeDescriptor().getComparator());
    }

}
