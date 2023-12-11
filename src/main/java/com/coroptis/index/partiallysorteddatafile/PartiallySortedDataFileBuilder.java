package com.coroptis.index.partiallysorteddatafile;

import java.util.Comparator;
import java.util.Objects;

import com.coroptis.index.ValueMerger;
import com.coroptis.index.basic.BasicIndex;

public class PartiallySortedDataFileBuilder<K, V> {

    private String fileName;

    private Comparator<K> keyComparator;

    private BasicIndex<K, V> basicIndex;

    private ValueMerger<K, V> merger;

    public PartiallySortedDataFileBuilder<K, V> withFileName(
            final String file) {
        this.fileName = Objects.requireNonNull(file);
        return this;
    }

    public PartiallySortedDataFileBuilder<K, V> withKeyComparator(
            final Comparator<K> keyComparator) {
        this.keyComparator = Objects.requireNonNull(keyComparator);
        return this;
    }

    public PartiallySortedDataFileBuilder<K, V> withBasicIndex(
            final BasicIndex<K, V> basicIndex) {
        this.basicIndex = Objects.requireNonNull(basicIndex);
        return this;
    }

    public PartiallySortedDataFileBuilder<K, V> withValueMerger(
            final ValueMerger<K, V> merger) {
        this.merger = Objects.requireNonNull(merger);
        return this;
    }

    public PartiallySortedDataFile<K, V> build() {
        return new PartiallySortedDataFile<>(fileName, keyComparator,
                basicIndex, merger);
    }

}
