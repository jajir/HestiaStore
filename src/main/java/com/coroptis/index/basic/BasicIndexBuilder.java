package com.coroptis.index.basic;

import com.coroptis.index.ValueMerger;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.directory.Directory;

public class BasicIndexBuilder<K, V> {

    private Directory directory;
    private TypeDescriptor<K> keyTypeDescriptor;
    private TypeDescriptor<V> valueTypeDescriptor;
    private ValueMerger<K, V> valueMerger;

    public BasicIndex<K, V> buid() {
        return new BasicIndex<>(directory, valueMerger, keyTypeDescriptor,
                valueTypeDescriptor);
    }

    public BasicIndexBuilder<K, V> directory(final Directory directory) {
        this.directory = directory;
        return this;
    }

    public BasicIndexBuilder<K, V> keyTypeDescriptor(
            final TypeDescriptor<K> keyTypeDescriptor) {
        this.keyTypeDescriptor = keyTypeDescriptor;
        return this;
    }

    public BasicIndexBuilder<K, V> valueTypeDescriptor(
            final TypeDescriptor<V> valueTypeDescriptor) {
        this.valueTypeDescriptor = valueTypeDescriptor;
        return this;
    }

    public BasicIndexBuilder<K, V> valueMerger(
            final ValueMerger<K, V> valueMerger) {
        this.valueMerger = valueMerger;
        return this;
    }

}
