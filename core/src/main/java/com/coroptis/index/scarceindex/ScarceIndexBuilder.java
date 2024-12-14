package com.coroptis.index.scarceindex;

import java.util.Objects;

import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.directory.Directory;

public class ScarceIndexBuilder<K> {

    private TypeDescriptor<K> keyTypeDescriptor;

    private Directory directory;

    private String fileName;

    ScarceIndexBuilder() {
        // just keep constructor with limited visibility
    }

    public ScarceIndexBuilder<K> withKeyTypeDescriptor(
            final TypeDescriptor<K> typeDescriptor) {
        this.keyTypeDescriptor = Objects.requireNonNull(typeDescriptor);
        return this;
    }

    public ScarceIndexBuilder<K> withDirectory(final Directory directory) {
        this.directory = Objects.requireNonNull(directory);
        return this;
    }

    public ScarceIndexBuilder<K> withFileName(final String fileName) {
        this.fileName = Objects.requireNonNull(fileName);
        return this;
    }

    public ScarceIndex<K> build() {
        return new ScarceIndex<K>(directory, fileName, keyTypeDescriptor);
    }

}
