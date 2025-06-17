package org.hestiastore.index.scarceindex;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;

public final class ScarceIndexBuilder<K> {

    private static final int DEFAULT_DISK_IO_BUFFER_SIZE = 4 * 1024;

    private TypeDescriptor<K> keyTypeDescriptor;
    private Directory directory;
    private String fileName;
    private int diskIoBufferSize = DEFAULT_DISK_IO_BUFFER_SIZE;

    ScarceIndexBuilder() {
        // just keep constructor with limited visibility
    }

    public ScarceIndexBuilder<K> withKeyTypeDescriptor(
            final TypeDescriptor<K> typeDescriptor) {
        this.keyTypeDescriptor = Vldtn.requireNonNull(typeDescriptor,
                "keyTypeDescriptor");
        return this;
    }

    public ScarceIndexBuilder<K> withDirectory(final Directory directory) {
        this.directory = Vldtn.requireNonNull(directory, "directory");
        return this;
    }

    public ScarceIndexBuilder<K> withFileName(final String fileName) {
        this.fileName = Vldtn.requireNonNull(fileName, "fileName");
        return this;
    }

    public ScarceIndexBuilder<K> withDiskIoBufferSize(
            final int diskIoBufferSize) {
        this.diskIoBufferSize = diskIoBufferSize;
        return this;
    }

    public ScarceIndex<K> build() {
        return new ScarceIndex<K>(directory, fileName, keyTypeDescriptor,
                diskIoBufferSize);
    }

}
