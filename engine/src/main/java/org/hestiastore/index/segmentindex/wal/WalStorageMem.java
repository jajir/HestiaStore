package org.hestiastore.index.segmentindex.wal;

import java.util.stream.Stream;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;

/**
 * WAL storage implementation backed by {@link MemDirectory}.
 */
final class WalStorageMem implements WalStorage {

    private final MemDirectory walDirectory;

    WalStorageMem(final MemDirectory walDirectory) {
        this.walDirectory = Vldtn.requireNonNull(walDirectory, "walDirectory");
    }

    @Override
    public boolean exists(final String fileName) {
        return walDirectory.isFileExists(fileName);
    }

    @Override
    public void touch(final String fileName) {
        walDirectory.touch(fileName);
    }

    @Override
    public long size(final String fileName) {
        if (!exists(fileName)) {
            return 0L;
        }
        return walDirectory.getFileSequence(fileName).length();
    }

    @Override
    public void append(final String fileName, final byte[] bytes,
            final int offset, final int length) {
        try (org.hestiastore.index.directory.FileWriter writer = walDirectory
                .getFileWriter(fileName, Directory.Access.APPEND)) {
            writer.write(bytes, offset, length);
        }
    }

    @Override
    public void overwrite(final String fileName, final byte[] bytes,
            final int offset, final int length) {
        try (org.hestiastore.index.directory.FileWriter writer = walDirectory
                .getFileWriter(fileName, Directory.Access.OVERWRITE)) {
            writer.write(bytes, offset, length);
        }
    }

    @Override
    public byte[] readAll(final String fileName) {
        if (!exists(fileName)) {
            return new byte[0];
        }
        return walDirectory.getFileSequence(fileName).toByteArrayCopy();
    }

    @Override
    public int read(final String fileName, final long position,
            final byte[] destination, final int offset, final int length) {
        if (!exists(fileName)) {
            return -1;
        }
        final byte[] data = walDirectory.getFileSequence(fileName)
                .toByteArrayCopy();
        if (position >= data.length) {
            return -1;
        }
        final int available = (int) Math.min(length, data.length - position);
        if (available <= 0) {
            return -1;
        }
        System.arraycopy(data, (int) position, destination, offset, available);
        return available;
    }

    @Override
    public void truncate(final String fileName, final long sizeBytes) {
        if (!exists(fileName)) {
            return;
        }
        final ByteSequence data = walDirectory.getFileSequence(fileName);
        final int target = (int) Math.min(sizeBytes, data.length());
        walDirectory.setFileSequence(fileName,
                target == data.length() ? data : data.slice(0, target));
    }

    @Override
    public boolean delete(final String fileName) {
        return walDirectory.deleteFile(fileName);
    }

    @Override
    public void rename(final String currentFileName, final String newFileName) {
        walDirectory.renameFile(currentFileName, newFileName);
    }

    @Override
    public Stream<String> listFileNames() {
        return walDirectory.getFileNames();
    }

    @Override
    public void sync(final String fileName) {
        // No-op for in-memory storage.
    }
}
