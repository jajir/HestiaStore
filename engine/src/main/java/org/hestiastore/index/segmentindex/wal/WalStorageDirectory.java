package org.hestiastore.index.segmentindex.wal;

import java.io.ByteArrayOutputStream;
import java.util.stream.Stream;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileReader;
import org.hestiastore.index.directory.FileWriter;

/**
 * Generic {@link Directory}-backed WAL storage.
 *
 * <p>
 * This implementation is portable across directory adapters, but it cannot
 * provide fsync durability guarantees and therefore implements
 * {@link #sync(String)} as a no-op.
 * </p>
 */
final class WalStorageDirectory implements WalStorage {

    private static final int BUFFER_SIZE = 8 * 1024;
    private final Directory walDirectory;

    WalStorageDirectory(final Directory walDirectory) {
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
        return readAll(fileName).length;
    }

    @Override
    public void append(final String fileName, final byte[] bytes,
            final int offset, final int length) {
        try (FileWriter writer = walDirectory.getFileWriter(fileName,
                Directory.Access.APPEND)) {
            writer.write(bytes, offset, length);
        }
    }

    @Override
    public void overwrite(final String fileName, final byte[] bytes,
            final int offset, final int length) {
        try (FileWriter writer = walDirectory.getFileWriter(fileName,
                Directory.Access.OVERWRITE)) {
            writer.write(bytes, offset, length);
        }
    }

    @Override
    public byte[] readAll(final String fileName) {
        if (!exists(fileName)) {
            return new byte[0];
        }
        try (FileReader reader = walDirectory.getFileReader(fileName)) {
            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            final byte[] buffer = new byte[BUFFER_SIZE];
            int read = reader.read(buffer, 0, buffer.length);
            while (read >= 0) {
                if (read > 0) {
                    out.write(buffer, 0, read);
                }
                read = reader.read(buffer, 0, buffer.length);
            }
            return out.toByteArray();
        }
    }

    @Override
    public int read(final String fileName, final long position,
            final byte[] destination, final int offset, final int length) {
        if (!exists(fileName)) {
            return -1;
        }
        try (FileReader reader = walDirectory.getFileReader(fileName)) {
            if (position > 0L) {
                reader.skip(position);
            }
            return reader.read(destination, offset, length);
        }
    }

    @Override
    public void truncate(final String fileName, final long sizeBytes) {
        if (!exists(fileName)) {
            return;
        }
        final byte[] data = readAll(fileName);
        final int newLength = (int) Math.min(sizeBytes, data.length);
        if (newLength < 0) {
            throw new IndexException(String.format(
                    "Unable to truncate '%s' to negative size %s.", fileName,
                    sizeBytes));
        }
        overwrite(fileName, data, 0, newLength);
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
        // No-op: generic directory abstraction does not expose fsync.
    }

    @Override
    public void syncMetadata() {
        // No-op: generic directory abstraction does not expose fsync.
    }
}
