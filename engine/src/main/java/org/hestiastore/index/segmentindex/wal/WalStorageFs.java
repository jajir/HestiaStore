package org.hestiastore.index.segmentindex.wal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.stream.Stream;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;

/**
 * WAL storage backed by {@link java.nio.file.Path} and file channels.
 */
final class WalStorageFs implements WalStorage {

    private final Path walDirectory;

    WalStorageFs(final Path walDirectory) {
        this.walDirectory = Vldtn.requireNonNull(walDirectory, "walDirectory");
        ensureDirectoryExists(walDirectory);
    }

    @Override
    public boolean exists(final String fileName) {
        return Files.isRegularFile(resolve(fileName));
    }

    @Override
    public void touch(final String fileName) {
        final Path path = resolve(fileName);
        try {
            if (!Files.exists(path)) {
                Files.createFile(path);
            } else {
                try (FileChannel ignored = FileChannel.open(path,
                        StandardOpenOption.WRITE)) {
                    // Keep existing file.
                }
            }
        } catch (IOException e) {
            throw new IndexException(
                    String.format("Unable to touch WAL file '%s'.", fileName),
                    e);
        }
    }

    @Override
    public long size(final String fileName) {
        final Path path = resolve(fileName);
        if (!Files.exists(path)) {
            return 0L;
        }
        try {
            return Files.size(path);
        } catch (IOException e) {
            throw new IndexException(String.format(
                    "Unable to read size for WAL file '%s'.", fileName), e);
        }
    }

    @Override
    public void append(final String fileName, final byte[] bytes,
            final int offset, final int length) {
        write(fileName, bytes, offset, length, true);
    }

    @Override
    public void overwrite(final String fileName, final byte[] bytes,
            final int offset, final int length) {
        write(fileName, bytes, offset, length, false);
    }

    @Override
    public byte[] readAll(final String fileName) {
        final Path path = resolve(fileName);
        if (!Files.exists(path)) {
            return new byte[0];
        }
        try {
            return Files.readAllBytes(path);
        } catch (IOException e) {
            throw new IndexException(
                    String.format("Unable to read WAL file '%s'.", fileName),
                    e);
        }
    }

    @Override
    public int read(final String fileName, final long position,
            final byte[] destination, final int offset, final int length) {
        final Path path = resolve(fileName);
        if (!Files.exists(path)) {
            return -1;
        }
        try (FileChannel channel = FileChannel.open(path,
                StandardOpenOption.READ)) {
            if (position >= channel.size()) {
                return -1;
            }
            channel.position(position);
            final ByteBuffer buffer = ByteBuffer.wrap(destination, offset,
                    length);
            return channel.read(buffer);
        } catch (IOException e) {
            throw new IndexException(String.format(
                    "Unable to read WAL file '%s' at position '%s'.", fileName,
                    position), e);
        }
    }

    @Override
    public void truncate(final String fileName, final long sizeBytes) {
        final Path path = resolve(fileName);
        if (!Files.exists(path)) {
            return;
        }
        try (FileChannel channel = FileChannel.open(path,
                StandardOpenOption.WRITE)) {
            channel.truncate(sizeBytes);
        } catch (IOException e) {
            throw new IndexException(String.format(
                    "Unable to truncate WAL file '%s' to %s bytes.", fileName,
                    sizeBytes), e);
        }
    }

    @Override
    public boolean delete(final String fileName) {
        final Path path = resolve(fileName);
        try {
            return Files.deleteIfExists(path);
        } catch (IOException e) {
            throw new IndexException(
                    String.format("Unable to delete WAL file '%s'.", fileName),
                    e);
        }
    }

    @Override
    public void rename(final String currentFileName, final String newFileName) {
        final Path source = resolve(currentFileName);
        final Path target = resolve(newFileName);
        try {
            if (!Files.exists(source)) {
                throw new IndexException(
                        String.format("Unable to rename missing WAL file '%s'.",
                                currentFileName));
            }
            Files.move(source, target,
                    java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                    java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new IndexException(
                    String.format("Unable to rename WAL file '%s' to '%s'.",
                            currentFileName, newFileName),
                    e);
        }
    }

    @Override
    public Stream<String> listFileNames() {
        try (final Stream<Path> listing = Files.list(walDirectory)) {
            return listing.sorted(Comparator.comparing(Path::getFileName))
                    .map(path -> path.getFileName().toString());
        } catch (IOException e) {
            throw new IndexException("Unable to list WAL files.", e);
        }
    }

    @Override
    public void sync(final String fileName) {
        final Path path = resolve(fileName);
        if (!Files.exists(path)) {
            return;
        }
        try (FileChannel channel = FileChannel.open(path,
                StandardOpenOption.WRITE)) {
            channel.force(true);
        } catch (IOException e) {
            throw new IndexException(
                    String.format("Unable to sync WAL file '%s'.", fileName),
                    e);
        }
    }

    @Override
    public void syncMetadata() {
        try (FileChannel channel = FileChannel.open(walDirectory,
                StandardOpenOption.READ)) {
            channel.force(true);
        } catch (IOException e) {
            throw new IndexException(
                    String.format("Unable to sync WAL directory metadata '%s'.",
                            walDirectory),
                    e);
        }
    }

    private void write(final String fileName, final byte[] bytes,
            final int offset, final int length, final boolean append) {
        Vldtn.requireNonNull(bytes, "bytes");
        final Path path = resolve(fileName);
        try {
            final StandardOpenOption[] options = append
                    ? new StandardOpenOption[] { StandardOpenOption.CREATE,
                            StandardOpenOption.WRITE,
                            StandardOpenOption.APPEND }
                    : new StandardOpenOption[] { StandardOpenOption.CREATE,
                            StandardOpenOption.WRITE,
                            StandardOpenOption.TRUNCATE_EXISTING };
            try (FileChannel channel = FileChannel.open(path, options)) {
                final ByteBuffer buffer = ByteBuffer.wrap(bytes, offset,
                        length);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
            }
        } catch (IOException e) {
            throw new IndexException(
                    String.format("Unable to write WAL file '%s'.", fileName),
                    e);
        }
    }

    private Path resolve(final String fileName) {
        return walDirectory.resolve(Vldtn.requireNonNull(fileName, "fileName"));
    }

    private static void ensureDirectoryExists(final Path walDirectory) {
        try {
            Files.createDirectories(walDirectory);
        } catch (IOException e) {
            throw new IndexException(String.format(
                    "Unable to create WAL directory '%s'.", walDirectory), e);
        }
    }
}
