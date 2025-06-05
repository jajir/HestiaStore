package org.hestiastore.index.directory;

import java.io.File;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;

import org.hestiastore.index.IndexException;

public final class FsDirectory extends AbstractDirectory {

    private static final int DEFAULT_BUFFER_SIZE = 1024 * 1 * 4;

    public FsDirectory(final File directory) {
        super(directory);
    }

    @Override
    public boolean isFileExists(final String fileName) {
        final File file = getFile(fileName);
        return file.exists();
    }

    @Override
    public FileReader getFileReader(final String fileName) {
        return getFileReader(fileName, DEFAULT_BUFFER_SIZE);
    }

    @Override
    public FileReader getFileReader(final String fileName,
            final int bufferSize) {
        final File file = getFile(fileName);
        assureThatFileExists(file);
        return new FsFileReaderStream(file, bufferSize);
    }

    @Override
    public FileWriter getFileWriter(final String fileName,
            final Access access) {
        Objects.requireNonNull(fileName, "File name can't be null.");
        return new FsFileWriterStream(getFile(fileName),
                Objects.requireNonNull(access, "Access name can't be null."));
    }

    @Override
    public FileWriter getFileWriter(final String fileName, final Access access,
            final int bufferSize) {
        Objects.requireNonNull(fileName, "File name can't be null.");
        return new FsFileWriterStream(getFile(fileName),
                Objects.requireNonNull(access, "Access name can't be null."),
                bufferSize);
    }

    @Override
    public FileLock getLock(String fileName) {
        return new FsFileLock(this, fileName);
    }

    @Override
    public String toString() {
        return "FsDirectory{directory=" + getDirectory().getPath() + "}";
    }

    @Override
    public FileReaderSeekable getFileReaderSeekable(final String fileName) {
        return new FsFileReaderSeekable(getFile(fileName));
    }

}
