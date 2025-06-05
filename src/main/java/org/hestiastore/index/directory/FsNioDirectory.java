package org.hestiastore.index.directory;

import java.io.File;
import java.util.Objects;

public final class FsNioDirectory extends AbstractDirectory {

    private static final int DEFAULT_BUFFER_SIZE = 1024 * 1 * 4;

    public FsNioDirectory(final File directory) {
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
        return new FsNioFileReaderStream(file);
    }

    @Override
    public FileWriter getFileWriter(final String fileName,
            final Access access) {
        Objects.requireNonNull(fileName, "File name can't be null.");
        return new FsNioFileWriterStream(getFile(fileName),
                Objects.requireNonNull(access, "Access name can't be null."));
    }

    @Override
    public FileWriter getFileWriter(final String fileName, final Access access,
            final int bufferSize) {
        Objects.requireNonNull(fileName, "File name can't be null.");
        return new FsNioFileWriterStream(getFile(fileName),
                Objects.requireNonNull(access, "Access name can't be null."));
    }

    @Override
    public FileLock getLock(String fileName) {
        return new FsFileLock(this, fileName);
    }

    @Override
    public String toString() {
        return "FsNioDirectory{directory=" + getDirectory().getPath() + "}";
    }

    @Override
    public FileReaderSeekable getFileReaderSeekable(final String fileName) {
        return new FsFileReaderSeekable(getFile(fileName));
    }

}
