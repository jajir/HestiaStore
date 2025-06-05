package org.hestiastore.index.directory;

import java.io.File;
import java.util.Objects;

import org.hestiastore.index.IndexException;

public final class FsZipDirectory extends AbstractDirectory {

    public FsZipDirectory(final File directory) {
        super(directory);
    }

    @Override
    public FileReader getFileReader(final String fileName) {
        final File file = getFile(fileName);
        assureThatFileExists(file);
        return new FsZipFileReaderStream(file);
    }

    @Override
    public FileReader getFileReader(final String fileName,
            final int bufferSize) {
        return getFileReader(fileName);
    }

    @Override
    public FileWriter getFileWriter(final String fileName,
            final Access access) {
        if (Access.APPEND == access) {
            throw new IndexException(
                    "Append to ZIP file system is not supported");
        }
        return new FsZipFileWriterStream(getFile(Objects.requireNonNull(
                fileName, () -> String.format("File name is required."))));
    }

    @Override
    public FileWriter getFileWriter(final String fileName, final Access access,
            final int bufferSize) {
        return getFileWriter(fileName, access);
    }

    @Override
    public boolean isFileExists(final String fileName) {
        final File file = getFile(fileName);
        return file.exists();
    }

    @Override
    public FileLock getLock(final String fileName) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public String toString() {
        return "FsZipDirectory{directory=" + getDirectory().getPath() + "}";
    }

    @Override
    public FileReaderSeekable getFileReaderSeekable(final String fileName) {
        throw new UnsupportedOperationException();
    }

}
