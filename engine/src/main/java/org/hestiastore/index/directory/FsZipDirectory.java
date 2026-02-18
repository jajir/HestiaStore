package org.hestiastore.index.directory;

import java.io.File;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;

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
        return new FsZipFileWriterStream(
                getFile(Vldtn.requireNonNull(fileName, "fileName")));
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
    public Directory openSubDirectory(final String directoryName) {
        Vldtn.requireNonNull(directoryName, "directoryName");
        final File subdirectory = getFile(directoryName);
        if (subdirectory.exists()) {
            if (subdirectory.isFile()) {
                throw new IndexException(String.format(
                        "There is required directory but '%s' is file.",
                        subdirectory.getAbsolutePath()));
            }
        } else if (!subdirectory.mkdirs()) {
            throw new IndexException(String.format(
                    "Unable to create directory '%s'.",
                    subdirectory.getAbsolutePath()));
        }
        return new FsZipDirectory(subdirectory);
    }

    @Override
    public boolean mkdir(final String directoryName) {
        Vldtn.requireNonNull(directoryName, "directoryName");
        final File subdirectory = getFile(directoryName);
        if (subdirectory.exists()) {
            if (subdirectory.isFile()) {
                throw new IndexException(String.format(
                        "There is required directory but '%s' is file.",
                        subdirectory.getAbsolutePath()));
            }
            return false;
        }
        if (!subdirectory.mkdirs()) {
            throw new IndexException(String.format(
                    "Unable to create directory '%s'.",
                    subdirectory.getAbsolutePath()));
        }
        return true;
    }

    @Override
    public boolean rmdir(final String directoryName) {
        Vldtn.requireNonNull(directoryName, "directoryName");
        final File subdirectory = getFile(directoryName);
        if (!subdirectory.exists()) {
            return false;
        }
        if (subdirectory.isFile()) {
            throw new IndexException(String.format(
                    "There is required directory but '%s' is file.",
                    subdirectory.getAbsolutePath()));
        }
        final String[] entries = subdirectory.list();
        if (entries == null) {
            throw new IndexException(String.format(
                    "Unable to list directory '%s'.",
                    subdirectory.getAbsolutePath()));
        }
        if (entries.length > 0) {
            throw new IndexException(String.format(
                    "Directory '%s' is not empty.",
                    subdirectory.getAbsolutePath()));
        }
        if (!subdirectory.delete()) {
            throw new IndexException(String.format(
                    "Unable to remove directory '%s'.",
                    subdirectory.getAbsolutePath()));
        }
        return true;
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
