package org.hestiastore.index.directory;

import java.io.File;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;

public final class FsNioDirectory extends AbstractDirectory {

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
        Vldtn.requireNonNull(fileName, "fileName");
        return new FsNioFileWriterStream(getFile(fileName),
                Vldtn.requireNonNull(access, "access"));
    }

    @Override
    public FileWriter getFileWriter(final String fileName, final Access access,
            final int bufferSize) {
        Vldtn.requireNonNull(fileName, "fileName");
        return new FsNioFileWriterStream(getFile(fileName),
                Vldtn.requireNonNull(access, "access"));
    }

    @Override
    public FileLock getLock(String fileName) {
        return new FsFileLock(this, fileName);
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
        return new FsNioDirectory(subdirectory);
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
        return "FsNioDirectory{directory=" + getDirectory().getPath() + "}";
    }

    @Override
    public FileReaderSeekable getFileReaderSeekable(final String fileName) {
        return new FsFileReaderSeekable(getFile(fileName));
    }

}
