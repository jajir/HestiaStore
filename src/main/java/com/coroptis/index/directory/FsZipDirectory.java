package com.coroptis.index.directory;

import java.io.File;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;

import com.coroptis.index.IndexException;

public class FsZipDirectory implements Directory {

    private final File directory;

    public FsZipDirectory(final File directory) {
        this.directory = Objects.requireNonNull(directory);
        if (!directory.exists() && !directory.mkdirs()) {
            throw new IndexException(
                    String.format("Unable to create directory '%s.",
                            directory.getAbsoluteFile()));
        }
        if (directory.isFile()) {
            throw new IndexException(String
                    .format("There is required directory but '%s' is file."));
        }
    }

    @Override
    public FileReader getFileReader(final String fileName) {
        final File file = getFile(fileName);
        if (!directory.exists()) {
            throw new IndexException(
                    String.format("File '%s' doesn't exists."));
        }
        return new FsZipFileReaderStream(file);
    }

    @Override
    public FileReader getFileReader(final String fileName,
            final int bufferSize) {
        return getFileReader(fileName);
    }

    @Override
    public void renameFile(final String currentFileName,
            final String newFileName) {
        final File file = getFile(currentFileName);
        if (!directory.exists()) {
            throw new IndexException(
                    String.format("File '%s' doesn't exists."));
        }
        if (!file.renameTo(getFile(newFileName))) {
            throw new IndexException(
                    String.format("Unable to rename file '%s' to '%s'.",
                            currentFileName, newFileName));
        }
    }

    private File getFile(final String fileName) {
        Objects.requireNonNull(fileName);
        return directory.toPath().resolve(fileName).toFile();
    }

    @Override
    public boolean deleteFile(final String fileName) {
        return getFile(fileName).delete();
    }

    @Override
    public Stream<String> getFileNames() {
        return Arrays.stream(directory.list());
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
    public boolean isFileExists(final String fileName) {
        final File file = getFile(fileName);
        return file.exists();
    }

    @Override
    public FileLock getLock(final String fileName) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public FileReaderSeekable getFileReaderSeekable(final String fileName) {
        throw new UnsupportedOperationException();
    }

}
