package com.coroptis.index.directory;

import java.io.File;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;

import com.coroptis.index.IndexException;

public class FsDirectory implements Directory {

    private final static int BUFFER_SIZE = 1024 * 1 * 4;

    private final File directory;

    public FsDirectory(final File directory) {
        this.directory = Objects.requireNonNull(directory);
        if (!directory.exists() && !directory.mkdirs()) {
            throw new IndexException(
                    String.format("Unable to create directory '%s'.",
                            directory.getAbsolutePath()));
        }
        if (directory.isFile()) {
            throw new IndexException(String.format(
                    "There is required directory but '%s' is file.",
                    directory.getAbsolutePath()));
        }
    }

    @Override
    public boolean isFileExists(final String fileName) {
        final File file = getFile(fileName);
        return file.exists();
    }

    @Override
    public FileReader getFileReader(final String fileName) {
        return getFileReader(fileName, BUFFER_SIZE);
    }

    @Override
    public FileReader getFileReader(final String fileName,
            final int bufferSize) {
        final File file = getFile(fileName);
        if (!directory.exists()) {
            throw new IndexException(
                    String.format("File '%s' doesn't exists."));
        }
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
    public void renameFile(final String currentFileName,
            final String newFileName) {
        final File file = getFile(currentFileName);
        if (!directory.exists()) {
            throw new IndexException(
                    String.format("File '%s' doesn't exists."));
        }
        if (!file.renameTo(getFile(newFileName))) {
            throw new IndexException(
                    String.format("Unable to rename file '%s' to name '%s'.",
                            file.getAbsolutePath(), newFileName));
        }
    }

    private File getFile(final String fileName) {
        Objects.requireNonNull(fileName, "file name can't be null.");
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
    public FileLock getLock(String fileName) {
        return new FsFileLock(this, fileName);
    }

    @Override
    public String toString() {
        return "FsDirectory{directory=" + directory.getPath() + "}";
    }

    @Override
    public FileReaderSeekable getFileReaderSeekable(final String fileName) {
        return new FsFileReaderSeekable(getFile(fileName));
    }

}
