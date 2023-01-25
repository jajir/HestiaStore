package com.coroptis.index.directory;

import java.io.File;
import java.util.Objects;
import java.util.stream.Stream;

import com.coroptis.index.IndexException;
import com.google.common.collect.Lists;

public class FsZipDirectory implements Directory {

    private final File directory;

    public FsZipDirectory(final File directory) {
        this.directory = Objects.requireNonNull(directory);
        if (!directory.exists()) {
            if (!directory.mkdirs()) {
                throw new IndexException(String.format("Unable to create directory '%s.",
                        directory.getAbsoluteFile()));
            }
        }
        if (directory.isFile()) {
            throw new IndexException(
                    String.format("There is required directory but '%s' is file."));
        }
    }

    @Override
    public FileReader getFileReader(final String fileName) {
        final File file = getFile(fileName);
        if (!directory.exists()) {
            throw new IndexException(String.format("File '%s' doesn't exists."));
        }
        return new FsZipFileReaderStream(file);
    }

    @Override
    public FileWriter getFileWriter(final String fileName) {
        Objects.requireNonNull(fileName);
        return new FsZipFileWriterStream(getFile(fileName));
    }

    @Override
    public void renameFile(final String currentFileName, final String newFileName) {
        final File file = getFile(currentFileName);
        if (!directory.exists()) {
            throw new IndexException(String.format("File '%s' doesn't exists."));
        }
        if (!file.renameTo(getFile(newFileName))) {
            throw new IndexException(String.format("Unable to rename file '%s' to '%s'.",
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
        return Lists.newArrayList(directory.list()).stream();
    }

}
