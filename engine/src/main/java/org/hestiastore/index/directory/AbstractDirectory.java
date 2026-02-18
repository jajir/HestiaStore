package org.hestiastore.index.directory;

import java.io.File;
import java.util.Arrays;
import java.util.stream.Stream;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;

/**
 * Abstract implementation of {@link Directory} interface.
 * 
 * This abstract class add some java.io.File suport methods.
 */
public abstract class AbstractDirectory implements Directory {

    protected static final int DEFAULT_BUFFER_SIZE = 1024 * 1 * 4;

    private final File directory;

    protected AbstractDirectory(final File directory) {
        this.directory = Vldtn.requireNonNull(directory, "directory");
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

    protected File getDirectory() {
        return directory;
    }

    protected void assureThatFileExists(final File file) {
        Vldtn.requireNonNull(file, "file");
        if (!file.exists()) {
            throw new IndexException(String.format("File '%s' doesn't exists.",
                    file.getAbsolutePath()));
        }
    }

    protected File getFile(final String fileName) {
        Vldtn.requireNonNull(fileName, "fileName");
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
    public void renameFile(final String currentFileName,
            final String newFileName) {
        final File file = getFile(currentFileName);
        assureThatFileExists(file);
        if (!file.renameTo(getFile(newFileName))) {
            throw new IndexException(
                    String.format("Unable to rename file '%s' to name '%s'.",
                            file.getAbsolutePath(), newFileName));
        }
    }

}
