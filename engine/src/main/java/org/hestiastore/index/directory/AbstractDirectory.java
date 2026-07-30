package org.hestiastore.index.directory;

import java.io.File;
import java.nio.file.Path;
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
    private static final String DIRECTORY_NAME_ARG = "directoryName";
    private static final String ERROR_REQUIRED_DIRECTORY_IS_FILE =
            "There is required directory but '%s' is file.";

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

    /**
     * Returns the filesystem path backing this directory.
     *
     * @return backing directory path
     */
    public final Path path() {
        return directory.toPath();
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
    public boolean isFileExists(final String fileName) {
        return getFile(fileName).exists();
    }

    @Override
    public boolean deleteFile(final String fileName) {
        return getFile(fileName).delete();
    }

    @Override
    public Stream<String> getFileNames() {
        final String[] fileNames = directory.list();
        if (fileNames == null) {
            throw new IndexException(String.format(
                    "Unable to list directory '%s'.",
                    directory.getAbsolutePath()));
        }
        return Arrays.stream(fileNames);
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

    @Override
    public Directory openSubDirectory(final String directoryName) {
        final File subdirectory = requireExistingOrCreatedDirectory(
                directoryName);
        return createSubDirectory(subdirectory);
    }

    @Override
    public boolean mkdir(final String directoryName) {
        final File subdirectory = getSubdirectory(directoryName);
        if (subdirectory.exists()) {
            assureThatDirectoryIsNotFile(subdirectory);
            return false;
        }
        createDirectory(subdirectory);
        return true;
    }

    @Override
    public boolean rmdir(final String directoryName) {
        final File subdirectory = getSubdirectory(directoryName);
        if (!subdirectory.exists()) {
            return false;
        }
        assureThatDirectoryIsNotFile(subdirectory);
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

    /**
     * Creates a directory implementation of the same storage flavor for a
     * validated subdirectory.
     *
     * @param subdirectory existing backing subdirectory
     * @return directory implementation for {@code subdirectory}
     */
    protected abstract Directory createSubDirectory(File subdirectory);

    private File requireExistingOrCreatedDirectory(final String directoryName) {
        final File subdirectory = getSubdirectory(directoryName);
        if (subdirectory.exists()) {
            assureThatDirectoryIsNotFile(subdirectory);
        } else {
            createDirectory(subdirectory);
        }
        return subdirectory;
    }

    private File getSubdirectory(final String directoryName) {
        Vldtn.requireNonNull(directoryName, DIRECTORY_NAME_ARG);
        return getFile(directoryName);
    }

    private void assureThatDirectoryIsNotFile(final File subdirectory) {
        if (subdirectory.isFile()) {
            throw new IndexException(String.format(
                    ERROR_REQUIRED_DIRECTORY_IS_FILE,
                    subdirectory.getAbsolutePath()));
        }
    }

    private void createDirectory(final File subdirectory) {
        if (!subdirectory.mkdirs()) {
            throw new IndexException(String.format(
                    "Unable to create directory '%s'.",
                    subdirectory.getAbsolutePath()));
        }
    }

}
