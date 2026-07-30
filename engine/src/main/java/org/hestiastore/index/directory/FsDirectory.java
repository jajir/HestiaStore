package org.hestiastore.index.directory;

import java.io.File;

import org.hestiastore.index.Vldtn;

public final class FsDirectory extends AbstractDirectory {

    public FsDirectory(final File directory) {
        super(directory);
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
        Vldtn.requireNonNull(fileName, "fileName");
        return new FsFileWriterStream(getFile(fileName),
                Vldtn.requireNonNull(access, "access"), DEFAULT_BUFFER_SIZE);
    }

    @Override
    public FileWriter getFileWriter(final String fileName, final Access access,
            final int bufferSize) {
        Vldtn.requireNonNull(fileName, "fileName");
        return new FsFileWriterStream(getFile(fileName),
                Vldtn.requireNonNull(access, "access"), bufferSize);
    }

    @Override
    public FileLock getLock(String fileName) {
        return new FsFileLock(this, fileName);
    }

    @Override
    protected Directory createSubDirectory(final File subdirectory) {
        return new FsDirectory(subdirectory);
    }

    @Override
    public String toString() {
        return "FsDirectory{directory=" + getDirectory().getPath() + "}";
    }

    @Override
    public FileReaderSeekable getFileReaderSeekable(final String fileName) {
        return new FsFileReaderSeekable(getFile(fileName));
    }

    @Override
    public FileReaderSeekableSupplier getFileReaderSeekableSupplier(
            final String fileName) {
        return new FsSharedFileReaderSeekableSupplier(getFile(fileName));
    }

}
