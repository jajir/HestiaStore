package org.hestiastore.index.directory;

import java.io.File;

import org.hestiastore.index.Vldtn;

public final class FsNioDirectory extends AbstractDirectory {

    public FsNioDirectory(final File directory) {
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
    protected Directory createSubDirectory(final File subdirectory) {
        return new FsNioDirectory(subdirectory);
    }

    @Override
    public String toString() {
        return "FsNioDirectory{directory=" + getDirectory().getPath() + "}";
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
