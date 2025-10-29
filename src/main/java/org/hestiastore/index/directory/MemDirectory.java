package org.hestiastore.index.directory;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.IndexException;

public class MemDirectory implements Directory {

    private static final String ERROR_MSG_NO_FILE = "There is no file '%s'";
    private final Map<String, Bytes> data = new HashMap<>();

    @Override
    public FileReader getFileReader(final String fileName) {
        final Bytes bytes = data.get(fileName);
        if (bytes == null) {
            throw new IndexException(
                    String.format(ERROR_MSG_NO_FILE, fileName));
        }
        return new MemFileReader(bytes);
    }

    public Bytes getFileBytes(final String fileName) {
        final Bytes bytes = data.get(fileName);
        if (bytes == null) {
            throw new IndexException(
                    String.format(ERROR_MSG_NO_FILE, fileName));
        }
        return bytes;
    }

    public void setFileBytes(final String fileName, final Bytes bytes) {
        data.put(fileName, bytes);
    }

    @Override
    public FileReader getFileReader(final String fileName,
            final int bufferSize) {
        final Bytes bytes = data.get(fileName);
        if (bytes == null) {
            throw new IndexException(
                    String.format(ERROR_MSG_NO_FILE, fileName));
        }
        return new MemFileReader(bytes);
    }

    @Override
    public FileWriter getFileWriter(final String fileName,
            final Access access) {
        return new MemFileWriter(fileName, this, access);
    }

    @Override
    public FileWriter getFileWriter(final String fileName, final Access access,
            final int bufferSize) {
        if (bufferSize <= 0) {
            throw new IllegalArgumentException(
                    "Buffer size must be greater than zero.");
        }
        return new MemFileWriter(fileName, this, access);
    }

    @Override
    public void renameFile(final String currentFileName,
            final String newFileName) {
        if (data.containsKey(currentFileName)) {
            final Bytes tmp = data.remove(currentFileName);
            data.put(newFileName, tmp);
        }
    }

    void addFile(final String fileName, final Bytes bytes,
            final Access access) {
        if (Access.OVERWRITE == access) {
            data.put(fileName, bytes);
        } else {
            final Bytes existing = data.get(fileName);
            if (existing == null) {
                throw new IndexException(
                        String.format("No such file '%s'", fileName));
            }
            data.put(fileName, existing.concat(bytes));
        }
    }

    @Override
    public String toString() {
        return "MemDirectory{" + "}";
    }

    @Override
    public boolean deleteFile(final String fileName) {
        return data.remove(fileName) != null;
    }

    @Override
    public Stream<String> getFileNames() {
        return data.keySet().stream();
    }

    @Override
    public boolean isFileExists(final String fileName) {
        return data.containsKey(fileName);
    }

    @Override
    public FileLock getLock(final String fileName) {
        return new MemFileLock(this, fileName);
    }

    @Override
    public FileReaderSeekable getFileReaderSeekable(final String fileName) {
        final Bytes fileData = data.get(fileName);
        if (fileData == null) {
            throw new IllegalArgumentException(
                    String.format("No such file '%s'.", fileName));
        }
        return new MemFileReaderSeekable(fileData);
    }

}
