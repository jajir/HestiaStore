package com.coroptis.index.directory;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import com.coroptis.index.IndexException;

public class MemDirectory implements Directory {

    private final Map<String, byte[]> data = new HashMap<>();

    @Override
    public FileReader getFileReader(final String fileName) {
        if (!data.containsKey(fileName)) {
            throw new IndexException(
                    String.format("There is no file '%s'", fileName));
        }
        return new MemFileReader(data.get(fileName));
    }

    @Override
    public FileReader getFileReader(final String fileName,
            final int bufferSize) {
        if (!data.containsKey(fileName)) {
            throw new IndexException(
                    String.format("There is no file '%s'", fileName));
        }
        return new MemFileReader(data.get(fileName));
    }

    @Override
    public FileWriter getFileWriter(final String fileName,
            final Access access) {
        return new MemFileWriter(fileName, this, access);
    }

    @Override
    public FileWriter getFileWriter(final String fileName, final Access access,
            final int bufferSize) {
        return new MemFileWriter(fileName, this, access);
    }

    @Override
    public void renameFile(final String currentFileName,
            final String newFileName) {
        if (data.containsKey(currentFileName)) {
            final byte[] tmp = data.remove(currentFileName);
            data.put(newFileName, tmp);
        }
    }

    void addFile(final String fileName, final byte bytes[],
            final Access access) {
        if (Access.OVERWRITE == access) {
            data.put(fileName, bytes);
        } else {
            final byte a[] = data.get(fileName);
            if (a == null) {
                throw new IndexException(
                        String.format("No such file '%s'", fileName));
            }
            byte[] c = new byte[a.length + bytes.length];
            System.arraycopy(a, 0, c, 0, a.length);
            System.arraycopy(bytes, 0, c, a.length, bytes.length);
            data.put(fileName, c);
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
        final byte[] fileData = data.get(fileName);
        if (fileData == null) {
            throw new IllegalArgumentException(
                    String.format("No such file '%s'.", fileName));
        }
        return new MemFileReaderSeekable(data.get(fileName));
    }

}
