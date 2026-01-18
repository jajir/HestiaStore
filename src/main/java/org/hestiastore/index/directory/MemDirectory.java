package org.hestiastore.index.directory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;

public class MemDirectory implements Directory {

    private static final String ERROR_MSG_NO_FILE = "There is no file '%s'";
    private final Map<String, byte[]> data = new HashMap<>();
    private final Map<String, MemDirectory> directories = new HashMap<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();

    @Override
    public FileReader getFileReader(final String fileName) {
        readLock.lock();
        try {
            final byte[] bytes = data.get(fileName);
            if (bytes == null) {
                throw new IndexException(
                        String.format(ERROR_MSG_NO_FILE, fileName));
            }
            return new MemFileReader(bytes);
        } finally {
            readLock.unlock();
        }
    }

    public Bytes getFileBytes(final String fileName) {
        readLock.lock();
        try {
            final byte[] bytes = data.get(fileName);
            if (bytes == null) {
                throw new IndexException(
                        String.format(ERROR_MSG_NO_FILE, fileName));
            }
            return Bytes.of(bytes);
        } finally {
            readLock.unlock();
        }
    }

    public void setFileBytes(final String fileName, final Bytes bytes) {
        writeLock.lock();
        try {
            data.put(fileName, bytes.getData());
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public FileReader getFileReader(final String fileName,
            final int bufferSize) {
        readLock.lock();
        try {
            final byte[] bytes = data.get(fileName);
            if (bytes == null) {
                throw new IndexException(
                        String.format(ERROR_MSG_NO_FILE, fileName));
            }
            return new MemFileReader(bytes);
        } finally {
            readLock.unlock();
        }
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
        writeLock.lock();
        try {
            if (directories.containsKey(newFileName)) {
                throw new IndexException(String.format(
                        "There is required file but '%s' is directory.",
                        newFileName));
            }
            if (data.containsKey(currentFileName)) {
                final byte[] tmp = data.remove(currentFileName);
                data.put(newFileName, tmp);
            }
        } finally {
            writeLock.unlock();
        }
    }

    void addFile(final String fileName, final byte[] bytes,
            final Access access) {
        writeLock.lock();
        try {
            if (directories.containsKey(fileName)) {
                throw new IndexException(String.format(
                        "There is required file but '%s' is directory.",
                        fileName));
            }
            if (Access.OVERWRITE == access) {
                data.put(fileName, bytes);
            } else {
                final byte[] a = data.get(fileName);
                if (a == null) {
                    throw new IndexException(
                            String.format("No such file '%s'", fileName));
                }
                byte[] c = new byte[a.length + bytes.length];
                System.arraycopy(a, 0, c, 0, a.length);
                System.arraycopy(bytes, 0, c, a.length, bytes.length);
                data.put(fileName, c);
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public String toString() {
        return "MemDirectory{" + "}";
    }

    @Override
    public boolean deleteFile(final String fileName) {
        writeLock.lock();
        try {
            return data.remove(fileName) != null;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public Stream<String> getFileNames() {
        readLock.lock();
        try {
            return new ArrayList<>(data.keySet()).stream();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Directory openSubDirectory(final String directoryName) {
        Vldtn.requireNonNull(directoryName, "directoryName");
        writeLock.lock();
        try {
            if (data.containsKey(directoryName)) {
                throw new IndexException(String.format(
                        "There is required directory but '%s' is file.",
                        directoryName));
            }
            MemDirectory directory = directories.get(directoryName);
            if (directory == null) {
                directory = new MemDirectory();
                directories.put(directoryName, directory);
            }
            return directory;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean mkdir(final String directoryName) {
        Vldtn.requireNonNull(directoryName, "directoryName");
        writeLock.lock();
        try {
            if (data.containsKey(directoryName)) {
                throw new IndexException(String.format(
                        "There is required directory but '%s' is file.",
                        directoryName));
            }
            if (directories.containsKey(directoryName)) {
                return false;
            }
            directories.put(directoryName, new MemDirectory());
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean rmdir(final String directoryName) {
        Vldtn.requireNonNull(directoryName, "directoryName");
        writeLock.lock();
        try {
            if (data.containsKey(directoryName)) {
                throw new IndexException(String.format(
                        "There is required directory but '%s' is file.",
                        directoryName));
            }
            final MemDirectory directory = directories.get(directoryName);
            if (directory == null) {
                return false;
            }
            if (!directory.isEmpty()) {
                throw new IndexException(
                        String.format("Directory '%s' is not empty.",
                                directoryName));
            }
            directories.remove(directoryName);
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean isFileExists(final String fileName) {
        readLock.lock();
        try {
            return data.containsKey(fileName);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public FileLock getLock(final String fileName) {
        return new MemFileLock(this, fileName);
    }

    @Override
    public FileReaderSeekable getFileReaderSeekable(final String fileName) {
        readLock.lock();
        try {
            final byte[] fileData = data.get(fileName);
            if (fileData == null) {
                throw new IllegalArgumentException(
                        String.format("No such file '%s'.", fileName));
            }
            return new MemFileReaderSeekable(fileData);
        } finally {
            readLock.unlock();
        }
    }

    private boolean isEmpty() {
        readLock.lock();
        try {
            return data.isEmpty() && directories.isEmpty();
        } finally {
            readLock.unlock();
        }
    }

}
