package com.coroptis.index.directory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import com.coroptis.index.IndexException;

/**
 * Same as FsFileReaderStream but uses java.nio.
 */
public class FsNioFileReaderStream implements FileReader {

    private final FileChannel channel;

    public FsNioFileReaderStream(final File file, final int bufferSize) {
        try {
            channel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    @Override
    public int read() {
        ByteBuffer oneByte = ByteBuffer.allocate(1);
        try {
            int readBytes = channel.read(oneByte);
            if (readBytes == -1) {
                return -1;
            }
            oneByte.flip();
            return oneByte.get() & 0xFF;
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    @Override
    public int read(final byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        try {
            return channel.read(buffer);
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    @Override
    public void skip(final long bytesToSkip) {
        try {
            long currentPos = channel.position();
            channel.position(currentPos + bytesToSkip);
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        try {
            channel.close();
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    @Override
    public String toString() {
        return String.format("FsNioFileReaderStream[channel='%s']", channel.toString());
    }
}