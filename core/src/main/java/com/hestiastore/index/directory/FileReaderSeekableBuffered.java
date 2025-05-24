package com.hestiastore.index.directory;

import java.util.Objects;

public class FileReaderSeekableBuffered implements FileReaderSeekable {

    private final FileReaderSeekable fileReader;

    final int bufferSizeInBytes;

    private BufferedReader bufferedReader;

    public FileReaderSeekableBuffered(final FileReaderSeekable fileReader,
            final int bufferSizeInBytes) {
        this.fileReader = Objects.requireNonNull(fileReader);
        this.bufferSizeInBytes = bufferSizeInBytes;
        bufferedReader = new BufferedReader(fileReader, bufferSizeInBytes, 0);
    }

    @Override
    public int read() {
        throw new UnsupportedOperationException("Not implementet");
    }

    @Override
    public int read(final byte[] bytes) {
        return bufferedReader.read(bytes);
    }

    @Override
    public void skip(final long position) {
        throw new UnsupportedOperationException("Not implementet");
    }

    @Override
    public void close() {
        fileReader.close();

    }

    @Override
    public void seek(final long position) {
        int positionInBuffer = (int) (position % bufferSizeInBytes);
        long positionInFile = position - positionInBuffer;
        fileReader.seek(positionInFile);

        bufferedReader = new BufferedReader(fileReader, bufferSizeInBytes,
                positionInBuffer);
    }

}
