package com.coroptis.index.directory;

public class MemFileReaderSeekable extends MemFileReader
        implements FileReaderSeekable {

    MemFileReaderSeekable(final byte[] data) {
        super(data);
    }

    @Override
    public void seek(final long position) {
        if (position < 0) {
            throw new IllegalArgumentException(
                    String.format("Seek position '%s' is invalid", position));
        }
        if (position >= getDataLength()) {
            throw new IllegalArgumentException(String.format(
                    "Seek position '%s' is out of data size", position));
        }
        setPosition(position);
    }

}
