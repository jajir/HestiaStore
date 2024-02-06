package com.coroptis.index.directory;

public class MemFileReaderSeekable extends MemFileReader
        implements FileReaderSeekable {

    MemFileReaderSeekable(final byte[] data) {
        super(data);
    }

    @Override
    public void seek(final long position) {
        setPosition(position);
    }

}
