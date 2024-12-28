package com.coroptis.index.directory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import com.coroptis.index.IndexException;
import com.coroptis.index.directory.Directory.Access;

public class FsNioFileWriterStream implements FileWriter {
    
    private final FileChannel channel;

    public FsNioFileWriterStream(final File file, final Access access) {
        try {
            if(access == Access.OVERWRITE) {
                channel = FileChannel.open(file.toPath(),
                    StandardOpenOption.CREATE,      // Create file if it doesn't exist
                    StandardOpenOption.TRUNCATE_EXISTING, // If it exists, truncate it
                    StandardOpenOption.WRITE        // Open for writing
                );
            } else {
                channel = FileChannel.open(file.toPath(), 
                StandardOpenOption.CREATE,      // Create file if it doesn't exist
                StandardOpenOption.APPEND, // If it exists, append to it
                StandardOpenOption.WRITE        // Open for writing            
                );
            }
        } catch (IOException e) {
            throw new IndexException("Error opening file channel for writing", e);
        }
    }

    @Override
    public void write(final byte b) {
        write(new byte[] { b },0,1);
    }

    @Override
    public void write(byte[] data) {
        write(data, 0, data.length);
    }

    public void write(byte[] data, int offset, int length) {
        ByteBuffer buffer = ByteBuffer.wrap(data, offset, length);
        try {
            channel.write(buffer);
        } catch (IOException e) {
            throw new IndexException("Error writing to file channel", e);
        }
    }

    public void flush() {
        try {
            channel.force(true);
        } catch (IOException e) {
            throw new IndexException("Error flushing file channel", e);
        }
    }

    @Override
    public void close() {
        try {
            channel.close();
        } catch (IOException e) {
            throw new IndexException("Error closing file channel", e);
        }
    }
}