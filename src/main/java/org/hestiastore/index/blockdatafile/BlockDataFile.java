package org.hestiastore.index.blockdatafile;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileReader;

public class BlockDataFile {

    /**
     * There is not block reserved for metadata. Each data block have it's own
     * small header.
     */
    private static final BlockPosition FIRST_BLOCK = BlockPosition.of(0);

    private final int blockSize;
    private final String fileName;
    private final Directory directory;

    public BlockDataFile(final Directory directory, final String fileName,
            final int blockSize) {
        this.directory = Vldtn.requireNonNull(directory, "directory");
        this.fileName = Vldtn.requireNonNull(fileName, "fileName");
        this.blockSize = Vldtn.ioBufferSize(blockSize);
    }

    public DataBlockReader openReader(final BlockPosition blockPosition) {
        Vldtn.requireNonNull(blockPosition, "blockPosition");
        if (blockPosition.getValue() < FIRST_BLOCK.getValue()) {
            throw new IllegalArgumentException(String.format(
                    "Block position must be >= '%s'", FIRST_BLOCK.getValue()));
        }
        return new DataBlockReaderImpl(getFileReader(blockPosition),
                blockPosition, blockSize);
    }

    private FileReader getFileReader(final BlockPosition blockPosition) {
        return directory.getFileReader(fileName, blockSize);
    }

    public DataBlockWriterTx getDataBlockWriterTx() {
        return new DataBlockWriterTx(fileName, directory, blockSize);
    }

    public int getBlockSize() {
        return blockSize;
    }

}
