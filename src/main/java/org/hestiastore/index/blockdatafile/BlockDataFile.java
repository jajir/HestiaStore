package org.hestiastore.index.blockdatafile;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;

public class BlockDataFile {

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
        // Implementation to retrieve a DataBlock by its position
        return null;
    }

    public DataBlockWriterTx getDataBlockWriterTx() {
        return new DataBlockWriterTx();
    }

    public int getBlockSize() {
        return blockSize;
    }

}
