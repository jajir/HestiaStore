package org.hestiastore.index.datablockfile;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileReader;

public class DataBlockFile {

    /**
     * There is not block reserved for metadata. Each data block have it's own
     * small header.
     */
    private static final DataBlockPosition FIRST_BLOCK = DataBlockPosition
            .of(0);

    private final int blockSize;
    private final String fileName;
    private final Directory directory;

    public DataBlockFile(final Directory directory, final String fileName,
            final int blockSize) {
        this.directory = Vldtn.requireNonNull(directory, "directory");
        this.fileName = Vldtn.requireNonNull(fileName, "fileName");
        this.blockSize = Vldtn.requiredIoBufferSize(blockSize);
    }

    public DataBlockReader openReader(final DataBlockPosition blockPosition) {
        Vldtn.requireNonNull(blockPosition, "blockPosition");
        if (blockPosition.getValue() < FIRST_BLOCK.getValue()) {
            throw new IllegalArgumentException(String.format(
                    "Block position must be >= '%s'", FIRST_BLOCK.getValue()));
        }
        return new DataBlockReaderImpl(getFileReader(blockPosition),
                blockPosition, blockSize);
    }

    private FileReader getFileReader(final DataBlockPosition blockPosition) {
        return directory.getFileReader(fileName, blockSize);
    }

    public DataBlockWriterTx getDataBlockWriterTx() {
        return new DataBlockWriterTx(fileName, directory, blockSize);
    }

    public int getBlockSize() {
        return blockSize;
    }

}
