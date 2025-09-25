package org.hestiastore.index.datablockfile;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileReader;

public class DataBlockFile {

    /**
     * There is not block reserved for metadata. Each data block have it's own
     * small header.
     */
    static final DataBlockPosition FIRST_BLOCK = DataBlockPosition.of(0);

    private final DataBlockSize blockSize;
    private final String fileName;
    private final Directory directory;

    public DataBlockFile(final Directory directory, final String fileName,
            final DataBlockSize blockSize) {
        this.directory = Vldtn.requireNonNull(directory, "directory");
        this.fileName = Vldtn.requireNonNull(fileName, "fileName");
        this.blockSize = Vldtn.requireNonNull(blockSize, "blockSize");
    }

    public DataBlockReader openReader(final DataBlockPosition blockPosition) {
        Vldtn.requireNonNull(blockPosition, "blockPosition");
        if (blockPosition.getValue() < FIRST_BLOCK.getValue()) {
            throw new IllegalArgumentException(String.format(
                    "Block position must be >= '%s'", FIRST_BLOCK.getValue()));
        }
        if (directory.isFileExists(fileName)) {
            return new DataBlockReaderImpl(getFileReader(blockPosition),
                    blockPosition, blockSize);
        } else {
            return new DataBlockReaderEmpty();
        }
    }

    private FileReader getFileReader(final DataBlockPosition blockPosition) {
        FileReader out = directory.getFileReader(fileName,
                blockSize.getDataBlockSize());
        out.skip(blockPosition.getValue());
        return out;
    }

    public DataBlockWriterTx getDataBlockWriterTx() {
        return new DataBlockWriterTx(fileName, directory, blockSize);
    }

}
