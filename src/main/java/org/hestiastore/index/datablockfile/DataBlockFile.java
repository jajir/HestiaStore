package org.hestiastore.index.datablockfile;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileReader;

/**
 * Data block file is a file that contains data blocks of fixed size. Each data
 * block have it's own small header.
 *
 * <p>
 * Data block file is used to store data blocks in a file. It provides methods
 * to read and write data blocks.
 * </p>
 *
 * <p>
 * Data block file is not thread safe. It is the responsibility of the caller to
 * ensure that only one thread is accessing the data block file at a time.
 * </p>
 *
 */
public class DataBlockFile {

    /**
     * There is not block reserved for metadata. Each data block have it's own
     * small header.
     */
    static final DataBlockPosition FIRST_BLOCK = DataBlockPosition.of(0);

    private final DataBlockSize blockSize;
    private final String fileName;
    private final Directory directory;

    /**
     * Creates a new data block file.
     *
     * @param directory the directory where the data block file is stored
     * @param fileName  the name of the data block file
     * @param blockSize the size of each data block
     */
    public DataBlockFile(final Directory directory, final String fileName,
            final DataBlockSize blockSize) {
        this.directory = Vldtn.requireNonNull(directory, "directory");
        this.fileName = Vldtn.requireNonNull(fileName, "fileName");
        this.blockSize = Vldtn.requireNonNull(blockSize, "blockSize");
    }

    /**
     * Opens a reader for the specified block position.
     *
     * @param blockPosition the position of the data block to read
     * @return a reader for the specified data block
     */
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

    /**
     * Opens a writer transaction for the data block file.
     *
     * @return a writer transaction for the data block file
     */
    public DataBlockWriterTx openWriterTx() {
        return new DataBlockWriterTx(fileName, directory, blockSize);
    }

}
