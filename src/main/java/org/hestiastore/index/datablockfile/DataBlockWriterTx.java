package org.hestiastore.index.datablockfile;

import org.hestiastore.index.Commitable;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileWriter;

/**
 * A transaction for writing a data block file. The data is first written to a
 * temporary file, which is then renamed to the target file name upon commit.
 */
public class DataBlockWriterTx implements Commitable {

    private static final String TEMP_FILE_SUFFIX = ".tmp";
    private final String fileName;
    private final Directory directory;
    private final DataBlockSize blockSize;

    /**
     * Constructs a new DataBlockWriterTx.
     *
     * @param fileName  required target file name
     * @param directory required directory to write to
     * @param blockSize the size of each data block
     */
    public DataBlockWriterTx(String fileName, Directory directory,
            DataBlockSize blockSize) {
        this.fileName = Vldtn.requireNonNull(fileName, "fileName");
        this.directory = Vldtn.requireNonNull(directory, "directory");
        this.blockSize = blockSize;
    }

    /**
     * Opens a DataBlockWriter to write data blocks to the temporary file.
     *
     * @return a DataBlockWriter instance
     */
    public DataBlockWriter openWriter() {
        FileWriter fileWriter = directory.getFileWriter(getTempFileName());
        return new DataBlockWriterImpl(fileWriter, blockSize);
    }

    @Override
    public void commit() {
        directory.renameFile(getTempFileName(), fileName);
    }

    private String getTempFileName() {
        return fileName + TEMP_FILE_SUFFIX;
    }

}
