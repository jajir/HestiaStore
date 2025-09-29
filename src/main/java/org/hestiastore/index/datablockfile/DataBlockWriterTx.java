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

    private final String fileName;
    private final Directory directory;
    private final DataBlockSize blockSize;

    public DataBlockWriterTx(String fileName, Directory directory,
            DataBlockSize blockSize) {
        this.fileName = Vldtn.requireNonNull(fileName, "fileName");
        this.directory = Vldtn.requireNonNull(directory, "directory");
        this.blockSize = blockSize;
    }

    public DataBlockWriter openWriter() {
        FileWriter fileWriter = directory.getFileWriter(getTempFileName());
        return new DataBlockWriterImpl(fileWriter, blockSize);
    }

    @Override
    public void commit() {
        directory.renameFile(getTempFileName(), fileName);
    }

    private String getTempFileName() {
        return fileName + ".tmp";
    }

}
