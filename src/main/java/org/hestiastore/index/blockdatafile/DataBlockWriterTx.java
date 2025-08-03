package org.hestiastore.index.blockdatafile;

import org.hestiastore.index.Commitable;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileWriter;

public class DataBlockWriterTx implements Commitable {

    private final String fileName;
    private final Directory directory;
    private final int blockSize;

    public DataBlockWriterTx(String fileName, Directory directory,
            int blockSize) {
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
