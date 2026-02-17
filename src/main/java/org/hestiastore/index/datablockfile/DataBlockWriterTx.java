package org.hestiastore.index.datablockfile;

import org.hestiastore.index.GuardedWriteTransaction;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileWriter;

/**
 * A transaction for writing a data block file. The data is first written to a
 * temporary file, which is then renamed to the target file name upon commit.
 */
public class DataBlockWriterTx
        extends GuardedWriteTransaction<DataBlockWriter> {

    private static final String TEMP_FILE_SUFFIX = ".tmp";
    private final String fileName;
    private final Directory directoryFacade;
    private final DataBlockSize blockSize;

    public DataBlockWriterTx(final String fileName,
            final Directory directoryFacade,
            final DataBlockSize blockSize) {
        this.fileName = Vldtn.requireNonNull(fileName, "fileName");
        this.directoryFacade = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
        this.blockSize = blockSize;
    }

    @Override
    protected DataBlockWriter doOpen() {
        final FileWriter fileWriter = directoryFacade
                .getFileWriter(getTempFileName());
        return new DataBlockWriterImpl(fileWriter, blockSize);
    }

    @Override
    protected void doCommit(final DataBlockWriter resource) {
        directoryFacade.renameFile(getTempFileName(), fileName);
    }

    private String getTempFileName() {
        return fileName + TEMP_FILE_SUFFIX;
    }
}
