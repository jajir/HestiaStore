package org.hestiastore.index.datablockfile;

import org.hestiastore.index.GuardedWriteTransaction;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.DirectoryFacade;
import org.hestiastore.index.directory.FileWriter;
import org.hestiastore.index.directory.async.AsyncFileWriterBlockingAdapter;

/**
 * A transaction for writing a data block file. The data is first written to a
 * temporary file, which is then renamed to the target file name upon commit.
 */
public class DataBlockWriterTx
        extends GuardedWriteTransaction<DataBlockWriter> {

    private static final String TEMP_FILE_SUFFIX = ".tmp";
    private final String fileName;
    private final DirectoryFacade directoryFacade;
    private final DataBlockSize blockSize;

    public DataBlockWriterTx(final String fileName,
            final DirectoryFacade directoryFacade,
            final DataBlockSize blockSize) {
        this.fileName = Vldtn.requireNonNull(fileName, "fileName");
        this.directoryFacade = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
        this.blockSize = blockSize;
    }

    @Override
    protected DataBlockWriter doOpen() {
        final FileWriter fileWriter = new AsyncFileWriterBlockingAdapter(
                directoryFacade.getFileWriterAsync(getTempFileName())
                        .toCompletableFuture().join());
        return new DataBlockWriterImpl(fileWriter, blockSize);
    }

    @Override
    protected void doCommit(final DataBlockWriter resource) {
        directoryFacade.renameFileAsync(getTempFileName(), fileName)
                .toCompletableFuture().join();
    }

    private String getTempFileName() {
        return fileName + TEMP_FILE_SUFFIX;
    }
}
