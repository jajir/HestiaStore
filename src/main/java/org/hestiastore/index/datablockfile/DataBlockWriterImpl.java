package org.hestiastore.index.datablockfile;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Bytes;
import org.hestiastore.index.MutableByteSequence;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileWriter;

/**
 * Implementation of {@link DataBlockWriter}.
 */
public class DataBlockWriterImpl extends AbstractCloseableResource
        implements DataBlockWriter {

    private final FileWriter fileWriter;

    private final DataBlockSize blockSize;

    public DataBlockWriterImpl(final FileWriter fileWriter,
            final DataBlockSize blockSize) {
        this.fileWriter = Vldtn.requireNonNull(fileWriter, "fileWriter");
        this.blockSize = Vldtn.requireNonNull(blockSize, "blockSize");
    }

    @Override
    protected void doClose() {
        fileWriter.close();
    }

    @Override
    public void write(final DataBlockPayload dataBlockPayload) {
        Vldtn.requireNonNull(dataBlockPayload, "dataBlockPayload");
        int actualPayloadSize = dataBlockPayload.getBytes().length();
        int requiredPayloadSize = blockSize.getPayloadSize();
        if (actualPayloadSize != requiredPayloadSize) {
            throw new IllegalArgumentException(String.format(
                    "Payload size '%d' does not match expected payload size '%d'",
                    actualPayloadSize, requiredPayloadSize));
        }
        final Bytes blockData = makeBlockData(dataBlockPayload);
        fileWriter.write(blockData);
    }

    private Bytes makeBlockData(final DataBlockPayload dataBlockPayload) {
    final MutableByteSequence blockData = MutableByteSequence
        .allocate(blockSize.getDataBlockSize());
        final DataBlockHeader header = DataBlockHeader.of(
                DataBlockHeader.MAGIC_NUMBER, dataBlockPayload.calculateCrc());
        blockData.setBytes(0, header.toBytes());
        // Copy the payload data after the header
        blockData.setBytes(DataBlockHeader.HEADER_SIZE,
                dataBlockPayload.getBytes());
        return blockData.toBytes();
    }

}
