package org.hestiastore.index.datablockfile;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileWriter;

/**
 * Implementation of {@link DataBlockWriter}.
 */
public class DataBlockWriterImpl implements DataBlockWriter {

    private final FileWriter fileWriter;

    private final DataBlockSize blockSize;

    public DataBlockWriterImpl(final FileWriter fileWriter,
            final DataBlockSize blockSize) {
        this.fileWriter = Vldtn.requireNonNull(fileWriter, "fileWriter");
        this.blockSize = Vldtn.requireNonNull(blockSize, "blockSize");
    }

    @Override
    public void close() {
        fileWriter.close();
    }

    @Override
    public void write(final DataBlockPayload dataBlockPayload) {
        Vldtn.requireNonNull(dataBlockPayload, "dataBlockPayload");
        int actualPayloadSize = dataBlockPayload.getBytes().getData().length;
        int requiredPayloadSize = blockSize.getPayloadSize();
        if (actualPayloadSize != requiredPayloadSize) {
            throw new IllegalArgumentException(String.format(
                    "Payload size '%d' does not match expected payload size '%d'",
                    actualPayloadSize, requiredPayloadSize));
        }
        byte[] blockData = makeBlockData(dataBlockPayload);
        fileWriter.write(blockData);
    }

    private byte[] makeBlockData(final DataBlockPayload dataBlockPayload) {
        final byte[] blockData = new byte[blockSize.getDataBlockSize()];
        final DataBlockHeader header = DataBlockHeader.of(
                DataBlockHeader.MAGIC_NUMBER, dataBlockPayload.calculateCrc());
        System.arraycopy(header.toBytes().getData(), 0, blockData, 0,
                DataBlockHeader.HEADER_SIZE);
        // Copy the payload data after the header
        System.arraycopy(dataBlockPayload.getBytes().getData(), 0, blockData,
                DataBlockHeader.HEADER_SIZE,
                dataBlockPayload.getBytes().length());
        return blockData;
    }

}
