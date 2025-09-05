package org.hestiastore.index.datablockfile;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileWriter;

public class DataBlockWriterImpl implements DataBlockWriter {

    private final FileWriter fileWriter;

    private final int blockSize;

    public DataBlockWriterImpl(final FileWriter fileWriter,
            final int blockSize) {
        this.fileWriter = Vldtn.requireNonNull(fileWriter, "fileWriter");
        this.blockSize = blockSize;
    }

    @Override
    public void close() {
        fileWriter.close();
    }

    @Override
    public void write(final DataBlockPayload dataBlockPayload) {
        Vldtn.requireNonNull(dataBlockPayload, "dataBlockPayload");
        int actualPayloadSize = dataBlockPayload.getBytes().getData().length;
        int requiredPayloadSize = blockSize - DataBlock.HEADER_SIZE;
        if (actualPayloadSize != requiredPayloadSize) {
            throw new IllegalArgumentException(String.format(
                    "Payload size '%d' does not match expected payload size '%d'",
                    actualPayloadSize, requiredPayloadSize));
        }
        byte[] blockData = makeBlockData(dataBlockPayload);
        fileWriter.write(blockData);
    }

    private byte[] makeBlockData(final DataBlockPayload dataBlockPayload) {
        final byte[] blockData = new byte[blockSize];
        final DataBlockHeader header = DataBlockHeader
                .of(DataBlock.MAGIC_NUMBER, dataBlockPayload.calculateCrc());
        System.arraycopy(header.toBytes().getData(), 0, blockData, 0,
                DataBlock.HEADER_SIZE);
        // Copy the payload data after the header
        System.arraycopy(dataBlockPayload.getBytes().getData(), 0, blockData,
                DataBlock.HEADER_SIZE, dataBlockPayload.getBytes().length());
        return blockData;
    }

}
