package org.hestiastore.index.datablockfile;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequenceCrc32;
import org.hestiastore.index.bytes.ByteSequences;
import org.hestiastore.index.directory.FileWriter;

/**
 * Implementation of {@link DataBlockWriter}.
 */
public class DataBlockWriterImpl extends AbstractCloseableResource
        implements DataBlockWriter {

    private final FileWriter fileWriter;

    private final DataBlockSize blockSize;
    private final byte[] blockDataBuffer;

    public DataBlockWriterImpl(final FileWriter fileWriter,
            final DataBlockSize blockSize) {
        this.fileWriter = Vldtn.requireNonNull(fileWriter, "fileWriter");
        this.blockSize = Vldtn.requireNonNull(blockSize, "blockSize");
        this.blockDataBuffer = new byte[blockSize.getDataBlockSize()];
    }

    @Override
    protected void doClose() {
        fileWriter.close();
    }

    @Override
    public void writeSequence(final ByteSequence dataBlockPayload) {
        final ByteSequence payload = Vldtn.requireNonNull(dataBlockPayload,
                "dataBlockPayload");
        int actualPayloadSize = payload.length();
        int requiredPayloadSize = blockSize.getPayloadSize();
        if (actualPayloadSize != requiredPayloadSize) {
            throw new IllegalArgumentException(String.format(
                    "Payload size '%d' does not match expected payload size '%d'",
                    actualPayloadSize, requiredPayloadSize));
        }
        writeIntoSharedBuffer(payload, calculateCrc(payload));
        fileWriter.write(blockDataBuffer);
    }

    private void writeIntoSharedBuffer(final ByteSequence payload,
            final long crc) {
        writeLong(blockDataBuffer, 0, DataBlockHeader.MAGIC_NUMBER);
        writeLong(blockDataBuffer, 8, crc);
        // Copy payload data after the fixed-size header.
        ByteSequences.copy(payload, 0, blockDataBuffer,
                DataBlockHeader.HEADER_SIZE,
                payload.length());
    }

    private static long calculateCrc(final ByteSequence payload) {
        final ByteSequenceCrc32 crc = new ByteSequenceCrc32();
        crc.update(payload);
        return crc.getValue();
    }

    private static void writeLong(final byte[] target, final int offset,
            final long value) {
        target[offset] = (byte) (value >>> 56);
        target[offset + 1] = (byte) (value >>> 48);
        target[offset + 2] = (byte) (value >>> 40);
        target[offset + 3] = (byte) (value >>> 32);
        target[offset + 4] = (byte) (value >>> 24);
        target[offset + 5] = (byte) (value >>> 16);
        target[offset + 6] = (byte) (value >>> 8);
        target[offset + 7] = (byte) value;
    }

}
