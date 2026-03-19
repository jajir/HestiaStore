package org.hestiastore.index.datablockfile;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequenceCrc32;
import org.hestiastore.index.bytes.ByteSequences;
import org.hestiastore.index.directory.FileReaderSeekable;

/**
 * Implementation of {@link DataBlockReader}.
 */
public class DataBlockReaderImpl extends AbstractCloseableResource
        implements DataBlockReader {

    private static final byte[] EOF = new byte[0];

    private final FileReaderSeekable fileReader;
    private final DataBlockSize blockSize;
    private final boolean closeReaderOnClose;
    private int position;

    DataBlockReaderImpl(final FileReaderSeekable fileReader,
            final DataBlockPosition blockPosition,
            final DataBlockSize blockSize,
            final boolean closeReaderOnClose) {
        this.fileReader = Vldtn.requireNonNull(fileReader, "fileReader");
        this.blockSize = Vldtn.requireNonNull(blockSize, "blockSize");
        this.position = Vldtn.requireNonNull(blockPosition, "blockPosition")
                .getValue();
        this.closeReaderOnClose = closeReaderOnClose;
    }

    @Override
    protected void doClose() {
        if (closeReaderOnClose) {
            fileReader.close();
        }
    }

    @Override
    public DataBlock read() {
        final byte[] buffer = readFullBlockData();
        if (buffer.length == 0) {
            return null;
        }
        final DataBlockPosition blockPosition = DataBlockPosition.of(position);
        final DataBlock dataBlock = DataBlock.ofSequence(ByteSequences.wrap(buffer),
                blockPosition);
        position += blockSize.getDataBlockSize();
        return dataBlock;
    }

    @Override
    public ByteSequence readPayloadSequence() {
        final byte[] buffer = readFullBlockData();
        if (buffer.length == 0) {
            return null;
        }
        validateBlockData(buffer);
        position += blockSize.getDataBlockSize();
        return ByteSequences.viewOf(buffer, DataBlockHeader.HEADER_SIZE,
                buffer.length);
    }

    private byte[] readFullBlockData() {
        final int blockDataSize = blockSize.getDataBlockSize();
        final byte[] buffer = new byte[blockDataSize];
        int offset = 0;
        while (offset < blockDataSize) {
            final int bytesRead = fileReader.read(buffer, offset,
                    blockDataSize - offset);
            if (bytesRead < 0) {
                if (offset == 0) {
                    return EOF; // End of file reached before reading block.
                }
                throw new IndexException("Unable to read full block");
            }
            if (bytesRead == 0) {
                throw new IndexException("Unable to read full block");
            }
            offset += bytesRead;
        }
        return buffer;
    }

    private void validateBlockData(final byte[] blockData) {
        final DataBlockHeader header = DataBlockHeader
                .ofSequence(ByteSequences.wrap(blockData));
        if (header.getMagicNumber() != DataBlockHeader.MAGIC_NUMBER) {
            throw new IllegalArgumentException(
                    "Invalid magic number in data block header");
        }
        if (header.getCrc() != calculatePayloadCrc(blockData)) {
            throw new IllegalArgumentException(
                    "CRC mismatch in data block header");
        }
    }

    private long calculatePayloadCrc(final byte[] blockData) {
        final ByteSequenceCrc32 crc = new ByteSequenceCrc32();
        crc.update(ByteSequences.viewOf(blockData, DataBlockHeader.HEADER_SIZE,
                blockData.length));
        return crc.getValue();
    }

}
