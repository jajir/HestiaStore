package org.hestiastore.index.chunkstore;

import java.util.Optional;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datablockfile.DataBlockByteReader;

/**
 * Implementation of {@link ChunkStoreReader}.
 */
public class ChunkStoreReaderImpl implements ChunkStoreReader {

    private DataBlockByteReader dataBlockByteReader = null;

    /**
     * Constructor.
     *
     * @param dataBlockByteReader required data block byte reader
     * @throws IllegalArgumentException when dataBlockByteReader is null
     */
    public ChunkStoreReaderImpl(final DataBlockByteReader dataBlockByteReader) {
        this.dataBlockByteReader = Vldtn.requireNonNull(dataBlockByteReader,
                "dataBlockByteReader");
    }

    @Override
    public void close() {
        dataBlockByteReader.close();
    }

    @Override
    public Chunk read() {
        final Bytes headerBytes = dataBlockByteReader
                .readExactly(ChunkHeader.HEADER_SIZE);
        final Optional<ChunkHeader> optChunkHeader = ChunkHeader
                .optionalOf(headerBytes);
        if (optChunkHeader.isEmpty()) {
            return null;
        }
        final ChunkHeader chunkHeader = optChunkHeader.get();
        int requiredLength = chunkHeader.getPayloadLength();
        int cellLength = convertLengthToWholeCells(requiredLength);
        Bytes payload = dataBlockByteReader.readExactly(cellLength);
        if (payload == null) {
            throw new IllegalStateException(
                    "Unexpected end of stream while reading chunk payload.");
        }
        if (cellLength != requiredLength) {
            payload = payload.subBytes(0, requiredLength);
        }
        final Chunk out = Chunk.of(chunkHeader, payload);
        if (chunkHeader.getCrc() != out.calculateCrc()) {
            throw new IllegalArgumentException(String.format(
                    "Invalid chunk CRC, expected: '%s', actual: '%s'",
                    chunkHeader.getCrc(), out.calculateCrc()));
        }
        return out;
    }

    private int convertLengthToWholeCells(final int length) {
        int out = length / CellPosition.CELL_SIZE;
        if (length % CellPosition.CELL_SIZE != 0) {
            out++;
        }
        return out * CellPosition.CELL_SIZE;

    }

}
