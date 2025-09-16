package org.hestiastore.index.chunkstore;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.Vldtn;

public class ChunkStoreReaderImpl implements ChunkStoreReader {

    private DataBlockByteReader dataBlockByteReader = null;

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
                .readExactly(Chunk.HEADER_SIZE);
        if (headerBytes == null) {
            return null;
        }
        final ChunkHeader chunkHeader = ChunkHeader.of(headerBytes);
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
        int out = length / Chunk.CELL_SIZE;
        if (length % Chunk.CELL_SIZE != 0) {
            out++;
        }
        return out * Chunk.CELL_SIZE;

    }

}
