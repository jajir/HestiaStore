package org.hestiastore.index.senku.internal;

import static org.hestiastore.index.senku.internal.LargeFileTestSupport.DATA_BLOCK_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequences;
import org.hestiastore.index.chunkstore.ChunkStoreWriter;
import org.hestiastore.index.chunkstore.ChunkStoreWriterTx;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SenkuShardIndexCodecTest {

    private MemDirectory directory;

    @BeforeEach
    void setUp() {
        directory = new MemDirectory();
    }

    @Test
    void codecRoundTripsPositionsCountsAndEmptyShards() {
        final long first = LargeFilePosition.of(0, 16).getPacked();
        final long third = LargeFilePosition.of(2, 32).getPacked();
        final SenkuShardIndex expected = new SenkuShardIndex(
                new long[] { first, 0L, third }, new long[] { 2L, 0L, 1L });

        SenkuShardIndexCodec.write(directory, DATA_BLOCK_SIZE, expected);
        final SenkuShardIndex actual = SenkuShardIndexCodec.read(directory,
                DATA_BLOCK_SIZE, 3);

        assertEquals(first, actual.packedPosition(0));
        assertEquals(2L, actual.recordCount(0));
        assertEquals(0L, actual.recordCount(1));
        assertEquals(third, actual.packedPosition(2));
        assertEquals(1L, actual.recordCount(2));
        assertFalse(directory.isFileExists("shard-index.dat.tmp"));
    }

    @Test
    void readRejectsMissingFile() {
        assertThrows(IndexException.class, () -> SenkuShardIndexCodec
                .read(directory, DATA_BLOCK_SIZE, 1));
    }

    @Test
    void writeRejectsEmptyFlushAndExistingFiles() {
        final SenkuShardIndex empty = new SenkuShardIndex(new long[] { 0L },
                new long[] { 0L });
        assertThrows(IndexException.class, () -> SenkuShardIndexCodec
                .write(directory, DATA_BLOCK_SIZE, empty));

        directory.touch(SenkuFileNames.SHARD_INDEX_FILE);
        final SenkuShardIndex nonEmpty = oneRecordIndex();
        assertThrows(IndexException.class, () -> SenkuShardIndexCodec
                .write(directory, DATA_BLOCK_SIZE, nonEmpty));
    }

    @Test
    void writeRejectsExistingTemporaryFile() {
        directory.touch("shard-index.dat.tmp");

        assertThrows(IndexException.class, () -> SenkuShardIndexCodec
                .write(directory, DATA_BLOCK_SIZE, oneRecordIndex()));
    }

    @Test
    void readRejectsWrongVersion() {
        writeRaw(2, validPayload(0, 0L, 1L));

        assertThrows(IndexException.class, () -> SenkuShardIndexCodec
                .read(directory, DATA_BLOCK_SIZE, 1));
    }

    @Test
    void readRejectsTruncatedAndOversizedPayload() {
        writeRaw(SenkuShardIndexCodec.SHARD_INDEX_VERSION,
                ByteSequences.wrap(new byte[19]));
        assertThrows(IndexException.class, () -> SenkuShardIndexCodec
                .read(directory, DATA_BLOCK_SIZE, 1));

        final MemDirectory oversizedDirectory = new MemDirectory();
        writeRaw(oversizedDirectory, SenkuShardIndexCodec.SHARD_INDEX_VERSION,
                ByteSequences.wrap(new byte[21]));
        assertThrows(IndexException.class, () -> SenkuShardIndexCodec
                .read(oversizedDirectory, DATA_BLOCK_SIZE, 1));
    }

    @Test
    void readRejectsWrongShardIdAndNegativeValues() {
        writeRaw(SenkuShardIndexCodec.SHARD_INDEX_VERSION,
                validPayload(1, 0L, 1L));
        assertThrows(IndexException.class, () -> SenkuShardIndexCodec
                .read(directory, DATA_BLOCK_SIZE, 1));

        final MemDirectory negativePositionDirectory = new MemDirectory();
        writeRaw(negativePositionDirectory,
                SenkuShardIndexCodec.SHARD_INDEX_VERSION,
                validPayload(0, -1L, 1L));
        assertThrows(IndexException.class, () -> SenkuShardIndexCodec
                .read(negativePositionDirectory, DATA_BLOCK_SIZE, 1));

        final MemDirectory negativeCountDirectory = new MemDirectory();
        writeRaw(negativeCountDirectory,
                SenkuShardIndexCodec.SHARD_INDEX_VERSION,
                validPayload(0, 0L, -1L));
        assertThrows(IndexException.class, () -> SenkuShardIndexCodec
                .read(negativeCountDirectory, DATA_BLOCK_SIZE, 1));
    }

    @Test
    void readRejectsAllEmptyCounts() {
        writeRaw(SenkuShardIndexCodec.SHARD_INDEX_VERSION,
                validPayload(0, 0L, 0L));

        assertThrows(IndexException.class, () -> SenkuShardIndexCodec
                .read(directory, DATA_BLOCK_SIZE, 1));
    }

    @Test
    void readRejectsExtraChunk() {
        final ChunkStoreWriterTx transaction = LargeFile
                .chunkStore(directory, DATA_BLOCK_SIZE,
                        SenkuFileNames.SHARD_INDEX_FILE)
                .openWriteTx();
        final ChunkStoreWriter writer = transaction.open();
        final ByteSequence payload = validPayload(0, 0L, 1L);
        writer.writeSequence(payload,
                SenkuShardIndexCodec.SHARD_INDEX_VERSION);
        writer.writeSequence(payload,
                SenkuShardIndexCodec.SHARD_INDEX_VERSION);
        writer.close();
        transaction.commit();

        assertThrows(IndexException.class, () -> SenkuShardIndexCodec
                .read(directory, DATA_BLOCK_SIZE, 1));
    }

    private static SenkuShardIndex oneRecordIndex() {
        return new SenkuShardIndex(new long[] { 0L }, new long[] { 1L });
    }

    private void writeRaw(final int version, final ByteSequence payload) {
        writeRaw(directory, version, payload);
    }

    private static void writeRaw(final MemDirectory target, final int version,
            final ByteSequence payload) {
        final ChunkStoreWriterTx transaction = LargeFile
                .chunkStore(target, DATA_BLOCK_SIZE,
                        SenkuFileNames.SHARD_INDEX_FILE)
                .openWriteTx();
        final ChunkStoreWriter writer = transaction.open();
        writer.writeSequence(payload, version);
        writer.close();
        transaction.commit();
    }

    private static ByteSequence validPayload(final int shardId,
            final long packedPosition, final long recordCount) {
        final ByteBuffer buffer = ByteBuffer
                .allocate(SenkuShardIndexCodec.RECORD_BYTES)
                .order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(shardId);
        buffer.putLong(packedPosition);
        buffer.putLong(recordCount);
        return ByteSequences.wrap(buffer.array());
    }
}
