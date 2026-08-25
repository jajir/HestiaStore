package org.hestiastore.index.senku.internal;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequences;
import org.hestiastore.index.chunkstore.Chunk;
import org.hestiastore.index.chunkstore.ChunkStoreFile;
import org.hestiastore.index.chunkstore.ChunkStoreReader;
import org.hestiastore.index.chunkstore.ChunkStoreWriter;
import org.hestiastore.index.chunkstore.ChunkStoreWriterTx;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.directory.Directory;

/**
 * Persists the single-chunk sparse shard table of one flush generation.
 */
final class SenkuShardIndexCodec {

    static final int SHARD_INDEX_VERSION = 1;
    static final int RECORD_BYTES = Integer.BYTES + Long.BYTES + Long.BYTES;

    private SenkuShardIndexCodec() {
        // Static codec.
    }

    static void write(final Directory directory,
            final DataBlockSize dataBlockSize, final SenkuShardIndex index) {
        final Directory validatedDirectory = Vldtn.requireNonNull(directory,
                "directory");
        final DataBlockSize validatedBlockSize = Vldtn
                .requireNonNull(dataBlockSize, "dataBlockSize");
        final SenkuShardIndex validatedIndex = Vldtn.requireNonNull(index,
                "index");
        if (validatedIndex.totalRecordCount() == 0L) {
            throw new IndexException(
                    "A committed flush shard index must contain records.");
        }
        requireAbsent(validatedDirectory, SenkuFileNames.SHARD_INDEX_FILE);
        requireAbsent(validatedDirectory,
                SenkuFileNames.temporary(SenkuFileNames.SHARD_INDEX_FILE));
        final ByteSequence payload = encode(validatedIndex);
        final ChunkStoreWriterTx transaction = chunkStore(validatedDirectory,
                validatedBlockSize).openWriteTx();
        final ChunkStoreWriter writer = transaction.open();
        try {
            writer.writeSequence(payload, SHARD_INDEX_VERSION);
            writer.close();
            transaction.commit();
        } catch (Exception e) {
            closeAfterFailure(writer, e);
            if (e instanceof IndexException) {
                throw (IndexException) e;
            }
            throw new IndexException("Unable to write flush shard index.", e);
        }
    }

    static SenkuShardIndex read(final Directory directory,
            final DataBlockSize dataBlockSize, final int shardCount) {
        final Directory validatedDirectory = Vldtn.requireNonNull(directory,
                "directory");
        final DataBlockSize validatedBlockSize = Vldtn
                .requireNonNull(dataBlockSize, "dataBlockSize");
        final int validatedShardCount = Vldtn.requireGreaterThanZero(shardCount,
                "shardCount");
        if (!validatedDirectory
                .isFileExists(SenkuFileNames.SHARD_INDEX_FILE)) {
            throw new IndexException("Required shard-index.dat is missing.");
        }
        final ChunkStoreFile store = chunkStore(validatedDirectory,
                validatedBlockSize);
        try (ChunkStoreReader reader = store
                .openReader(store.getFirstChunkStorePosition())) {
            final Chunk chunk = reader.read();
            if (chunk == null) {
                throw new IndexException("Shard index contains no chunk.");
            }
            if (chunk.getHeader().getVersion() != SHARD_INDEX_VERSION) {
                throw new IndexException("Unexpected shard-index version "
                        + chunk.getHeader().getVersion() + "; expected "
                        + SHARD_INDEX_VERSION + ".");
            }
            final SenkuShardIndex index = decode(chunk.getPayloadSequence(),
                    validatedShardCount);
            if (reader.read() != null) {
                throw new IndexException(
                        "Shard index must contain exactly one chunk.");
            }
            if (index.totalRecordCount() == 0L) {
                throw new IndexException(
                        "A committed flush shard index must contain records.");
            }
            return index;
        } catch (IndexException e) {
            throw e;
        } catch (Exception e) {
            throw new IndexException("Unable to read flush shard index.", e);
        }
    }

    private static ByteSequence encode(final SenkuShardIndex index) {
        final ByteBuffer buffer = ByteBuffer
                .allocate(tableBytes(index.shardCount()))
                .order(ByteOrder.BIG_ENDIAN);
        for (int shardId = 0; shardId < index.shardCount(); shardId++) {
            buffer.putInt(shardId);
            buffer.putLong(index.packedPosition(shardId));
            buffer.putLong(index.recordCount(shardId));
        }
        return ByteSequences.wrap(buffer.array());
    }

    private static SenkuShardIndex decode(final ByteSequence payload,
            final int shardCount) {
        final int expectedBytes = tableBytes(shardCount);
        if (payload.length() != expectedBytes) {
            throw new IndexException("Shard-index payload length "
                    + payload.length() + " does not equal expected length "
                    + expectedBytes + ".");
        }
        final ByteBuffer buffer = ByteBuffer.wrap(payload.toByteArray())
                .order(ByteOrder.BIG_ENDIAN);
        final long[] positions = new long[shardCount];
        final long[] counts = new long[shardCount];
        for (int expectedShardId = 0; expectedShardId < shardCount;
                expectedShardId++) {
            final int storedShardId = buffer.getInt();
            if (storedShardId != expectedShardId) {
                throw new IndexException("Shard-index record "
                        + expectedShardId + " contains shard ID "
                        + storedShardId + ".");
            }
            positions[expectedShardId] = buffer.getLong();
            counts[expectedShardId] = buffer.getLong();
        }
        try {
            return new SenkuShardIndex(positions, counts);
        } catch (IllegalArgumentException e) {
            throw new IndexException("Invalid shard-index record.", e);
        }
    }

    private static int tableBytes(final int shardCount) {
        try {
            return Math.multiplyExact(shardCount, RECORD_BYTES);
        } catch (ArithmeticException e) {
            throw new IndexException("Shard-index table is too large.", e);
        }
    }

    private static ChunkStoreFile chunkStore(final Directory directory,
            final DataBlockSize dataBlockSize) {
        return LargeFile.chunkStore(directory, dataBlockSize,
                SenkuFileNames.SHARD_INDEX_FILE);
    }

    private static void requireAbsent(final Directory directory,
            final String fileName) {
        if (directory.isFileExists(fileName)) {
            throw new IndexException(
                    "Shard-index file '" + fileName + "' already exists.");
        }
    }

    private static void closeAfterFailure(final ChunkStoreWriter writer,
            final Exception primary) {
        if (writer.wasClosed()) {
            return;
        }
        try {
            writer.close();
        } catch (Exception cleanupFailure) {
            primary.addSuppressed(cleanupFailure);
        }
    }
}
