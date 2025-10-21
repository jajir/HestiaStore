package org.hestiastore.index.chunkpairfile;

import org.hestiastore.index.Pair;
import org.hestiastore.index.PairWriter;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.ChunkPayload;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.sorteddatafile.SortedDataFile;
import org.hestiastore.index.sorteddatafile.SortedDataFileWriterTx;

/**
 * Simplest Chunk writer that writes all data into memmory and than create from
 * it Chunk payload.
 */
public class SingleChunkPairWriterImpl<K, V>
        implements SingleChunkPairWriter<K, V> {

    private static final String CHUNK_FILE_NAME = "chunk";

    private final MemDirectory directory = new MemDirectory();

    private final PairWriter<K, V> writer;
    private final SortedDataFileWriterTx<K, V> txWriter;

    /**
     * Creates a new chunk writer.
     * 
     * @param keyTypeDescriptor   required key type descriptor
     * @param valueTypeDescriptor required value type descriptor
     */
    SingleChunkPairWriterImpl(final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor) {
        Vldtn.requireNonNull(keyTypeDescriptor, "keyTypeDescriptor");
        Vldtn.requireNonNull(valueTypeDescriptor, "valueTypeDescriptor");
        SortedDataFile<K, V> sortedDataFile = SortedDataFile.<K, V>builder() //
                .withDirectory(directory) //
                .withFileName(CHUNK_FILE_NAME)//
                .withKeyTypeDescriptor(keyTypeDescriptor) //
                .withValueTypeDescriptor(valueTypeDescriptor) //
                .withDiskIoBufferSize(1024)//
                .build();
        this.txWriter = sortedDataFile.openWriterTx();
        this.writer = txWriter.open();
    }

    @Override
    public void put(final Pair<K, V> pair) {
        Vldtn.requireNonNull(pair, "pair");
        writer.write(pair);
    }

    @Override
    public ChunkPayload close() {
        writer.close();
        txWriter.commit();
        return ChunkPayload.of(directory.getFileBytes(CHUNK_FILE_NAME));
    }

}
