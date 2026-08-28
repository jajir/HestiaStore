package org.hestiastore.index.chunkentryfile;

import org.hestiastore.index.Entry;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.datatype.TypeWriter;
import org.hestiastore.index.sorteddatafile.DiffKeyWriter;

/**
 * Encodes one sorted entry page into a reusable in-memory byte buffer.
 *
 * <p>
 * Exact built-in long descriptors use primitive key/value methods that retain
 * the existing differential-key wire format without intermediate boxing.
 * </p>
 */
public class SingleChunkEntryWriterImpl<K, V>
        implements SingleChunkEntryWriter<K, V> {

    private static final int LONG_BYTES = Long.BYTES;

    private final InMemoryFileWriter fileWriter;
    private final TypeWriter<V> valueWriter;
    private final DiffKeyWriter<K> diffKeyWriter;
    private final boolean primitiveLongKey;
    private final boolean primitiveLongValue;
    private final byte[] longBuffer = new byte[LONG_BYTES];
    private boolean hasPreviousLongKey;
    private long previousLongKey;
    private boolean closed = false;

    /**
     * Creates a new chunk writer.
     * 
     * @param keyTypeDescriptor   required key type descriptor
     * @param valueTypeDescriptor required value type descriptor
     */
    public SingleChunkEntryWriterImpl(final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor) {
        this(keyTypeDescriptor, valueTypeDescriptor, Integer.MAX_VALUE - 8);
    }

    /**
     * Creates a new chunk writer with an explicit encoded-page byte limit.
     * The limit bounds the retained growable buffer and causes encoding to fail
     * before allocating beyond it.
     *
     * @param keyTypeDescriptor required key type descriptor
     * @param valueTypeDescriptor required value type descriptor
     * @param maxEncodedBytes positive maximum encoded page size
     */
    public SingleChunkEntryWriterImpl(final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final int maxEncodedBytes) {
        Vldtn.requireNonNull(keyTypeDescriptor, "keyTypeDescriptor");
        Vldtn.requireNonNull(valueTypeDescriptor, "valueTypeDescriptor");
        fileWriter = new InMemoryFileWriter(maxEncodedBytes);
        this.valueWriter = valueTypeDescriptor.getTypeWriter();
        primitiveLongKey = keyTypeDescriptor
                .getClass() == TypeDescriptorLong.class;
        primitiveLongValue = valueTypeDescriptor
                .getClass() == TypeDescriptorLong.class;
        diffKeyWriter = primitiveLongKey ? null
                : new DiffKeyWriter<>(keyTypeDescriptor.getTypeEncoder(),
                        keyTypeDescriptor.getComparator());
    }

    @Override
    public void put(final Entry<K, V> entry) {
        final Entry<K, V> validatedEntry = Vldtn.requireNonNull(entry,
                "entry");
        put(validatedEntry.getKey(), validatedEntry.getValue());
    }

    /**
     * Writes one key/value pair directly without requiring a temporary
     * {@link Entry} wrapper.
     *
     * @param key   key to write
     * @param value value to write
     */
    public void put(final K key, final V value) {
        ensureOpen();
        if (primitiveLongKey) {
            writeLongKey(((Long) Vldtn.requireNonNull(key, "key")).longValue());
        } else {
            diffKeyWriter.writeTo(fileWriter, key);
        }
        valueWriter.write(fileWriter, value);
    }

    /**
     * Writes a primitive long key and a generic value without boxing the key.
     * This method is available only when the configured key descriptor is
     * exactly {@link TypeDescriptorLong}; subclasses may define different
     * ordering or encoding semantics and therefore use the generic path.
     *
     * @param key primitive long key to write
     * @param value value to write
     */
    public void putLongKey(final long key, final V value) {
        ensureOpen();
        requirePrimitiveLongKey();
        writeLongKey(key);
        valueWriter.write(fileWriter, value);
    }

    /**
     * Writes a primitive long key/value pair without allocating boxed values.
     * This method is available only when both configured descriptors are
     * exactly {@link TypeDescriptorLong}.
     *
     * @param key primitive long key to write
     * @param value primitive long value to write
     */
    public void putLongs(final long key, final long value) {
        ensureOpen();
        requirePrimitiveLongKey();
        if (!primitiveLongValue) {
            throw new IllegalStateException(
                    "Primitive long values require TypeDescriptorLong");
        }
        writeLongKey(key);
        writeLong(value, longBuffer);
        fileWriter.write(longBuffer, 0, LONG_BYTES);
    }

    @Override
    public ByteSequence closeSequence() {
        if (!closed) {
            closed = true;
        }
        return fileWriter.closeSequence();
    }

    private void writeLongKey(final long key) {
        if (hasPreviousLongKey) {
            final int compared = Long.compare(previousLongKey, key);
            if (compared == 0) {
                throw new IllegalArgumentException(
                        "Attempt to insert same key as previous long key");
            }
            if (compared > 0) {
                throw new IllegalArgumentException(
                        "Attempt to insert long key in invalid order");
            }
        }
        writeLong(key, longBuffer);
        final int sharedBytes = hasPreviousLongKey
                ? Long.numberOfLeadingZeros(previousLongKey ^ key) / Byte.SIZE
                : 0;
        final int diffBytes = LONG_BYTES - sharedBytes;
        fileWriter.write((byte) sharedBytes);
        fileWriter.write((byte) diffBytes);
        fileWriter.write(longBuffer, sharedBytes, diffBytes);
        previousLongKey = key;
        hasPreviousLongKey = true;
    }

    private void requirePrimitiveLongKey() {
        if (!primitiveLongKey) {
            throw new IllegalStateException(
                    "Primitive long keys require TypeDescriptorLong");
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Chunk writer already closed");
        }
    }

    private static void writeLong(final long value, final byte[] target) {
        target[0] = (byte) (value >>> 56);
        target[1] = (byte) (value >>> 48);
        target[2] = (byte) (value >>> 40);
        target[3] = (byte) (value >>> 32);
        target[4] = (byte) (value >>> 24);
        target[5] = (byte) (value >>> 16);
        target[6] = (byte) (value >>> 8);
        target[7] = (byte) value;
    }

}
