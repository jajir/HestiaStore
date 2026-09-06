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
 * Exact built-in long descriptors use primitive key/value methods without
 * intermediate boxing. Existing constructors select byte-prefix keys; an
 * explicit codec can select numeric delta-varints instead.
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
    private final LongKeyPageWriter longKeyWriter;
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
     * Creates a new chunk writer with an explicit encoded-page byte limit. The
     * limit bounds the retained growable buffer and causes encoding to fail
     * before allocating beyond it.
     *
     * @param keyTypeDescriptor   required key type descriptor
     * @param valueTypeDescriptor required value type descriptor
     * @param maxEncodedBytes     positive maximum encoded page size
     */
    public SingleChunkEntryWriterImpl(final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final int maxEncodedBytes) {
        this(keyTypeDescriptor, valueTypeDescriptor, maxEncodedBytes,
                KeyPageCodecs.prefix());
    }

    /**
     * Creates a bounded page writer using the selected sorted-key encoding.
     *
     * @param keyTypeDescriptor   logical keys
     * @param valueTypeDescriptor value encoding
     * @param maxEncodedBytes     maximum retained page bytes
     * @param keyPageCodec        matched key writer/reader selection
     */
    public SingleChunkEntryWriterImpl(final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final int maxEncodedBytes, final KeyPageCodec<K> keyPageCodec) {
        Vldtn.requireNonNull(keyTypeDescriptor, "keyTypeDescriptor");
        Vldtn.requireNonNull(valueTypeDescriptor, "valueTypeDescriptor");
        Vldtn.requireNonNull(keyPageCodec, "keyPageCodec")
                .validate(keyTypeDescriptor);
        fileWriter = new InMemoryFileWriter(maxEncodedBytes);
        this.valueWriter = valueTypeDescriptor.getTypeWriter();
        primitiveLongKey = keyTypeDescriptor
                .getClass() == TypeDescriptorLong.class;
        primitiveLongValue = valueTypeDescriptor
                .getClass() == TypeDescriptorLong.class;
        longKeyWriter = primitiveLongKey ? new LongKeyPageWriter(keyPageCodec)
                : null;
        diffKeyWriter = primitiveLongKey ? null
                : new DiffKeyWriter<>(keyTypeDescriptor.getTypeEncoder(),
                        keyTypeDescriptor.getComparator());
    }

    @Override
    public void put(final Entry<K, V> entry) {
        final Entry<K, V> validatedEntry = Vldtn.requireNonNull(entry, "entry");
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
     * @param key   primitive long key to write
     * @param value value to write
     */
    public void putLongKey(final long key, final V value) {
        ensureOpen();
        requirePrimitiveLongKey();
        writeLongKey(key);
        valueWriter.write(fileWriter, value);
    }

    /**
     * Writes an already encoded numeric key or fixed-weight rank and a generic
     * value. The complete source codec must match this page's codec. Framing,
     * rank bounds and strict ordering are checked as for logical-key writes.
     * Existing logical-key methods are unaffected.
     *
     * @param encodedKey  encoded primitive key, not a logical board key
     * @param value       value to write
     * @param sourceCodec immutable domain that produced the encoded key
     */
    public void putEncodedLongKey(final long encodedKey, final V value,
            final KeyPageCodec<?> sourceCodec) {
        ensureOpen();
        requirePrimitiveLongKey();
        longKeyWriter.writeEncoded(fileWriter, encodedKey, sourceCodec);
        valueWriter.write(fileWriter, value);
    }

    /**
     * Writes a primitive long key/value pair without allocating boxed values.
     * This method is available only when both configured descriptors are
     * exactly {@link TypeDescriptorLong}.
     *
     * @param key   primitive long key to write
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

    /**
     * Writes an already encoded numeric key or fixed-weight rank and a
     * primitive long value without boxing. Both descriptors must be exact
     * built-in longs and the complete source codec must match this page.
     *
     * @param encodedKey  encoded primitive key, not a logical board key
     * @param value       primitive long value
     * @param sourceCodec immutable domain that produced the encoded key
     */
    public void putEncodedLongs(final long encodedKey, final long value,
            final KeyPageCodec<?> sourceCodec) {
        ensureOpen();
        requirePrimitiveLongKey();
        if (!primitiveLongValue) {
            throw new IllegalStateException(
                    "Primitive long values require TypeDescriptorLong");
        }
        longKeyWriter.writeEncoded(fileWriter, encodedKey, sourceCodec);
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
        longKeyWriter.write(fileWriter, key);
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
