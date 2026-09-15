package org.hestiastore.index.senku.internal;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.chunkentryfile.KeyPageCodec;
import org.hestiastore.index.chunkentryfile.LongKeyPageReader;
import org.hestiastore.index.directory.MemFileReader;

/**
 * Allocation-free primitive-long-key cursor over one exact Senku source range.
 *
 * <p>
 * The cursor uses the same page-local long decoder as generic reads, selected
 * from persisted page metadata. It is intentionally limited to the built-in
 * long keys with either built-in long values or zero-byte null values; generic
 * codecs continue to use {@link SenkuSourceEntryIterator}.
 * </p>
 */
final class SenkuLongSourceCursor implements AutoCloseable {

    private static final int LONG_BYTES = Long.BYTES;

    private final LargeFileReader pages;
    private final boolean requireSourceEof;
    private final boolean primitiveLongValue;
    private final int ordinal;
    private MemFileReader pageReader;
    private LongKeyPageReader keyReader;
    private KeyPageCodec<?> currentCodec;

    private long remaining;
    private ByteSequence currentPage;
    private int pageOffset;
    private boolean hasCurrent;
    private long currentKey;
    private long currentValue;
    private boolean closed;

    /**
     * Opens one exact primitive-long source range and loads its first value.
     *
     * @param pages              source page reader owned by this cursor
     * @param recordCount        exact number of records in the range
     * @param requireSourceEof   whether the range must end with the large file
     * @param primitiveLongValue whether each value is an encoded long; false
     *                           selects the zero-byte null-value format
     * @param ordinal            stable source ordinal used for duplicate
     *                           ordering
     */
    SenkuLongSourceCursor(final LargeFileReader pages, final long recordCount,
            final boolean requireSourceEof, final boolean primitiveLongValue,
            final int ordinal) {
        this.pages = Vldtn.requireNonNull(pages, "pages");
        remaining = Vldtn.requireGreaterThanOrEqualToZero(recordCount,
                "recordCount");
        this.requireSourceEof = requireSourceEof;
        this.primitiveLongValue = primitiveLongValue;
        this.ordinal = Vldtn.requireGreaterThanOrEqualToZero(ordinal,
                "ordinal");
        advance();
    }

    /**
     * Returns whether a decoded record is available.
     *
     * @return true when key and value accessors are valid
     */
    boolean hasCurrent() {
        return hasCurrent;
    }

    /**
     * Returns the current logical primitive key, decoding a stored rank only at
     * this explicit logical-key boundary.
     *
     * @return current key
     */
    long key() {
        requireCurrent();
        return currentCodec.decodeLongKey(currentKey);
    }

    /**
     * Returns the validated encoded numeric key or rank for maintenance. The
     * caller must establish complete codec compatibility before comparing or
     * transferring encoded keys between sources.
     *
     * @return current encoded key
     */
    long encodedKey() {
        requireCurrent();
        return currentKey;
    }

    /** Returns the full immutable codec of the current encoded key. */
    KeyPageCodec<?> keyCodec() {
        requireCurrent();
        return currentCodec;
    }

    /**
     * Returns the current primitive long value.
     *
     * @return current long value
     */
    long value() {
        requireCurrent();
        if (!primitiveLongValue) {
            throw new IllegalStateException(
                    "Primitive long value is not configured.");
        }
        return currentValue;
    }

    /**
     * Returns the stable input ordinal.
     *
     * @return source ordinal
     */
    int ordinal() {
        return ordinal;
    }

    /**
     * Advances to the next exact source record and closes on exhaustion.
     * Validates logical key order across pages, whose encoders independently
     * reset their delta or prefix state. Rank decoding is needed only at this
     * boundary; the maintenance loop otherwise retains encoded keys. Equal
     * boundary keys remain valid inputs for duplicate reduction.
     */
    void advance() {
        ensureOpen();
        try {
            if (remaining == 0L) {
                validateBoundaryAndClose();
                hasCurrent = false;
                return;
            }
            final boolean pageBoundary = currentPage != null
                    && pageOffset == currentPage.length();
            final long previousKey = pageBoundary ? key() : 0L;
            ensurePage();
            decodeCurrent();
            if (pageBoundary
                    && currentCodec.decodeLongKey(currentKey) < previousKey) {
                throw new IndexException(
                        "Source keys must not descend across pages.");
            }
            remaining--;
            hasCurrent = true;
        } catch (Exception e) {
            closeAfterFailure(e);
            if (e instanceof IndexException) {
                throw (IndexException) e;
            }
            throw new IndexException("Unable to read primitive-long source.",
                    e);
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        hasCurrent = false;
        currentPage = null;
        pageReader = null;
        keyReader = null;
        pages.close();
    }

    private void ensurePage() {
        while (currentPage == null || pageOffset == currentPage.length()) {
            currentPage = pages.read();
            pageOffset = 0;
            if (currentPage != null) {
                pageReader = new MemFileReader(currentPage);
                currentCodec = pages.keyCodec();
                keyReader = new LongKeyPageReader(currentCodec);
            }
            if (currentPage == null) {
                throw new IndexException(
                        "Source ended before its declared recordCount.");
            }
            if (!currentPage.isEmpty()) {
                return;
            }
        }
    }

    private void decodeCurrent() {
        currentKey = keyReader.readEncodedLong(pageReader);
        pageOffset = pageReader.getPosition();
        requireRemaining(primitiveLongValue ? LONG_BYTES : 0, "long value");
        if (primitiveLongValue) {
            currentValue = readLong(currentPage, pageOffset);
            pageOffset += LONG_BYTES;
            pageReader.skip(LONG_BYTES);
        }
    }

    private void validateBoundaryAndClose() {
        if (currentPage != null && pageOffset != currentPage.length()) {
            throw new IndexException(
                    "Source page contains entries beyond recordCount.");
        }
        currentPage = null;
        if (requireSourceEof && pages.read() != null) {
            throw new IndexException(
                    "Sorted run contains entries beyond recordCount.");
        }
        close();
    }

    private void requireRemaining(final int byteCount,
            final String description) {
        if (currentPage.length() - pageOffset < byteCount) {
            throw new IndexException("Truncated " + description + ".");
        }
    }

    private void requireCurrent() {
        if (!hasCurrent) {
            throw new IllegalStateException("Primitive-long cursor is empty.");
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IndexException("Primitive-long source is closed.");
        }
    }

    private void closeAfterFailure(final Exception primary) {
        hasCurrent = false;
        if (closed) {
            return;
        }
        closed = true;
        try {
            pages.close();
        } catch (Exception cleanupFailure) {
            primary.addSuppressed(cleanupFailure);
        }
    }

    private static long readLong(final ByteSequence bytes, final int offset) {
        return ((long) bytes.getByte(offset) & 0xFFL) << 56
                | ((long) bytes.getByte(offset + 1) & 0xFFL) << 48
                | ((long) bytes.getByte(offset + 2) & 0xFFL) << 40
                | ((long) bytes.getByte(offset + 3) & 0xFFL) << 32
                | ((long) bytes.getByte(offset + 4) & 0xFFL) << 24
                | ((long) bytes.getByte(offset + 5) & 0xFFL) << 16
                | ((long) bytes.getByte(offset + 6) & 0xFFL) << 8
                | ((long) bytes.getByte(offset + 7) & 0xFFL);
    }
}
