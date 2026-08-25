package org.hestiastore.index.senku.internal;

import java.util.NoSuchElementException;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.chunkentryfile.SingleChunkEntryIterator;
import org.hestiastore.index.datatype.TypeDescriptor;

/**
 * Exact-record-count entry cursor over large-file entry pages.
 */
final class SenkuSourceEntryIterator<K, V> extends AbstractCloseableResource
        implements EntryIterator<K, V> {

    private final LargeFileReader pages;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final boolean requireSourceEof;

    private long remaining;
    private SingleChunkEntryIterator<K, V> currentPage;
    private Entry<K, V> next;

    /**
     * Opens one exact persisted entry range.
     *
     * @param pages source large-file pages
     * @param keyTypeDescriptor key codec
     * @param valueTypeDescriptor value codec
     * @param recordCount exact number of entries in the range
     * @param requireSourceEof whether the range must end with the large file
     */
    SenkuSourceEntryIterator(final LargeFileReader pages,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final long recordCount, final boolean requireSourceEof) {
        this.pages = Vldtn.requireNonNull(pages, "pages");
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        this.remaining = Vldtn.requireGreaterThanOrEqualToZero(recordCount,
                "recordCount");
        this.requireSourceEof = requireSourceEof;
        loadNext();
    }

    @Override
    public boolean hasNext() {
        return !wasClosed() && next != null;
    }

    @Override
    public Entry<K, V> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        final Entry<K, V> current = next;
        loadNext();
        return current;
    }

    @Override
    protected void doClose() {
        closeCurrentPage();
        pages.close();
        next = null;
    }

    private void loadNext() {
        try {
            if (remaining == 0L) {
                validateBoundaryAndClose();
                next = null;
                return;
            }
            while (currentPage == null || !currentPage.hasNext()) {
                closeCurrentPage();
                final ByteSequence payload = pages.read();
                if (payload == null) {
                    throw new IndexException(
                            "Source ended before its declared recordCount.");
                }
                currentPage = new SingleChunkEntryIterator<>(payload,
                        keyTypeDescriptor, valueTypeDescriptor);
            }
            next = Vldtn.requireNonNull(currentPage.next(), "entry");
            remaining--;
        } catch (Exception e) {
            closeAfterFailure(e);
            if (e instanceof IndexException) {
                throw (IndexException) e;
            }
            throw new IndexException("Unable to read sorted source entry.", e);
        }
    }

    private void validateBoundaryAndClose() {
        if (currentPage != null && currentPage.hasNext()) {
            throw new IndexException(
                    "Source page contains entries beyond recordCount.");
        }
        closeCurrentPage();
        if (requireSourceEof && pages.read() != null) {
            throw new IndexException(
                    "Sorted run contains entries beyond recordCount.");
        }
        pages.close();
    }

    private void closeCurrentPage() {
        final SingleChunkEntryIterator<K, V> page = currentPage;
        currentPage = null;
        if (page != null && !page.wasClosed()) {
            page.close();
        }
    }

    private void closeAfterFailure(final Exception primary) {
        try {
            closeCurrentPage();
        } catch (Exception cleanupFailure) {
            primary.addSuppressed(cleanupFailure);
        }
        try {
            pages.close();
        } catch (Exception cleanupFailure) {
            primary.addSuppressed(cleanupFailure);
        }
        next = null;
    }
}
