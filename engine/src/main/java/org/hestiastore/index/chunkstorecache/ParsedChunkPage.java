package org.hestiastore.index.chunkstorecache;

import java.util.Comparator;
import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.Vldtn;

/**
 * Immutable parsed representation of one persisted chunk page.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class ParsedChunkPage<K, V> {

    private static final ParsedChunkPage<?, ?> EMPTY =
            new ParsedChunkPage<>(List.of());

    private final List<Entry<K, V>> entries;

    private ParsedChunkPage(final List<Entry<K, V>> entries) {
        this.entries = List.copyOf(entries);
    }

    /**
     * Creates a parsed page from sorted entries.
     *
     * @param entries sorted key/value entries
     * @param <K> key type
     * @param <V> value type
     * @return parsed page
     */
    public static <K, V> ParsedChunkPage<K, V> of(
            final List<Entry<K, V>> entries) {
        final List<Entry<K, V>> validatedEntries = Vldtn.requireNonNull(
                entries, "entries");
        validatedEntries.forEach(ParsedChunkPage::validateEntry);
        if (validatedEntries.isEmpty()) {
            return empty();
        }
        return new ParsedChunkPage<>(validatedEntries);
    }

    /**
     * Returns an empty parsed page.
     *
     * @param <K> key type
     * @param <V> value type
     * @return empty page
     */
    @SuppressWarnings("unchecked")
    public static <K, V> ParsedChunkPage<K, V> empty() {
        return (ParsedChunkPage<K, V>) EMPTY;
    }

    /**
     * Finds a key in the parsed page using binary search.
     *
     * @param key lookup key
     * @param comparator key comparator
     * @return value when found, otherwise {@code null}
     */
    public V find(final K key, final Comparator<K> comparator) {
        final K resolvedKey = Vldtn.requireNonNull(key, "key");
        final Comparator<K> resolvedComparator = Vldtn.requireNonNull(
                comparator, "comparator");
        if (entries.isEmpty()) {
            return null;
        }
        int low = 0;
        int high = entries.size() - 1;
        while (low <= high) {
            final int mid = low + ((high - low) / 2);
            final Entry<K, V> entry = entries.get(mid);
            final int cmp = resolvedComparator.compare(entry.getKey(),
                    resolvedKey);
            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                return entry.getValue();
            }
        }
        return null;
    }

    public int size() {
        return entries.size();
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    public List<Entry<K, V>> entries() {
        return entries;
    }

    private static <K, V> void validateEntry(final Entry<K, V> entry) {
        Vldtn.requireNonNull(entry, "entry");
        Vldtn.requireNonNull(entry.getKey(), "entry.key");
    }
}
