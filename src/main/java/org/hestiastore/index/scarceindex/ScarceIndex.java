package org.hestiastore.index.scarceindex;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.sorteddatafile.SortedDataFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scarce index contain map that contain just subset of keys from SST. It's a
 * map 'key,integer' records ordered by key value. It allows faster search for
 * exact key in SST.
 * 
 * Scarce index is written during process of creating main SST file. Later can't
 * be changed.
 * 
 * Scarce index structure:
 * <ul>
 * <li>first key - it's lover key in scarce index. Value is 0, because it's
 * first record in main index file.</li>
 * <li>remaining keys - value point to main index file where can be found value
 * of this key and higher key values.</li>
 * <li>last key - higher key in main index file</li>
 * </ul>
 * 
 * Index contains no key value pairs when main index is empty. Index contains
 * one key value pair when main index contains just one key value pair. In all
 * other cases scarce index contain more than one key value pairs.
 * 
 */
public class ScarceIndex<K> {

    private static final TypeDescriptorInteger typeDescriptorInteger = new TypeDescriptorInteger();

    private static final Logger LOGGER = LoggerFactory
            .getLogger(ScarceIndex.class);

    private final Comparator<K> keyComparator;

    private final Directory directory;

    private final String fileName;

    private final SortedDataFile<K, Integer> sortedDataFile;

    private final ScarceIndexValidator<K> validator;
    private ScarceIndexSnapshot<K> snapshot;

    public static <M> ScarceIndexBuilder<M> builder() {
        return new ScarceIndexBuilder<M>();
    }

    ScarceIndex(final Directory directory, final String fileName,
            final TypeDescriptor<K> keyTypeDescriptor,
            final int diskIoBufferSize) {
        this.directory = Vldtn.requireNonNull(directory, "directory");
        this.fileName = Vldtn.requireNonNull(fileName, "fileName");
        Vldtn.requireNonNull(keyTypeDescriptor, "keyTypeDescriptor");
        this.keyComparator = Vldtn.requireNonNull(
                keyTypeDescriptor.getComparator(),
                "keyTypeDescriptor.getComparator()");
        this.sortedDataFile = SortedDataFile.<K, Integer>builder() //
                .withDirectory(directory) //
                .withFileName(fileName)//
                .withKeyTypeDescriptor(keyTypeDescriptor) //
                .withValueTypeDescriptor(typeDescriptorInteger)
                .withDiskIoBufferSize(diskIoBufferSize) //
                .build();
        this.validator = new ScarceIndexValidator<>(keyComparator);
        this.snapshot = new ScarceIndexSnapshot<>(keyComparator, List.of());
        loadCache();
    }

    private final List<Pair<K, Integer>> loadCacheEntries() {
        final List<Pair<K, Integer>> entries = new ArrayList<>();
        if (directory.isFileExists(fileName)) {
            try (PairIterator<K, Integer> pairIterator = sortedDataFile
                    .openIterator()) {
                while (pairIterator.hasNext()) {
                    final Pair<K, Integer> pair = pairIterator.next();
                    entries.add(pair);
                }
            }
        }
        return entries;
    }

    public void loadCache() {
        final ScarceIndexSnapshot<K> newSnapshot = new ScarceIndexSnapshot<>(
                keyComparator, loadCacheEntries());
        final boolean valid = validator.validate(newSnapshot,
                message -> LOGGER.error(message));
        if (!valid) {
            throw new IllegalStateException(
                    "Unable to load scarce index, sanity check failed.");
        }
        this.snapshot = newSnapshot;
    }

    public K getMaxKey() {
        return snapshot.getMaxKey();
    }

    public K getMinKey() {
        return snapshot.getMinKey();
    }

    public int getKeyCount() {
        return snapshot.getKeyCount();
    }

    public Integer get(final K key) {
        return snapshot.findSegmentId(key);
    }

    public ScarceIndexWriterTx<K> openWriterTx() {
        return new ScarceIndexWriterTx<>(this, sortedDataFile.openWriterTx());
    }

}
