package com.hestiastore.index.scarceindex;

import java.util.Objects;

import com.hestiastore.index.Pair;
import com.hestiastore.index.PairIterator;
import com.hestiastore.index.datatype.TypeDescriptor;
import com.hestiastore.index.datatype.TypeDescriptorInteger;
import com.hestiastore.index.directory.Directory;
import com.hestiastore.index.sorteddatafile.SortedDataFile;

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

    private final static TypeDescriptorInteger typeDescriptorInteger = new TypeDescriptorInteger();

    private final TypeDescriptor<K> keyTypeDescriptor;

    private final Directory directory;

    private final String fileName;

    private final SortedDataFile<K, Integer> cacheDataFile;

    private ScarceIndexCache<K> cache;

    public static <M> ScarceIndexBuilder<M> builder() {
        return new ScarceIndexBuilder<M>();
    }

    ScarceIndex(final Directory directory, final String fileName,
            final TypeDescriptor<K> keyTypeDescriptor,
            final int diskIoBufferSize) {
        this.directory = Objects.requireNonNull(directory,
                "Directory object is null.");
        this.fileName = Objects.requireNonNull(fileName,
                "File name object is null.");
        this.keyTypeDescriptor = Objects.requireNonNull(keyTypeDescriptor,
                "Key type descriptor object is null.");
        this.cacheDataFile = SortedDataFile.<K, Integer>builder() //
                .withDirectory(directory) //
                .withFileName(fileName)//
                .withKeyTypeDescriptor(keyTypeDescriptor) //
                .withValueTypeDescriptor(typeDescriptorInteger)
                .withDiskIoBufferSize(diskIoBufferSize) //
                .build();
        this.cache = new ScarceIndexCache<>(keyTypeDescriptor);
        loadCache();
    }

    public void loadCache() {
        ScarceIndexCache<K> tmp = new ScarceIndexCache<>(keyTypeDescriptor);
        if (directory.isFileExists(fileName)) {
            try (PairIterator<K, Integer> pairIterator = cacheDataFile
                    .openIterator()) {
                while (pairIterator.hasNext()) {
                    final Pair<K, Integer> pair = pairIterator.next();
                    tmp.put(pair);
                }
            }
        }
        tmp.sanityCheck();
        this.cache = tmp;
    }

    public K getMaxKey() {
        return cache.getMaxKey();
    }

    public K getMinKey() {
        return cache.getMinKey();
    }

    public int getKeyCount() {
        return cache.getKeyCount();
    }

    public Integer get(final K key) {
        return cache.findSegmentId(key);
    }

    public ScarceIndexWriter<K> openWriter() {
        return new ScarceIndexWriter<>(this, cacheDataFile.openWriter());
    }

}
