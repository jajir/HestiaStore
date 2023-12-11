package com.coroptis.index.simpledatafile;

import java.util.Comparator;
import java.util.Objects;

import com.coroptis.index.PairReader;
import com.coroptis.index.PairWriter;
import com.coroptis.index.ValueMerger;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.directory.Directory;

/**
 * Thread safe Sorted String Table implementation.
 * 
 * @author honza
 *
 */
public class SortedStringTable<K, V> {

    private final SimpleDataFile<K, V> sdf;
    private final ReadersManager<K, V> readersManager = new ReadersManager<>();
    private final Comparator<K> keyComparator;

    public static <M, N> SortedStringTable<M, N> make(final Directory directory,
            final String fileName, final TypeDescriptor<M> keyTypeDescriptor,
            final TypeDescriptor<N> valueTypeDescriptor,
            final ValueMerger<M, N> valueMerger) {
        final SimpleDataFile<M, N> sdf = new SimpleDataFile<>(directory,
                fileName, keyTypeDescriptor, valueTypeDescriptor, valueMerger);
        final SortedStringTable<M, N> sst = new SortedStringTable<>(sdf,
                keyTypeDescriptor.getComparator());
        return sst;
    }

    SortedStringTable(final SimpleDataFile<K, V> sdf,
            final Comparator<K> keyComparator) {
        this.sdf = Objects.requireNonNull(sdf);
        this.keyComparator = Objects.requireNonNull(keyComparator);
    }

    public Stats getStats() {
        return sdf.getStats();
    }

    public void compact() {
        readersManager.makeDirty();
        sdf.compact();
    }

    public PairReader<K, V> openReader() {
        final SstPairReader<K, V> out = new SstPairReader<>(sdf, keyComparator);
        readersManager.register(out);
        return sdf.openReader();
    }

    public K split(final String smallerDataFileName) {
        readersManager.makeDirty();
        return sdf.split(smallerDataFileName);
    }

    public PairWriter<K, V> openCacheWriter() {
        readersManager.makeDirty();
        return sdf.openCacheWriter();
    }

}
