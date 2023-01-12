package com.coroptis.index.basic;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.coroptis.index.sorteddatafile.Pair;
import com.coroptis.index.sorteddatafile.SortedDataFileIterator;
import com.coroptis.index.sorteddatafile.SortedDataFileReader;
import com.coroptis.index.unsorteddatafile.ValueMerger;

public class MergeSpliterator<K, V> implements Spliterator<Pair<K, V>> {

    private final List<SortedDataFileIterator<K, V>> readers;

    private final Comparator<? super K> keyComparator;

    private final ValueMerger<K, V> merger;
    
    //FIXME make constructor protected
    public MergeSpliterator(final List<SortedDataFileReader<K, V>> readers, final Comparator<? super K> keyComparator,
	    final ValueMerger<K, V> merger) {
	this.readers = readers.stream().map(
		sortedDataFileReader -> new SortedDataFileIterator<K, V>(Objects.requireNonNull(sortedDataFileReader)))
		.collect(Collectors.toList());
	this.keyComparator = Objects.requireNonNull(keyComparator);
	this.merger = Objects.requireNonNull(merger);
    }

    @Override
    public int characteristics() {
	return Spliterator.SORTED | Spliterator.NONNULL | Spliterator.DISTINCT | Spliterator.IMMUTABLE;
    }

    @Override
    public long estimateSize() {
	/*
	 * Size is not known.
	 */
	return Long.MAX_VALUE;
    }

    @Override
    public boolean tryAdvance(final Consumer<? super Pair<K, V>> consumer) {
	final List<SortedDataFileIterator<K, V>> iteratorsWithBiggerKey = getIteratorsWithSmallerValue();
	if (iteratorsWithBiggerKey.isEmpty()) {
	    // there are no more items to read
	    return false;
	}

	Pair<K, V> out = null;
	for (final SortedDataFileIterator<K, V> reader : iteratorsWithBiggerKey) {
	    final Pair<K, V> readed = reader.readCurrent().get();
	    if (out == null) {
		out = readed;
	    } else {
		out = merger.merge(out, readed);
	    }
	    reader.next();
	}
	consumer.accept(out);
	return true;
    }

    /**
     * Minimal value is chosen because index order is ascending.
     * 
     * @return
     */
    private List<SortedDataFileIterator<K, V>> getIteratorsWithSmallerValue() {
	final Optional<K> maxValue = readers.stream().filter(reader -> reader.readCurrent().isPresent())
		.map(reader -> reader.readCurrent().get().getKey()).min(keyComparator);
	if (maxValue.isEmpty()) {
	    return Collections.emptyList();
	}
	return readers.stream().filter(reader -> reader.readCurrent().isPresent()).filter(reader -> {
	    final K key = reader.readCurrent().get().getKey();
	    return keyComparator.compare(key, maxValue.get()) == 0;
	}).collect(Collectors.toList());
    }

    @Override
    public Comparator<? super Pair<K, V>> getComparator() {
	return (pair1, pair2) -> keyComparator.compare(pair1.getKey(), pair2.getKey());
    }

    @Override
    public Spliterator<Pair<K, V>> trySplit() {
	return null;
    }
}
