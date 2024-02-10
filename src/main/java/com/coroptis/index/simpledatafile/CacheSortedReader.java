package com.coroptis.index.simpledatafile;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coroptis.index.Pair;
import com.coroptis.index.PairReader;
import com.coroptis.index.ValueMerger;
import com.coroptis.index.cache.UniqueCache;
import com.coroptis.index.sstfile.PairComparator;
import com.coroptis.index.unsorteddatafile.UnsortedDataFile;
import com.coroptis.index.unsorteddatafile.UnsortedDataFileStreamer;

public class CacheSortedReader<K, V> implements PairReader<K, V> {

    private final Logger logger = LoggerFactory
            .getLogger(CacheSortedReader.class);

    private final List<Pair<K, V>> sortedPairs;

    private int indexToReturn = 0;

    CacheSortedReader(final ValueMerger<K, V> merger,
            final UnsortedDataFile<K, V> unsortedDataFile,
            final Comparator<K> keyComparator) {
        logger.debug("Initilizing of sorter cache reader started.");
        final UniqueCache<K, V> cache = new UniqueCache<>(keyComparator);
        try (UnsortedDataFileStreamer<K, V> streamer = unsortedDataFile
                .openStreamer()) {
            streamer.stream().forEach(cache::put);
        }
        final PairComparator<K, V> pairComparator = new PairComparator<>(
                Objects.requireNonNull(keyComparator));
        sortedPairs = cache.getStream().sorted(pairComparator)
                .collect(Collectors.toList());
        cache.clear();
        logger.debug("Initilization is done with '{}' key value pairs.",
                sortedPairs.size());
    }

    @Override
    public Pair<K, V> read() {
        if (indexToReturn < sortedPairs.size()) {
            final Pair<K, V> out = sortedPairs.get(indexToReturn);
            indexToReturn++;
            return out;
        } else {
            return null;
        }
    }

    @Override
    public void close() {
        sortedPairs.clear();
    }

}
