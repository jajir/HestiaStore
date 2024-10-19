package com.coroptis.index.simpledatafile;

import java.util.Comparator;
import java.util.Objects;
import java.util.function.Consumer;

import com.coroptis.index.Pair;
import com.coroptis.index.CloseablePairReader;

/**
 * Pair reader with dirty flag. When some other process set dirty flag to true
 * than reading process will be reset and starts from beginning.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class SstPairReader<K, V> implements CloseablePairReader<K, V> {

    private final SimpleDataFile<K, V> sdf;
    private final Comparator<K> keyComparator;
    private CloseablePairReader<K, V> currentReader;
    private boolean isDirty = false;
    private Pair<K, V> lastReadedPair;
    private Consumer<SstPairReader<K, V>> onCloseConsumer;

    SstPairReader(final SimpleDataFile<K, V> sdf,
            final Comparator<K> keyComparator) {
        this.sdf = Objects.requireNonNull(sdf);
        this.keyComparator = Objects.requireNonNull(keyComparator);
        currentReader = sdf.openReader();
    }

    @Override
    public Pair<K, V> read() {
        if (currentReader == null) {
            // It's closed reader
            return null;
        }
        if (isDirty) {
            close();
            currentReader = sdf.openReader();
            isDirty = false;
            if (lastReadedPair != null) {
                /*
                 * Pairs will be read until find key smaller that
                 * lastReadedPair. It provide good chance to consistently
                 * continue in reading.
                 */
                Pair<K, V> current = currentReader.read();
                int cmp = keyComparator.compare(lastReadedPair.getKey(),
                        current.getKey());
                while (cmp >= 0 && current != null) {
                    current = currentReader.read();
                    if (current != null) {
                        cmp = keyComparator.compare(lastReadedPair.getKey(),
                                current.getKey());
                    }
                }
                lastReadedPair = current;
                return lastReadedPair;
            } else {
                lastReadedPair = currentReader.read();
                return lastReadedPair;
            }
        } else {
            lastReadedPair = currentReader.read();
            return lastReadedPair;
        }
    }

    public void setOnCloseConsumer(
            final Consumer<SstPairReader<K, V>> onCloseConsumer) {
        this.onCloseConsumer = Objects.requireNonNull(onCloseConsumer);
    }

    @Override
    public void close() {
        currentReader.close();
        currentReader = null;
        if (onCloseConsumer != null) {
            onCloseConsumer.accept(this);
            onCloseConsumer = null;
        }
    }

    public void makeDirty() {
        this.isDirty = true;
    }

    public boolean isDirty() {
        return this.isDirty;
    }

}
