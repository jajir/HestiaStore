package org.hestiastore.index.segmentindex.wal;

final class WalDecodedRecord<K, V> {

    private final long lsn;
    private final WalRuntime.Operation operation;
    private final K key;
    private final V value;

    WalDecodedRecord(final long lsn, final WalRuntime.Operation operation,
            final K key, final V value) {
        this.lsn = lsn;
        this.operation = operation;
        this.key = key;
        this.value = value;
    }

    long lsn() {
        return lsn;
    }

    WalRuntime.Operation operation() {
        return operation;
    }

    K key() {
        return key;
    }

    V value() {
        return value;
    }
}
