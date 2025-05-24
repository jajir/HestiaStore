package com.hestiastore.index.log;

import java.util.Objects;

public class LoggedKey<K> {

    private final LogOperation logOperation;

    private final K key;

    public final static <M> LoggedKey<M> of(final LogOperation logOperation,
            final M key) {
        return new LoggedKey<M>(logOperation, key);
    }

    private LoggedKey(final LogOperation logOperation, final K key) {
        this.logOperation = Objects.requireNonNull(logOperation);
        this.key = Objects.requireNonNull(key);
    }

    public LogOperation getLogOperation() {
        return logOperation;
    }

    public K getKey() {
        return key;
    }

    @Override
    public String toString() {
        return String.format("LoggedKey[operation='%s',key='%s']",
                logOperation.name(), key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, logOperation);
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(final Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        final LoggedKey<K> other = (LoggedKey<K>) obj;
        return Objects.equals(key, other.key)
                && logOperation == other.logOperation;
    }

}