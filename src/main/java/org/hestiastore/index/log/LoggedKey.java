package org.hestiastore.index.log;

import java.util.Objects;

import org.hestiastore.index.Vldtn;

public class LoggedKey<K> {

    private final LogOperation logOperation;

    private final K key;

    public static final <M> LoggedKey<M> of(final LogOperation logOperation,
            final M key) {
        return new LoggedKey<M>(logOperation, key);
    }

    private LoggedKey(final LogOperation logOperation, final K key) {
        this.logOperation = Vldtn.requireNonNull(logOperation, "logOperation");
        this.key = Vldtn.requireNonNull(key, "key");
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