package com.coroptis.index.log;

import java.util.Objects;

public class LoggedKey<K> {

    private final LogOperation logOperation;

    private final K key;

    public final static <M> LoggedKey<M> of(final LogOperation logOperation, final M key) {
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
        return String.format("LoggedKey[operation='%s',key='%s']", logOperation.name(), key);
    }

    @Override
    public int hashCode() {
        // TODO Auto-generated method stub
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        
        // TODO Auto-generated method stub
        return super.equals(obj);
    }

}