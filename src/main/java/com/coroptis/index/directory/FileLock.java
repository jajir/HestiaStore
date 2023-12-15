package com.coroptis.index.directory;

public interface FileLock {

    boolean isLocked();

    void lock();

    void unlock();

}
