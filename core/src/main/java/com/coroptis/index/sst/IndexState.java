package com.coroptis.index.sst;

public interface IndexState<K, V> {

    void onReady(SstIndexImpl<K, V> index);

    void onClose(SstIndexImpl<K, V> index);

    /**
     * Method check that index is ready for search and manipulation operation.
     * 
     * @throws IllegalStateException when index manipulation is not allowed
     */
    void tryPerformOperation();

}
