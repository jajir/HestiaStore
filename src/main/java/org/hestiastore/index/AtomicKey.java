package org.hestiastore.index;

/**
 * KeyAtomic is a simple atomic wrapper for a key of type K. It provides
 * thread-safe access to the key.
 * 
 *
 * @param <K> the type of the key
 */
public class AtomicKey<K> {

    private K key;

    public AtomicKey() {
        this.key = null;
    }

    public AtomicKey(final K key) {
        this.key = Vldtn.requireNonNull(key, "key");
    }

    public K get() {
        return key;
    }

    public void set(final K newKey) {
        key = Vldtn.requireNonNull(newKey, "key");
    }

    public boolean isEmpty() {
        return key == null;
    }

}
