package org.hestiastore.index.segmentindex.wal;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Array-backed test queue that keeps the append worker parked until producers
 * have reached a deterministic admission state.
 *
 * @param <K> WAL key type
 * @param <V> WAL value type
 */
final class GatedWalAppendQueue<K, V>
        extends ArrayBlockingQueue<WalAppendTask<K, V>> {

    private static final long serialVersionUID = 1L;

    private final CountDownLatch offerAttempts;
    private final CountDownLatch acceptedOffers;
    private final CountDownLatch workerGate = new CountDownLatch(1);

    /**
     * Creates a queue with deterministic producer and worker gates.
     *
     * @param capacity queue capacity
     * @param expectedOffers number of producer offers expected by the test
     */
    GatedWalAppendQueue(final int capacity, final int expectedOffers) {
        super(capacity);
        offerAttempts = new CountDownLatch(expectedOffers);
        acceptedOffers = new CountDownLatch(expectedOffers);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean offer(final WalAppendTask<K, V> task,
            final long timeout, final TimeUnit unit) throws InterruptedException {
        offerAttempts.countDown();
        final boolean accepted = super.offer(task, timeout, unit);
        if (accepted) {
            acceptedOffers.countDown();
        }
        return accepted;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public WalAppendTask<K, V> take() throws InterruptedException {
        workerGate.await();
        return super.take();
    }

    /**
     * Waits until the expected timed offers have entered queue admission.
     *
     * @param timeout maximum wait
     * @param unit timeout unit
     * @return true when every expected offer was attempted
     * @throws InterruptedException when the test thread is interrupted
     */
    boolean awaitOfferAttempts(final long timeout, final TimeUnit unit)
            throws InterruptedException {
        return offerAttempts.await(timeout, unit);
    }

    /**
     * Waits until the expected offers have been accepted by the queue.
     *
     * @param timeout maximum wait
     * @param unit timeout unit
     * @return true when every expected offer was accepted
     * @throws InterruptedException when the test thread is interrupted
     */
    boolean awaitAccepted(final long timeout, final TimeUnit unit)
            throws InterruptedException {
        return acceptedOffers.await(timeout, unit);
    }

    /**
     * Allows the append worker to start consuming queued tasks.
     */
    void releaseWorker() {
        workerGate.countDown();
    }
}
