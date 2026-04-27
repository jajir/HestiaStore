package org.hestiastore.index.segmentindex.core.maintenance;

/**
 * Public maintenance contract for foreground and stable segment maintenance
 * operations.
 */
public interface MaintenanceService {

    /**
     * Creates a builder for maintenance services.
     *
     * @param <M> key type
     * @param <N> value type
     * @return maintenance service builder
     */
    static <M, N> MaintenanceServiceBuilder<M, N> builder() {
        return new MaintenanceServiceBuilder<>();
    }

    void compact();

    /**
     * Compacts mapped segments and waits until durable maintenance is settled.
     */
    void compactAndWait();

    void flush();

    /**
     * Flushes mapped segments and waits until durable maintenance is settled.
     */
    void flushAndWait();
}
