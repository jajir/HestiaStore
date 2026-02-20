package org.hestiastore.index.control;

/**
 * Root access point for runtime monitoring and runtime configuration
 * management.
 */
public interface IndexControlPlane {

    /**
     * Returns logical index name.
     *
     * @return index name
     */
    String indexName();

    /**
     * Returns view for runtime parameters that are changing constantly.
     *
     * @return runtime view
     */
    IndexRuntimeView runtime();

    /**
     * Returns runtime configuration management API.
     *
     * @return runtime configuration API
     */
    IndexConfigurationManagement configuration();
}
