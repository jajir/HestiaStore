package org.hestiastore.console;

import java.util.concurrent.CountDownLatch;

/**
 * CLI launcher for MonitoringConsoleServer.
 */
public final class MonitoringConsoleLauncher {

    private MonitoringConsoleLauncher() {
    }

    /**
     * Starts monitoring console and blocks until process termination.
     *
     * Supported system properties:
     * - hestia.console.bindAddress (default: 127.0.0.1)
     * - hestia.console.port (default: 8085)
     * - hestia.console.writeToken (default: empty)
     * - hestia.console.requireTlsToNodes (default: false)
     * - hestia.console.actionRetryAttempts (default: 3)
     *
     * @param args ignored
     * @throws Exception when startup fails
     */
    public static void main(final String[] args) throws Exception {
        final String bindAddress = System.getProperty(
                "hestia.console.bindAddress", "127.0.0.1");
        final int port = Integer
                .parseInt(System.getProperty("hestia.console.port", "8085"));
        final String writeToken = System.getProperty(
                "hestia.console.writeToken", "");
        final boolean requireTlsToNodes = Boolean.parseBoolean(System
                .getProperty("hestia.console.requireTlsToNodes", "false"));
        final int actionRetryAttempts = Integer.parseInt(System.getProperty(
                "hestia.console.actionRetryAttempts", "3"));

        final MonitoringConsoleServer server = new MonitoringConsoleServer(
                bindAddress, port, writeToken, requireTlsToNodes,
                actionRetryAttempts);
        server.start();
        System.out.println("HestiaStore Monitoring Console started on http://"
                + bindAddress + ":" + server.getPort() + "/console/v1/dashboard");
        System.out.println("UI is provided by monitoring-console-web.");

        final CountDownLatch shutdownLatch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.close();
            shutdownLatch.countDown();
        }));
        shutdownLatch.await();
    }
}
