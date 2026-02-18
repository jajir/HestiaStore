package org.hestiastore.console.web;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * Spring Boot entrypoint for Monitoring Console Web UI.
 */
@SpringBootApplication
@EnableConfigurationProperties(MonitoringConsoleWebProperties.class)
public class MonitoringConsoleWebApplication {

    /**
     * Application entrypoint.
     *
     * @param args startup arguments
     */
    public static void main(final String[] args) {
        SpringApplication.run(MonitoringConsoleWebApplication.class, args);
    }
}
