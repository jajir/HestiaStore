package com.coroptis.index;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.spi.LocationAwareLogger;

public class ContextAwareLogger {
    private final LocationAwareLogger logger;
    private final LoggingContext context;
    private final String fqcn = ContextAwareLogger.class.getName();

    public ContextAwareLogger(Class<?> clazz, LoggingContext context) {
        if (clazz == null) {
            throw new IllegalArgumentException("Class cannot be null");
        }
        if (context == null) {
            throw new IllegalArgumentException("LoggingContext cannot be null");
        }
        final Logger rawLogger = LoggerFactory.getLogger(clazz);
        if (!(rawLogger instanceof LocationAwareLogger)) {
            throw new IllegalStateException(
                    "Logger must be LocationAwareLogger (use Logback or Log4j)");
        }
        this.logger = (LocationAwareLogger) rawLogger;
        this.context = context;
    }

    public void trace(String message, Object... args) {
        if (logger.isTraceEnabled()) {
            log(LocationAwareLogger.TRACE_INT, message, args);
        }
    }

    public void debug(String message, Object... args) {
        if (logger.isDebugEnabled()) {
            log(LocationAwareLogger.DEBUG_INT, message, args);
        }
    }

    public void info(String message, Object... args) {
        if (logger.isInfoEnabled()) {
            log(LocationAwareLogger.INFO_INT, message, args);
        }
    }

    public void warn(String message, Object... args) {
        if (logger.isWarnEnabled()) {
            log(LocationAwareLogger.WARN_INT, message, args);
        }
    }

    public void error(String message, Object... args) {
        if (logger.isErrorEnabled()) {
            log(LocationAwareLogger.ERROR_INT, message, args);
        }
    }

    // Optionally expose the underlying SLF4J logger if needed
    public Logger getUnderlyingLogger() {
        return logger;
    }

    private final void log(int level, String message, Object... args) {
        logger.log(null, fqcn, level, format(message), args, null);
    }

    private String format(String message) {
        return context.formatPrefix() + message;
    }
}
