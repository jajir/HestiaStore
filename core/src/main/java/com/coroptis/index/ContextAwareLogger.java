package com.coroptis.index;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContextAwareLogger {
    private final Logger logger;
    private final LoggingContext context;

    public ContextAwareLogger(Class<?> clazz, LoggingContext context) {
        if (clazz == null) {
            throw new IllegalArgumentException("Class cannot be null");
        }
        if (context == null) {
            throw new IllegalArgumentException("LoggingContext cannot be null");
        }
        this.logger = LoggerFactory.getLogger(clazz);
        this.context = context;
    }

    private String format(String message) {
        return context.formatPrefix() + message;
    }

    public void trace(String message, Object... args) {
        if (logger.isTraceEnabled()) {
            logger.trace(format(message), args);
        }
    }

    public void debug(String message, Object... args) {
        if (logger.isDebugEnabled()) {
            logger.debug(format(message), args);
        }
    }

    public void info(String message, Object... args) {
        if (logger.isInfoEnabled()) {
            logger.info(format(message), args);
        }
    }

    public void warn(String message, Object... args) {
        if (logger.isWarnEnabled()) {
            logger.warn(format(message), args);
        }
    }

    public void error(String message, Object... args) {
        if (logger.isErrorEnabled()) {
            logger.error(format(message), args);
        }
    }

    // Optionally expose the underlying SLF4J logger if needed
    public Logger getUnderlyingLogger() {
        return logger;
    }
}
