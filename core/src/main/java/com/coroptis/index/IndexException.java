package com.coroptis.index;

/**
 * Main project exception. Allows to wrap checked exceptions and allows to
 * throws project specific exceptions.
 * 
 * @author honza
 *
 */
public class IndexException extends RuntimeException {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public IndexException() {
        super();
    }

    public IndexException(String message, Throwable cause) {
        super(message, cause);
    }

    public IndexException(String message) {
        super(message);
    }

}
