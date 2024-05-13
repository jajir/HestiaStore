package com.coroptis.index.log;

/**
 * Data type representing Log operation.
 */
public enum LogOperation {

    POST(Constants.POST_CODE), //
    DELETE(Constants.DELETE_CODE);

    private final byte code;

    private LogOperation(final byte code) {
        this.code = code;
    }

    public byte getByte() {
        return code;
    }

    public static LogOperation fromByte(final byte b) {
        if (b == Constants.POST_CODE) {
            return POST;
        } else if (b == Constants.DELETE_CODE) {
            return DELETE;
        } else {
            throw new IllegalArgumentException(String.format("Unable to extract log operation from byte '%s'", b));
        }
    }

    private static class Constants {
        private final static byte POST_CODE = (byte) 1;
        private final static byte DELETE_CODE = (byte) 2;
    }
}
