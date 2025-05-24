package com.hestiastore.index.log;

/**
 * Data type representing Log operation.
 */
public enum LogOperation {

    POST(Constants.POST_CODE), //
    DELETE(Constants.DELETE_CODE);

    private final byte code;

    LogOperation(final byte code) {
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
            throw new IllegalArgumentException(String.format(
                    "Unable to extract log operation from byte '%s'", b));
        }
    }

    @Override
    public String toString() {
        return String.format("Logged[operation='%s']", code);
    }

    private static class Constants {
        private final static byte POST_CODE = (byte) 80; // ASCII value for 'P'
        private final static byte DELETE_CODE = (byte) 68;// ASCII value for 'D'
    }
}
