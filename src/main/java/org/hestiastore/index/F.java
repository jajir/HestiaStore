package org.hestiastore.index;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Locale;
import java.util.Base64;

/**
 * Class just format number with some separator.
 * 
 * 
 * When I'll find how to format numbers in log4j, this class can be removed.
 * 
 * @author honza
 *
 */
public class F {

    private F() {
        /**
         * I don't want any instances.
         */
    }

    /**
     * Format given number and convert it to string.
     * 
     * @param number
     * @return
     */
    public static final String fmt(final long number) {
        // DecimalFormat is not thread safe.
        DecimalFormat df = (DecimalFormat) NumberFormat
                .getNumberInstance(Locale.getDefault());
        return df.format(number);
    }

    /**
     * Format given number and convert it to string.
     * 
     * @param number
     * @return
     */
    public static final String fmt(final int number) {
        // DecimalFormat is not thread safe.
        DecimalFormat df = (DecimalFormat) NumberFormat
                .getNumberInstance(Locale.getDefault());
        return df.format(number);
    }

    /**
     * Formats arbitrary binary data as a Base64 string, safe for logs and
     * error messages.
     *
     * @param data required byte array
     * @return Base64-encoded string
     */
    public static final String b64(final byte[] data) {
        Vldtn.requireNonNull(data, "data");
        return Base64.getEncoder().encodeToString(data);
    }

}
