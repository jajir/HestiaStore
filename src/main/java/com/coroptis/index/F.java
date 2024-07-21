package com.coroptis.index;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Locale;

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

}
