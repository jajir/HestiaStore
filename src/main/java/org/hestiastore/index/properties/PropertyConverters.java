package org.hestiastore.index.properties;

import java.math.BigDecimal;

import org.hestiastore.index.IndexException;

final class PropertyConverters {

    private static final char GROUP_SEPARATOR = '_';
    private static final char DECIMAL_SEPARATOR = '.';

    String toString(final Object value) {
        if (value == null) {
            throw new IndexException("Provided property value is null.");
        }
        return String.valueOf(value);
    }

    int toInt(final String value) {
        return value == null ? 0 : Integer.parseInt(normalizeNumber(value));
    }

    long toLong(final String value) {
        return value == null ? 0L : Long.parseLong(normalizeNumber(value));
    }

    double toDouble(final String value) {
        return value == null ? 0D : Double.parseDouble(normalizeNumber(value));
    }

    boolean toBoolean(final String value) {
        return value != null && Boolean.parseBoolean(value);
    }

    String formatInt(final int value) {
        return addIntegerGrouping(Integer.toString(value));
    }

    String formatLong(final long value) {
        return addIntegerGrouping(Long.toString(value));
    }

    String formatDouble(final double value) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return Double.toString(value);
        }
        final String plain = BigDecimal.valueOf(value).toPlainString();
        final int dot = plain.indexOf(DECIMAL_SEPARATOR);
        if (dot < 0) {
            return addIntegerGrouping(plain);
        }
        final String integerPart = plain.substring(0, dot);
        final String fractionPart = plain.substring(dot + 1);
        return addIntegerGrouping(integerPart) + DECIMAL_SEPARATOR
                + fractionPart;
    }

    private String normalizeNumber(final String value) {
        return value.replace(String.valueOf(GROUP_SEPARATOR), "");
    }

    private String addIntegerGrouping(final String input) {
        final boolean negative = input.startsWith("-");
        final String digits = negative ? input.substring(1) : input;
        if (digits.length() <= 3) {
            return input;
        }
        final StringBuilder out = new StringBuilder(input.length() + 4);
        if (negative) {
            out.append('-');
        }
        final int leading = digits.length() % 3;
        int index = 0;
        if (leading > 0) {
            out.append(digits, 0, leading);
            index = leading;
            if (index < digits.length()) {
                out.append(GROUP_SEPARATOR);
            }
        }
        while (index < digits.length()) {
            out.append(digits, index, index + 3);
            index += 3;
            if (index < digits.length()) {
                out.append(GROUP_SEPARATOR);
            }
        }
        return out.toString();
    }
}
