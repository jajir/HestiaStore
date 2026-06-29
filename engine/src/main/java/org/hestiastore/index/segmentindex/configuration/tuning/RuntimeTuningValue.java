package org.hestiastore.index.segmentindex.configuration.tuning;

/**
 * Integer value used by runtime tuning patches and change reports.
 */
public final class RuntimeTuningValue {

    private final int value;

    private RuntimeTuningValue(final int value) {
        this.value = value;
    }

    /**
     * Creates an integer tuning value.
     *
     * @param value integer value
     * @return runtime tuning value
     */
    public static RuntimeTuningValue ofInt(final int value) {
        return new RuntimeTuningValue(value);
    }

    /**
     * Returns this value as an integer.
     *
     * @return integer value
     */
    public int asInt() {
        return value;
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof RuntimeTuningValue)) {
            return false;
        }
        final RuntimeTuningValue that = (RuntimeTuningValue) other;
        return value == that.value;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(value);
    }

    @Override
    public String toString() {
        return Integer.toString(value);
    }
}
