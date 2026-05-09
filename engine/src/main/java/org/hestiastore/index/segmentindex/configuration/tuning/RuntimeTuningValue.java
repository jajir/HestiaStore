package org.hestiastore.index.segmentindex.configuration.tuning;

import java.util.Objects;

import org.hestiastore.index.Vldtn;

/**
 * Typed value used by runtime tuning patches and change reports.
 */
public final class RuntimeTuningValue {

    private final RuntimeTuningValueType type;
    private final Object value;

    private RuntimeTuningValue(final RuntimeTuningValueType type,
            final Object value) {
        this.type = Vldtn.requireNonNull(type, "type");
        this.value = Vldtn.requireNonNull(value, "value");
    }

    public static RuntimeTuningValue ofInt(final int value) {
        return new RuntimeTuningValue(RuntimeTuningValueType.INT,
                Integer.valueOf(value));
    }

    public static RuntimeTuningValue ofLong(final long value) {
        return new RuntimeTuningValue(RuntimeTuningValueType.LONG,
                Long.valueOf(value));
    }

    public static RuntimeTuningValue ofBoolean(final boolean value) {
        return new RuntimeTuningValue(RuntimeTuningValueType.BOOLEAN,
                Boolean.valueOf(value));
    }

    public static RuntimeTuningValue ofDouble(final double value) {
        return new RuntimeTuningValue(RuntimeTuningValueType.DOUBLE,
                Double.valueOf(value));
    }

    public static RuntimeTuningValue ofString(final String value) {
        return new RuntimeTuningValue(RuntimeTuningValueType.STRING,
                Vldtn.requireNonNull(value, "value"));
    }

    public static RuntimeTuningValue ofEnum(final Enum<?> value) {
        final Enum<?> enumValue = Vldtn.requireNonNull(value, "value");
        return new RuntimeTuningValue(RuntimeTuningValueType.ENUM,
                enumValue.name());
    }

    public RuntimeTuningValueType type() {
        return type;
    }

    public int asInt() {
        requireType(RuntimeTuningValueType.INT);
        return ((Integer) value).intValue();
    }

    public long asLong() {
        requireType(RuntimeTuningValueType.LONG);
        return ((Long) value).longValue();
    }

    public boolean asBoolean() {
        requireType(RuntimeTuningValueType.BOOLEAN);
        return ((Boolean) value).booleanValue();
    }

    public double asDouble() {
        requireType(RuntimeTuningValueType.DOUBLE);
        return ((Double) value).doubleValue();
    }

    public String asString() {
        if (type == RuntimeTuningValueType.STRING
                || type == RuntimeTuningValueType.ENUM) {
            return (String) value;
        }
        throw new IllegalStateException(
                "Runtime tuning value type is " + type
                        + ", expected STRING or ENUM.");
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
        return type == that.type && value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, value);
    }

    @Override
    public String toString() {
        return value.toString();
    }

    private void requireType(final RuntimeTuningValueType expected) {
        if (type != expected) {
            throw new IllegalStateException(
                    "Runtime tuning value type is " + type + ", expected "
                            + expected + ".");
        }
    }
}
