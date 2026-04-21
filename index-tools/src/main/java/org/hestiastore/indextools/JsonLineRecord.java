package org.hestiastore.indextools;

class JsonLineRecord {

    private ValueEnvelope key;
    private ValueEnvelope value;

    public ValueEnvelope getKey() {
        return key;
    }

    public void setKey(final ValueEnvelope key) {
        this.key = key;
    }

    public ValueEnvelope getValue() {
        return value;
    }

    public void setValue(final ValueEnvelope value) {
        this.value = value;
    }
}
