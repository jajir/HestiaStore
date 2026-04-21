package org.hestiastore.indextools;

import java.util.LinkedHashMap;
import java.util.Map;

class ChunkFilterSpecManifest {

    private String providerId;
    private Map<String, String> parameters = new LinkedHashMap<>();

    public String getProviderId() {
        return providerId;
    }

    public void setProviderId(final String providerId) {
        this.providerId = providerId;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public void setParameters(final Map<String, String> parameters) {
        this.parameters = parameters == null ? new LinkedHashMap<>()
                : new LinkedHashMap<>(parameters);
    }
}
