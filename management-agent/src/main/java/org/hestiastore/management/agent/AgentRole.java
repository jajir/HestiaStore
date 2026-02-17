package org.hestiastore.management.agent;

/**
 * Agent role hierarchy for management endpoint authorization.
 */
public enum AgentRole {
    /**
     * Read-only access to state/metrics/health endpoints.
     */
    READ,
    /**
     * Operational access to run actions (flush/compact).
     */
    OPERATE,
    /**
     * Administrative access to runtime config patch operations.
     */
    ADMIN;

    /**
     * Returns true when this role satisfies a required role.
     *
     * @param required required minimum role
     * @return true when allowed
     */
    public boolean allows(final AgentRole required) {
        return this.ordinal() >= required.ordinal();
    }
}
