package org.hestiastore.management.agent;

import java.util.Map;
import java.util.Objects;

/**
 * Security policy for management-agent authentication/authorization.
 *
 * @param requireTls                  true when only HTTPS transport is allowed
 * @param tokenRoles                  token to role mapping
 * @param maxMutatingRequestsPerMinute per-actor mutating request limit
 */
public record ManagementAgentSecurityPolicy(boolean requireTls,
        Map<String, AgentRole> tokenRoles, int maxMutatingRequestsPerMinute) {

    /**
     * Creates a validated policy.
     *
     * @param requireTls                   true to enforce HTTPS
     * @param tokenRoles                   token role map
     * @param maxMutatingRequestsPerMinute mutating request limit per minute
     */
    public ManagementAgentSecurityPolicy {
        tokenRoles = Map.copyOf(Objects.requireNonNull(tokenRoles, "tokenRoles"));
        if (maxMutatingRequestsPerMinute <= 0) {
            throw new IllegalArgumentException(
                    "maxMutatingRequestsPerMinute must be > 0");
        }
    }

    /**
     * Default permissive policy suitable for local development.
     *
     * @return permissive policy
     */
    public static ManagementAgentSecurityPolicy permissive() {
        return new ManagementAgentSecurityPolicy(false, Map.of(), 120);
    }

    /**
     * Token-based policy helper.
     *
     * @param tokenRoles token to role mapping
     * @return policy with token auth enabled
     */
    public static ManagementAgentSecurityPolicy tokenBased(
            final Map<String, AgentRole> tokenRoles) {
        return new ManagementAgentSecurityPolicy(false, tokenRoles, 120);
    }
}
