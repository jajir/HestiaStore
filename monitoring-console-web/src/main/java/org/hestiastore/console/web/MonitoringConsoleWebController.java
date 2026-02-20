package org.hestiastore.console.web;

import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.LinkedHashMap;

import jakarta.validation.constraints.NotBlank;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

/**
 * MVC controller for monitoring console web UI.
 */
@Controller
@Validated
public class MonitoringConsoleWebController {

    private final ConsoleBackendClient backendClient;
    private final MonitoringConsoleWebProperties properties;

    /**
     * Creates web controller.
     *
     * @param backendClient backend client
     * @param properties    app properties
     */
    public MonitoringConsoleWebController(
            final ConsoleBackendClient backendClient,
            final MonitoringConsoleWebProperties properties) {
        this.backendClient = backendClient;
        this.properties = properties;
    }

    /**
     * Dashboard page.
     *
     * @param model thymeleaf model
     * @return template name
     */
    @GetMapping("/")
    public String dashboard(final Model model) {
        fillModel(model);
        model.addAttribute("nodeEndpoints", properties.nodes());
        model.addAttribute("refreshMillis", properties.refreshMillis());
        model.addAttribute("refreshMillisDisplay",
                ConsoleBackendClient.NodeRow
                        .formatWholeNumberValue(properties.refreshMillis()));
        return "dashboard";
    }

    /**
     * Node details page.
     *
     * @param nodeId node id
     * @param model  thymeleaf model
     * @return template name
     */
    @GetMapping("/nodes/{nodeId}")
    public String nodeDetail(@PathVariable("nodeId") final String nodeId,
            @RequestParam(name = "indexName",
                    required = false) final String indexName,
            final Model model) {
        model.addAttribute("refreshMillis", properties.refreshMillis());
        final Optional<ConsoleBackendClient.NodeDetails> nodeDetails = backendClient
                .fetchNodeDetails(nodeId);
        if (nodeDetails.isEmpty()) {
            model.addAttribute("missingNodeId", nodeId);
            return "node-detail";
        }
        model.addAttribute("nodeDetail", nodeDetails.get());
        model.addAttribute("node", nodeDetails.get().node());
        model.addAttribute("indexSections", nodeDetails.get().indexes());
        final List<String> indexNames = nodeDetails.get().indexes().stream()
                .map(ConsoleBackendClient.IndexRow::indexName).sorted().toList();
        model.addAttribute("indexNames", indexNames);
        final String selectedIndexName = resolveSelectedIndex(indexName,
                indexNames);
        model.addAttribute("selectedIndexName", selectedIndexName);
        if (!selectedIndexName.isBlank()) {
            final Optional<ConsoleBackendClient.RuntimeConfigView> config = backendClient
                    .fetchRuntimeConfig(nodeId, selectedIndexName);
            if (config.isPresent()) {
                fillRuntimeConfigModel(model, config.get());
            } else {
                model.addAttribute("runtimeConfigUnavailable", true);
            }
        } else {
            model.addAttribute("runtimeConfigUnavailable", true);
        }
        return "node-detail";
    }

    /**
     * Node details live fragment.
     *
     * @param nodeId node id
     * @param model  thymeleaf model
     * @return fragment name
     */
    @GetMapping("/fragments/nodes/{nodeId}/live")
    public String nodeDetailLive(@PathVariable("nodeId") final String nodeId,
            final Model model) {
        model.addAttribute("refreshMillis", properties.refreshMillis());
        final Optional<ConsoleBackendClient.NodeDetails> nodeDetails = backendClient
                .fetchNodeDetails(nodeId);
        if (nodeDetails.isEmpty()) {
            model.addAttribute("missingNodeId", nodeId);
        } else {
            model.addAttribute("nodeDetail", nodeDetails.get());
            model.addAttribute("node", nodeDetails.get().node());
            model.addAttribute("indexSections", nodeDetails.get().indexes());
        }
        return "fragments/node-detail-live :: nodeLive";
    }

    /**
     * Nodes fragment.
     *
     * @param model thymeleaf model
     * @return fragment name
     */
    @GetMapping("/fragments/nodes")
    public String nodes(final Model model) {
        fillNodes(model);
        return "fragments/nodes :: nodesTable";
    }

    /**
     * Actions fragment.
     *
     * @param model thymeleaf model
     * @return fragment name
     */
    @GetMapping("/fragments/actions")
    public String actions(final Model model) {
        model.addAttribute("actions", backendClient.fetchActions());
        return "fragments/actions :: actionsList";
    }

    /**
     * Events fragment.
     *
     * @param model thymeleaf model
     * @return fragment name
     */
    @GetMapping("/fragments/events")
    public String events(final Model model) {
        model.addAttribute("events", backendClient.fetchEvents());
        return "fragments/events :: eventsList";
    }

    /**
     * Action trigger from table button.
     *
     * @param action action name
     * @param nodeId node id
     * @param model  thymeleaf model
     * @return flash fragment
     */
    @PostMapping("/actions/{action}")
    public String triggerAction(@PathVariable("action") final String action,
            @org.springframework.web.bind.annotation.RequestParam("nodeId")
            @NotBlank final String nodeId,
            @RequestParam(name = "returnTo",
                    defaultValue = "dashboard") final String returnTo,
            @RequestHeader(value = "HX-Request",
                    required = false) final String hxRequest,
            final Model model, final RedirectAttributes redirectAttributes) {
        final boolean htmx = hxRequest != null && !hxRequest.isBlank();
        final boolean returnToNode = "node".equalsIgnoreCase(returnTo);
        try {
            backendClient.triggerAction(action, nodeId);
            if (htmx) {
                model.addAttribute("message",
                        action.toUpperCase() + " accepted for " + nodeId);
                model.addAttribute("level", "ok");
            } else {
                redirectAttributes.addFlashAttribute("message",
                        action.toUpperCase() + " accepted for " + nodeId);
                redirectAttributes.addFlashAttribute("level", "ok");
                if (returnToNode) {
                    return "redirect:/nodes/" + nodeId;
                }
                return "redirect:/";
            }
        } catch (final Exception e) {
            if (htmx) {
                model.addAttribute("message",
                        "Action failed: " + e.getMessage());
                model.addAttribute("level", "bad");
            } else {
                redirectAttributes.addFlashAttribute("message",
                        "Action failed: " + e.getMessage());
                redirectAttributes.addFlashAttribute("level", "bad");
                if (returnToNode) {
                    return "redirect:/nodes/" + nodeId;
                }
                return "redirect:/";
            }
        }
        return "fragments/flash :: flash";
    }

    /**
     * Applies or validates runtime config patch from node detail page.
     *
     * @param nodeId             node id
     * @param indexName          selected index name
     * @param operation          validate|apply
     * @param requestParameters  raw request parameters
     * @param redirectAttributes flash attributes
     * @return redirect back to node detail
     */
    @PostMapping("/nodes/{nodeId}/runtime-config")
    public String updateRuntimeConfig(
            @PathVariable("nodeId") final String nodeId,
            @RequestParam("indexName") @NotBlank final String indexName,
            @RequestParam(name = "operation",
                    defaultValue = "apply") final String operation,
            @RequestParam final Map<String, String> requestParameters,
            final RedirectAttributes redirectAttributes) {
        final Map<String, String> values = extractConfigValues(
                requestParameters);
        if (values.isEmpty()) {
            redirectAttributes.addFlashAttribute("message",
                    "No editable values provided.");
            redirectAttributes.addFlashAttribute("level", "warn");
            redirectAttributes.addAttribute("indexName", indexName);
            return "redirect:/nodes/" + nodeId;
        }
        final boolean dryRun = "validate".equalsIgnoreCase(operation);
        try {
            backendClient.patchRuntimeConfig(nodeId, indexName, values, dryRun);
            redirectAttributes.addFlashAttribute("message",
                    dryRun
                            ? "Validation succeeded for " + indexName + "."
                            : "Configuration applied for " + indexName + ".");
            redirectAttributes.addFlashAttribute("level", "ok");
        } catch (final Exception e) {
            redirectAttributes.addFlashAttribute("message",
                    "Config update failed: " + rootMessage(e));
            redirectAttributes.addFlashAttribute("level", "bad");
        }
        redirectAttributes.addAttribute("indexName", indexName);
        return "redirect:/nodes/" + nodeId;
    }

    private void fillModel(final Model model) {
        fillNodes(model);
        model.addAttribute("actions", backendClient.fetchActions());
        model.addAttribute("events", backendClient.fetchEvents());
    }

    private void fillNodes(final Model model) {
        final List<ConsoleBackendClient.NodeRow> nodes = backendClient
                .fetchDashboard();
        final long reachable = nodes.stream().filter(n -> n.reachable()).count();
        final long unreachable = nodes.size() - reachable;
        final long getOps = nodes.stream().mapToLong(ConsoleBackendClient.NodeRow::getOps)
                .sum();
        final long writeOps = nodes.stream()
                .mapToLong(n -> n.putOps() + n.deleteOps()).sum();
        final OptionalDouble avgLatency = nodes.stream()
                .mapToLong(ConsoleBackendClient.NodeRow::latencyMs)
                .average();
        final long maxLatency = nodes.stream()
                .mapToLong(ConsoleBackendClient.NodeRow::latencyMs).max()
                .orElse(0L);
        final long cacheHits = nodes.stream()
                .mapToLong(ConsoleBackendClient.NodeRow::cacheHitCount).sum();
        final long cacheMisses = nodes.stream()
                .mapToLong(ConsoleBackendClient.NodeRow::cacheMissCount).sum();
        final long cacheSize = nodes.stream()
                .mapToLong(ConsoleBackendClient.NodeRow::cacheSize).sum();
        final long cacheLimit = nodes.stream()
                .mapToLong(ConsoleBackendClient.NodeRow::cacheLimit).sum();
        final long cacheHitRatio = cacheHits + cacheMisses == 0L ? 0L
                : Math.round((cacheHits * 100D) / (cacheHits + cacheMisses));
        final long cacheFillRatio = cacheLimit == 0L ? 0L
                : Math.round((cacheSize * 100D) / cacheLimit);
        model.addAttribute("nodes", nodes);
        model.addAttribute("statNodes", nodes.size());
        model.addAttribute("statNodesDisplay",
                ConsoleBackendClient.NodeRow.formatWholeNumberValue(
                        nodes.size()));
        model.addAttribute("statReachable", reachable);
        model.addAttribute("statReachableDisplay",
                ConsoleBackendClient.NodeRow.formatWholeNumberValue(reachable));
        model.addAttribute("statUnreachable", unreachable);
        model.addAttribute("statGetOps", getOps);
        model.addAttribute("statGetOpsDisplay",
                ConsoleBackendClient.NodeRow.formatWholeNumberValue(getOps));
        model.addAttribute("statWriteOps", writeOps);
        model.addAttribute("statWriteOpsDisplay",
                ConsoleBackendClient.NodeRow.formatWholeNumberValue(writeOps));
        model.addAttribute("statCacheHits", cacheHits);
        model.addAttribute("statCacheMisses", cacheMisses);
        model.addAttribute("statCacheHitRatio", cacheHitRatio);
        model.addAttribute("statCacheHitRatioDisplay",
                ConsoleBackendClient.NodeRow
                        .formatWholeNumberValue(cacheHitRatio));
        model.addAttribute("statCacheFillRatio", cacheFillRatio);
        model.addAttribute("statCacheFillRatioDisplay",
                ConsoleBackendClient.NodeRow
                        .formatWholeNumberValue(cacheFillRatio));
        model.addAttribute("statAvgLatency", Math.round(avgLatency.orElse(0D)));
        model.addAttribute("statMaxLatency", maxLatency);
    }

    private String resolveSelectedIndex(final String requestedIndexName,
            final List<String> indexNames) {
        if (requestedIndexName != null && !requestedIndexName.isBlank()) {
            final String requested = requestedIndexName.trim();
            if (indexNames.contains(requested)) {
                return requested;
            }
        }
        if (indexNames.isEmpty()) {
            return "";
        }
        return indexNames.get(0);
    }

    private void fillRuntimeConfigModel(final Model model,
            final ConsoleBackendClient.RuntimeConfigView configView) {
        final Set<String> supportedKeys = Set.copyOf(configView.supportedKeys());
        final Set<String> allKeys = new TreeSet<>();
        allKeys.addAll(configView.original().keySet());
        allKeys.addAll(configView.current().keySet());
        final List<RuntimeConfigRow> rows = allKeys.stream().map(key -> {
            final Integer original = configView.original().get(key);
            final Integer current = configView.current().get(key);
            final boolean editable = supportedKeys.contains(key);
            final boolean overridden = original != null && current != null
                    && !original.equals(current);
            return new RuntimeConfigRow(key, original, current, editable,
                    overridden);
        }).toList();
        final long changedCount = rows.stream().filter(RuntimeConfigRow::overridden)
                .count();
        model.addAttribute("runtimeConfigView", configView);
        model.addAttribute("runtimeConfigRows", rows);
        model.addAttribute("runtimeConfigChangedCount", changedCount);
        model.addAttribute("runtimeConfigPendingCount", 0);
    }

    private Map<String, String> extractConfigValues(
            final Map<String, String> requestParameters) {
        final Map<String, String> values = new LinkedHashMap<>();
        for (final Map.Entry<String, String> entry : requestParameters
                .entrySet()) {
            final String rawKey = entry.getKey();
            if (rawKey == null || !rawKey.startsWith("value.")) {
                continue;
            }
            final String key = rawKey.substring("value.".length()).trim();
            final String value = entry.getValue() == null ? ""
                    : entry.getValue().trim();
            if (key.isEmpty() || value.isEmpty()) {
                continue;
            }
            values.put(key, value);
        }
        return values;
    }

    private String rootMessage(final Throwable throwable) {
        Throwable current = throwable;
        while (current.getCause() != null
                && current.getCause() != current) {
            current = current.getCause();
        }
        if (current.getMessage() == null || current.getMessage().isBlank()) {
            return "Unknown error";
        }
        return current.getMessage();
    }

    /**
     * Runtime config row model for node detail table.
     */
    public record RuntimeConfigRow(String key, Integer original, Integer current,
            boolean editable, boolean overridden) {

        /**
         * Original value display.
         *
         * @return formatted display
         */
        public String originalDisplay() {
            if (original == null) {
                return "-";
            }
            return ConsoleBackendClient.NodeRow
                    .formatWholeNumberValue(original.longValue());
        }

        /**
         * Current value display.
         *
         * @return formatted display
         */
        public String currentDisplay() {
            if (current == null) {
                return "-";
            }
            return ConsoleBackendClient.NodeRow
                    .formatWholeNumberValue(current.longValue());
        }

        /**
         * Value used to prefill editable input.
         *
         * @return current value as plain integer string
         */
        public String inputValue() {
            if (current == null) {
                return "";
            }
            return Integer.toString(current.intValue());
        }

        /**
         * Server-computed status label.
         *
         * @return status text
         */
        public String statusLabel() {
            return overridden ? "Overridden" : "Unchanged";
        }
    }
}
