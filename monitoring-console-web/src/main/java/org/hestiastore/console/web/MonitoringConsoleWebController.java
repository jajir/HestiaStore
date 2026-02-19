package org.hestiastore.console.web;

import java.util.List;
import java.util.OptionalDouble;
import java.util.Optional;

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
}
