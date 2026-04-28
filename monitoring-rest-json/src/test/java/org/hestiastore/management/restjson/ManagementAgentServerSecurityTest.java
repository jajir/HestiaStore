package org.hestiastore.management.restjson;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;

import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.monitoring.json.api.ManagementApiPaths;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ManagementAgentServerSecurityTest {

    private SegmentIndex<Integer, String> index;
    private ManagementAgentServer server;
    private HttpClient client;
    private String baseUrl;

    @BeforeEach
    void setUp() throws IOException {
        final Directory directory = new MemDirectory();
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()//
                .identity(identity -> identity.keyClass(Integer.class)
                        .valueClass(String.class)
                        .keyTypeDescriptor(new TypeDescriptorInteger())
                        .valueTypeDescriptor(new TypeDescriptorShortString())
                        .name("secure-index")) //
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(0)) //
                .logging(logging -> logging.contextEnabled(false)) //
                .build();
        index = SegmentIndex.create(directory, conf);
        final ManagementAgentSecurityPolicy policy = new ManagementAgentSecurityPolicy(
                false,
                Map.of(
                        "read-token", AgentRole.READ,
                        "operate-token", AgentRole.OPERATE,
                        "admin-token", AgentRole.ADMIN),
                2);
        server = new ManagementAgentServer("127.0.0.1", 0, index,
                "secure-index", policy);
        server.start();
        client = HttpClient.newHttpClient();
        baseUrl = "http://127.0.0.1:" + server.getPort();
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.close();
        }
        if (index != null && !index.wasClosed()) {
            index.close();
        }
    }

    @Test
    void unauthorizedAndForbiddenAreRejected() throws Exception {
        final HttpResponse<String> unauthorized = send("GET",
                ManagementApiPaths.REPORT, null, null);
        assertEquals(401, unauthorized.statusCode());

        final HttpResponse<String> forbidden = send("POST",
                ManagementApiPaths.ACTION_COMPACT, "{\"requestId\":\"req-1\"}",
                "read-token");
        assertEquals(403, forbidden.statusCode());
    }

    @Test
    void roleBasedMutatingAccessAndRateLimit() throws Exception {
        final HttpResponse<String> compactAllowed = send("POST",
                ManagementApiPaths.ACTION_COMPACT, "{\"requestId\":\"req-2\"}",
                "operate-token");
        assertEquals(200, compactAllowed.statusCode());

        final HttpResponse<String> configForbidden = send("PATCH",
                ManagementApiPaths.CONFIG + "?indexName=secure-index",
                "{\"values\":{\"maxNumberOfSegmentsInCache\":\"100\"},\"dryRun\":true}",
                "operate-token");
        assertEquals(403, configForbidden.statusCode());

        final HttpResponse<String> configAllowed = send("PATCH",
                ManagementApiPaths.CONFIG + "?indexName=secure-index",
                "{\"values\":{\"maxNumberOfSegmentsInCache\":\"100\"},\"dryRun\":true}",
                "admin-token");
        assertEquals(204, configAllowed.statusCode());

        final HttpResponse<String> first = send("POST",
                ManagementApiPaths.ACTION_FLUSH, "{\"requestId\":\"req-3\"}",
                "admin-token");
        final HttpResponse<String> second = send("POST",
                ManagementApiPaths.ACTION_FLUSH, "{\"requestId\":\"req-4\"}",
                "admin-token");
        final HttpResponse<String> limited = send("POST",
                ManagementApiPaths.ACTION_FLUSH, "{\"requestId\":\"req-5\"}",
                "admin-token");
        assertEquals(200, first.statusCode());
        assertEquals(200, second.statusCode());
        assertEquals(429, limited.statusCode());
    }

    @Test
    void duplicateRequestIdDoesNotConsumeAdditionalRateLimitQuota()
            throws Exception {
        final HttpResponse<String> first = send("POST",
                ManagementApiPaths.ACTION_FLUSH,
                "{\"requestId\":\"dup-1\"}",
                "admin-token");
        final HttpResponse<String> replay = send("POST",
                ManagementApiPaths.ACTION_FLUSH,
                "{\"requestId\":\"dup-1\"}",
                "admin-token");
        final HttpResponse<String> secondUnique = send("POST",
                ManagementApiPaths.ACTION_FLUSH,
                "{\"requestId\":\"dup-2\"}",
                "admin-token");
        final HttpResponse<String> limited = send("POST",
                ManagementApiPaths.ACTION_FLUSH,
                "{\"requestId\":\"dup-3\"}",
                "admin-token");

        assertEquals(200, first.statusCode());
        assertEquals(200, replay.statusCode());
        assertEquals(first.body(), replay.body());
        assertEquals(200, secondUnique.statusCode());
        assertEquals(429, limited.statusCode());
    }

    @Test
    void mutatingCallsProduceImmutableAuditEntries() throws Exception {
        send("POST", ManagementApiPaths.ACTION_FLUSH, "{\"requestId\":\"a\"}",
                "admin-token");
        send("POST", ManagementApiPaths.ACTION_COMPACT, "{\"requestId\":\"b\"}",
                "admin-token");
        send("PATCH", ManagementApiPaths.CONFIG + "?indexName=secure-index",
                "{\"values\":{\"forbidden\":\"1\"},\"dryRun\":false}",
                "admin-token");

        final List<ManagementAgentServer.AuditRecord> records = server
                .auditTrailSnapshot();
        assertTrue(records.size() >= 3);
        assertTrue(records.stream().anyMatch(r -> r.endpoint()
                .equals(ManagementApiPaths.ACTION_FLUSH)));
        assertTrue(records.stream().anyMatch(r -> r.endpoint()
                .equals(ManagementApiPaths.ACTION_COMPACT)));
        assertTrue(records.stream().anyMatch(r -> r.endpoint()
                .equals(ManagementApiPaths.CONFIG)));
        assertTrue(records.stream().allMatch(r -> !r.digest().isBlank()));
        assertTrue(records.stream().allMatch(r -> r.timestamp() != null));
    }

    @Test
    void configGetRequiresIndexNameAndReadRoleCanAccess() throws Exception {
        final HttpResponse<String> missing = send("GET",
                ManagementApiPaths.CONFIG, null, "read-token");
        assertEquals(400, missing.statusCode());

        final HttpResponse<String> ok = send("GET",
                ManagementApiPaths.CONFIG + "?indexName=secure-index", null,
                "read-token");
        assertEquals(200, ok.statusCode());
    }

    private HttpResponse<String> send(final String method, final String path,
            final String body, final String token) throws Exception {
        HttpRequest.BodyPublisher publisher = HttpRequest.BodyPublishers
                .noBody();
        if (body != null) {
            publisher = HttpRequest.BodyPublishers.ofString(body);
        }
        final HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path))
                .header("Content-Type", "application/json");
        if (token != null) {
            builder.header("Authorization", "Bearer " + token);
        }
        final HttpRequest request = builder.method(method, publisher).build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
