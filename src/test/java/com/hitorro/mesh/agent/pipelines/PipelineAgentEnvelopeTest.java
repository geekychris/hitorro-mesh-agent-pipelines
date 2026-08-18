/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.agent.pipelines;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.mesh.pipelines.model.NodeSpec;
import com.hitorro.mesh.pipelines.runtime.JobRunner;
import com.hitorro.mesh.pipelines.sinks.SinkRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Exercises {@link PipelineAgent#handleEnvelope} — the NATS-independent
 * core of the agent — without a running NATS server. Each test drives
 * a wire-shape envelope through the same path a real dispatched
 * message takes, captures the emitted publish calls, and asserts:
 *
 * <ul>
 *   <li>Envelope parsing (taskId / resultSubject / nodeSpec text)</li>
 *   <li>Both accepted nodeSpec text shapes: bare NodeSpec and full JobSpec</li>
 *   <li>The real {@link JobRunner} executes the node and produces rows</li>
 *   <li>Publisher call sequence: one row envelope per output row,
 *       terminated by an EOS envelope carrying {@code state} +
 *       {@code rowCount}</li>
 *   <li>Error paths — missing envelope fields + empty-nodes spec</li>
 * </ul>
 *
 * <p>Fanning out to the JVM's NATS client isn't tested here — that's
 * a boundary integration concern. What the driver-side
 * {@code PipelineScheduler} actually consumes off the result subject
 * is the byte payloads we assert on below, so anything that changes
 * this wire shape breaks a real deployment.</p>
 */
class PipelineAgentEnvelopeTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @TempDir Path home;
    SinkRegistry registry;
    JobRunner runner;
    RecordingPublisher publisher;

    @BeforeEach
    void setUp() {
        registry = new SinkRegistry(home);
        runner   = new JobRunner(registry);
        publisher = new RecordingPublisher();
    }

    @AfterEach
    void tearDown() {
        runner.close();
    }

    // -------------------------------------------------- extractSingleNode

    @Test
    void extractSingleNode_jsonWrappedFullJob_unwrapsFirstNode() throws Exception {
        // The shape PipelineScheduler actually sends: JobSpecYaml.toJson(...)
        // — a JSON-wrapped one-node JobSpec. This is the hot path in
        // production, so tests must cover it verbatim.
        String jsonWrapped = """
                {"id":"pt-1","version":"1","nodes":[
                  {"id":"my-node","pipeline":{
                    "source":{"kind":"inline","rows":[{"n":1}]},
                    "sinks":[]
                  }}
                ]}
                """;
        NodeSpec node = PipelineAgent.extractSingleNode(jsonWrapped);
        assertThat(node.id()).isEqualTo("my-node");
        assertThat(node.pipeline().source()).isNotNull();
        // Explicit sinks list is empty; NodeRunner auto-attaches a
        // memory-table:<nodeId> sink at execution time.
        assertThat(node.pipeline().sinks()).isEmpty();
    }

    @Test
    void extractSingleNode_fullJobYaml_unwrapsFirstNode() throws Exception {
        String full = """
                job: wrapped
                version: "1"
                nodes:
                  - id: first-node
                    pipeline:
                      source: {kind: inline, rows: [{"a": 1}]}
                      sinks: [{kind: memory-table, name: first-node}]
                """;
        NodeSpec node = PipelineAgent.extractSingleNode(full);
        assertThat(node.id()).isEqualTo("first-node");
    }

    @Test
    void extractSingleNode_emptyNodes_isRejected() {
        String empty = "job: broken\nversion: \"1\"\nnodes: []\n";
        assertThatThrownBy(() -> PipelineAgent.extractSingleNode(empty))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("empty nodes");
    }

    // -------------------------------------------------- handleEnvelope: happy path

    @Test
    void handleEnvelope_runsNode_publishesRowsThenEos() throws Exception {
        byte[] payload = envelope("t1", "mesh.result.q99.n1", """
                {"id":"pt-1","version":"1","nodes":[
                  {"id":"filter-node","pipeline":{
                    "source":{"kind":"inline","rows":[
                      {"name":"apple","qty":5},
                      {"name":"banana","qty":1},
                      {"name":"cherry","qty":8}
                    ]},
                    "steps":[{"kind":"filter","expr":"qty > 3"}],
                    "sinks":[]
                  }}
                ]}
                """);

        String taskId = PipelineAgent.handleEnvelope(payload, runner, registry, publisher);

        assertThat(taskId).isEqualTo("t1");
        // 2 rows survive the filter (qty > 3) + 1 EOS.
        assertThat(publisher.records).hasSize(3);
        assertThat(publisher.records).allSatisfy(r ->
                assertThat(r.subject()).isEqualTo("mesh.result.q99.n1"));

        List<JsonNode> rowEnvelopes = publisher.rowEnvelopes();
        assertThat(rowEnvelopes).hasSize(2);
        assertThat(rowEnvelopes).extracting(r -> r.get("row").get("name").asText())
                .containsExactly("apple", "cherry");

        JsonNode eos = publisher.eosEnvelope();
        assertThat(eos.get("eos").asBoolean()).isTrue();
        assertThat(eos.get("state").asText()).isEqualTo("SUCCEEDED");
        assertThat(eos.get("rowCount").asInt()).isEqualTo(2);
    }

    @Test
    void handleEnvelope_transformSteps_flowThrough() throws Exception {
        // Prove the whole step chain (project + rename + set-field)
        // runs on the agent side, not just filter.
        byte[] payload = envelope("t2", "mesh.result.q99.n2", """
                {"id":"pt-2","version":"1","nodes":[
                  {"id":"xform","pipeline":{
                    "source":{"kind":"inline","rows":[
                      {"a":1,"b":2,"c":3},
                      {"a":4,"b":5,"c":6}
                    ]},
                    "steps":[
                      {"kind":"project","cols":["a","b"]},
                      {"kind":"rename","from":"a","to":"alpha"},
                      {"kind":"set-field","name":"source","value":"agent"}
                    ],
                    "sinks":[]
                  }}
                ]}
                """);

        PipelineAgent.handleEnvelope(payload, runner, registry, publisher);

        List<JsonNode> rows = publisher.rowEnvelopes();
        assertThat(rows).hasSize(2);
        JsonNode first = rows.get(0).get("row");
        assertThat(first.get("alpha").asInt()).isEqualTo(1);
        assertThat(first.get("b").asInt()).isEqualTo(2);
        assertThat(first.has("c")).isFalse();     // dropped by project
        assertThat(first.get("source").asText()).isEqualTo("agent");
    }

    @Test
    void handleEnvelope_zeroRowNode_stillEmitsEos() throws Exception {
        // A node whose source is empty must still terminate the stream
        // with an EOS so the driver-side scheduler doesn't hang on the
        // 60s timeout.
        byte[] payload = envelope("t3", "mesh.result.q99.n3", """
                {"id":"pt-3","version":"1","nodes":[
                  {"id":"empty","pipeline":{
                    "source":{"kind":"inline","rows":[]},
                    "sinks":[]
                  }}
                ]}
                """);

        PipelineAgent.handleEnvelope(payload, runner, registry, publisher);

        assertThat(publisher.rowEnvelopes()).isEmpty();
        JsonNode eos = publisher.eosEnvelope();
        assertThat(eos.get("eos").asBoolean()).isTrue();
        assertThat(eos.get("rowCount").asInt()).isEqualTo(0);
    }

    @Test
    void handleEnvelope_fullJobWrapper_alsoWorks() throws Exception {
        // Agent must accept both flavours — proves the driver can send
        // either shape without a serialisation dance.
        byte[] payload = envelope("t4", "mesh.result.q99.n4", """
                job: dispatched
                version: "1"
                nodes:
                  - id: full-form
                    pipeline:
                      source: {kind: inline, rows: [{n: 42}]}
                      sinks: []
                """);
        // YAML `job:` prefix triggers the parse-as-JobSpec branch — even
        // though the scheduler always sends JSON, the code accepts YAML
        // too; this proves both branches route through the runner.

        PipelineAgent.handleEnvelope(payload, runner, registry, publisher);

        assertThat(publisher.rowEnvelopes()).hasSize(1);
        assertThat(publisher.eosEnvelope().get("rowCount").asInt()).isEqualTo(1);
    }

    // -------------------------------------------------- handleEnvelope: error paths

    @Test
    void handleEnvelope_missingField_throws() {
        // No resultSubject in the envelope — must fail fast, not silently
        // publish nothing.
        String bad = "{\"taskId\":\"t5\",\"nodeSpec\":\"{}\"}";
        assertThatThrownBy(() ->
                PipelineAgent.handleEnvelope(bad.getBytes(), runner, registry, publisher))
                .isInstanceOf(Exception.class);
        assertThat(publisher.records).isEmpty();
    }

    @Test
    void handleEnvelope_badYamlInSpec_throws() throws Exception {
        byte[] payload = envelope("t6", "mesh.result.q99.n6", "not: {valid: yaml [");
        assertThatThrownBy(() ->
                PipelineAgent.handleEnvelope(payload, runner, registry, publisher))
                .isInstanceOf(Exception.class);
    }

    // -------------------------------------------------- helpers

    private byte[] envelope(String taskId, String resultSubject, String nodeSpecText) throws Exception {
        return JSON.writeValueAsBytes(JSON.createObjectNode()
                .put("taskId", taskId)
                .put("resultSubject", resultSubject)
                .put("nodeSpec", nodeSpecText));
    }

    /**
     * Captures every (subject, bytes) publish the agent makes. Parses
     * on read so tests can filter row vs EOS trivially.
     */
    static final class RecordingPublisher implements java.util.function.BiConsumer<String, byte[]> {
        final List<Record> records = new ArrayList<>();

        @Override public void accept(String subject, byte[] bytes) {
            records.add(new Record(subject, bytes));
        }

        List<JsonNode> rowEnvelopes() {
            List<JsonNode> out = new ArrayList<>();
            for (Record r : records) {
                try {
                    JsonNode n = JSON.readTree(r.bytes());
                    if (n.has("row")) out.add(n);
                } catch (Exception e) { throw new RuntimeException(e); }
            }
            return out;
        }

        JsonNode eosEnvelope() {
            for (Record r : records) {
                try {
                    JsonNode n = JSON.readTree(r.bytes());
                    if (n.has("eos") && n.get("eos").asBoolean()) return n;
                } catch (Exception e) { throw new RuntimeException(e); }
            }
            throw new AssertionError("no EOS envelope in " + records.size() + " records");
        }

        record Record(String subject, byte[] bytes) { }
    }
}
