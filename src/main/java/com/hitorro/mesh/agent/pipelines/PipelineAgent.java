/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.agent.pipelines;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.mesh.pipelines.model.JobSpec;
import com.hitorro.mesh.pipelines.model.NodeSpec;
import com.hitorro.mesh.pipelines.parse.JobSpecYaml;
import com.hitorro.mesh.pipelines.runtime.JobRunner;
import com.hitorro.mesh.pipelines.runtime.JobStatus;
import com.hitorro.mesh.pipelines.sinks.SinkRegistry;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.MessageHandler;
import io.nats.client.Nats;
import io.nats.client.Options;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

/**
 * Agent-side pipeline executor. Subscribes to the pipeline-dispatch
 * subject for this agent id; each incoming envelope carries a NodeSpec
 * (YAML/JSON) plus a resultSubject to publish rows into. Runs the node
 * via a local {@link JobRunner} wrapped in a synthetic single-node
 * {@link JobSpec}, then EOS's the result stream.
 *
 * <p>Envelope schema (JSON):</p>
 * <pre>
 *   {
 *     "taskId":        "…",
 *     "resultSubject": "mesh.query.result.q42.n1",
 *     "nodeSpec":      "&lt;YAML text of a hitorro-mesh-pipelines NodeSpec&gt;"
 *   }
 * </pre>
 *
 * <p>This is intentionally a separate subject namespace
 * ({@code mesh.agent.pipeline.&lt;agentId&gt;}) rather than the SQL
 * task subject — keeps TaskExecutor untouched and lets agents opt into
 * pipelines by adding this jar. Future work: driver-side
 * {@code PipelineScheduler} that fans one JobSpec's partitioned nodes
 * out to agents that advertise the {@code pipeline-node} capability.</p>
 */
public final class PipelineAgent implements AutoCloseable {

    public static final String PIPELINE_TASK_PREFIX = "mesh.agent.pipeline.";
    public static final String CAPABILITY           = "pipeline-node";

    private static final ObjectMapper JSON = new ObjectMapper();

    private final String agentId;
    private final Connection nats;
    private final SinkRegistry sinkRegistry;
    private final JobRunner runner;
    private final ExecutorService pool;
    private final Dispatcher dispatcher;
    private final AtomicLong received = new AtomicLong();

    public PipelineAgent(String agentId, String natsUrl, SinkRegistry sinkRegistry) throws Exception {
        if (agentId == null || agentId.isBlank()) {
            throw new IllegalArgumentException("PipelineAgent requires hitorro.mesh.pipelines.agent-id");
        }
        this.agentId = agentId;
        this.nats = Nats.connect(new Options.Builder().server(natsUrl).build());
        this.sinkRegistry = sinkRegistry;
        this.runner = new JobRunner(sinkRegistry);
        this.pool = Executors.newFixedThreadPool(
                Math.max(2, Runtime.getRuntime().availableProcessors() / 2),
                r -> { Thread t = new Thread(r, "pipeline-agent-" + agentId); t.setDaemon(true); return t; });

        MessageHandler handler = msg -> pool.submit(() -> onMessage(msg.getData()));
        this.dispatcher = nats.createDispatcher(handler);
        this.dispatcher.subscribe(PIPELINE_TASK_PREFIX + agentId);
    }

    public String agentId() { return agentId; }
    public long   received() { return received.get(); }

    private void onMessage(byte[] payload) {
        try {
            handleEnvelope(payload, runner, sinkRegistry,
                    (subject, bytes) -> nats.publish(subject, bytes));
            nats.flush(java.time.Duration.ofSeconds(2));
            received.incrementAndGet();
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
    }

    /**
     * Envelope-handling separated from the NATS boundary so it can be
     * driven from tests (or any other transport) without a running
     * NATS server. Production callers plug {@code nats::publish} in;
     * tests plug an in-memory queue.
     *
     * <p>Steps: decode envelope → parse NodeSpec text (bare or full
     * JobSpec) → wrap in a synthetic single-node {@link JobSpec} →
     * run via {@code runner} → drain the node's memory table to the
     * result subject one row at a time → publish a final EOS envelope
     * carrying {@code state} + {@code rowCount}. Row / EOS wire
     * shape matches what {@code PipelineScheduler} expects on the
     * consumer side.</p>
     *
     * @param publisher  called as {@code (subject, bytes)} for each row
     *                   envelope and once for the trailing EOS
     * @return the {@code taskId} parsed off the envelope — useful for
     *         test asserts and driver-side correlation
     */
    static String handleEnvelope(byte[] payload,
                                 JobRunner runner,
                                 SinkRegistry sinkRegistry,
                                 BiConsumer<String, byte[]> publisher) throws Exception {
        JsonNode env = JSON.readTree(payload);
        String taskId        = env.get("taskId").asText();
        String resultSubject = env.get("resultSubject").asText();
        String nodeSpecText  = env.get("nodeSpec").asText();

        NodeSpec node = extractSingleNode(nodeSpecText);
        JobSpec wrapper = new JobSpec(taskId, "1", List.of(node));
        JobStatus status = new JobStatus(taskId, taskId);

        runner.run(wrapper, status);

        // Publish node summary + row snapshot (bounded — big streams
        // should use a streaming sink like nats-sink in the spec itself
        // rather than piggybacking on the result subject).
        var memRows = sinkRegistry.memoryTable(node.id());
        for (JsonNode row : memRows) {
            publisher.accept(resultSubject,
                    JSON.writeValueAsBytes(JSON.createObjectNode().set("row", row)));
        }
        publisher.accept(resultSubject,
                JSON.writeValueAsBytes(JSON.createObjectNode()
                        .put("eos", true)
                        .put("state", status.state.name())
                        .put("rowCount", memRows.size())));
        return taskId;
    }

    /**
     * Accept either a bare NodeSpec YAML/JSON block OR a full JobSpec
     * document (single-node), unwrapping to the concrete {@link NodeSpec}
     * the agent will run. Package-private so
     * {@code PipelineAgentEnvelopeTest} can exercise both branches
     * without going through {@link #handleEnvelope}.
     */
    static NodeSpec extractSingleNode(String text) throws Exception {
        String trimmed = text.trim();
        if (trimmed.startsWith("{") && trimmed.contains("\"nodes\"")
                || trimmed.startsWith("job:")) {
            JobSpec js = JobSpecYaml.parse(trimmed);
            if (js.nodes().isEmpty()) throw new IllegalArgumentException("empty nodes[] in spec");
            return js.nodes().get(0);
        }
        JobSpec wrap = JobSpecYaml.parse("job: adhoc\nnodes:\n  - " + text.replaceAll("(?m)^", "    "));
        return wrap.nodes().get(0);
    }

    @Override
    public void close() {
        try { dispatcher.unsubscribe(PIPELINE_TASK_PREFIX + agentId); } catch (Exception ignored) { }
        try { nats.close(); } catch (Exception ignored) { }
        pool.shutdownNow();
        runner.close();
    }
}
