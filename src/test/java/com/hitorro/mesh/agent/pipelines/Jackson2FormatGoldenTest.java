/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.agent.pipelines;

import tools.jackson.databind.*; import tools.jackson.databind.node.*;
import com.hitorro.mesh.pipelines.runtime.JobRunner; import com.hitorro.mesh.pipelines.sinks.SinkRegistry;
import java.io.*; import java.nio.charset.StandardCharsets; import java.nio.file.*; import java.util.*;
/**
 * Pins the pipeline agent's NATS wire protocol to what 3.1.0 did on Jackson 2.21: task envelopes
 * in (full YAML and JSON job specs, numeric/null fields, trailing tokens, malformed or incomplete
 * envelopes; the bare-node form, which never parsed in 3.x, is pinned as it was) and the row and
 * EOS envelopes published back, including YAML 1.1 scalar typing of inline rows, plus
 * extractSingleNode. {@code jackson2-agentpipes.txt} is the output of the same code run on the
 * 3.1.0 sources (generator: ~/hitorro-j3/tools/Jackson2MeshAgentPipelinesGolden.java.txt in the
 * migration workspace).
 */
class Jackson2FormatGoldenTest {
  static final Map<String, String> out = new LinkedHashMap<>();
  static void p(String k, Object v){ out.put(k, String.valueOf(v).replace("\\","\\\\").replace("\n","\\n").replace("\r","\\r")); }
  static boolean jackson(Throwable t){ return t.getClass().getName().startsWith("com.fasterxml.jackson") || t.getClass().getName().startsWith("tools.jackson"); }
  static String desc(Throwable t, boolean top){ boolean ioe = t instanceof IOException;
    if (t.getClass() == IOException.class && t.getCause() != null && jackson(t.getCause())) t = t.getCause();
    String s = (top ? (ioe ? "IOE:" : "RTE:") : "") + (jackson(t) ? "jackson" : t.getClass().getSimpleName());
    if (!jackson(t) && t.getCause() != null) s += "<" + desc(t.getCause(), false) + ">"; return s; }
  static String tryIt(java.util.concurrent.Callable<Object> c){ try { return String.valueOf(c.call()); } catch(Throwable t){ return "!"+desc(t, true); } }
  static final String ROWS = "[{name: Alpha, pop: 1500000000, score: 1.10, ok: yes, tags: [a, b], n: ~, big: 12345678901234567890}, {name: \"B\u00e9/\", pop: \"12\", score: -0.0, ok: off, o: {k: 0x1F}}]";
  public static Map<String, String> compute(Path dir) throws Exception {
    out.clear();
    ObjectMapper om = com.hitorro.util.json.JsonMappers.json();
    String bare = "id: n1\npipeline:\n  source: {kind: inline, rows: " + ROWS + "}\n  steps: [{kind: set-field, name: s, value: 1.50}]\n  sinks: [{kind: memory-table, name: n1}]\n";
    String full = "job: j\nnodes:\n  - id: n2\n    pipeline:\n      source: {kind: inline, rows: " + ROWS + "}\n      sinks: [{kind: memory-table, name: n2}]\n";
    String json = "{\"job\":\"j\",\"nodes\":[{\"id\":\"n3\",\"pipeline\":{\"source\":{\"kind\":\"inline\",\"rows\":[{\"x\":1.10,\"y\":[1,\"2\",null],\"z\":12345678901}]},\"sinks\":[{\"kind\":\"memory-table\",\"name\":\"n3\"}]}}]}";
    String failing = "id: n4\npipeline:\n  source: {kind: ndjson-file, url: /does/not/exist.ndjson}\n  sinks: [{kind: memory-table, name: n4}]\n";
    String noMem = "id: n5\npipeline:\n  source: {kind: inline, rows: [{a: 1}]}\n  sinks: [{kind: counting, label: c}]\n";
    java.util.function.Function<String, String> job = n -> "job: j\nnodes:\n  - " + n.replace("\n", "\n    ").trim() + "\n";
    failing = job.apply(failing); noMem = job.apply(noMem);
    Map<String, String> envs = new LinkedHashMap<>();
    envs.put("bare", om.writeValueAsString(Map.of("taskId", "t-1", "resultSubject", "res.1", "nodeSpec", bare)));
    envs.put("full", "{\"taskId\":\"t-2\",\"resultSubject\":\"res.2\",\"nodeSpec\":" + om.writeValueAsString(full) + ",\"extra\":{\"x\":1}}");
    envs.put("json", "{\"taskId\":\"t-3\",\"resultSubject\":\"res.3\",\"nodeSpec\":" + om.writeValueAsString(json) + "}");
    envs.put("failing", "{\"taskId\":\"t-4\",\"resultSubject\":\"res.4\",\"nodeSpec\":" + om.writeValueAsString(failing) + "}");
    envs.put("noMemory", "{\"taskId\":\"t-5\",\"resultSubject\":\"res.5\",\"nodeSpec\":" + om.writeValueAsString(noMem) + "}");
    envs.put("numericTaskId", "{\"taskId\":42,\"resultSubject\":\"res.6\",\"nodeSpec\":" + om.writeValueAsString(job.apply(bare.replace("n1", "n6"))) + "}");
    envs.put("objectSpec", "{\"taskId\":\"t-7\",\"resultSubject\":\"res.7\",\"nodeSpec\":{\"id\":\"x\"}}");
    envs.put("missingSpec", "{\"taskId\":\"t-8\",\"resultSubject\":\"res.8\"}");
    envs.put("nullSubject", "{\"taskId\":\"t-9\",\"resultSubject\":null,\"nodeSpec\":" + om.writeValueAsString(job.apply(bare.replace("n1", "n9"))) + "}");
    envs.put("trailing", "{\"taskId\":\"t-10\",\"resultSubject\":\"res.10\",\"nodeSpec\":" + om.writeValueAsString(job.apply(bare.replace("n1", "n10"))) + "} trailing");
    envs.put("malformed", "{\"taskId\":");
    envs.put("emptyNodes", "{\"taskId\":\"t-12\",\"resultSubject\":\"r\",\"nodeSpec\":\"job: j\\nnodes: []\\n\"}");
    for (Map.Entry<String, String> e : envs.entrySet()) {
      Path home = dir.resolve(e.getKey()); Files.createDirectories(home);
      SinkRegistry reg = new SinkRegistry(home);
      List<String> pub = new ArrayList<>();
      p("envelope." + e.getKey(), tryIt(() -> { try (JobRunner r = new JobRunner(reg)) { return PipelineAgent.handleEnvelope(e.getValue().getBytes(StandardCharsets.UTF_8), r, reg,
          (subj, bytes) -> pub.add(subj + " " + new String(bytes, StandardCharsets.UTF_8))); } }));
      p("published." + e.getKey(), pub);
    }
    for (String t : new String[]{bare, full, json, "{\"nodes\":[]}", "id: q\nzzz: 1\npipeline: {source: {kind: inline, rows: []}, sinks: []}\n", "- not a map\n"})
      p("extract." + Integer.toHexString(t.hashCode()), tryIt(() -> PipelineAgent.extractSingleNode(t)));
    return out;
  }

  static Map<String, String> load(String name) throws IOException {
    Map<String, String> m = new LinkedHashMap<>();
    try (InputStream in = Jackson2FormatGoldenTest.class.getResourceAsStream(name)) {
      if (in == null) return m;
      for (String line : new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8).split("\n")) {
        if (line.isEmpty()) continue;
        int i = line.indexOf('=');
        m.put(line.substring(0, i), line.substring(i + 1));
      }
    }
    return m;
  }

  @org.junit.jupiter.api.Test
  void matchesJackson2Output(@org.junit.jupiter.api.io.TempDir Path dir) throws Exception {
    Map<String, String> expected = load("jackson2-agentpipes.txt");
    
    Map<String, String> actual = new LinkedHashMap<>();
    compute(dir).forEach((k, v) -> actual.put(k, v.replace("StringNode", "TextNode")));
    org.assertj.core.api.Assertions.assertThat(expected).hasSize(30);
    org.assertj.core.api.Assertions.assertThat(actual).containsExactlyEntriesOf(expected);
  }
}

