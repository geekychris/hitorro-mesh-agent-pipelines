# hitorro-mesh-agent-pipelines

Agent-side executor for [hitorro-mesh-pipelines](https://github.com/geekychris/hitorro-mesh-pipelines).
Drop this jar onto a `hitorro-mesh-agent-app` deployment and Spring Boot
autoconfig wires a `PipelineAgent` bean that subscribes to
`mesh.agent.pipeline.<agentId>` on NATS. Each incoming envelope carries
a `NodeSpec` (YAML or JSON) plus a result subject to publish rows into;
the agent runs the node via a local `JobRunner`, drains the resulting
memory table, and publishes one row envelope per output row terminated
by an EOS.

## When to add it

- You have `hitorro-mesh-pipelines` and want to distribute node
  execution across a mesh, not just run driver-local.
- The driver's `PipelineScheduler` (in the -pipelines module) uses the
  `pipeline-node` capability advertised by this bean to know which
  agents to dispatch to.

## Config

```yaml
hitorro:
  mesh:
    pipelines:
      enabled: true                            # opt-in — off by default
      agent-id: agent-us-east                  # required
      nats-url: nats://localhost:4222          # defaults
      home:     /var/hitorro/pipelines         # defaults to ~/.hitorro/pipelines
```

## Envelope wire shape

```json
{
  "taskId":        "pt-42",
  "resultSubject": "mesh.pipeline.result.pt-42",
  "nodeSpec":      "<YAML or JSON body of the NodeSpec / single-node JobSpec>"
}
```

Row payload → `{"row": <original json>}`; final envelope →
`{"eos": true, "state": "SUCCEEDED", "rowCount": N}`.

## Tests

9 tests in `PipelineAgentEnvelopeTest` — `handleEnvelope` is extracted
from the NATS boundary so the parse → run → publish path can be
driven directly without a running broker. Covers filter + project +
rename + set-field chains, zero-row nodes, both accepted spec shapes
(bare JSON JobSpec vs full YAML `job:`), and the missing-field / bad-
YAML error paths.
