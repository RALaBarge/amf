# Agent Mesh Framework (AMF)

**v0.2.0** · Apache 2.0 · [SPEC.md](SPEC.md)

Open specification and reference implementation for secure, local-first, multi-agent coordination. Agents discover one another via mDNS, communicate via A2A, expose capabilities via MCP, and coordinate through a structured CloudEvents event fabric backed by NATS — no cloud vendor required.

## The Problem

Enterprise platforms (Microsoft Fabric, Salesforce MuleSoft Agent Fabric) are building multi-agent coordination layers, but both anchor to their cloud identity and event infrastructure. AMF is the neutral alternative: the same layered architecture, composed entirely from open-source tools, running on personal hardware or on-premises. AMF events are CloudEvents v1.0 — they ingest natively into MS Fabric Eventstreams and travel inside A2A `Part.data` without transformation.

## Protocol Stack

| Layer | Tool | License |
|---|---|---|
| Discovery | mDNS/DNS-SD (`_amf-agent._tcp.local`) | RFC 6763 / Avahi |
| Identity | SPIFFE/SPIRE or DIDs + VCs | Apache 2.0 |
| Capability | MCP (Streamable HTTP, 2024-11-05+) | Apache 2.0 |
| Communication | A2A (Linux Foundation) | Apache 2.0 |
| Event Fabric | NATS JetStream | Apache 2.0 |
| Policy | OPA (Rego) | Apache 2.0 |
| Auth | OAuth2/OIDC (Keycloak or any IdP) | Apache 2.0 |

## Quick Start

```bash
git clone https://github.com/RALaBarge/amf
cd amf/stack
go build -o amf-server .
./amf-server
# open http://localhost:8765
```

**Requires:** Go 1.24+, `nats-server` in PATH or `~/bin/`, `opa` in PATH or `~/.local/bin/` (optional).

Install nats-server:
```bash
curl -L https://github.com/nats-io/nats-server/releases/download/v2.10.24/nats-server-v2.10.24-linux-amd64.tar.gz | tar xz
mv nats-server-v2.10.24-linux-amd64/nats-server ~/bin/
```

Run a worker agent (separate terminal):
```bash
cd amf/stack
go build -o amf-worker ./cmd/worker
./amf-worker --name my-worker --tags text-summarize,code-review
```

## What's Running

### Coordinator (`amf-server`, port 8765)

Starts NATS (4222), OPA (8181), mDNS browser, DMZ watcher, and coordinator in a single binary.

| Endpoint | Description |
|---|---|
| `GET /` | Live event stream UI with Mesh Agents tab |
| `GET /events` | SSE stream of all `amf.>` events |
| `POST /publish` | Inject a test event |
| `GET /agents` | Currently discovered and admitted mesh agents |
| `GET /.well-known/agent-card.json` | A2A agent card |
| `GET /health` | NATS + OPA status |
| `POST /v1/chat/completions` | OpenAI-compatible chat — dispatches to mesh workers |
| `GET /v1/models` | Lists available agents as OpenAI model IDs |

The `/v1/chat/completions` endpoint accepts any OpenAI-compatible middleware or SDK. Model name `amf/<tag>` routes to workers with that capability tag (e.g. `amf/text-summarize`). Supports both streaming (`"stream": true`) and non-streaming.

### Worker Agent (`amf-worker`, port 8766)

```bash
./amf-worker --port 8766 --name my-worker --tags text-summarize,code-review --trust local
```

Registers on mDNS, subscribes to `amf.task.announce`, claims tasks matching its capability tags, publishes progress and final results. Serves `/.well-known/agent-card.json` and `POST /tasks/send` for direct A2A submission.

### Beigebox MCP Node

[Beigebox](../beigebox) is a local OpenAI-compatible LLM proxy. When `amf_mesh.enabled: true` is set in its `config.yaml`, it:

1. Registers on mDNS with `mcp=http://localhost:8001/mcp` in the TXT record
2. Publishes a NATS heartbeat on `amf.discovery.agent.heartbeat`
3. Serves `GET /.well-known/agent-card.json` with `x-amf.mcp_endpoint`

The AMF coordinator discovers beigebox via mDNS, validates it through the DMZ watcher and OPA, then admits it to the mesh registry.

## Security Model

All inbound advertisements are untrusted. Three layers before anything reaches the coordinator:

```
[mDNS advertisement]
        │
        ▼
[1. Deterministic validation]   — size ≤ 512B, schema, required fields
        │
        ▼
[2. DMZ watcher]                — one goroutine per advertisement, discarded immediately
   LLM risk-scoring (optional)    no shared state, no durable memory
        │
        ▼
[3. Trusted coordinator]        — sees WatcherSummary only, never raw advertisement
   OPA policy check               routing decision
```

The DMZ watcher is the core primitive: a fresh goroutine (or process) handles each inbound message and is garbage-collected when done. If the watcher is compromised, it has no accumulated state to leak and no persistent access to exploit. Set `ANTHROPIC_API_KEY` to enable Claude Haiku for LLM risk scoring; falls back to deterministic rules if unset.

## Repository Layout

```
SPEC.md                        canonical protocol specification
README.md                      this file
schemas/
  event-envelope-1.0.0.json   CloudEvents AMF envelope schema
  agent-record-1.0.0.json     mDNS advertisement schema
stack/                         Go reference implementation
  main.go                      coordinator, NATS, HTTP, mDNS
  event.go                     CloudEvents envelope + A2A types
  discovery.go                 mDNS registration and browsing
  watcher.go                   DMZ watcher (per-connection)
  policy.go                    OPA integration
  openai.go                    OpenAI-compatible API layer
  policies/
    allow_advertisement.rego   default admission policy
  cmd/
    worker/                    standalone specialist agent
    beigebox/                  MCP adapter template
2600/                          design discussion archive
```

## Specification

See [SPEC.md](SPEC.md) for the full protocol specification: event types, schemas, discovery flow, DMZ watcher architecture, task state machine, MCP integration, and A2A/CloudEvents compatibility.
