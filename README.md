# Agent Mesh Framework (AMF)

**v0.1.0** · Apache 2.0 · [SPEC.md](SPEC.md)

Open specification and reference implementation for secure, local-first, multi-agent coordination. Agents discover one another via mDNS, communicate via A2A, expose capabilities via MCP, and coordinate through a structured CloudEvents event fabric backed by NATS — no cloud vendor required.

## The Gap AMF Fills

Enterprise platforms (Microsoft Fabric, Salesforce MuleSoft) are building powerful multi-agent coordination layers, but both anchor to their cloud identity and event infrastructure. AMF is the neutral alternative: the same layered architecture, composed entirely from open-source tools, running on personal hardware or on-premises.

```
aDNS finds them · A2A talks to them · MCP describes capabilities · Event fabric coordinates everything
```

## Protocol Stack

| Layer | Tool | License |
|---|---|---|
| Discovery | Avahi (mDNS/DNS-SD, RFC 6763) | LGPL 2.1 |
| Identity | SPIFFE/SPIRE or DIDs + VCs | Apache 2.0 |
| Capability | MCP | Apache 2.0 |
| Communication | A2A | Apache 2.0 |
| Event Fabric | NATS JetStream | Apache 2.0 |
| Schema Registry | Apicurio Registry | Apache 2.0 |
| Policy | OPA (Rego) | Apache 2.0 |
| Auth | Keycloak / OIDC | Apache 2.0 |

## Compatibility

AMF events are **CloudEvents v1.0** envelopes. One format, three transports:

- **NATS** — subject = CloudEvents `type` field (`amf.task.announce`, etc.)
- **A2A** — drop the entire event into `Part.data` with `media_type: application/cloudevents+json`
- **MS Fabric Eventstreams** — native CloudEvents ingestion, no transformation needed

## Quick Start

```bash
git clone https://github.com/RALaBarge/amf
cd amf/stack
go build -o amf-server .
./amf-server
# open http://localhost:8765
```

Requires: Go 1.24+, `nats-server` in PATH or `~/bin/`

Install nats-server:
```bash
curl -L https://github.com/nats-io/nats-server/releases/download/v2.10.24/nats-server-v2.10.24-linux-amd64.tar.gz | tar xz
mv nats-server-v2.10.24-linux-amd64/nats-server ~/bin/
```

## What's Running

The reference server starts:
- **NATS** on `4222` — event fabric
- **mDNS** — registers `_amf-agent._tcp.local` and browses for peers
- **HTTP** on `8765`:
  - `GET /` — live event stream UI with Mesh Agents tab
  - `GET /events` — SSE stream of all `amf.>` events
  - `POST /publish` — inject a test event
  - `GET /agents` — currently discovered mesh agents
  - `GET /.well-known/agent-card.json` — A2A agent card
  - `GET /health` — NATS connection status

## Security Model

All remote advertisements are untrusted. Inbound advertisements pass through three layers before reaching the trusted coordinator:

1. **Deterministic validation** — size, schema, rate limit (no LLM involved)
2. **DMZ watcher** — one disposable LLM instance per connection, stateless, discarded on close
3. **Trusted coordinator** — sees only the sanitized summary

The DMZ watcher is the core security primitive: it absorbs risk at the boundary so the trusted core never touches untrusted content directly.

## Repository Layout

```
SPEC.md                  — canonical specification
schemas/                 — versioned JSON schemas
  event-envelope-1.0.0.json
  agent-record-1.0.0.json
stack/                   — Go reference implementation
  main.go
  event.go               — CloudEvents envelope + A2A types
  discovery.go           — mDNS registration and browsing
  DESIGN.md              — implementation notes
2600/                    — design discussion archive
```

## Specification

See [SPEC.md](SPEC.md) for the full protocol specification including event types, schema definitions, discovery flow, DMZ watcher architecture, and A2A/CloudEvents compatibility details.
