# AMF Stack Design

## Concrete Implementation

This document maps the AMF architecture layers to specific open-source tools and defines how they wire together.

```
┌─────────────────────────────────────────────────────┐
│                 AMF Open Stack                      │
├─────────────────────────────────────────────────────┤
│  LAYER            TOOL              LICENSE         │
├─────────────────────────────────────────────────────┤
│  Discovery        Avahi (mDNS/DNS-SD) LGPL 2.1     │
│  Identity         SPIFFE/SPIRE       Apache 2.0     │
│  Capability       MCP                Apache 2.0     │
│  Communication    A2A                Apache 2.0     │
│  Event Fabric     NATS + JetStream   Apache 2.0     │
│  Schema Registry  Apicurio           Apache 2.0     │
│  Policy           OPA                Apache 2.0     │
│  Auth             OAuth2/OIDC        open standards │
└─────────────────────────────────────────────────────┘
```

## Event Fabric: NATS Topic Model

All AMF events flow through NATS using a hierarchical subject namespace:

```
amf.<domain>.<event_type>

amf.discovery.capability.advertise
amf.discovery.agent.heartbeat
amf.task.announce
amf.task.claim
amf.task.progress
amf.task.complete
amf.artifact.publish
amf.evidence.publish
amf.result.partial
amf.result.final
amf.policy.warning
amf.policy.deny
```

Wildcard subscriptions:
- `amf.>` — everything (observability layer, DMZ watcher)
- `amf.task.>` — all task lifecycle events
- `amf.discovery.>` — all discovery events

## Event Envelope Schema

Every AMF event shares this envelope, regardless of type:

```json
{
  "message_id":        "uuid",
  "trace_id":          "uuid",
  "parent_message_id": "uuid | null",
  "task_id":           "uuid | null",
  "agent_id":          "string",
  "agent_role":        "coordinator | specialist | watcher | connector",
  "timestamp":         "RFC3339",
  "message_type":      "amf.task.announce",
  "visibility":        "local | mesh | public",
  "confidence":        0.0-1.0,
  "ttl":               60,
  "payload_type":      "string (MIME or custom type)",
  "payload":           {},
  "artifact_refs":     [],
  "evidence_refs":     [],
  "auth_context": {
    "identity":        "spiffe://trust-domain/agent/id",
    "scopes":          [],
    "trust_domain":    "string"
  },
  "schema_version":    "1.0"
}
```

## Agent Record (aDNS / Avahi)

Agents register via mDNS service type `_amf._tcp.local` with a minimal TXT record:

```
agent_id=<uuid>
endpoint=<url>
protocols=a2a,mcp
trust_domain=<string>
version=1.0
```

Full capability details are fetched on-demand from `/.well-known/agent-card.json` (A2A spec).

## DMZ Watcher

- Subscribes to `amf.discovery.>` only
- Stateless — no writes to durable storage
- Output: normalized summary published to `amf.internal.classified.<agent_id>`
- Reset policy: new instance per external connection (one connector LLM per session)

## Processing Pipeline for Inbound Advertisements

```
[mDNS advertisement arrives]
        │
        ▼
[1. Deterministic validation]
   - size limit (TXT record ≤ 256 bytes)
   - schema check
   - rate limit (max 10 ads/min per IP)
   - allowlist check
        │
        ▼
[2. DMZ watcher LLM]
   - summarize, classify, risk-score
   - forbidden: durable writes, tool invocation, user context
   - output: structured summary + confidence + risk_label
        │
        ▼
[3. Trusted coordinator]
   - receives only normalized summary
   - decides: ignore / fetch agent card / initiate A2A session
```

## Local Stack Startup Sequence

```
1. nats-server starts (event fabric)
2. coordinator starts, connects to NATS
3. avahi-daemon registers agent on local mDNS
4. coordinator subscribes amf.> for observability
5. watcher process starts, subscribes amf.discovery.>
6. HTTP observability server starts (tails amf.>)
```

## Observability Server

`GET /events` — SSE stream of all `amf.>` events in real-time
`POST /publish` — inject a test event into the fabric
`GET /health` — NATS connection status

Used for development, debugging, and integration testing.
