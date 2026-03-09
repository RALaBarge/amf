# AMF Specification

**Version**: 0.1.0
**Status**: Working Draft
**License**: Apache 2.0

> AMF is an open specification for secure, local-first, multi-agent coordination. Agents discover one another via mDNS, communicate via A2A, expose capabilities via MCP, and coordinate via a structured CloudEvents-based event fabric backed by NATS — without requiring a cloud vendor.

---

## Design Principles

1. **Local-first** — discovery and coordination default to the local network; cloud is opt-in
2. **Passive discovery** — agents advertise; the local stack selectively connects. No outbound broadcasting by default
3. **Structured events only** — all coordination uses typed, schema-validated CloudEvents. Raw chain-of-thought is never shared across trust boundaries
4. **Disposable boundaries** — the DMZ watcher is stateless, one instance per connection, discarded when the connection closes
5. **Composable open tools** — Avahi, SPIFFE/SPIRE, NATS, OPA, A2A, MCP. No new infrastructure invented where existing standards suffice
6. **Schema-first** — all payloads conform to versioned JSON Schema. Unknown fields are ignored; breaking changes require a major version bump
7. **Observability by design** — every event carries trace IDs, provenance, and confidence scores

---

## Protocol Stack

| Layer | Function | Implementation |
|---|---|---|
| **Discovery** | Local presence advertising | Avahi mDNS/DNS-SD · `_amf-agent._tcp.local` · RFC 6763 |
| **Identity** | Agent authentication | SPIFFE/SPIRE workload IDs or W3C DIDs + VCs |
| **Capability** | Tool/resource exposure | MCP · served at `/.well-known/mcp.json` |
| **Communication** | Agent-to-agent tasking | A2A · served at `/.well-known/agent-card.json` |
| **Event Fabric** | Typed message routing | NATS JetStream · subject = CloudEvents `type` field |
| **Schema Registry** | Schema versioning | Apicurio Registry (or compatible) |
| **Policy** | Authorization | OPA Rego · evaluated against `auth_context` |
| **Auth** | Token issuance | OAuth2/OIDC · Keycloak or any compliant IdP |

---

## Discovery

### mDNS Service Type

```
_amf-agent._tcp.local
```

### Agent Registration

Agents register via Avahi on startup. The DNS-SD TXT record encodes a minimal `AgentRecord` as key=value pairs. Total TXT record size **MUST NOT exceed 512 bytes**.

**TXT record keys:**

| Key | Field | Required |
|---|---|---|
| `id` | `agent_id` (SPIFFE ID or DID) | Yes |
| `ep` | `endpoint` (base URL) | Yes |
| `proto` | `protocols_supported` (JSON array) | Yes |
| `tags` | `capability_tags` (JSON array) | No |
| `td` | `trust_domain` | No |
| `vis` | `visibility` | No |
| `v` | `version` (semver) | No |
| `status` | `status` | No |
| `card` | `card_url` | No |

### Discovery Flow

```
1. Agent starts → registers _amf-agent._tcp via Avahi
2. Local stack browses _amf-agent._tcp passively
3. On new advertisement:
   a. Deterministic validator checks: size ≤512B, valid keys, rate limit
   b. DMZ watcher LLM: summarizes + risk-scores (stateless, one per connection)
   c. Trusted coordinator receives sanitized summary only
   d. Coordinator decides: ignore / fetch card_url / initiate A2A session
4. capability.advertise event published to NATS fabric
```

### Agent Card

Full capability description served at:

```
GET /.well-known/agent-card.json
```

Conforms to the A2A `AgentCard` spec with an additional `x-amf` extension block:

```json
{
  "name": "...",
  "url": "...",
  "version": "1.0.0",
  "capabilities": { "streaming": true },
  "skills": [...],
  "x-amf": {
    "agent_record": { ... },
    "schema_version": "1.0.0",
    "protocols": ["A2A/1.0", "MCP/2024-11-05"],
    "trust_domain": "local",
    "nats_url": "nats://..."
  }
}
```

---

## Event Envelope

All AMF events conform to **CloudEvents v1.0** JSON format. This enables:
- Native ingestion by MS Fabric Eventstreams
- Transport inside A2A `Part.data` with no transformation
- NATS delivery where subject = CloudEvents `type` field

**JSON Schema**: [`schemas/event-envelope-1.0.0.json`](schemas/event-envelope-1.0.0.json)

### Required Fields (CloudEvents core)

| Field | Type | Description |
|---|---|---|
| `specversion` | `"1.0"` | CloudEvents version, always `"1.0"` |
| `id` | UUID string | Unique message ID |
| `source` | URI | SPIFFE ID or agent endpoint |
| `type` | string | AMF event type (= NATS subject) |
| `time` | RFC3339 | Event timestamp |
| `datacontenttype` | `"application/json"` | Always JSON |
| `data` | object | Payload wrapper (see below) |

### AMF Extension Fields (CloudEvents extensions, scalar types)

| Field | Type | Description |
|---|---|---|
| `amftraceid` | UUID string | Correlates messages across a task |
| `amfparentid` | UUID string | Parent message ID |
| `amftaskid` | UUID string | Logical task this event belongs to |
| `amfagentrole` | string | `coordinator` · `specialist` · `watcher` · `connector` |
| `amfvisibility` | string | `local` · `mesh` · `public` |
| `amfconfidence` | string | Confidence 0.0–1.0 encoded as string (CE has no float type) |
| `amfttl` | integer | Seconds until event expires |
| `amfschemaversion` | string | SemVer of envelope schema |
| `amftrustdomain` | string | Trust domain of sender |

> CloudEvents extension values must be scalar types. Arrays and objects go inside `data`.

### Data Field

```json
{
  "data": {
    "payload":       { },
    "auth_context":  { "identity": "spiffe://...", "scopes": [], "trust_domain": "local" },
    "artifact_refs": [],
    "evidence_refs": []
  }
}
```

### Event Types (NATS subjects)

| Type | Description |
|---|---|
| `amf.discovery.capability.advertise` | Agent announces presence and capabilities |
| `amf.discovery.agent.heartbeat` | Periodic liveness signal |
| `amf.task.announce` | Agent declares a new task it intends to perform |
| `amf.task.claim` | Agent takes ownership of an announced task |
| `amf.task.delegate` | Agent hands a task to another agent |
| `amf.task.progress` | Incremental progress update |
| `amf.task.blocked` | Agent is blocked; requests assistance |
| `amf.artifact.publish` | Agent publishes a versioned artifact |
| `amf.evidence.publish` | Agent shares evidence supporting a result |
| `amf.result.partial` | Partial result |
| `amf.result.final` | Final result with typed payload |
| `amf.policy.warning` | Policy monitor flags a potential violation |
| `amf.policy.deny` | Policy engine blocks an action |

### NATS Topic Model

```
amf.>                       — all events (observability, DMZ watcher)
amf.discovery.>             — discovery layer only
amf.task.>                  — task lifecycle
amf.result.>                — results
amf.policy.>                — policy events
```

---

## A2A Transport

AMF events travel inside A2A messages as `Part.data`:

```json
{
  "role": "agent",
  "parts": [{
    "data": { ...AMFEvent (CloudEvents v1.0)... },
    "media_type": "application/cloudevents+json"
  }]
}
```

The A2A `Task.contextId` maps to `amftraceid`. A2A task lifecycle states map to AMF event types:

| A2A State | AMF Event |
|---|---|
| `working` | `amf.task.progress` |
| `input-required` | `amf.task.blocked` |
| `completed` | `amf.result.final` |
| `failed` | `amf.policy.deny` |

---

## DMZ Watcher

The DMZ watcher is a **sacrificial boundary component**:

- **One instance per external connection** — spawned when a connection opens, discarded when it closes
- **Stateless** — no durable memory, no access to user context or privileged tools
- **Sandboxed** — may only read the inbound advertisement and write a structured summary
- **Output only** — emits a normalized summary + risk score to the trusted coordinator via NATS

### Processing Pipeline

```
[inbound advertisement]
        │
        ▼
[1. Deterministic validation]    ← no LLM involved
   size ≤ 512B, valid JSON,
   schema check, rate limit,
   allowlist, signature (if present)
        │
        ▼
[2. DMZ watcher LLM]             ← disposable, one per connection
   summarize, classify, risk-score
   forbidden: durable writes, tool calls, user context
        │
        ▼
[3. Trusted coordinator]         ← sees sanitized summary only
   policy check (OPA)
   routing decision
```

---

## AgentRecord Schema

**JSON Schema**: [`schemas/agent-record-1.0.0.json`](schemas/agent-record-1.0.0.json)

Minimal advertisement. Must be ≤512 bytes when TXT-encoded. Full details are in the agent card.

---

## Versioning

- Schemas follow SemVer: `major.minor.patch`
- New optional fields → `minor` bump
- Removed fields or changed semantics → `major` bump
- The `amfschemaversion` extension field on every event identifies the envelope schema version

---

## Open-Stack Component Licensing

| Component | License | Role |
|---|---|---|
| Avahi | LGPL 2.1 | mDNS/DNS-SD discovery |
| SPIFFE/SPIRE | Apache 2.0 | Workload identity |
| NATS | Apache 2.0 | Event fabric |
| Apicurio Registry | Apache 2.0 | Schema registry |
| OPA | Apache 2.0 | Policy engine |
| Keycloak | Apache 2.0 | OAuth2/OIDC IdP |
| A2A | Apache 2.0 | Agent communication protocol |
| MCP | Apache 2.0 | Capability exposure protocol |
