# Agent Mesh Framework (AMF)

**v0.3.0** · Apache 2.0 · [SPEC.md](SPEC.md)

Open specification and reference implementation for secure, local-first, multi-agent coordination. Agents discover one another via mDNS, communicate via A2A, expose capabilities via MCP, and coordinate through a structured CloudEvents event fabric backed by NATS — no cloud vendor required.

**⭐ Now with formal security verification**: Ring Queue architecture with TLA+ formal specifications and Dafny proofs covering fault tolerance, adversary isolation, traffic analysis resistance, crypto identity, and side-channel protection.

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

Run a beigebox LLM proxy (requires local Ollama or compatible backend):
```bash
go build -o beigebox ./cmd/beigebox
./beigebox --name my-box --model llama3.2
```

Discover agents beyond the local link via DNS-SD:
```bash
# coordinator
./amf-server --dns-domain agents.example.com

# agent — prints zone records to add on startup
./beigebox --dns-domain agents.example.com --public-host mybox.example.com
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

`cmd/beigebox` is the AMF mesh adapter for [BeigeBox](https://github.com/RALaBarge/beigebox) — a thin Go shim that handles mDNS registration, NATS heartbeats, and MCP tool exposure. The full BeigeBox project (Python, multi-backend routing, semantic caching, RAG, plugins) runs separately; this adapter connects it to the mesh.

> **Note:** If BeigeBox is your local backend, point `--backend` at its OpenAI-compatible endpoint. The adapter does not replace BeigeBox — it announces its existence and capabilities to the coordinator so it can be discovered and dispatched to.

```bash
go build -o beigebox ./cmd/beigebox

# Point at Ollama (default: http://localhost:11434, model: llama3.2)
./beigebox

# Specify backend and model
./beigebox --backend http://localhost:11434 --model qwen2.5:14b --name my-box

# Advertise into a DNS zone (see DNS-SD section below)
./beigebox --dns-domain agents.example.com --public-host mybox.example.com
```

On startup beigebox:
1. Registers on mDNS (`_amf-agent._tcp.local`) with `mcp=<url>` in the TXT record
2. Connects to NATS as `specialist` and publishes heartbeats every 30s on `amf.discovery.agent.heartbeat`
3. Serves `POST /mcp` — MCP JSON-RPC with tools: `chat`, `list_models`, `echo`
4. Proxies `POST /v1/chat/completions` directly to the local LLM backend
5. Serves `GET /.well-known/agent-card.json` with `x-amf.mcp_endpoint`
6. Prints DNS zone entries to stdout if `--dns-domain` is set

The coordinator discovers beigebox via mDNS (or DNS-SD), validates it through the DMZ watcher and OPA, then admits it to the mesh registry. The coordinator's federated `POST /mcp` endpoint then exposes beigebox's tools namespaced as `<agent_id>/chat` etc.

Environment variables:
| Variable | Default | Description |
|---|---|---|
| `AMF_SPECIALIST_PASS` | `amf-specialist-local` | NATS specialist password |
| `AMF_BACKEND_URL` | `http://localhost:11434` | LLM backend base URL |
| `AMF_BACKEND_MODEL` | `llama3.2` | Default model |

## Security Model

All inbound advertisements are untrusted regardless of how they arrive. Three layers before anything reaches the coordinator:

```
[mDNS advertisement]  [DNS-SD advertisement]
         │                      │
         └──────────┬───────────┘
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
CLAUDE.md                      development guidance for Claude Code
specs/                         ⭐ TLA+ formal specifications (Ring Queue)
  RingLatency.tla              fault tolerance with circuit breakers
  BoundedHistory.tla           adversary isolation with bounded history
  MessagePadding.tla           traffic analysis resistance
  CryptoIdentity.tla           Ed25519 signing, nonce tracking, quorum voting
  PolicyEnforcement.tla        static capability roles, rate limiting
  IsolationBoundary.tla        information flow isolation, respawn safety
  SideChannel.tla              constant-time guarantees
  *.cfg                        model checker configuration per spec
proofs/                        ⭐ Dafny formal proofs
  *_Proofs_Enhanced.dfy        verified lemmas: nonce uniqueness, replay safety, etc.
AMF_RING_ARCHITECTURE.md       Ring Queue architecture with threat model matrix
PHASE_3_STATUS.md              adversarial analysis & security fixes (15 attack vectors)
ADVERSARIAL_ANALYSIS_FIXES.md  detailed fix documentation for each attack
SESSION_SUMMARY.md             formalization session notes and design decisions
INDEX.md                       index of all formal specs and their properties
schemas/
  event-envelope-1.0.0.json   CloudEvents AMF envelope schema
  agent-record-1.0.0.json     mDNS advertisement schema
stack/                         Go reference implementation
  main.go                      coordinator, NATS, HTTP, mDNS, DNS-SD
  event.go                     CloudEvents envelope + A2A types
  discovery.go                 mDNS registration, mDNS browser, DNS-SD browser
  watcher.go                   DMZ watcher (per-connection)
  policy.go                    OPA integration
  openai.go                    OpenAI-compatible API layer
  identity.go                  SPIFFE/static identity provider
  nats_auth.go                 NATS ACL config (per-role credentials)
  policies/
    allow_advertisement.rego   default admission policy
  cmd/
    worker/                    standalone specialist agent
    beigebox/                  local LLM proxy + MCP node
2600/                          design discussion archive
```

## Discovery

AMF uses two complementary discovery mechanisms, both producing the same TXT record format and both routing through the same DMZ watcher pipeline.

### mDNS (local link)

Default. No configuration needed. Agents register on `_amf-agent._tcp.local` via Avahi/zeroconf and are visible to any coordinator on the same network segment.

### DNS-SD via unicast DNS (RFC 6763 §11)

For agents beyond the local link. Add DNS records to any zone you control, then point the coordinator at that zone.

**Coordinator:**
```bash
./amf-server --dns-domain agents.example.com
```
Polls `_amf-agent._tcp.agents.example.com` PTR records every 60s. Each discovered agent goes through the same DMZ watcher + OPA admission pipeline as mDNS.

**Agent (beigebox example):**
```bash
./beigebox --dns-domain agents.example.com --public-host mybox.example.com
```
Prints the DNS records to add on startup:
```
; PTR — service type enumeration
_amf-agent._tcp.agents.example.com. 300 IN PTR my-llm._amf-agent._tcp.agents.example.com.
; SRV — service location
my-llm._amf-agent._tcp.agents.example.com. 300 IN SRV 0 0 8768 mybox.example.com.
; TXT — agent metadata
my-llm._amf-agent._tcp.agents.example.com. 300 IN TXT "id=..." "ep=..." "mcp=..." ...
```

Add these to your zone once. The TXT record carries the same key=value pairs as the mDNS TXT record — same parser, same watcher, same admission policy.

**Why this works without a new standard:** RFC 6763 DNS-SD already defines unicast DNS as an equal peer to mDNS. The only difference is `.local.` multicast vs. a real domain over port 53. The AgentDNS IETF drafts are attempting to standardize this at internet scale; AMF uses the same mechanism today on any zone you control.

## Formal Verification & Security Architecture

AMF includes a formally verified **Ring Queue** architecture for secure agent communication in adversarial environments. Three core TLA+ specifications formalize critical security layers:

### Ring Queue Layers (Formally Verified)

```
┌─────────────────────────────────────────┐
│ MESSAGE PADDING (constant 512 bytes)    │ ← Traffic analysis resistance
├─────────────────────────────────────────┤
│ BOUNDED HISTORY (20 hot + archive)      │ ← Adversary isolation
├─────────────────────────────────────────┤
│ RING QUEUE (A→B→C→A topology)           │ ← Fault tolerance & healing
└─────────────────────────────────────────┘
```

**Key Properties Proven**:
- **Fault Tolerance** (RingLatency.tla): Ring topology with latency detection; circuit breaker removes failed agents without halting the system
- **Adversary Isolation** (BoundedHistory.tla): Sliding window of recent messages + archive with separate keys; compromise of active queue ≠ access to archive
- **Traffic Analysis Resistance** (MessagePadding.tla): All messages fixed 512 bytes; padding pre-generated at startup
- **Crypto Identity** (CryptoIdentity.tla): Ed25519 deterministic signing, monotonic nonces, replay prevention, redundant attestation with quorum voting
- **Policy Enforcement** (PolicyEnforcement.tla): Static capability roles, rate limiting, no delegation
- **Isolation Boundary** (IsolationBoundary.tla): Information flow isolation; DMZ respawn with grace period + dedup; prompt injection sanitization predicate
- **Side-Channel Protection** (SideChannel.tla): Constant-time processing, no observable timing leaks

**Files**:
- [AMF_RING_ARCHITECTURE.md](AMF_RING_ARCHITECTURE.md) — Complete architecture guide with threat model matrix
- [PHASE_3_STATUS.md](PHASE_3_STATUS.md) — Security fixes and verification status (15 adversarial attacks covered)
- [specs/](specs/) — TLA+ specifications and `.cfg` files for model checking
- [proofs/](proofs/) — Dafny formal proofs of crypto and isolation properties

**Implementation Guide**: See [ADVERSARIAL_ANALYSIS_FIXES.md](ADVERSARIAL_ANALYSIS_FIXES.md) for integration patterns and attack scenario coverage.

---

## Threat Model & Attacks Addressed (Ring Queue)

The Ring Queue formally defends against these adversarial scenarios:

| Attack | Defense Layer | How It Works |
|--------|---------------|--------------|
| **Traffic analysis** | Message Padding | All messages exactly 512 bytes; size leaks nothing |
| **Archive breach** | Bounded History | Compromised active queue ≠ access to archive (different keys) |
| **Slowness DoS** | Ring Topology + Backoff | Exponential backoff tolerates lateness; only after 3 failures is agent removed |
| **TEE attestation forged** | Redundant Attestation | Two independent services; manual override if both fail |
| **Nonce overflow replay** | Crypto Identity | Nonce counter bounded < MAX_NONCE; overflow prevents wrap-around attacks |
| **DMZ respawn confusion** | Respawn Grace Period | 3-5 tick grace period; old DMZ still processes; dedup log prevents duplication |
| **Prompt injection** | Formal Sanitization | Predicate blocks known injection markers (`<|im_start|>`, `[SYSTEM]`, etc.) |
| **Privilege escalation** | Static Roles | Roles immutable at runtime; no delegation ever |
| **Message flooding** | Rate Limiting | Per-agent ceiling (100 msgs/sec); attacker can't overwhelm |
| **Nonce reuse** | Crypto Identity | Monotonic counters + nonce uniqueness formally proven |

**Coverage**: 15+ adversarial attacks modeled and defended. See [ADVERSARIAL_ANALYSIS_FIXES.md](ADVERSARIAL_ANALYSIS_FIXES.md) for complete matrix.

---

## Ring Queue: Security Layer Decisions

The Ring Queue architecture includes critical security decisions addressing 15 adversarial attacks. Key tradeoffs:

| Component | Decision | Rationale | Cost |
|-----------|----------|-----------|------|
| **Message Padding** | All 512 bytes (constant size) | Prevents traffic analysis | +43 bytes overhead |
| **Attestation** | Redundant (2 services) + manual override | No single point of failure | Slightly more complex state |
| **Circuit Breaker** | Exponential backoff (3 chances) | Prevents slowness DoS | More state tracking |
| **Roles** | Static, immutable, no delegation | Eliminates privilege escalation | Less flexible at runtime |
| **Respawn** | 3-phase with grace period + dedup | Prevents message loss/confusion | Extra logging overhead |
| **Sanitization** | Formal predicate (blocks known markers) | Injection markers formally rejected | Guard enforcement overhead |
| **Rate Limits** | Per-agent ceiling (100 msgs/sec) | Bounds message flooding | Minor per-message cost |

**Full analysis**: [ADVERSARIAL_ANALYSIS_FIXES.md](ADVERSARIAL_ANALYSIS_FIXES.md) documents all 15 attacks and defenses. Each decision is paired with a TLA+ formal specification that has been model-checked (4,295+ states verified).

---

## Mesh Coordination Architecture Decisions

The original AMF coordination layer decisions follow. See [2600/open-decisions-session-1.md](2600/open-decisions-session-1.md) for full rationale.

**Discovery**
- Agents that declare `MCP/...` in `proto` MUST include `mcp=<url>` in their TXT record. Omitting it fails deterministic validation before the watcher runs.
- `nats_url` is included in agent cards only for coordinators and fabric relay nodes. Workers and specialists omit it.

**Identity and authority**
- `local` visibility events: self-asserted delegation chains are accepted. OPA checks structural validity only.
- `mesh` and `public` visibility events: a verifiable SVID (`svid_verified: true`) is required. StaticIdentity deployments are restricted to `local` visibility.
- Delegated scopes must be a strict subset of the delegator's effective scopes. Coordinators hold the full scope vocabulary implicitly.
- SPIFFE fallback policy: `AMF_IDENTITY_MODE=spiffe` with no socket → hard fail. `SPIFFE_ENDPOINT_SOCKET` present but unavailable → fall back to static for `local` agents only; `mesh`/`public` denied at OPA. Neither set → static, `local` only.

**NATS topology**
- v1: single NATS server with per-role username/password ACLs (coordinator, specialist, watcher, connector). Migration path to per-trust-domain account separation is a config change, not a protocol change.

**Task claiming**
- Workers subscribe to `amf.task.announce.<capability_tag>` as a NATS queue group keyed `workers.<capability_tag>`. NATS guarantees single delivery; no coordinator arbitration is needed for claim races.

**Task lifecycle**
- TTL expiry: coordinator emits `amf.policy.warning`, signals requester via `reply_subject` if set, then discards. Optional `max_retries` and `retry_delay_seconds` in the task payload enable bounded republishing before escalation.
- Delegation: cycle detection (same agent appears twice in chain) is mandatory and non-overridable. Max delegation depth defaults to 5, configurable in OPA policy.
- Reply subjects: MUST match `amf.internal.reply.<task_id>`. Enforced by NATS ACL on specialist credentials and validated by the coordinator before routing.

**Admission policy**
- Risk score thresholds are defined in the OPA data document per trust domain (`data.policy.thresholds`). Defaults: `local` → 0.7, `mesh` → 0.3, `public` → 0.1.
- Watcher field cross-verification: after receiving a WatcherSummary, the coordinator independently re-parses the raw advertisement and verifies `original_agent_id`, `endpoint`, `protocols_supported`, `trust_domain`, and `card_url` against TXT record fields. Discrepancies floor `risk_score` to 1.0 and emit a policy warning.
- Capability tags MUST match `[a-z0-9-]+`. Tags outside this charset are rejected at deterministic validation. The watcher LLM receives advertisement content in a data-role turn, not the instruction turn.

**Connector role**
- Connector NATS credentials grant publish rights to `amf.internal.raw` only. Rate limiting is deferred to the first concrete connector implementation; external rate limiting (gateway, nginx) is recommended in the interim.

**A2A interop**
- NATS subscription is the canonical push mechanism. A2A push notifications (SSE callback URLs) are not supported in v1. A bridge adapter is a v2 roadmap item.

**Watcher output integrity**
- When SPIFFE is active, each watcher goroutine is issued a short-lived JWT-SVID at spawn time and MUST sign its WatcherSummary. The coordinator rejects unsigned output with `amf.policy.deny`. When SPIFFE is not active, the coordinator emits `amf.policy.warning` on every admission cycle (`watcher_output_unverified`). This warning is not suppressible without explicitly setting `AMF_WATCHER_INTEGRITY_WARN=false`. The integrity gap is surfaced, not hidden.

**MCP routing**
- The coordinator exposes a single `POST /mcp` endpoint (Model C — federated aggregate) that aggregates all admitted agents' tools. All external LLM clients connect here. OPA policy runs per call, all calls are logged. The internal dispatch layer (Model B proxy) remains as the mechanism the coordinator uses to forward calls to individual agents. Model A (direct client access, coordinator out of the call path) is rejected — it removes the coordinator from the audit and policy path.
- Tool names are namespaced `<agent_id>/<tool_name>` (guaranteed unique). Agents may declare a short alias in `x-amf.tool_alias`; aliases are registered first-come, collision = hard reject (both aliases rejected, both fall back to agent ID namespace, `amf.policy.warning` emitted).

**MCP call authentication**
- Three tiers, in priority order:
  1. **SPIFFE active:** coordinator presents JWT-SVID as `Authorization: Bearer` on every call. Agent SHOULD verify against trust bundle.
  2. **No SPIFFE, `https://`:** TOFU TLS fingerprint model (SSH-style). Coordinator records the agent's TLS cert fingerprint (SHA-256) at admission and verifies on every call. Agent records coordinator's fingerprint on first contact via `X-AMF-Coordinator-Fingerprint` header. Fingerprint mismatch → call rejected, `amf.policy.warning` emitted.
  3. **No SPIFFE, `http://`:** `amf.policy.warning` with reason `mcp_call_unauthenticated` emitted on every individual call (not just at startup). Blockable with `AMF_MCP_REQUIRE_TLS=true`, which denies admission to any agent with a plaintext MCP endpoint.

---

## Specification

See [SPEC.md](SPEC.md) for the full protocol specification: event types, schemas, discovery flow, DMZ watcher architecture, task state machine, MCP integration, and A2A/CloudEvents compatibility.
