# Agent Mesh Framework (AMF) — Specification & Reference Implementation

**Repository**: https://github.com/RALaBarge/amf  
**Version**: 0.1.0-draft  
**License**: Apache 2.0  
**Status**: Working Draft — Community Feedback Welcome  

> AMF is an open specification for secure, local-first, multi-agent coordination. It defines schemas, protocols, and trust boundaries for agents to discover, advertise, and collaborate without requiring a central cloud provider.

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Design Principles](#design-principles)
3. [Core Schemas](#core-schemas)
   - [EventEnvelope](#eventenvelope)
   - [AgentRecord (aDNS Advertisement)](#agentrecord-adns-advertisement)
4. [Protocol Layers](#protocol-layers)
5. [Discovery Flow](#discovery-flow)
6. [Trust & Security Model](#trust--security-model)
7. [Versioning & Extension](#versioning--extension)
8. [Reference Implementation](#reference-implementation)
   - [Directory Structure](#directory-structure)
   - [Key File: DMZ Watcher](#key-file-dmz_watcherwatcher_processpy)
   - [Key File: OPA Policy Example](#key-file-policiesallow_task_claimrego)
9. [Open-Stack Component Mapping](#open-stack-component-mapping)
10. [Licensing Overview](#licensing-overview)
11. [Strategic Positioning](#strategic-positioning)
12. [Next Steps](#next-steps)
13. [Contributing](#contributing)

---

## Executive Summary

The Agent Mesh Framework addresses a critical gap in the emerging multi-agent ecosystem: **a vendor-neutral, local-first, privacy-preserving coordination layer**. While enterprises like Microsoft and Salesforce are building powerful agent platforms anchored to their clouds, neither provides a specification for:

- Local-network agent discovery without cloud identity providers
- Disposable security boundaries for untrusted external advertisements
- Structured event coordination that avoids raw "thought sharing"
- Composable open-source tools that run entirely on-premises or on personal hardware

AMF fills this gap by defining:
1. **Minimal, versioned JSON Schemas** for events and agent advertisements
2. **A layered protocol architecture** that delegates to existing open standards (Avahi, SPIFFE, NATS, OPA, A2A, MCP)
3. **A security model** centered on passive discovery, deterministic validation, and sacrificial boundary components
4. **A reference implementation** demonstrating the full stack in <500 lines of Python

This document serves as both specification and implementation guide. It is intentionally minimal: implementers may extend functionality, but MUST NOT violate the core principles or schema contracts.

---

## Design Principles

1. **Local-first**: Discovery and coordination default to the local network; cloud integration is optional and explicit.
2. **Passive discovery**: Agents advertise capabilities; the local stack selectively inspects and initiates connections. No outbound broadcasting by default.
3. **Structured events only**: All coordination occurs via typed, schema-validated events. Raw chain-of-thought or unbounded reasoning is NEVER shared across trust boundaries.
4. **Disposable boundaries**: The DMZ watcher component is stateless, frequently reset, and explicitly untrusted. It absorbs risk; it does not hold authority.
5. **Composable open tools**: Leverage existing, well-maintained open-source projects (Avahi, SPIFFE, NATS, OPA) rather than reinventing infrastructure.
6. **Schema-first**: All payloads conform to versioned JSON Schema. Unknown fields are ignored; breaking changes require major version bumps.
7. **Observability by design**: Every event includes trace IDs, provenance metadata, and confidence scores to enable auditing, replay, and debugging.

---

## Core Schemas

### EventEnvelope

The backbone of all agent communication. Every message on the event fabric MUST conform to this schema.

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://amf.spec/event-envelope/1.0",
  "title": "EventEnvelope",
  "type": "object",
  "required": ["message_id", "trace_id", "timestamp", "message_type", "agent_id", "payload"],
  "properties": {
    "message_id": { "type": "string", "format": "uuid", "description": "Unique ID for this message" },
    "trace_id": { "type": "string", "format": "uuid", "description": "Correlates messages across a task lifecycle" },
    "parent_message_id": { "type": ["string", "null"], "format": "uuid", "description": "ID of the message this responds to" },
    "task_id": { "type": ["string", "null"], "format": "uuid", "description": "Logical task this event belongs to" },
    "agent_id": { "type": "string", "description": "DID or SPIFFE ID of the sending agent" },
    "agent_role": { "type": "string", "enum": ["coordinator", "worker", "watcher", "policy"], "description": "Functional role of the sender" },
    "timestamp": { "type": "string", "format": "date-time", "description": "ISO 8601 UTC timestamp" },
    "message_type": { "type": "string", "enum": [
      "capability.advertise", "agent.heartbeat", "task.announce", "task.claim",
      "task.delegate", "task.progress", "task.blocked", "artifact.publish",
      "evidence.publish", "result.partial", "result.final",
      "policy.warning", "policy.deny"
    ], "description": "Typed event category" },
    "visibility": { "type": "string", "enum": ["public", "trust-domain", "private"], "default": "public" },
    "confidence": { "type": "number", "minimum": 0, "maximum": 1, "description": "Sender's confidence in payload accuracy" },
    "ttl": { "type": "integer", "minimum": 0, "description": "Seconds until this event expires" },
    "payload_type": { "type": "string", "description": "JSON Schema $id for the payload structure" },
    "payload": { "type": "object", "description": "Event-specific data; schema defined by payload_type" },
    "artifact_refs": { "type": "array", "items": { "type": "string", "format": "uri" }, "description": "Handles to external artifacts" },
    "evidence_refs": { "type": "array", "items": { "type": "string", "format": "uri" } },
    "auth_context": { "type": "object", "description": "OAuth2/SPIFFE claims relevant to authorization" },
    "schema_version": { "type": "string", "pattern": "^\\d+\\.\\d+\\.\\d+$", "description": "SemVer of this envelope schema" }
  },
  "additionalProperties": false
}
```

### AgentRecord (aDNS Advertisement)

Minimal metadata published via mDNS/DNS-SD for local discovery.

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://amf.spec/agent-record/1.0",
  "title": "AgentRecord",
  "type": "object",
  "required": ["agent_id", "endpoint", "protocols_supported"],
  "properties": {
    "agent_id": { "type": "string", "description": "DID or SPIFFE ID" },
    "display_name": { "type": "string", "maxLength": 64 },
    "endpoint": { "type": "string", "format": "uri", "description": "Base URL for A2A/MCP endpoints" },
    "protocols_supported": { "type": "array", "items": { "type": "string", "enum": ["A2A/1.0", "MCP/2024-11-05"] } },
    "capability_tags": { "type": "array", "items": { "type": "string" }, "description": "Low-risk keywords: ['code-exec', 'web-search', 'pdf-parse']" },
    "trust_domain": { "type": "string", "description": "Logical boundary: 'personal', 'team', 'public'" },
    "visibility": { "type": "string", "enum": ["public", "trust-domain", "private"], "default": "public" },
    "subscription_topics": { "type": "array", "items": { "type": "string" }, "description": "Event types this agent wants to receive" },
    "publish_topics": { "type": "array", "items": { "type": "string" }, "description": "Event types this agent may emit" },
    "latency_class": { "type": "string", "enum": ["realtime", "fast", "batch"] },
    "cost_class": { "type": "string", "enum": ["free", "low", "high"] },
    "auth_requirements": { "type": "object", "description": "Expected auth method: {'type': 'oauth2', 'scopes': ['read:tasks']}" },
    "version": { "type": "string", "pattern": "^\\d+\\.\\d+\\.\\d+$" },
    "status": { "type": "string", "enum": ["active", "draining", "offline"], "default": "active" },
    "card_url": { "type": "string", "format": "uri", "description": "HTTPS endpoint to fetch full capability description (MCP/A2A card)" }
  },
  "additionalProperties": false
}
```

> **Critical constraint**: The `AgentRecord` is published via mDNS/DNS-SD with **size limit ≤ 512 bytes**. Richer details MUST be fetched from `card_url` by the trusted coordinator after validation.

---

## Protocol Layers

| Layer | Function | Open-Stack Implementation |
|-------|----------|---------------------------|
| **Discovery** | Local presence advertising | Avahi (mDNS/DNS-SD) + RFC 6763 service types |
| **Identity** | Agent authentication | SPIFFE/SPIRE workload identities OR W3C DIDs + VCs |
| **Event Fabric** | Typed message routing | NATS (JetStream for persistence) + Apicurio Registry for schema validation |
| **Policy** | Authorization decisions | OPA (Rego policies) with context from `auth_context` |
| **Auth** | Token issuance & verification | Keycloak (OIDC) or any OAuth2-compatible IdP |
| **Communication** | Agent-to-agent tasking | A2A protocol (open spec) |
| **Capability** | Tool/resource exposure | MCP protocol (open spec) |

---

## Discovery Flow

```
1. Agent starts
   ↓
2. Publishes _amf-agent._tcp service via Avahi
   ↓
3. TXT record contains minimal AgentRecord (JSON, ≤512B)
   ↓
4. Local DMZ listener receives advertisement via mDNS
   ↓
5. Deterministic validator checks:
   • Size ≤ 512B
   • Valid JSON Schema
   • Signature (if present)
   • Rate limit compliance
   ↓
6. If valid → disposable watcher LLM summarizes + risk-scores
   ↓
7. Trusted coordinator receives structured summary
   ↓
8. Coordinator decides:
   • Ignore (high risk / irrelevant)
   • Fetch card_url for richer details
   • Initiate A2A session for task delegation
```

---

## Trust & Security Model

### Boundary Architecture

```
┌─────────────────────────────────────┐
│   UNTRUSTED EXTERNAL WORLD          │
│   • Remote agents                   │
│   • Advertisements                  │
│   • Metadata prompts                │
└────────────┬────────────────────────┘
             │
┌────────────▼────────────┐
│   DMZ WATCHER           │
│   (sacrificial layer)   │
│   • Stateless           │
│   • Disposable          │
│   • No durable memory   │
│   • Validates + summarizes│
└────────────┬────────────┘
             │ sanitized summary only
┌────────────▼────────────┐
│   TRUSTED COORDINATOR   │
│   • User policy engine  │
│   • Durable memory      │
│   • Final routing       │
│   • OPA policy checks   │
└────────────────────────┘
```

### Security Requirements

1. All remote advertisements are **untrusted by default**
2. DMZ watcher MUST be restarted every N minutes (configurable; default: 15)
3. No raw prompts from external agents reach the trusted coordinator
4. All events MUST include `auth_context` with verifiable claims
5. Policy decisions (OPA) MUST be logged with `trace_id` for audit
6. The trusted coordinator is the sole authority for durable state and privileged actions

---

## Versioning & Extension

- Schema versions follow SemVer: `major.minor.patch`
- Backward compatibility: new fields may be added; unknown fields are ignored
- Breaking changes require `major` version bump and migration guide
- Extension mechanism: `payload_type` references external JSON Schema `$id`
- Registry of approved `payload_type` schemas maintained at `https://amf.spec/schemas/`

---

## Reference Implementation

### Directory Structure

```
REFERENCE_IMPL/
├── README.md                 # Quickstart guide
├── requirements.txt          # Python dependencies
├── run_local_mesh.py         # Entry point: starts all components
│
├── discovery/
│   ├── __init__.py
│   ├── avahi_publisher.py    # Publishes _amf-agent._tcp with minimal AgentRecord
│   ├── avahi_listener.py     # mDNS browser; emits raw ads to DMZ queue
│   └── validator.py          # Deterministic checks: size, schema, signature
│
├── dmz_watcher/
│   ├── __init__.py
│   ├── watcher_process.py    # Disposable LLM wrapper: summarize + risk-score
│   ├── prompt_templates.py   # Strict templates to prevent prompt injection
│   └── restart_policy.py     # Cron-style restart logic; max lifetime config
│
├── coordinator/
│   ├── __init__.py
│   ├── trusted_core.py       # Main orchestrator: policy, routing, memory
│   ├── policy_engine.py      # OPA Rego integration
│   └── memory_store.py       # Local SQLite/JSONL for task state
│
├── event_fabric/
│   ├── __init__.py
│   ├── nats_client.py        # NATS JetStream setup + schema validation
│   ├── envelope.py           # EventEnvelope Pydantic model
│   └── topics.py             # Topic naming: amf.{trust_domain}.{event_type}
│
├── identity/
│   ├── __init__.py
│   ├── spiffe_loader.py      # Loads SPIFFE SVIDs; mTLS setup
│   └── did_resolver.py       # Optional: resolve W3C DIDs to public keys
│
├── policies/
│   ├── allow_task_claim.rego # Example policy
│   ├── rate_limit.rego       # Example policy
│   └── README.md             # How to write/test OPA policies
│
├── schemas/
│   ├── event-envelope-1.0.json
│   ├── agent-record-1.0.json
│   └── registry.json
│
└── examples/
    ├── simple_worker_agent.py    # Minimal agent implementation
    ├── coordinator_cli.py        # CLI for manual inspection/approval
    └── threat_demo/              # Scripts showing DMZ absorbing malicious ads
```

### Key File: `dmz_watcher/watcher_process.py`

```python
#!/usr/bin/env python3
"""
Disposable DMZ Watcher Process

This process:
1. Receives raw, untrusted advertisements from avahi_listener
2. Runs deterministic validation (size, schema, signature)
3. If valid, uses a small LLM to summarize + risk-score
4. Outputs ONLY a structured summary to the trusted coordinator queue
5. Exits after N messages or M minutes (enforced by restart_policy)

CRITICAL: This process has NO access to:
- Durable memory
- User secrets
- Privileged tools
- The main reasoning model
"""

import os, sys, json, signal, time
from datetime import datetime, timedelta
from jsonschema import validate, ValidationError
from .prompt_templates import SUMMARIZE_AD_PROMPT, RISK_SCORE_PROMPT

# Hard limits
MAX_AD_SIZE = 512  # bytes
MAX_LIFETIME_MINUTES = int(os.getenv("WATCHER_LIFETIME_MIN", "15"))
MAX_MESSAGES = int(os.getenv("WATCHER_MAX_MSGS", "50"))

def main():
    start_time = datetime.now()
    message_count = 0
    
    # Load schemas
    agent_record_schema = load_schema("agent-record-1.0.json")
    
    while True:
        # Check exit conditions
        if datetime.now() - start_time > timedelta(minutes=MAX_LIFETIME_MINUTES):
            log("Lifetime exceeded; exiting")
            break
        if message_count >= MAX_MESSAGES:
            log("Message limit reached; exiting")
            break
            
        # Receive raw ad (from stdin or queue)
        raw_ad = receive_raw_advertisement()
        
        # Layer 1: Deterministic validation
        try:
            if len(raw_ad) > MAX_AD_SIZE:
                raise ValueError(f"Advertisement too large: {len(raw_ad)} > {MAX_AD_SIZE}")
            ad_obj = json.loads(raw_ad)
            validate(instance=ad_obj, schema=agent_record_schema)
            # TODO: signature verification if auth_requirements present
        except (ValidationError, ValueError, json.JSONDecodeError) as e:
            emit_rejection_event(reason=str(e), raw_ad_hash=hash(raw_ad))
            continue
            
        # Layer 2: Disposable LLM summarization
        try:
            summary = llm_summarize(ad_obj, prompt=SUMMARIZE_AD_PROMPT)
            risk_score = llm_risk_score(ad_obj, prompt=RISK_SCORE_PROMPT)  # 0.0-1.0
        except Exception as e:
            # LLM failure should not crash; treat as high-risk
            summary = {"error": "summarization_failed"}
            risk_score = 0.95
            
        # Layer 3: Emit structured summary to trusted coordinator
        trusted_payload = {
            "original_agent_id": ad_obj["agent_id"],
            "summary": summary,
            "risk_score": risk_score,
            "extracted_capabilities": extract_capability_tags(ad_obj),
            "card_url": ad_obj.get("card_url"),
            "timestamp": datetime.utcnow().isoformat()
        }
        emit_to_coordinator_queue(trusted_payload)
        
        message_count += 1
        
    # Clean exit; process will be restarted by supervisor
    sys.exit(0)

def llm_summarize(ad_obj: dict, prompt: str) -> dict:
    """
    Call a small, local LLM (e.g., Phi-3, TinyLlama) with strict output schema.
    NEVER pass user context or internal state to this call.
    """
    # Implementation depends on chosen local LLM runtime
    # Output MUST be JSON matching a predefined schema
    pass

# ... helper functions omitted for brevity
```

### Key File: `policies/allow_task_claim.rego`

```rego
package amf.policy

import future.keywords.if
import future.keywords.in

# Default deny
default allow_task_claim = false

# Allow if:
# 1. Agent is in same trust_domain as coordinator
# 2. Agent has required scope in auth_context
# 3. Rate limit not exceeded (checked externally or via OPA cache)
allow_task_claim if {
    input.agent.trust_domain == input.coordinator.trust_domain
    "claim:tasks" in input.agent.auth_context.scopes
    not rate_limit_exceeded(input.agent.agent_id)
}

# Helper: rate limit check (simplified; real impl uses OPA cache or external store)
rate_limit_exceeded(agent_id) if {
    count := input.request_history[agent_id].task_claims_last_minute
    count >= 10
}

# Log all decisions for audit
decision_log[trace_id] := {
    "agent_id": input.agent.agent_id,
    "action": "task.claim",
    "allowed": allow_task_claim,
    "timestamp": time.now_ns(),
} if {
    trace_id := input.trace_id
}
```

---

## Open-Stack Component Mapping

| Layer | MS/Salesforce Answer | Linux/Open Answer | Notes |
|-------|---------------------|-------------------|-------|
| **Discovery** | Entra ID agent metadata | Avahi (mDNS/DNS-SD) | Ships with every Linux distro; RFC 6763 compliant |
| **Identity** | Entra ID / managed identity | SPIFFE/SPIRE or DIDs+VCs | CNCF-backed or W3C standard; cloud-agnostic |
| **Event Fabric** | Fabric EventSchemaSet | NATS + Apicurio Registry | Lightweight, embeddable, schema versioning built-in |
| **Policy** | Azure Policy / Entra scopes | OPA (Open Policy Agent) | CNCF; declarative Rego policies |
| **Auth** | Azure OAuth2 | Keycloak + OIDC | Self-hostable; standards-compliant |
| **Communication** | A2A (open) | A2A (same) | Already open governance; Linux Foundation |
| **Capability** | MCP (open) | MCP (same) | Already open governance; Anthropic |

---

## Licensing Overview

| Component | License | Notes |
|-----------|---------|-------|
| **Avahi** | LGPL 2.1 | Fully free; ships in every Linux distro |
| **SPIFFE/SPIRE** | Apache 2.0 | CNCF; fully free |
| **NATS** | Apache 2.0 | Fully free; no commercial hooks; CNCF |
| **Apicurio Registry** | Apache 2.0 | Fully free |
| **OPA** | Apache 2.0 | CNCF; fully free |
| **Keycloak** | Apache 2.0 | Fully free |
| **Redpanda** | Apache 2.0* | *Was BSL; converted to Apache 2.0 in 2024 — verify current terms before committing |

> **Recommendation**: Use NATS for the event fabric. It is Apache 2.0 from inception, CNCF-graduated, runs embedded or distributed, and has JetStream for persistence without licensing risk.

---

## Strategic Positioning

### The Gap AMF Fills

```
Enterprise Cloud Platforms (MS/Salesforce)
├─ Solve: Multi-agent coordination at scale
├─ Anchor: Proprietary identity + event infrastructure
└─ Limitation: Requires commitment to vendor ecosystem

AMF (Open, Local-First)
├─ Solves: Same coordination problems
├─ Anchors: Composable open-source tools
└─ Advantage: Runs on personal hardware; privacy-preserving; vendor-neutral
```

### Target Use Cases

1. **Personal AI stacks**: Your local agents collaborating with a colleague's local agents
2. **Sensitive workflows**: Process data on-premises; selectively pull external capabilities without exfiltrating context
3. **Edge/IoT deployments**: Resource-constrained devices coordinating via lightweight protocols
4. **Research & experimentation**: Rapid prototyping without cloud account setup or vendor lock-in

### Messaging

> *"AMF is the open specification for secure, local-first multi-agent coordination. Use it to build agent systems that discover, trust, and collaborate without locking into a cloud vendor."*

---

## Next Steps

### Immediate (Week 1)

- [ ] Commit `SPEC.md` and `REFERENCE_IMPL/` to https://github.com/RALaBarge/amf
- [ ] Add CI check: validate `schemas/*.json` are valid JSON Schema
- [ ] Write `CONTRIBUTING.md` with "How to add a new event type"
- [ ] Record 2-min demo: `simple_worker_agent.py` advertising via Avahi → coordinator approval → task completion via NATS

### Short-Term (Month 1)

- [ ] Publish spec to https://amf.spec (GitHub Pages)
- [ ] Implement schema registry endpoint (Apicurio-compatible)
- [ ] Add threat model document (`THREAT_MODEL.md`) with STRIDE analysis
- [ ] Post to Hacker News / Lobsters for community feedback

### Medium-Term (Quarter 1)

- [ ] Reference implementations in 2+ languages (Python, Go)
- [ ] Interop testing suite: verify independent implementations can exchange events
- [ ] CNCF sandbox application (if community interest warrants)

---

## Contributing

### How to Add a New Event Type

1. Draft the payload schema in `schemas/payloads/your-event-type-1.0.json`
2. Add the new type to `EventEnvelope.message_type` enum in `event-envelope-1.0.json`
3. Update `SPEC.md` documentation with purpose and example payload
4. Submit PR with:
   - Schema file
   - Documentation update
   - Example usage in `examples/`
   - OPA policy example if authorization is involved

### How to Test the Reference Implementation

```bash
# Clone and install
git clone https://github.com/RALaBarge/amf
cd amf/REFERENCE_IMPL
pip install -r requirements.txt

# Run local mesh (starts Avahi advertiser + NATS + watcher + coordinator)
python run_local_mesh.py

# In another terminal, run a sample worker agent
python examples/simple_worker_agent.py

# Use the CLI to inspect advertisements and approve connections
python examples/coordinator_cli.py list-ads
python examples/coordinator_cli.py approve <agent_id>
```

### Reporting Issues

- Security vulnerabilities: Email security@amf.spec (PGP key in repo)
- Spec bugs or ambiguities: GitHub Issues with label `spec`
- Implementation bugs: GitHub Issues with label `reference-impl`

---

## Appendix: Acknowledgments

This specification draws inspiration from:
- RFC 6763 (DNS-Based Service Discovery)
- SPIFFE/SPIRE (CNCF)
- NATS.io (CNCF)
- Open Policy Agent (CNCF)
- Agent-to-Agent Protocol (A2A)
- Model Context Protocol (MCP)
- W3C Decentralized Identifiers (DIDs) and Verifiable Credentials (VCs)

Thank you to the open-source communities maintaining these projects. AMF stands on your shoulders.

---

*Document generated: March 2026*  
*License: Apache 2.0 — See LICENSE file in repository*
