# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Purpose

This is a **design specification repository** for the **Agent Mesh Framework (AMF)** — a multi-agent coordination system architecture. The primary artifact is `agenticmeshframework.md`, a living design memo capturing architectural decisions, open questions, and implementation directions.

## Core Architecture Concepts

The AMF is built around four distinct layers that must remain separate:

- **Discovery Layer (aDNS)**: Local passive service discovery via mDNS/DNS-SD (RFC 6763). Agents advertise themselves; the local stack listens and selectively connects. No global broadcasting.
- **Capability Layer (MCP)**: Exposes tools, resources, prompts, and structured results. Handles what a system *can do* and how to invoke it.
- **Coordination Layer (A2A)**: Agent-to-agent messaging for task requests, claims, delegation, and progress tracking. Handles actual agent-to-agent communication.
- **Event Fabric**: Structured events for coordination — capability advertisements, task lifecycle, artifacts, results. Never raw chain-of-thought or secrets.

## The Walking Stack

A local-first trusted system that mediates all external agent contact, layered by trust and cost:

1. **Local Trusted Coordinator** (innermost): Policy enforcement, routing, durable memory, escalation
2. **Fast/Small LLM** (reflex): Classify requests, triage tasks, determine relevance
3. **DMZ Watcher** (disposable edge): Receives untrusted advertisements, validates, summarizes, labels risk — **treated as compromised, reset frequently**
4. **Big Reasoning Model** (deep thinking): Planning, synthesis, arbitration

## Security Model

- All remote advertisements are **untrusted until validated**
- **Passive discovery** preferred over active outbound exposure
- DMZ watcher is **stateless, disposable, untrusted** — never holds durable authority or memory
- Processing pipeline: deterministic validation → disposable watcher LLM → trusted coordinator (sees only normalized summaries)
- Share: intent, state transitions, artifact handles, bounded results
- Never share: raw chain-of-thought, secrets, full context windows

## Key Design Decisions (Do Not Reverse Without Discussion)

These reflect explicit user positions captured in the design memo:
- Discovery is **passive on the local side** — listen for offers, don't broadcast broadly
- The DMZ listener is **disposable** and **frequently reset** — this is by design
- Structured typed events over prose; typed results over raw outputs
- The "walking stack" architecture keeps trusted components local
- MCP alone is insufficient — the full stack requires aDNS + A2A + MCP + event bus

## Open Questions (Active Areas)

The following are explicitly unresolved in the spec (see `agenticmeshframework.md`):
- Service type naming conventions for aDNS advertisements
- Trust domain representation and local advertiser authentication/revocation
- Canonical event schemas, topic models, trace propagation format
- A2A/MCP composition patterns
- DMZ watcher restart policy and safe advertisement size limits

## Proposed Event Envelope Schema

When implementing the event fabric, each message should include: `message_id`, `trace_id`, `parent_message_id`, `task_id`, `agent_id`, `agent_role`, `timestamp`, `message_type`, `visibility`, `confidence`, `ttl`, `payload_type`, `payload`, `artifact_refs`, `evidence_refs`, `auth_context`, `schema_version`.

## Proposed Agent Record Fields (aDNS)

`agent_id`, `display_name`, `endpoint`, `protocols_supported`, `capability_tags`, `trust_domain`, `visibility`, `subscription_topics`, `publish_topics`, `latency_class`, `cost_class`, `auth_requirements`, `version`, `status`.
