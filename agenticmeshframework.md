# Agent Fabric Design Memo

## Purpose

This memo captures the main ideas, decisions, and design direction that emerged from the conversation about MCP, A2A, local agent discovery, event fabrics, and secure multi-agent coordination. It is written as a design memo rather than a transcript, with an appendix preserving key user positions that shaped the architecture.

---

## Core Thesis

The long-term problem is not just giving one model tools. The bigger problem is enabling many specialist models and agents to discover one another, advertise capabilities, coordinate work, and exchange structured results in a standardized, secure, and observable way.

MCP solves only part of that problem. It helps with capability exposure and invocation. It does not by itself solve local discovery, identity, trust, delegation, memory boundaries, or multi-agent coordination.

The conversation converged on a stronger model:

- agents should **advertise themselves**
- a local trusted stack should **observe those advertisements**
- the local stack should **selectively choose whether to inspect or connect**
- raw internal reasoning should **not** be broadly shared
- external advertisements and metadata should be treated as **untrusted**
- a disposable watcher layer should sit at the edge like a **DMZ**
- the real system should be built around **structured events**, not a shared global “thought stream”

---

## What MCP Actually Covers

MCP is useful as a capability protocol.

It is good for exposing:

- tools
- resources
- prompts
- structured results

That means MCP can serve as the layer where a system learns:

- what a remote system can do
- how to ask for it
- what kind of result comes back

This is valuable, but it is not a complete inter-agent fabric.

### Practical interpretation

MCP is best understood as:

> a standard interface for exposing callable capabilities and retrievable context

It is not, by itself:

- a service discovery system
- an agent presence protocol
- a trust framework
- a task delegation framework
- a shared memory system
- a global coordination bus

---

## Why MCP Is Not Enough

The future being described is one where there are many specialist models everywhere, not one universal model doing everything. In that world, the system needs more than capability exposure. It also needs:

- discovery
- identity
- trust
- delegation
- observability
- policy boundaries
- result provenance
- selective communication

A larger protocol stack is required.

### Proposed protocol layers

1. **Discovery layer**
   - who is present
   - what they claim
   - where they are
   - how to talk to them

2. **Capability layer**
   - tools
   - resources
   - prompts
   - structured results

3. **Coordination layer**
   - task requests
   - claims
   - delegation
   - progress
   - results
   - cancellation
   - escalation

4. **Trust and policy layer**
   - authentication
   - authorization
   - scopes
   - revocation
   - auditability

5. **Observability layer**
   - trace IDs
   - spans
   - metrics
   - event lineage
   - provenance

MCP only occupies part of layer 2.

---

## A2A as the Better Fit for Inter-Agent Communication

A2A is a closer match than MCP for the actual “agents talking to agents” problem.

### Why A2A matters

A2A aligns better with the need for:

- agent-to-agent messaging
- task lifecycle handling
- capability advertisement
- task/result exchange
- richer multi-agent coordination

In practical terms:

- **MCP** describes and exposes capabilities
- **A2A** is closer to a protocol for communicating with agents as agents

That distinction matters. A tool server and an agent are not the same thing.

---

## Discovery: The aDNS Concept

One of the strongest ideas in the conversation was the introduction of an **aDNS** concept.

### What aDNS means here

aDNS is a shorthand for:

> an agent discovery layer that maintains an active list of currently advertising local agents and services

It is not the bus itself and not the coordination protocol itself. Its role is to answer:

- what agents are advertising right now
- where they are
- what protocol they support
- what they claim to offer
- what trust boundary they belong to
- how to fetch more detail

### Best analogy

aDNS is conceptually similar to:

- service discovery
- local presence advertising
- DNS-SD style discovery
- zero-config local service announcement

### Strong design principle

The user explicitly rejected “everyone broadcasting to everyone” as the right default. The preferred design is:

- agents advertise to a listener
- the local stack decides whether to inspect them
- the local stack chooses whether to connect further

That is discovery-first, selective-connect-second.

---

## RFC 6763, mDNS, and Local Discovery

For local environments, the closest existing standards family is:

- **mDNS**
- **DNS-SD / RFC 6763**

This fits the aDNS concept well.

### Why it fits

The user was aiming toward something like local advertising over an existing network substrate. That is exactly the shape DNS-SD solves at the service layer:

- a service advertises presence
- a listener discovers it
- metadata can point to richer endpoints
- connection is optional and selective

### Recommended use

For a first local implementation:

- use local-link discovery
- advertise minimal metadata only
- expose a follow-up endpoint for richer information
- let the trusted local stack fetch and inspect details later

### Important constraint

Do not pack full capability descriptions into the advertisement itself.  
Advertisements should stay small and low-risk.

The advertisement should contain things like:

- service type
- endpoint
- protocol version
- card URL
- auth mode
- trust domain
- minimal tags

Richer details should be fetched separately.

---

## The Event Fabric

A major design correction in the conversation was moving away from a shared “thought channel” toward a shared **event fabric**.

### Key distinction

Do **not** build a system where agents freely share raw hidden reasoning.  
Do build a system where they publish structured events about:

- what they are offering
- what task they are claiming
- what they did
- what evidence they found
- what result they produced
- how confident they are

### Why this matters

A structured event fabric is better for:

- observability
- replayability
- loose coupling
- bounded interfaces
- security review
- policy enforcement

A shared thought stream creates:

- prompt injection propagation
- privacy leakage
- feedback loops
- token bloat
- coordination noise
- bad epistemics

### The right model

The durable design is:

> share actions, state transitions, evidence handles, and results — not raw chain-of-thought

---

## Proposed Event Types

A coordination fabric should have typed messages rather than freeform broadcasting.

### Candidate event types

- `capability.advertise`
- `agent.heartbeat`
- `task.announce`
- `task.claim`
- `task.delegate`
- `task.progress`
- `task.blocked`
- `artifact.publish`
- `evidence.publish`
- `result.partial`
- `result.final`
- `policy.warning`
- `policy.deny`

This gives the ecosystem a shared vocabulary without requiring all participants to share internal reasoning.

---

## Proposed Event Envelope Schema

A first-pass event envelope should include at minimum:

- `message_id`
- `trace_id`
- `parent_message_id`
- `task_id`
- `agent_id`
- `agent_role`
- `timestamp`
- `message_type`
- `visibility`
- `confidence`
- `ttl`
- `payload_type`
- `payload`
- `artifact_refs`
- `evidence_refs`
- `auth_context`
- `schema_version`

### Why this matters

The event envelope is the backbone for:

- correlation
- policy
- filtering
- replay
- debugging
- analytics
- trust decisions

---

## Proposed Agent Record / Discovery Object

A first-pass aDNS or registry object should include fields like:

- `agent_id`
- `display_name`
- `endpoint`
- `protocols_supported`
- `capability_tags`
- `trust_domain`
- `visibility`
- `subscription_topics`
- `publish_topics`
- `latency_class`
- `cost_class`
- `auth_requirements`
- `version`
- `status`

This record is how the system separates discovery from communication.

---

## The Walking Stack

One of the strongest conceptual contributions in the conversation was the idea of a **walking stack**: a layered local system that travels with the user and mediates all external agent contact.

### Proposed walking stack layers

#### 1. Local trusted coordinator
This is the system closest to the user.

Responsibilities:

- apply user policy and preferences
- decide what can leave the trust boundary
- route tasks
- decide when to escalate
- own durable memory
- remain the main trusted orchestrator

#### 2. Fast/small LLM
This is the reflex layer.

Responsibilities:

- classify requests
- triage tasks
- decide whether remote agents are relevant
- determine whether the big model is needed
- keep latency low

#### 3. DMZ watcher / listener
This is the advertisement inspection boundary.

Responsibilities:

- receive untrusted advertisements
- validate or summarize them
- label risk
- prevent unsafe content from reaching inward systems directly

#### 4. Big reasoning model
This is the type-2 thinking layer.

Responsibilities:

- deep planning
- synthesis
- arbitration
- harder logic
- integration across many sources

---

## The DMZ Watcher Pattern

A major conclusion was that the listening/inspection LLM should be treated like a **DMZ component**.

### Core principle

The watcher should be:

- disposable
- untrusted
- stateless or near-stateless
- frequently restarted
- prevented from holding durable authority

### It should not be:

- the core planner
- the keeper of memory
- the policy authority
- a privileged execution engine

### Why

Anything that listens to remote agent advertisements, metadata, prompts, or capability descriptions is exposed to:

- malformed data
- prompt injection
- poisoned context
- spam
- deceptive metadata
- repeated adversarial shaping

So the boundary layer should absorb and reduce risk, then emit only a bounded summary.

---

## Recommended DMZ Processing Pipeline

The boundary process should not be “one LLM and done.”

### Better layered approach

#### Layer 1: deterministic validation
Before an LLM sees anything:

- size limits
- schema validation
- rate limits
- deduplication
- allowlists
- string/content constraints
- auth/signature checks

#### Layer 2: disposable watcher LLM
This layer may:

- summarize
- classify
- risk-score
- extract structured fields
- recommend escalation or quarantine

This layer may not:

- write to durable memory
- invoke sensitive tools
- become authoritative
- see unnecessary private user context

#### Layer 3: trusted coordinator
The inner trusted system sees only:

- normalized structured summaries
- confidence/risk labels
- extracted capabilities
- endpoint references
- provenance metadata

---

## Structured Results and MCP

Another important conclusion was that structured results matter because they can be relayed, revalidated, and shared more safely than prose blobs.

### Why structured results help

If MCP or another system returns typed structured output, that output can be:

- forwarded on the event bus
- stored as an artifact
- validated
- transformed
- consumed by another agent without reparsing prose

This is far better than forcing one agent to interpret another agent’s natural-language output every time.

### Design implication

The architecture should favor:

- typed payloads
- versioned schemas
- explicit artifact handles
- provenance references

---

## Security Model

The conversation repeatedly converged on a cautious security model.

### Security assumptions

All remote advertisers and advertisements should be treated as untrusted until validated.

### Security posture

- passive discovery is preferred over active outbound exposure
- advertisements are accepted, not freeform conversations
- the local stack remains authoritative
- private models do not broadcast internals
- privileged actions require stronger trust boundaries
- watchers are disposable
- durable memory remains local/trusted

### Publication policy

The right default is:

- publish intent
- publish state transitions
- publish artifacts/evidence handles
- publish bounded results
- do **not** publish raw chain-of-thought
- do **not** publish secrets
- do **not** publish unnecessary full context

---

## Broadcast vs Selective Subscription

A key refinement was moving from “broadcast” language to a more selective subscription model.

### Better interpretation

The user’s preferred model became:

- remote agents advertise
- the local system listens
- the local system chooses whether to inspect
- the local system chooses whether to interact

This is much closer to:

- service discovery
- capability advertisement
- selective subscription
- controlled session establishment

than to universal broadcasting.

### Strong phrase

A helpful summary is:

> I do not want to broadcast at all; I want them to advertise to me and I selectively choose to communicate with them.

That statement materially shaped the architecture.

---

## Relationship Between Existing Standards

### Suggested role split

#### aDNS / mDNS / DNS-SD
Used for:
- local discovery
- service advertising
- active advertiser list
- low-level presence information

#### A2A
Used for:
- agent-to-agent tasking
- communication with agents as agents
- lifecycle handling
- richer inter-agent messages

#### MCP
Used for:
- tools
- resources
- prompts
- structured capability invocation/results

#### Event bus / event fabric
Used for:
- coordination
- announcements
- status
- artifacts
- results
- observability

### Compact shorthand

> aDNS finds them, A2A talks to them, MCP describes/calls capabilities, and the event fabric coordinates everything.

---

## First Implementation Recommendation

A practical first implementation should stay local and bounded.

### Minimal viable architecture

1. **Local trusted coordinator**
2. **Fast classifier/router model**
3. **DMZ listener process**
4. **Local discovery via mDNS/DNS-SD-style records**
5. **Agent card fetch on demand**
6. **Selective connection via A2A-style sessions**
7. **MCP where tool/resource capability surfaces are relevant**
8. **Structured event logging with trace IDs**
9. **No raw thought sharing**
10. **Frequent reset of DMZ watcher**

### What not to do first

- global open broadcast
- full peer-to-peer freeform multi-agent chat
- durable watcher memory
- direct ingestion of remote prompt text into the main reasoning model
- capability sprawl without schema discipline

---

## Design Principles Captured from the Conversation

### 1. Discovery should be passive on the local side
The user’s system should listen for offers rather than exposing itself broadly.

### 2. Advertisement is lower-risk than broad conversation
Presence and basic capability signals should precede session establishment.

### 3. Thought sharing is the wrong primitive
Structured events and state transitions are the right primitive.

### 4. The listener must be sacrificial
Anything that watches the outside world should be disposable and frequently reset.

### 5. Keep the trusted core close
Durable memory, policy, and final routing should stay in the local trusted orchestrator.

### 6. Separate discovery from communication
Finding an agent and talking to an agent are different layers.

### 7. Typed results beat prose blobs
Structured outputs are easier to validate, relay, and reason about.

### 8. The stack should be layered by trust and cost
Fast model for triage, watcher for untrusted intake, big model for deep reasoning.

---

## Open Questions

Several areas remain open and would need specification work.

### Discovery
- exact local service type naming
- what metadata belongs in the advertisement
- what metadata must only be available after fetch

### Trust
- how to represent trust domain
- how to authenticate local advertisers
- whether OAuth-style delegated auth is enough
- how to revoke or quarantine bad actors

### Eventing
- exact event types
- canonical payload schemas
- topic model or subscription filters
- trace/span propagation model

### Agent interaction
- exact A2A card fields used locally
- how A2A and MCP compose in one stack
- how to represent agent cost/latency/quality tiers

### Security
- maximum safe advertisement size
- how to detect malicious or manipulative metadata
- whether a tiny deterministic parser can eliminate most watcher LLM exposure
- restart policy and lifetime for the DMZ watcher

---

## Recommended Next Artifact

The next useful artifact after this memo would be one of the following:

1. **JSON schema draft**
   - `AgentRecord`
   - `EventEnvelope`
   - `TaskAnnouncement`
   - `CapabilityAdvertisement`

2. **protocol flow document**
   - advertiser appears
   - local listener sees it
   - validator screens it
   - watcher summarizes it
   - coordinator decides whether to fetch card
   - optional A2A session starts
   - optional MCP capability usage follows

3. **threat model**
   - hostile advertiser
   - poisoned agent card
   - noisy spam advertiser
   - fake capability lure
   - recursive handoff loop
   - private context leakage

---

## Appendix: Key User Positions Preserved

This appendix preserves the most important user positions and design instincts from the conversation in cleaned-up form.

### On the bigger goal
The real long-term goal is not just tool invocation. It is making many specialist LLMs talk to one another in a standardized, secure, and coordinated way while AGI is not here yet.

### On MCP
MCP only solves part of the problem. It helps with discovery and execution after discovery, but not the full inter-agent communication problem.

### On discovery
A useful model is for agents to advertise themselves and for a local stack to decide how to interact with them after they expose themselves.

### On broadcasting
The preferred system is not one where the user’s system broadcasts broadly. It is one where outside agents advertise to the user’s local system and the user’s system selectively chooses to communicate.

### On aDNS
The idea of “aDNS” emerged as an active list of local advertisers that the user can inspect and selectively interact with once they expose themselves.

### On architecture
A “walking stack” should travel with the user: a small coordinator model, something to watch and evaluate advertisements securely, and a larger model for deep type-2 thinking.

### On the listener
The listener should be treated like a DMZ and thrown away regularly. It should not be a durable trusted component.

### On security
A throwaway LLM instance or equivalent boundary process should be used when unpacking external advertisements and metadata.

### On event sharing
The right primitive is not a shared thought stream. The right primitive is structured events, bounded messages, and explicit schemas.

### On local networking
There is a strong intuition that local-network standards should be borrowed where possible because they exist for a reason and already solve parts of the presence/discovery problem.

### On standards direction
RFC 6763 and related local discovery ideas are promising for local advertising and aDNS-style discovery. A2A is promising for actual agent communication once discovery has happened.

---

## Final Summary

The conversation converged on a clear architecture direction:

- use **local discovery** instead of open broadcasting
- maintain an **aDNS-like active view of local advertisers**
- use **A2A** for actual agent-level communication
- use **MCP** where capability exposure and structured invocation matter
- coordinate through a **structured event fabric**
- keep a **trusted local orchestrator**
- put a **disposable DMZ watcher** at the edge
- share **events and artifacts**, not raw internal reasoning

That is a coherent path toward a future multi-agent system that is local-first, selective, observable, and far safer than a naïve global thought-sharing mesh.
