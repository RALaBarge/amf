package main

import (
	"fmt"
	"time"
)

// AMF event types — also used as NATS subjects and CloudEvents type field
const (
	TypeCapabilityAdvertise = "amf.discovery.capability.advertise"
	TypeAgentHeartbeat      = "amf.discovery.agent.heartbeat"
	TypeTaskAnnounce        = "amf.task.announce"
	TypeTaskClaim           = "amf.task.claim"
	TypeTaskDelegate        = "amf.task.delegate"
	TypeTaskProgress        = "amf.task.progress"
	TypeTaskBlocked         = "amf.task.blocked"
	TypeArtifactPublish     = "amf.artifact.publish"
	TypeEvidencePublish     = "amf.evidence.publish"
	TypeResultPartial       = "amf.result.partial"
	TypeResultFinal         = "amf.result.final"
	TypePolicyWarning       = "amf.policy.warning"
	TypePolicyDeny          = "amf.policy.deny"
)

type Visibility string
type AgentRole string

const (
	VisibilityLocal  Visibility = "local"
	VisibilityMesh   Visibility = "mesh"
	VisibilityPublic Visibility = "public"

	RoleCoordinator AgentRole = "coordinator"
	RoleSpecialist  AgentRole = "specialist"
	RoleWatcher     AgentRole = "watcher"
	RoleConnector   AgentRole = "connector"
)

// Scope is a defined AMF capability grant.
// Scopes are declared in auth_context.scopes and enforced by OPA.
// An agent may only claim scopes that were granted to it by an authority
// (coordinator or delegating agent) — self-asserted scopes beyond what the
// NATS ACL permits are ignored by policy.
type Scope = string

const (
	// Task lifecycle
	ScopeDispatchTasks Scope = "dispatch:tasks"  // announce and delegate tasks
	ScopeClaimTasks    Scope = "claim:tasks"     // take ownership of announced tasks

	// Registry
	ScopeReadRegistry  Scope = "read:registry"   // read the admitted agent list
	ScopeWriteRegistry Scope = "write:registry"  // admit / evict agents (coordinator only)

	// MCP
	ScopeCallMCP Scope = "call:mcp" // invoke MCP tools on admitted agents

	// Internal pipeline (enforced by NATS ACL, not just policy)
	ScopeIngestRaw     Scope = "ingest:raw"     // publish to amf.internal.raw (connector)
	ScopeClassify      Scope = "classify"       // publish to amf.internal.classified (watcher)
)

// AuthContext captures both origin and authority for an event.
//
// Origin  = who sent this (Identity — verified by SPIFFE if available)
// Authority = under what grant are they acting, and who issued that grant
//
// These are separate concerns. A specialist agent's identity is always its
// own SPIFFE ID, but its authority to claim a specific task was granted by
// the coordinator when it announced that task. The authority chain lets OPA
// verify the full provenance: not just "is this a valid agent" but "did
// a trusted authority actually authorize this action."
//
// For locally-initiated actions (coordinator acting on its own authority),
// GrantedBy == Identity and DelegationChain is empty.
//
// For delegated actions (coordinator → specialist → sub-task),
// GrantedBy is the immediate delegator, DelegationChain is the full path
// from the root authority to the current actor (root-first order).
type AuthContext struct {
	// Origin
	Identity    string `json:"identity"`     // SPIFFE ID of the sender
	TrustDomain string `json:"trust_domain"` // trust domain of the sender

	// Authority
	Scopes          []string `json:"scopes"`                    // what this agent is authorized to do in this event
	GrantedBy       string   `json:"granted_by,omitempty"`      // identity that issued this authority grant
	DelegationChain []string `json:"delegation_chain,omitempty"` // [root, ..., immediate_parent] — empty if self-issued
	IssuedAt        string   `json:"issued_at,omitempty"`       // RFC3339 — when authority was granted
	ExpiresAt       string   `json:"expires_at,omitempty"`      // RFC3339 — optional expiry (omit = task TTL applies)
}

// AMFData is the CloudEvents `data` field.
// Wraps the event payload along with fields that can't be CE extensions (arrays, objects).
type AMFData struct {
	Payload      any         `json:"payload"`
	AuthContext  AuthContext `json:"auth_context"`
	ArtifactRefs []string    `json:"artifact_refs,omitempty"`
	EvidenceRefs []string    `json:"evidence_refs,omitempty"`
}

// AMFEvent is a CloudEvents v1.0 envelope.
//
// CloudEvents compatibility:
//   - Required fields: specversion, id, source, type
//   - Optional fields: time, datacontenttype, dataschema, subject, data
//   - AMF-specific scalars are CloudEvents extensions (lowercase alphanum, ≤20 chars)
//   - Non-scalar AMF fields (auth_context, artifact_refs, evidence_refs) live in Data
//
// A2A compatibility:
//   - Drop the entire struct into Part.data for A2A transport
//
// MS Fabric compatibility:
//   - CloudEvents v1.0 JSON is natively ingested by Fabric Eventstreams
//   - type field maps to Fabric event routing
//
// NATS transport:
//   - Subject = Type field (amf.task.announce etc.)
//   - Serialized as JSON, published as NATS message body
type AMFEvent struct {
	// CloudEvents v1.0 — required
	SpecVersion string `json:"specversion"` // always "1.0"
	ID          string `json:"id"`          // = message_id, UUID
	Source      string `json:"source"`      // SPIFFE ID or agent endpoint URI
	Type        string `json:"type"`        // amf.task.announce etc. (also the NATS subject)

	// CloudEvents v1.0 — optional
	Time            time.Time `json:"time"`
	DataContentType string    `json:"datacontenttype"` // "application/json"
	DataSchema      string    `json:"dataschema,omitempty"` // schema registry URL
	Subject         string    `json:"subject,omitempty"`    // task_id when relevant

	// CloudEvents extensions — AMF scalar metadata (lowercase alphanum ≤20 chars)
	TraceID       string     `json:"amftraceid,omitempty"`
	ParentID      string     `json:"amfparentid,omitempty"`
	TaskID        string     `json:"amftaskid,omitempty"`
	AgentRole     AgentRole  `json:"amfagentrole"`
	Visibility    Visibility `json:"amfvisibility"`
	Confidence    string     `json:"amfconfidence"` // string — CloudEvents has no float type
	TTL           int        `json:"amfttl"`
	SchemaVersion string     `json:"amfschemaversion"` // semver e.g. "1.0.0"
	TrustDomain   string     `json:"amftrustdomain,omitempty"`

	// CloudEvents data field — payload + non-scalar AMF fields
	Data *AMFData `json:"data"`
}

// NewEvent constructs an AMFEvent where the sender acts on its own authority.
// GrantedBy == source (self-issued). Use NewDelegatedEvent when acting under
// authority granted by another agent.
func NewEvent(id, traceID, source, eventType string, role AgentRole, payload any) *AMFEvent {
	now := time.Now().UTC().Format(time.RFC3339)
	scopes := defaultScopesForRole(role)
	return &AMFEvent{
		SpecVersion:     "1.0",
		ID:              id,
		Source:          source,
		Type:            eventType,
		Time:            time.Now().UTC(),
		DataContentType: "application/json",
		DataSchema:      fmt.Sprintf("https://amf.spec/schemas/event-envelope/1.0.0"),
		TraceID:         traceID,
		AgentRole:       role,
		Visibility:      VisibilityLocal,
		Confidence:      "1.0",
		TTL:             60,
		SchemaVersion:   "1.0.0",
		Data: &AMFData{
			Payload: payload,
			AuthContext: AuthContext{
				Identity:    source,
				TrustDomain: trustDomainFrom(source),
				Scopes:      scopes,
				GrantedBy:   source, // self-issued — sender is its own authority root
				IssuedAt:    now,
			},
		},
	}
}

// NewDelegatedEvent constructs an AMFEvent where the sender acts under authority
// granted by another agent (e.g. a specialist acting on a task delegated by the
// coordinator). The delegation chain records the provenance so OPA can verify
// the full authority path.
//
// grantedBy: the immediate delegator's SPIFFE ID
// chain: ordered from root authority to immediate parent (empty if direct delegation from root)
// scopes: the specific scopes granted for this delegation (should be ⊆ delegator's scopes)
func NewDelegatedEvent(id, traceID, source, eventType string, role AgentRole,
	grantedBy string, chain []string, scopes []Scope, payload any) *AMFEvent {
	evt := NewEvent(id, traceID, source, eventType, role, payload)
	evt.Data.AuthContext.Scopes = scopes
	evt.Data.AuthContext.GrantedBy = grantedBy
	evt.Data.AuthContext.DelegationChain = chain
	return evt
}

// defaultScopesForRole returns the baseline scope set for a role acting
// under its own authority. Delegated grants may be narrower.
func defaultScopesForRole(role AgentRole) []Scope {
	switch role {
	case RoleCoordinator:
		return []Scope{ScopeDispatchTasks, ScopeReadRegistry, ScopeWriteRegistry, ScopeCallMCP}
	case RoleSpecialist:
		return []Scope{ScopeClaimTasks}
	case RoleWatcher:
		return []Scope{ScopeClassify}
	case RoleConnector:
		return []Scope{ScopeIngestRaw}
	default:
		return []Scope{}
	}
}

// trustDomainFrom extracts the trust domain from a SPIFFE ID.
// "spiffe://local/agent/abc" → "local"
// Returns "unknown" if the ID is not a SPIFFE URI.
func trustDomainFrom(spiffeID string) string {
	const prefix = "spiffe://"
	if len(spiffeID) <= len(prefix) {
		return "unknown"
	}
	rest := spiffeID[len(prefix):]
	for i, c := range rest {
		if c == '/' {
			return rest[:i]
		}
	}
	return rest // no path component, whole thing is trust domain
}

// A2APart wraps an AMFEvent for transport inside an A2A message.
// Drop this into the parts[] array of an A2A Message.
type A2APart struct {
	Data      *AMFEvent `json:"data"`
	MediaType string    `json:"media_type"` // "application/cloudevents+json"
}

// A2AMessage is a minimal A2A message carrying AMF events.
type A2AMessage struct {
	Role  string    `json:"role"` // "user" | "agent"
	Parts []A2APart `json:"parts"`
}
