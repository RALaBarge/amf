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

// AuthContext — lives inside Data, not as a CE extension (CE extensions are scalars only)
type AuthContext struct {
	Identity    string   `json:"identity"`
	Scopes      []string `json:"scopes"`
	TrustDomain string   `json:"trust_domain"`
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

// NewEvent constructs an AMFEvent with CloudEvents defaults filled in.
func NewEvent(id, traceID, source, eventType string, role AgentRole, payload any) *AMFEvent {
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
				Identity:    "spiffe://local/agent/" + id,
				TrustDomain: "local",
				Scopes:      []string{},
			},
		},
	}
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
