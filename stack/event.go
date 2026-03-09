package main

import "time"

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

type AuthContext struct {
	Identity    string   `json:"identity"`
	Scopes      []string `json:"scopes"`
	TrustDomain string   `json:"trust_domain"`
}

type AMFEvent struct {
	MessageID       string        `json:"message_id"`
	TraceID         string        `json:"trace_id"`
	ParentMessageID string        `json:"parent_message_id,omitempty"`
	TaskID          string        `json:"task_id,omitempty"`
	AgentID         string        `json:"agent_id"`
	AgentRole       AgentRole     `json:"agent_role"`
	Timestamp       time.Time     `json:"timestamp"`
	MessageType     string        `json:"message_type"`
	Visibility      Visibility    `json:"visibility"`
	Confidence      float64       `json:"confidence"`
	TTL             int           `json:"ttl"`
	PayloadType     string        `json:"payload_type"`
	Payload         any           `json:"payload"`
	ArtifactRefs    []string      `json:"artifact_refs,omitempty"`
	EvidenceRefs    []string      `json:"evidence_refs,omitempty"`
	AuthContext     AuthContext   `json:"auth_context"`
	SchemaVersion   string        `json:"schema_version"`
}
