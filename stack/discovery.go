package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/grandcat/zeroconf"
)

const (
	mdnsService  = "_amf-agent._tcp"
	mDNSDomain   = "local."
	mdnsPort     = 7777 // port agents advertise on (A2A/MCP endpoint port)
	maxTXTBytes  = 512
)

// AgentRecord is the minimal advertisement published via mDNS TXT record.
// Must serialize to ≤512 bytes.
type AgentRecord struct {
	AgentID            string   `json:"agent_id"`
	Endpoint           string   `json:"endpoint"`
	ProtocolsSupported []string `json:"protocols_supported"`
	CapabilityTags     []string `json:"capability_tags,omitempty"`
	TrustDomain        string   `json:"trust_domain"`
	Visibility         string   `json:"visibility"`
	Version            string   `json:"version"`
	Status             string   `json:"status"`
	CardURL            string   `json:"card_url,omitempty"`
}

// DiscoveredAgent is an AgentRecord enriched with discovery metadata.
type DiscoveredAgent struct {
	Record      AgentRecord `json:"record"`
	InstanceName string     `json:"instance_name"`
	Host        string      `json:"host"`
	Port        int         `json:"port"`
	SeenAt      time.Time   `json:"seen_at"`
}

// agentRegistry holds all currently known agents discovered via mDNS.
var agentRegistry = struct {
	sync.RWMutex
	agents map[string]*DiscoveredAgent
}{agents: make(map[string]*DiscoveredAgent)}

// RegisterSelf publishes this agent to the local mDNS network.
// TXT record encodes the AgentRecord as key=value pairs, total ≤512 bytes.
func RegisterSelf(ctx context.Context, instanceName string, port int, rec AgentRecord) (*zeroconf.Server, error) {
	txt := agentRecordToTXT(rec)

	// Validate size
	total := 0
	for _, kv := range txt {
		total += len(kv) + 1 // +1 for length prefix byte in DNS-SD
	}
	if total > maxTXTBytes {
		return nil, fmt.Errorf("TXT record too large: %d bytes (max %d)", total, maxTXTBytes)
	}

	server, err := zeroconf.Register(instanceName, mdnsService, mDNSDomain, port, txt, nil)
	if err != nil {
		return nil, fmt.Errorf("mDNS register: %w", err)
	}
	log.Printf("mDNS: registered %s on %s port %d (%d TXT bytes)", instanceName, mdnsService, port, total)
	return server, nil
}

// BrowseAgents listens for _amf-agent._tcp advertisements on the local network.
// Discovered agents are added to the registry and published to NATS as capability.advertise events.
func BrowseAgents(ctx context.Context) {
	resolver, err := zeroconf.NewResolver(nil)
	if err != nil {
		log.Printf("mDNS: failed to create resolver: %v", err)
		return
	}

	entries := make(chan *zeroconf.ServiceEntry)

	go func() {
		for {
			select {
			case entry, ok := <-entries:
				if !ok {
					return
				}
				handleDiscoveredEntry(entry)
			case <-ctx.Done():
				return
			}
		}
	}()

	log.Printf("mDNS: browsing for %s", mdnsService)
	if err := resolver.Browse(ctx, mdnsService, mDNSDomain, entries); err != nil {
		log.Printf("mDNS browse error: %v", err)
	}
}

func handleDiscoveredEntry(entry *zeroconf.ServiceEntry) {
	rec, err := txtToAgentRecord(entry.Text)
	if err != nil {
		log.Printf("mDNS: invalid TXT from %s: %v", entry.Instance, err)
		return
	}

	agent := &DiscoveredAgent{
		Record:       rec,
		InstanceName: entry.Instance,
		Host:         entry.HostName,
		Port:         entry.Port,
		SeenAt:       time.Now(),
	}

	agentRegistry.Lock()
	agentRegistry.agents[rec.AgentID] = agent
	agentRegistry.Unlock()

	log.Printf("mDNS: discovered agent %s at %s:%d (tags: %v)", rec.AgentID, entry.HostName, entry.Port, rec.CapabilityTags)

	// Publish discovery event to NATS fabric
	if nc != nil && nc.IsConnected() {
		id := uuid.New().String()
		evt := NewEvent(
			id, uuid.New().String(),
			"spiffe://local/agent/amf-discovery",
			TypeCapabilityAdvertise,
			RoleWatcher,
			agent,
		)
		evt.TrustDomain = rec.TrustDomain
		if data, err := json.Marshal(evt); err == nil {
			nc.Publish(evt.Type, data)
		}
	}
}

// agentRecordToTXT encodes an AgentRecord as DNS-SD TXT key=value pairs.
// Uses short keys to stay under the 512-byte limit.
func agentRecordToTXT(rec AgentRecord) []string {
	protos, _ := json.Marshal(rec.ProtocolsSupported)
	tags, _ := json.Marshal(rec.CapabilityTags)
	return []string{
		"id=" + rec.AgentID,
		"ep=" + rec.Endpoint,
		"proto=" + string(protos),
		"tags=" + string(tags),
		"td=" + rec.TrustDomain,
		"vis=" + rec.Visibility,
		"v=" + rec.Version,
		"status=" + rec.Status,
		"card=" + rec.CardURL,
	}
}

// txtToAgentRecord parses DNS-SD TXT key=value pairs back into an AgentRecord.
func txtToAgentRecord(txt []string) (AgentRecord, error) {
	m := make(map[string]string)
	for _, kv := range txt {
		for i, c := range kv {
			if c == '=' {
				m[kv[:i]] = kv[i+1:]
				break
			}
		}
	}
	if m["id"] == "" || m["ep"] == "" {
		return AgentRecord{}, fmt.Errorf("missing required fields id or ep")
	}
	var protos, tags []string
	json.Unmarshal([]byte(m["proto"]), &protos)
	json.Unmarshal([]byte(m["tags"]), &tags)

	return AgentRecord{
		AgentID:            m["id"],
		Endpoint:           m["ep"],
		ProtocolsSupported: protos,
		CapabilityTags:     tags,
		TrustDomain:        m["td"],
		Visibility:         m["vis"],
		Version:            m["v"],
		Status:             m["status"],
		CardURL:            m["card"],
	}, nil
}

// GET /agents — returns all currently known agents from the registry
func handleAgents(w http.ResponseWriter, r *http.Request) {
	agentRegistry.RLock()
	defer agentRegistry.RUnlock()

	agents := make([]*DiscoveredAgent, 0, len(agentRegistry.agents))
	for _, a := range agentRegistry.agents {
		agents = append(agents, a)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(map[string]any{
		"agents": agents,
		"count":  len(agents),
	})
}
