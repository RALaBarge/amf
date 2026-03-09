package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

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

	log.Printf("mDNS: raw advertisement from %s at %s:%d — routing through DMZ watcher", entry.Instance, entry.HostName, entry.Port)

	// Route through DMZ watcher — do NOT add to registry or publish directly.
	// The watcher validates, risk-scores, and publishes to amf.internal.classified.
	// The coordinator subscribes to amf.internal.classified and makes the final decision.
	if nc != nil && nc.IsConnected() {
		raw, err := json.Marshal(rec)
		if err == nil {
			nc.Publish(rawSubject, raw)
		}
	}
}

// agentRecordToTXT encodes an AgentRecord as DNS-SD TXT key=value pairs.
// Uses comma-separated values (not JSON) to avoid quote-escaping issues in zeroconf.
func agentRecordToTXT(rec AgentRecord) []string {
	return []string{
		"id=" + rec.AgentID,
		"ep=" + rec.Endpoint,
		"proto=" + strings.Join(rec.ProtocolsSupported, ","),
		"tags=" + strings.Join(rec.CapabilityTags, ","),
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
	splitCSV := func(s string) []string {
		if s == "" {
			return nil
		}
		return strings.Split(s, ",")
	}
	return AgentRecord{
		AgentID:            m["id"],
		Endpoint:           m["ep"],
		ProtocolsSupported: splitCSV(m["proto"]),
		CapabilityTags:     splitCSV(m["tags"]),
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
