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
	"github.com/miekg/dns"
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

// ---------------------------------------------------------------------------
// DNS-SD via unicast DNS (RFC 6763 §11)
//
// Same service type (_amf-agent._tcp) and TXT record format as mDNS, but
// queried against a real DNS zone over unicast. This extends discovery to
// agents beyond the local link — any agent that publishes DNS records in
// the zone is discovered automatically.
//
// Record layout in the zone (operator adds these once):
//
//   ; PTR — enables enumeration of service instances
//   _amf-agent._tcp.<domain>.   300 IN PTR  <instance>._amf-agent._tcp.<domain>.
//
//   ; SRV — host and port
//   <instance>._amf-agent._tcp.<domain>.  300 IN SRV  0 0 <port> <host>.
//
//   ; TXT — same key=value pairs as mDNS TXT record
//   <instance>._amf-agent._tcp.<domain>.  300 IN TXT  "id=..." "ep=..." ...
//
// The coordinator polls every 60s. Newly-seen agents are routed through the
// same DMZ watcher pipeline as mDNS-discovered agents — no special trust.

const dnsPollInterval = 60 * time.Second

// BrowseAgentsDNS polls a DNS zone for _amf-agent._tcp.<domain> PTR records
// and routes each newly-discovered agent through the DMZ watcher pipeline.
func BrowseAgentsDNS(ctx context.Context, domain string) {
	log.Printf("dns-sd: browsing _amf-agent._tcp.%s (poll every %s)", domain, dnsPollInterval)
	seen := make(map[string]bool)

	poll := func() {
		records, err := lookupAMFAgents(domain)
		if err != nil {
			log.Printf("dns-sd: lookup error: %v", err)
			return
		}
		for _, rec := range records {
			key := rec.AgentID + "|" + rec.Endpoint
			if seen[key] {
				continue
			}
			seen[key] = true
			log.Printf("dns-sd: discovered agent %s at %s — routing through DMZ watcher", rec.AgentID, rec.Endpoint)
			if nc != nil && nc.IsConnected() {
				raw, err := json.Marshal(rec)
				if err == nil {
					nc.Publish(rawSubject, raw)
				}
			}
		}
	}

	poll() // immediate first poll on startup
	t := time.NewTicker(dnsPollInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			poll()
		case <-ctx.Done():
			return
		}
	}
}

// lookupAMFAgents enumerates service instances via PTR then resolves each one.
func lookupAMFAgents(domain string) ([]AgentRecord, error) {
	resolver, err := systemResolver()
	if err != nil {
		return nil, err
	}
	c := &dns.Client{Timeout: 5 * time.Second}
	service := dns.Fqdn("_amf-agent._tcp." + domain)

	m := new(dns.Msg)
	m.SetQuestion(service, dns.TypePTR)
	resp, _, err := c.Exchange(m, resolver)
	if err != nil {
		return nil, fmt.Errorf("PTR %s: %w", service, err)
	}

	var records []AgentRecord
	for _, ans := range resp.Answer {
		ptr, ok := ans.(*dns.PTR)
		if !ok {
			continue
		}
		rec, err := resolveServiceInstance(c, resolver, ptr.Ptr)
		if err != nil {
			log.Printf("dns-sd: instance %s: %v", ptr.Ptr, err)
			continue
		}
		records = append(records, rec)
	}
	return records, nil
}

// resolveServiceInstance fetches TXT (and SRV as fallback) for one instance.
func resolveServiceInstance(c *dns.Client, resolver, instance string) (AgentRecord, error) {
	m := new(dns.Msg)
	m.SetQuestion(dns.Fqdn(instance), dns.TypeTXT)
	resp, _, err := c.Exchange(m, resolver)
	if err != nil {
		return AgentRecord{}, fmt.Errorf("TXT query: %w", err)
	}

	var kv []string
	for _, ans := range resp.Answer {
		if t, ok := ans.(*dns.TXT); ok {
			kv = append(kv, t.Txt...)
		}
	}
	if len(kv) == 0 {
		return AgentRecord{}, fmt.Errorf("no TXT records for %s", instance)
	}

	rec, err := txtToAgentRecord(kv)
	if err != nil {
		return AgentRecord{}, fmt.Errorf("TXT parse: %w", err)
	}

	// If ep= was absent in TXT, synthesize endpoint from SRV target:port
	if rec.Endpoint == "" {
		m2 := new(dns.Msg)
		m2.SetQuestion(dns.Fqdn(instance), dns.TypeSRV)
		if resp2, _, err2 := c.Exchange(m2, resolver); err2 == nil {
			for _, ans := range resp2.Answer {
				if srv, ok := ans.(*dns.SRV); ok {
					host := strings.TrimSuffix(srv.Target, ".")
					rec.Endpoint = fmt.Sprintf("http://%s:%d", host, srv.Port)
					break
				}
			}
		}
	}

	return rec, nil
}

// systemResolver returns "host:port" for the first nameserver in /etc/resolv.conf.
// Falls back to 8.8.8.8:53 if the file is missing or empty.
func systemResolver() (string, error) {
	cfg, err := dns.ClientConfigFromFile("/etc/resolv.conf")
	if err != nil || len(cfg.Servers) == 0 {
		return "8.8.8.8:53", nil
	}
	return cfg.Servers[0] + ":" + cfg.Port, nil
}

// DNSZoneEntries returns the DNS records an agent needs to be discoverable
// via BrowseAgentsDNS. Print these and add them to the zone.
func DNSZoneEntries(domain, publicHost, instanceName string, port int, txt []string) string {
	fqdn := instanceName + "._amf-agent._tcp." + domain + "."
	quoted := make([]string, len(txt))
	for i, t := range txt {
		quoted[i] = `"` + t + `"`
	}
	return fmt.Sprintf(
		"; --- AMF DNS-SD records for %s (add to your %s zone) ---\n"+
			"; PTR — service type enumeration\n"+
			"_amf-agent._tcp.%s. 300 IN PTR %s\n"+
			"; SRV — service location\n"+
			"%s 300 IN SRV 0 0 %d %s.\n"+
			"; TXT — agent metadata\n"+
			"%s 300 IN TXT %s\n"+
			"; --- end ---",
		instanceName, domain,
		domain, fqdn,
		fqdn, port, publicHost,
		fqdn, strings.Join(quoted, " "),
	)
}
