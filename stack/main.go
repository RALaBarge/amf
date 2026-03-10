package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	nats "github.com/nats-io/nats.go"
)

const (
	httpAddr   = ":8765"
	amfSubject = "amf.>"
)

var (
	nc       *nats.Conn
	natsCfg  *AMFNATSConfig
	identity IdentityProvider
)

func connectNATSWithRole(url string) *nats.Conn {
	var conn *nats.Conn
	var err error
	for i := range 5 {
		conn, err = nats.Connect(url)
		if err == nil {
			return conn
		}
		log.Printf("NATS connect attempt %d failed: %v", i+1, err)
		time.Sleep(500 * time.Millisecond)
	}
	log.Fatalf("could not connect to NATS: %v", err)
	return nil
}

// GET /events — SSE stream of all amf.> events
func handleEvents(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	msgs := make(chan *nats.Msg, 64)
	sub, err := nc.ChanSubscribe(amfSubject, msgs)
	if err != nil {
		http.Error(w, "subscription failed", http.StatusInternalServerError)
		return
	}
	defer sub.Unsubscribe()

	log.Printf("SSE client connected from %s", r.RemoteAddr)
	fmt.Fprintf(w, "data: {\"type\":\"connected\",\"ts\":%d}\n\n", time.Now().UnixMilli())
	flusher.Flush()

	for {
		select {
		case msg := <-msgs:
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", msg.Subject, msg.Data)
			flusher.Flush()
		case <-r.Context().Done():
			log.Printf("SSE client disconnected from %s", r.RemoteAddr)
			return
		}
	}
}

// POST /publish — inject a test event into the fabric
func handlePublish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	var payload map[string]any
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	msgType, _ := payload["message_type"].(string)
	if msgType == "" {
		msgType = "amf.test.event"
	}
	delete(payload, "message_type")

	id := uuid.New().String()
	evt := NewEvent(
		id, uuid.New().String(),
		"spiffe://local/agent/amf-dev",
		msgType,
		RoleCoordinator,
		payload,
	)

	data, err := json.Marshal(evt)
	if err != nil {
		http.Error(w, "marshal error", http.StatusInternalServerError)
		return
	}

	if err := nc.Publish(evt.Type, data); err != nil {
		http.Error(w, "publish failed", http.StatusInternalServerError)
		return
	}

	log.Printf("published %s", evt.Type)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "subject": evt.Type, "message_id": evt.ID})
}

// GET /health
func handleHealth(w http.ResponseWriter, r *http.Request) {
	status := "ok"
	if nc == nil || !nc.IsConnected() {
		status = "nats_disconnected"
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": status, "nats": "nats://127.0.0.1:4222"})
}

// GET /.well-known/agent-card.json — A2A agent card (full capability description)
func handleAgentCard(rec AgentRecord) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		card := map[string]any{
			"name":        "AMF Dev Node",
			"description": "AMF observability and event fabric node",
			"url":         rec.Endpoint,
			"version":     rec.Version,
			"capabilities": map[string]bool{
				"streaming":         true,
				"pushNotifications": false,
			},
			"skills": []map[string]any{
				{
					"id":          "event-fabric",
					"name":        "Event Fabric",
					"description": "Publish and subscribe to typed AMF events via NATS",
					"tags":        rec.CapabilityTags,
				},
			},
			"defaultInputModes":  []string{"application/json"},
			"defaultOutputModes": []string{"application/json", "text/event-stream"},
			// AMF extensions
			"x-amf": map[string]any{
				"agent_record":    rec,
				"schema_version":  "1.0.0",
				"protocols":       rec.ProtocolsSupported,
				"trust_domain":    rec.TrustDomain,
				"nats_url":        "nats://127.0.0.1:4222",
			},
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		json.NewEncoder(w).Encode(card)
	}
}

// GET / — web UI
func handleUI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(indexHTML))
}

func main() {
	// Identity provider — static (default) or SPIFFE if socket present
	identity = NewIdentityProvider("coordinator", "amf-dev", "local")
	log.Printf("identity: mode=%s id=%s", identity.Mode(), identity.AgentID())

	// NATS with ACL config
	var err error
	natsCfg, err := LoadNATSConfig()
	if err != nil {
		log.Fatalf("nats config: %v", err)
	}
	natscmd := startNATSWithACL(natsCfg)
	defer natscmd.Process.Kill()

	nc = connectNATSWithRole(natsCfg.CoordinatorURL(""))
	defer nc.Close()
	log.Printf("coordinator: connected to NATS (role=coordinator)")

	// Log specialist URL so workers can be started manually
	log.Printf("workers connect with: %s", natsCfg.SpecialistURL())

	// Publish a startup heartbeat
	id := uuid.New().String()
	startEvt := NewEvent(
		id, uuid.New().String(),
		identity.AgentID(),
		TypeAgentHeartbeat,
		RoleCoordinator,
		map[string]string{"status": "online", "note": "AMF dev stack started"},
	)
	if data, err := json.Marshal(startEvt); err == nil {
		nc.Publish(startEvt.Type, data)
	}

	// Start OPA policy engine
	StartOPA("policies")
	defer StopOPA()

	// Start DMZ watcher loop (auto-respawning)
	watcherCtx, watcherCancel := context.WithCancel(context.Background())
	defer watcherCancel()
	StartWatcherLoop(watcherCtx)

	// Coordinator: subscribe to classified summaries from the watcher, run OPA, admit to registry
	nc.Subscribe(classifiedSubject, func(msg *nats.Msg) {
		var summary WatcherSummary
		if err := json.Unmarshal(msg.Data, &summary); err != nil {
			log.Printf("coordinator: bad classified message: %v", err)
			return
		}

		// OPA policy check
		allowed := EvaluateAdvertisement(&summary)
		summary.Approved = allowed

		if !allowed {
			return
		}

		// Admit to registry
		agent := &DiscoveredAgent{
			Record: AgentRecord{
				AgentID:            summary.OriginalAgentID,
				Endpoint:           summary.Endpoint,
				ProtocolsSupported: summary.ProtocolsSupported,
				CapabilityTags:     summary.ExtractedCapabilities,
				TrustDomain:        summary.TrustDomain,
				CardURL:            summary.CardURL,
				Status:             "active",
			},
			SeenAt: time.Now(),
		}
		agentRegistry.Lock()
		agentRegistry.agents[summary.OriginalAgentID] = agent
		agentRegistry.Unlock()

		// Publish capability.advertise to the public fabric
		id := uuid.New().String()
		evt := NewEvent(id, uuid.New().String(),
			"spiffe://local/coordinator",
			TypeCapabilityAdvertise,
			RoleCoordinator,
			summary,
		)
		evt.TrustDomain = summary.TrustDomain
		if data, err := json.Marshal(evt); err == nil {
			nc.Publish(evt.Type, data)
		}
		log.Printf("coordinator: admitted agent %s to mesh", summary.OriginalAgentID)
	})

	// Start mDNS — register self and browse for peers
	selfRecord := AgentRecord{
		AgentID:            identity.AgentID(),
		Endpoint:           fmt.Sprintf("http://localhost%s", httpAddr),
		ProtocolsSupported: []string{"A2A/1.0", "MCP/2024-11-05"},
		CapabilityTags:     []string{"observability", "event-fabric"},
		TrustDomain:        identity.TrustDomain(),
		Visibility:         "local",
		Version:            "1.0.0",
		Status:             "active",
		CardURL:            fmt.Sprintf("http://localhost%s/.well-known/agent-card.json", httpAddr),
	}

	mdnsCtx, mdnsCancel := context.WithCancel(context.Background())
	defer mdnsCancel()

	mdnsSrv, err := RegisterSelf(mdnsCtx, "amf-dev", mdnsPort, selfRecord)
	if err != nil {
		log.Printf("mDNS register warning: %v", err)
	} else {
		defer mdnsSrv.Shutdown()
	}
	go BrowseAgents(mdnsCtx)

	mux := http.NewServeMux()
	mux.HandleFunc("/events", handleEvents)
	mux.HandleFunc("/publish", handlePublish)
	mux.HandleFunc("/agents", handleAgents)
	mux.HandleFunc("/.well-known/agent-card.json", handleAgentCard(selfRecord))
	mux.HandleFunc("/health", handleHealth)
	// OpenAI-compatible API
	mux.HandleFunc("/v1/models", handleOAIModels)
	mux.HandleFunc("/v1/chat/completions", handleOAIChat)
	mux.HandleFunc("/", handleUI)

	srv := &http.Server{Addr: httpAddr, Handler: mux}

	go func() {
		log.Printf("AMF observability server listening on http://localhost%s", httpAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server: %v", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("shutting down")
}

const indexHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>AMF Event Fabric</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: #0d1117; color: #e6edf3; font-family: 'SF Mono', 'Fira Code', monospace; font-size: 13px; }
  header { padding: 16px 24px; border-bottom: 1px solid #21262d; display: flex; align-items: center; gap: 16px; }
  h1 { font-size: 15px; font-weight: 600; color: #58a6ff; }
  .status { font-size: 11px; padding: 3px 8px; border-radius: 12px; background: #161b22; border: 1px solid #30363d; }
  .status.connected { border-color: #3fb950; color: #3fb950; }
  .status.disconnected { border-color: #f85149; color: #f85149; }
  .controls { margin-left: auto; display: flex; gap: 8px; }
  button { background: #21262d; border: 1px solid #30363d; color: #e6edf3; padding: 4px 12px; border-radius: 6px; cursor: pointer; font-size: 12px; font-family: inherit; }
  button:hover { background: #30363d; }
  button.primary { background: #238636; border-color: #2ea043; }
  button.primary:hover { background: #2ea043; }
  .tabs { display: flex; gap: 0; border-bottom: 1px solid #21262d; padding: 0 24px; }
  .tab { padding: 8px 16px; cursor: pointer; border-bottom: 2px solid transparent; color: #8b949e; font-size: 12px; }
  .tab.active { color: #58a6ff; border-bottom-color: #58a6ff; }
  .panel { display: none; }
  .panel.active { display: block; }
  #log { height: calc(100vh - 175px); overflow-y: auto; padding: 12px 24px; }
  #agents-panel { height: calc(100vh - 175px); overflow-y: auto; padding: 16px 24px; }
  .event { padding: 8px 0; border-bottom: 1px solid #161b22; display: grid; grid-template-columns: 140px 240px 1fr; gap: 12px; align-items: start; }
  .event:hover { background: #161b22; margin: 0 -24px; padding: 8px 24px; }
  .ts { color: #8b949e; font-size: 11px; padding-top: 2px; }
  .subject { color: #79c0ff; }
  .subject.discovery { color: #56d364; }
  .subject.task { color: #f0883e; }
  .subject.policy { color: #f85149; }
  .subject.artifact { color: #d2a8ff; }
  .subject.result { color: #58a6ff; }
  .payload { color: #8b949e; white-space: pre-wrap; word-break: break-all; }
  .payload .key { color: #79c0ff; }
  .payload .str { color: #a5d6ff; }
  .payload .num { color: #79c0ff; }
  .agent-card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 14px 16px; margin-bottom: 12px; display: grid; grid-template-columns: 1fr auto; gap: 8px; }
  .agent-id { color: #58a6ff; font-size: 12px; }
  .agent-meta { color: #8b949e; font-size: 11px; margin-top: 4px; }
  .agent-tags { display: flex; gap: 6px; flex-wrap: wrap; margin-top: 6px; }
  .tag { background: #21262d; border: 1px solid #30363d; padding: 2px 8px; border-radius: 10px; font-size: 11px; color: #79c0ff; }
  .proto-badge { background: #1f3a2d; border: 1px solid #2ea043; color: #3fb950; padding: 2px 8px; border-radius: 10px; font-size: 10px; }
  .seen { color: #8b949e; font-size: 10px; text-align: right; }
  .no-agents { color: #8b949e; padding: 32px; text-align: center; }
  .publish-bar { padding: 12px 24px; border-top: 1px solid #21262d; display: flex; gap: 8px; }
  input[type=text] { flex: 1; background: #161b22; border: 1px solid #30363d; color: #e6edf3; padding: 6px 12px; border-radius: 6px; font-family: inherit; font-size: 12px; }
  input[type=text]:focus { outline: none; border-color: #58a6ff; }
  #count { color: #8b949e; font-size: 11px; }
  #agent-count { color: #8b949e; font-size: 11px; }
</style>
</head>
<body>
<header>
  <h1>AMF Event Fabric</h1>
  <span class="status disconnected" id="status">disconnected</span>
  <span id="count">0 events</span>
  <span id="agent-count">0 agents</span>
  <div class="controls">
    <button onclick="refreshAgents()">Refresh</button>
    <button onclick="clearLog()">Clear</button>
    <button onclick="togglePause()" id="pauseBtn">Pause</button>
  </div>
</header>
<div class="tabs">
  <div class="tab active" onclick="switchTab('events')">Events</div>
  <div class="tab" onclick="switchTab('agents')">Mesh Agents</div>
</div>
<div id="events-panel" class="panel active">
  <div id="log"></div>
  <div class="publish-bar">
    <input type="text" id="msgType" value="amf.task.announce" placeholder="message_type">
    <input type="text" id="payload" value='{"task": "test task", "priority": "normal"}' placeholder='{"key": "value"}'>
    <button class="primary" onclick="publish()">Publish</button>
  </div>
</div>
<div id="agents-panel" class="panel">
  <div id="agents-list"><div class="no-agents">Browsing for agents on local mesh...</div></div>
</div>
<script>
let count = 0, paused = false;

function colorSubject(s) {
  if (s.includes('.discovery.')) return 'discovery';
  if (s.includes('.task.')) return 'task';
  if (s.includes('.policy.')) return 'policy';
  if (s.includes('.artifact.') || s.includes('.evidence.')) return 'artifact';
  if (s.includes('.result.')) return 'result';
  return '';
}

function syntaxHighlight(json) {
  return json
    .replace(/"([^"]+)":/g, '<span class="key">"$1"</span>:')
    .replace(/: "([^"]*)"/g, ': <span class="str">"$1"</span>')
    .replace(/: (\d+\.?\d*)/g, ': <span class="num">$1</span>');
}

function addEvent(subject, data) {
  if (paused) return;
  const log = document.getElementById('log');
  const div = document.createElement('div');
  div.className = 'event';
  const ts = new Date().toISOString().slice(11,23);
  let pretty = data;
  try { pretty = JSON.stringify(JSON.parse(data), null, 2); } catch(e) {}
  div.innerHTML =
    '<span class="ts">' + ts + '</span>' +
    '<span class="subject ' + colorSubject(subject) + '">' + subject + '</span>' +
    '<span class="payload">' + syntaxHighlight(pretty) + '</span>';
  log.appendChild(div);
  log.scrollTop = log.scrollHeight;
  document.getElementById('count').textContent = (++count) + ' events';
  if (subject.includes('discovery')) refreshAgents();
}

function connectSSE() {
  const statusEl = document.getElementById('status');
  fetch('/events').then(res => {
    const r = res.body.getReader();
    statusEl.textContent = 'connected'; statusEl.className = 'status connected';
    let buf = '';
    function pump() {
      return r.read().then(({ done, value }) => {
        if (done) { statusEl.textContent = 'reconnecting...'; statusEl.className = 'status disconnected'; setTimeout(connectSSE, 2000); return; }
        buf += new TextDecoder().decode(value);
        const blocks = buf.split('\n\n');
        buf = blocks.pop();
        for (const block of blocks) {
          const lines = block.split('\n');
          let subject = 'amf.event', data = '';
          for (const l of lines) {
            if (l.startsWith('event: ')) subject = l.slice(7);
            if (l.startsWith('data: ')) data = l.slice(6);
          }
          if (data) addEvent(subject, data);
        }
        return pump();
      });
    }
    return pump();
  }).catch(() => { setTimeout(connectSSE, 2000); });
}

async function publish() {
  const msgType = document.getElementById('msgType').value;
  const payloadStr = document.getElementById('payload').value;
  let payload;
  try { payload = JSON.parse(payloadStr); } catch(e) { alert('invalid JSON payload'); return; }
  payload.message_type = msgType;
  await fetch('/publish', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload) });
}

async function refreshAgents() {
  const res = await fetch('/agents');
  const j = await res.json();
  document.getElementById('agent-count').textContent = j.count + ' agent' + (j.count !== 1 ? 's' : '');
  const list = document.getElementById('agents-list');
  if (!j.agents || j.agents.length === 0) {
    list.innerHTML = '<div class="no-agents">No agents discovered yet. Browsing ' + mdnsService + '...</div>';
    return;
  }
  list.innerHTML = j.agents.map(a => {
    const r = a.record;
    const protos = (r.protocols_supported||[]).map(p => '<span class="proto-badge">'+p+'</span>').join(' ');
    const tags = (r.capability_tags||[]).map(t => '<span class="tag">'+t+'</span>').join('');
    const ago = Math.round((Date.now() - new Date(a.seen_at)) / 1000);
    return '<div class="agent-card">' +
      '<div>' +
        '<div class="agent-id">' + (r.agent_id||a.instance_name) + '</div>' +
        '<div class="agent-meta">' + (r.endpoint||a.host+':'+a.port) + ' · ' + r.trust_domain + ' · ' + protos + '</div>' +
        '<div class="agent-tags">' + tags + '</div>' +
      '</div>' +
      '<div class="seen">seen ' + ago + 's ago<br>' + r.status + '</div>' +
    '</div>';
  }).join('');
}

const mdnsService = '_amf-agent._tcp';
function clearLog() { document.getElementById('log').innerHTML = ''; count = 0; document.getElementById('count').textContent = '0 events'; }
function togglePause() { paused = !paused; document.getElementById('pauseBtn').textContent = paused ? 'Resume' : 'Pause'; }
function switchTab(t) {
  document.querySelectorAll('.tab').forEach((el,i) => el.classList.toggle('active', ['events','agents'][i]===t));
  document.querySelectorAll('.panel').forEach(el => el.classList.remove('active'));
  document.getElementById(t+'-panel').classList.add('active');
  if (t === 'agents') refreshAgents();
}

connectSSE();
refreshAgents();
setInterval(refreshAgents, 10000);
</script>
</body>
</html>`
