package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	nats "github.com/nats-io/nats.go"
)

const (
	natsURL    = "nats://127.0.0.1:4222"
	httpAddr   = ":8765"
	amfSubject = "amf.>"
)

var nc *nats.Conn

func startNATS() *exec.Cmd {
	// Find nats-server in PATH or ~/bin
	natsPath, err := exec.LookPath("nats-server")
	if err != nil {
		home, _ := os.UserHomeDir()
		natsPath = home + "/bin/nats-server"
	}
	cmd := exec.Command(natsPath, "--port", "4222")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		log.Fatalf("failed to start nats-server: %v", err)
	}
	log.Printf("nats-server started (pid %d)", cmd.Process.Pid)
	time.Sleep(300 * time.Millisecond)
	return cmd
}

func connectNATS() {
	var err error
	for i := range 5 {
		nc, err = nats.Connect(natsURL)
		if err == nil {
			log.Printf("connected to NATS at %s", natsURL)
			return
		}
		log.Printf("NATS connect attempt %d failed: %v", i+1, err)
		time.Sleep(500 * time.Millisecond)
	}
	log.Fatalf("could not connect to NATS: %v", err)
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
	json.NewEncoder(w).Encode(map[string]string{"status": status, "nats": natsURL})
}

// GET / — web UI
func handleUI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(indexHTML))
}

func main() {
	natscmd := startNATS()
	defer natscmd.Process.Kill()

	connectNATS()
	defer nc.Close()

	// Publish a startup heartbeat
	id := uuid.New().String()
	startEvt := NewEvent(
		id, uuid.New().String(),
		"spiffe://local/agent/amf-dev",
		TypeAgentHeartbeat,
		RoleCoordinator,
		map[string]string{"status": "online", "note": "AMF dev stack started"},
	)
	if data, err := json.Marshal(startEvt); err == nil {
		nc.Publish(startEvt.Type, data)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/events", handleEvents)
	mux.HandleFunc("/publish", handlePublish)
	mux.HandleFunc("/health", handleHealth)
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
  #log { height: calc(100vh - 130px); overflow-y: auto; padding: 12px 24px; }
  .event { padding: 8px 0; border-bottom: 1px solid #161b22; display: grid; grid-template-columns: 140px 220px 1fr; gap: 12px; align-items: start; }
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
  .publish-bar { padding: 12px 24px; border-top: 1px solid #21262d; display: flex; gap: 8px; }
  input[type=text] { flex: 1; background: #161b22; border: 1px solid #30363d; color: #e6edf3; padding: 6px 12px; border-radius: 6px; font-family: inherit; font-size: 12px; }
  input[type=text]:focus { outline: none; border-color: #58a6ff; }
  #count { color: #8b949e; font-size: 11px; }
</style>
</head>
<body>
<header>
  <h1>AMF Event Fabric</h1>
  <span class="status disconnected" id="status">disconnected</span>
  <span id="count">0 events</span>
  <div class="controls">
    <button onclick="clearLog()">Clear</button>
    <button onclick="togglePause()" id="pauseBtn">Pause</button>
  </div>
</header>
<div id="log"></div>
<div class="publish-bar">
  <input type="text" id="msgType" value="amf.task.announce" placeholder="message_type">
  <input type="text" id="payload" value='{"task": "test task", "priority": "normal"}' placeholder='{"key": "value"}'>
  <button class="primary" onclick="publish()">Publish</button>
</div>
<script>
let count = 0, paused = false, es;

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
}

function connect() {
  es = new EventSource('/events');
  es.onopen = () => {
    const s = document.getElementById('status');
    s.textContent = 'connected'; s.className = 'status connected';
  };
  es.onerror = () => {
    const s = document.getElementById('status');
    s.textContent = 'reconnecting...'; s.className = 'status disconnected';
    setTimeout(connect, 2000);
  };
  es.addEventListener('message', e => addEvent('system', e.data));
  // catch all named events (amf subjects)
  const origAddEventListener = es.addEventListener.bind(es);
  es.onmessage = e => addEvent('system', e.data);
  // Use a polling approach for named events via EventSource
}

// EventSource only fires onmessage for unnamed events.
// Our server sends named events (event: amf.task.announce).
// We need a custom reader.
function connectSSE() {
  const statusEl = document.getElementById('status');
  const reader = new ReadableStream({
    start(controller) {
      fetch('/events').then(res => {
        const r = res.body.getReader();
        statusEl.textContent = 'connected'; statusEl.className = 'status connected';
        let buf = '';
        function pump() {
          return r.read().then(({ done, value }) => {
            if (done) {
              statusEl.textContent = 'reconnecting...'; statusEl.className = 'status disconnected';
              setTimeout(connectSSE, 2000);
              return;
            }
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
      }).catch(() => {
        statusEl.textContent = 'reconnecting...'; statusEl.className = 'status disconnected';
        setTimeout(connectSSE, 2000);
      });
    }
  });
}

async function publish() {
  const msgType = document.getElementById('msgType').value;
  const payloadStr = document.getElementById('payload').value;
  let payload;
  try { payload = JSON.parse(payloadStr); } catch(e) { alert('invalid JSON payload'); return; }
  payload.message_type = msgType;
  await fetch('/publish', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(payload)
  });
}

function clearLog() {
  document.getElementById('log').innerHTML = '';
  count = 0;
  document.getElementById('count').textContent = '0 events';
}

function togglePause() {
  paused = !paused;
  document.getElementById('pauseBtn').textContent = paused ? 'Resume' : 'Pause';
}

connectSSE();
</script>
</body>
</html>`
