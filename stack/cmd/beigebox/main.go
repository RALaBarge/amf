// Beigebox — local LLM proxy with MCP advertisement for the AMF mesh.
//
// The core job: take a local LLM backend (Ollama by default) and advertise
// it into the AMF mesh as an MCP-capable node. The coordinator discovers
// beigebox via mDNS, validates it through the DMZ watcher, and can then
// invoke local LLM inference via standardized MCP tools.
//
// What it does:
//   1. Proxies /v1/chat/completions → local LLM backend (Ollama-compatible)
//   2. Exposes POST /mcp — MCP JSON-RPC with tools: chat, list_models, echo
//   3. Registers on mDNS (_amf-agent._tcp.local) with mcp=<url> in TXT record
//   4. Publishes NATS heartbeats on amf.discovery.agent.heartbeat (every 30s)
//   5. Serves /.well-known/agent-card.json with x-amf.mcp_endpoint
//
// The mDNS TXT record is the advertisement: it tells the coordinator
// "there is an MCP endpoint here with these capabilities." Everything
// else flows from that admission.
//
// Run:
//   go run ./cmd/beigebox
//   go build -o beigebox ./cmd/beigebox && ./beigebox --backend http://localhost:11434
//
// Environment variables:
//   AMF_SPECIALIST_PASS   NATS specialist password (default: amf-specialist-local)
//   AMF_BACKEND_URL       LLM backend base URL    (default: http://localhost:11434)
//   AMF_BACKEND_MODEL     Default model to use    (default: llama3.2)
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/grandcat/zeroconf"
	nats "github.com/nats-io/nats.go"
)

// --- Config ------------------------------------------------------------------

var (
	port       = flag.Int("port", 8768, "HTTP port for MCP and agent card")
	name       = flag.String("name", "beigebox", "Agent display name")
	backendURL = flag.String("backend", "", "LLM backend base URL (e.g. http://localhost:11434)")
	model      = flag.String("model", "", "Default LLM model name")
	trust      = flag.String("trust", "local", "Trust domain")
	natsAddr   = flag.String("nats", "127.0.0.1:4222", "NATS server address (host:port)")
	dnsDomain  = flag.String("dns-domain", "", "DNS zone for agent advertisement (e.g. agents.example.com)")
	publicHost = flag.String("public-host", "", "Externally-resolvable hostname for DNS SRV record (e.g. mybox.example.com)")
)

const (
	mdnsService    = "_amf-agent._tcp"
	mDNSDomain     = "local."
	heartbeatEvery = 30 * time.Second
)

var (
	nc      *nats.Conn
	agentID string
	backend *backendClient
)

// --- Main --------------------------------------------------------------------

func main() {
	flag.Parse()

	agentID = fmt.Sprintf("spiffe://%s/beigebox/%s", *trust, uuid.New().String())

	// Resolve backend URL: flag → env → default
	bURL := *backendURL
	if bURL == "" {
		bURL = os.Getenv("AMF_BACKEND_URL")
	}
	if bURL == "" {
		bURL = "http://localhost:11434"
	}

	// Resolve model: flag → env → default
	mdl := *model
	if mdl == "" {
		mdl = os.Getenv("AMF_BACKEND_MODEL")
	}
	if mdl == "" {
		mdl = "llama3.2"
	}

	backend = newBackendClient(bURL, mdl)
	log.Printf("beigebox[%s]: backend=%s model=%s port=%d", *name, bURL, mdl, *port)

	// Connect to NATS as specialist — degrade gracefully if unavailable
	pass := os.Getenv("AMF_SPECIALIST_PASS")
	if pass == "" {
		pass = "amf-specialist-local"
	}
	nURL := fmt.Sprintf("nats://specialist:%s@%s", pass, *natsAddr)

	var err error
	for i := range 5 {
		nc, err = nats.Connect(nURL)
		if err == nil {
			break
		}
		log.Printf("beigebox: NATS attempt %d: %v", i+1, err)
		time.Sleep(500 * time.Millisecond)
	}
	if err != nil {
		log.Printf("beigebox: running without NATS (mesh events disabled): %v", err)
		nc = nil
	} else {
		defer nc.Close()
		log.Printf("beigebox: connected to NATS as specialist")
	}

	endpoint := fmt.Sprintf("http://localhost:%d", *port)
	mcpEndpoint := endpoint + "/mcp"
	cardURL := endpoint + "/.well-known/agent-card.json"

	// Register on mDNS — the mcp= field is what the coordinator keys on
	txt := buildTXT(agentID, endpoint, cardURL, mcpEndpoint, mdl)
	mdnsSrv, err := zeroconf.Register(*name, mdnsService, mDNSDomain, *port, txt, nil)
	if err != nil {
		log.Printf("beigebox: mDNS register warning: %v", err)
	} else {
		defer mdnsSrv.Shutdown()
		log.Printf("beigebox: registered on mDNS as %q with MCP at %s", *name, mcpEndpoint)
	}

	// Print DNS zone entries if a domain was specified
	if *dnsDomain != "" {
		host := *publicHost
		if host == "" {
			var hostnameErr error
			host, hostnameErr = os.Hostname()
			if hostnameErr != nil {
				host = "localhost"
			}
		}
		fmt.Println(dnsZoneEntries(*dnsDomain, host, *name, *port, txt))
	}

	// Heartbeat — announce on startup then every 30s
	publishHeartbeat("online", bURL, mdl, mcpEndpoint)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		t := time.NewTicker(heartbeatEvery)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				publishHeartbeat("online", bURL, mdl, mcpEndpoint)
			case <-ctx.Done():
				return
			}
		}
	}()

	// HTTP routes
	mux := http.NewServeMux()
	mux.HandleFunc("/mcp", handleMCP)
	mux.HandleFunc("/v1/chat/completions", handleOAIProxy(bURL, mdl))
	mux.HandleFunc("/v1/models", handleModels(bURL))
	mux.HandleFunc("/.well-known/agent-card.json", serveAgentCard(endpoint, mcpEndpoint, cardURL, mdl))
	mux.HandleFunc("/health", handleHealth(bURL))

	srv := &http.Server{Addr: fmt.Sprintf(":%d", *port), Handler: mux}
	go func() {
		log.Printf("beigebox: listening on %s", endpoint)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("beigebox: http: %v", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	publishHeartbeat("offline", bURL, mdl, mcpEndpoint)
	log.Printf("beigebox: shutting down")
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer shutCancel()
	srv.Shutdown(shutCtx)
}

// buildTXT encodes the mDNS TXT record.
// mcp= is required per AMF spec when declaring MCP support.
// Total must stay ≤ 512 bytes.
func buildTXT(id, endpoint, cardURL, mcpEndpoint, mdl string) []string {
	return []string{
		"id=" + id,
		"ep=" + endpoint,
		"proto=MCP/2024-11-05",
		"tags=llm-chat,llm-completion,local-inference",
		"td=" + *trust,
		"vis=local",
		"v=1.0.0",
		"status=active",
		"card=" + cardURL,
		"mcp=" + mcpEndpoint,
		"model=" + mdl,
	}
}

// --- MCP endpoint ------------------------------------------------------------
//
// Streamable HTTP transport (MCP 2024-11-05):
//   POST /mcp  — all JSON-RPC requests and responses
//   GET  /mcp  — SSE stream for server-initiated messages (keep-alive only for now)

type mcpRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type mcpResponse struct {
	JSONRPC string    `json:"jsonrpc"`
	ID      any       `json:"id,omitempty"`
	Result  any       `json:"result,omitempty"`
	Error   *mcpError `json:"error,omitempty"`
}

type mcpError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type mcpTool struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
}

func handleMCP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Accept, Mcp-Session-Id")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// GET — SSE keep-alive (server-push reserved for future use)
	if r.Method == http.MethodGet {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming not supported", http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, ": beigebox mcp ready\n\n")
		flusher.Flush()
		<-r.Context().Done()
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "POST or GET only", http.StatusMethodNotAllowed)
		return
	}

	var req mcpRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeMCPError(w, nil, -32700, "parse error")
		return
	}

	w.Header().Set("Content-Type", "application/json")

	switch req.Method {
	case "initialize":
		json.NewEncoder(w).Encode(mcpResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result: map[string]any{
				"protocolVersion": "2024-11-05",
				"capabilities":    map[string]any{"tools": map[string]any{}},
				"serverInfo":      map[string]any{"name": *name, "version": "1.0.0"},
				"instructions":    fmt.Sprintf("Beigebox exposes local LLM inference (%s) to the AMF mesh via MCP.", backend.model),
			},
		})

	case "notifications/initialized":
		// Notification — no response body needed
		w.WriteHeader(http.StatusAccepted)

	case "tools/list":
		json.NewEncoder(w).Encode(mcpResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result:  map[string]any{"tools": mcpTools()},
		})

	case "tools/call":
		var p struct {
			Name      string         `json:"name"`
			Arguments map[string]any `json:"arguments"`
		}
		if err := json.Unmarshal(req.Params, &p); err != nil {
			writeMCPError(w, req.ID, -32602, "invalid params")
			return
		}
		result, toolErr := dispatchTool(r.Context(), p.Name, p.Arguments)
		if toolErr != "" {
			json.NewEncoder(w).Encode(mcpResponse{
				JSONRPC: "2.0",
				ID:      req.ID,
				Result: map[string]any{
					"content": []map[string]any{{"type": "text", "text": toolErr}},
					"isError": true,
				},
			})
			return
		}
		json.NewEncoder(w).Encode(mcpResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result: map[string]any{
				"content": []map[string]any{{"type": "text", "text": result}},
			},
		})

	default:
		writeMCPError(w, req.ID, -32601, "method not found: "+req.Method)
	}
}

func writeMCPError(w http.ResponseWriter, id any, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(mcpResponse{
		JSONRPC: "2.0",
		ID:      id,
		Error:   &mcpError{Code: code, Message: msg},
	})
}

// --- MCP tool definitions ----------------------------------------------------

func mcpTools() []mcpTool {
	return []mcpTool{
		{
			Name:        "chat",
			Description: fmt.Sprintf("Send a prompt to the local LLM (%s) and get a completion.", backend.model),
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"prompt": map[string]any{
						"type":        "string",
						"description": "The prompt to send to the LLM",
					},
					"model": map[string]any{
						"type":        "string",
						"description": fmt.Sprintf("Model to use (default: %s)", backend.model),
					},
					"system": map[string]any{
						"type":        "string",
						"description": "Optional system message",
					},
				},
				"required": []string{"prompt"},
			},
		},
		{
			Name:        "list_models",
			Description: "List models available on the local LLM backend.",
			InputSchema: map[string]any{
				"type":       "object",
				"properties": map[string]any{},
			},
		},
		{
			Name:        "echo",
			Description: "Echo a message. Useful for testing connectivity.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"message": map[string]any{"type": "string"},
				},
				"required": []string{"message"},
			},
		},
	}
}

func dispatchTool(ctx context.Context, toolName string, args map[string]any) (result string, errMsg string) {
	switch toolName {
	case "chat":
		prompt, _ := args["prompt"].(string)
		if prompt == "" {
			return "", "prompt is required"
		}
		mdl, _ := args["model"].(string)
		if mdl == "" {
			mdl = backend.model
		}
		system, _ := args["system"].(string)
		return backend.chat(ctx, mdl, system, prompt)

	case "list_models":
		return backend.listModels(ctx)

	case "echo":
		msg, _ := args["message"].(string)
		return msg, ""

	default:
		return "", fmt.Sprintf("unknown tool: %s", toolName)
	}
}

// --- LLM backend client ------------------------------------------------------
//
// Speaks the Ollama API (/api/chat, /api/tags) which is also compatible with
// llama.cpp server and other OpenAI-compatible local inference backends.
// Falls back to OpenAI-compatible endpoints if Ollama-specific calls fail.

type backendClient struct {
	baseURL string
	model   string
	http    *http.Client
}

func newBackendClient(baseURL, model string) *backendClient {
	return &backendClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		model:   model,
		http:    &http.Client{Timeout: 120 * time.Second},
	}
}

// chat sends a prompt to the backend and returns the assistant reply.
func (b *backendClient) chat(ctx context.Context, model, system, prompt string) (string, string) {
	messages := []map[string]string{}
	if system != "" {
		messages = append(messages, map[string]string{"role": "system", "content": system})
	}
	messages = append(messages, map[string]string{"role": "user", "content": prompt})

	// Try Ollama /api/chat first
	body, _ := json.Marshal(map[string]any{
		"model":    model,
		"messages": messages,
		"stream":   false,
	})
	resp, err := b.post(ctx, "/api/chat", body)
	if err == nil {
		var result struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		}
		if json.Unmarshal(resp, &result) == nil && result.Message.Content != "" {
			return result.Message.Content, ""
		}
	}

	// Fallback: OpenAI-compatible /v1/chat/completions
	body, _ = json.Marshal(map[string]any{
		"model":    model,
		"messages": messages,
	})
	resp, err = b.post(ctx, "/v1/chat/completions", body)
	if err != nil {
		return "", fmt.Sprintf("backend unreachable: %v", err)
	}
	var oaiResult struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(resp, &oaiResult); err != nil {
		return "", fmt.Sprintf("backend parse error: %v", err)
	}
	if oaiResult.Error != nil {
		return "", "backend error: " + oaiResult.Error.Message
	}
	if len(oaiResult.Choices) == 0 {
		return "", "backend returned no choices"
	}
	return oaiResult.Choices[0].Message.Content, ""
}

// listModels returns a newline-separated list of available models.
func (b *backendClient) listModels(ctx context.Context) (string, string) {
	// Try Ollama /api/tags
	resp, err := b.get(ctx, "/api/tags")
	if err == nil {
		var result struct {
			Models []struct {
				Name string `json:"name"`
			} `json:"models"`
		}
		if json.Unmarshal(resp, &result) == nil && len(result.Models) > 0 {
			names := make([]string, len(result.Models))
			for i, m := range result.Models {
				names[i] = m.Name
			}
			return strings.Join(names, "\n"), ""
		}
	}

	// Fallback: OpenAI /v1/models
	resp, err = b.get(ctx, "/v1/models")
	if err != nil {
		return "", fmt.Sprintf("backend unreachable: %v", err)
	}
	var result struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(resp, &result); err != nil {
		return "", fmt.Sprintf("parse error: %v", err)
	}
	names := make([]string, len(result.Data))
	for i, m := range result.Data {
		names[i] = m.ID
	}
	return strings.Join(names, "\n"), ""
}

func (b *backendClient) post(ctx context.Context, path string, body []byte) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		b.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := b.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func (b *backendClient) get(ctx context.Context, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.baseURL+path, nil)
	if err != nil {
		return nil, err
	}
	resp, err := b.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// --- OpenAI proxy ------------------------------------------------------------
//
// Pass-through proxy for /v1/chat/completions and /v1/models.
// Allows OpenAI-compatible clients to reach the local LLM directly,
// without going through the mesh coordinator.

func handleOAIProxy(backendBase, defaultModel string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
		if err != nil {
			http.Error(w, "read error", http.StatusBadRequest)
			return
		}

		// Inject default model if not specified
		var req map[string]any
		if json.Unmarshal(body, &req) == nil {
			if _, ok := req["model"]; !ok {
				req["model"] = defaultModel
				body, _ = json.Marshal(req)
			}
		}

		upstreamReq, err := http.NewRequestWithContext(r.Context(), http.MethodPost,
			strings.TrimRight(backendBase, "/")+"/v1/chat/completions",
			bytes.NewReader(body))
		if err != nil {
			http.Error(w, "proxy error", http.StatusInternalServerError)
			return
		}
		upstreamReq.Header.Set("Content-Type", "application/json")
		for _, h := range []string{"Accept", "Authorization"} {
			if v := r.Header.Get(h); v != "" {
				upstreamReq.Header.Set(h, v)
			}
		}

		client := &http.Client{Timeout: 120 * time.Second}
		resp, err := client.Do(upstreamReq)
		if err != nil {
			http.Error(w, fmt.Sprintf("backend unreachable: %v", err), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()

		for k, vs := range resp.Header {
			for _, v := range vs {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
	}
}

func handleModels(backendBase string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		upstream, err := http.Get(strings.TrimRight(backendBase, "/") + "/v1/models")
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"object": "list",
				"data": []map[string]any{{
					"id":       backend.model,
					"object":   "model",
					"owned_by": "local",
				}},
			})
			return
		}
		defer upstream.Body.Close()
		w.Header().Set("Content-Type", "application/json")
		io.Copy(w, upstream.Body)
	}
}

// --- Agent card --------------------------------------------------------------

func serveAgentCard(endpoint, mcpEndpoint, cardURL, mdl string) http.HandlerFunc {
	skills := make([]map[string]any, 0)
	for _, t := range mcpTools() {
		skills = append(skills, map[string]any{
			"id":          t.Name,
			"name":        t.Name,
			"description": t.Description,
			"tags":        []string{t.Name},
		})
	}
	card := map[string]any{
		"name":        *name,
		"description": fmt.Sprintf("Beigebox — local LLM proxy (model: %s) with MCP advertisement", mdl),
		"url":         endpoint,
		"version":     "1.0.0",
		"capabilities": map[string]bool{
			"streaming":         true,
			"pushNotifications": false,
		},
		"skills":             skills,
		"defaultInputModes":  []string{"application/json"},
		"defaultOutputModes": []string{"application/json"},
		"x-amf": map[string]any{
			"agent_id":     agentID,
			"trust_domain": *trust,
			"protocols":    []string{"MCP/2024-11-05"},
			"mcp_endpoint": mcpEndpoint,
			// nats_url intentionally omitted — beigebox is a specialist, not a coordinator
		},
	}
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		json.NewEncoder(w).Encode(card)
	}
}

// --- Health ------------------------------------------------------------------

func handleHealth(backendBase string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		backendOK := false

		// Try Ollama health
		resp, err := http.Get(strings.TrimRight(backendBase, "/") + "/api/tags")
		if err == nil {
			resp.Body.Close()
			backendOK = resp.StatusCode == http.StatusOK
		}
		if !backendOK {
			// Try OpenAI-style health
			resp2, err2 := http.Get(strings.TrimRight(backendBase, "/") + "/v1/models")
			if err2 == nil {
				resp2.Body.Close()
				backendOK = resp2.StatusCode == http.StatusOK
			}
		}

		natsOK := nc != nil && nc.IsConnected()
		status := "ok"
		if !backendOK {
			status = "backend_unavailable"
		}

		w.Header().Set("Content-Type", "application/json")
		if !backendOK {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		json.NewEncoder(w).Encode(map[string]any{
			"status":     status,
			"agent_id":   agentID,
			"backend":    backendBase,
			"backend_ok": backendOK,
			"nats_ok":    natsOK,
			"model":      backend.model,
		})
	}
}

// --- NATS helpers ------------------------------------------------------------

// dnsZoneEntries returns the DNS records to add to the zone for unicast DNS-SD.
// Same format as the coordinator's BrowseAgentsDNS expects.
func dnsZoneEntries(domain, publicHost, instanceName string, port int, txt []string) string {
	fqdn := instanceName + "._amf-agent._tcp." + domain + "."
	quoted := make([]string, len(txt))
	for i, t := range txt {
		quoted[i] = `"` + t + `"`
	}
	return fmt.Sprintf(
		"\n; --- AMF DNS-SD records for %s (add to your %s zone) ---\n"+
			"; PTR — service type enumeration\n"+
			"_amf-agent._tcp.%s. 300 IN PTR %s\n"+
			"; SRV — service location\n"+
			"%s 300 IN SRV 0 0 %d %s.\n"+
			"; TXT — agent metadata\n"+
			"%s 300 IN TXT %s\n"+
			"; --- end ---\n",
		instanceName, domain,
		domain, fqdn,
		fqdn, port, publicHost,
		fqdn, strings.Join(quoted, " "),
	)
}

func publishHeartbeat(status, backendBase, mdl, mcpEndpoint string) {
	if nc == nil {
		return
	}
	ttl := int(heartbeatEvery.Seconds()) * 2 // TTL = 2× heartbeat interval
	evt := map[string]any{
		"specversion":      "1.0",
		"id":               uuid.New().String(),
		"source":           agentID,
		"type":             "amf.discovery.agent.heartbeat",
		"time":             time.Now().UTC().Format(time.RFC3339),
		"datacontenttype":  "application/json",
		"amfagentrole":     "specialist",
		"amfvisibility":    "local",
		"amfconfidence":    "1.0",
		"amfttl":           ttl,
		"amfschemaversion": "1.0.0",
		"amftrustdomain":   *trust,
		"data": map[string]any{
			"payload": map[string]any{
				"status":       status,
				"name":         *name,
				"mcp_endpoint": mcpEndpoint,
				"backend":      backendBase,
				"model":        mdl,
				"os":           runtime.GOOS,
				"arch":         runtime.GOARCH,
			},
			"auth_context": map[string]any{
				"identity":     agentID,
				"trust_domain": *trust,
				"scopes":       []string{"claim:tasks"},
			},
		},
	}
	data, _ := json.Marshal(evt)
	nc.Publish("amf.discovery.agent.heartbeat", data)
}
