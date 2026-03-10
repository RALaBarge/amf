// Beigebox — local MCP skill node for the AMF mesh.
//
// A beigebox is a local machine that exposes its capabilities as MCP tools,
// then advertises that MCP endpoint on the AMF mesh via mDNS so the
// coordinator can discover and route work to it.
//
// Architecture:
//   mDNS advertisement (includes mcp_endpoint in agent card)
//     → AMF coordinator discovers via _amf-agent._tcp.local
//     → coordinator proxies or dispatches directly to POST /mcp
//
// MCP transport: Streamable HTTP (2025-03-26)
//   POST /mcp  — all JSON-RPC requests/responses
//   GET  /mcp  — SSE stream for server-initiated messages (reserved)
//   GET  /.well-known/agent-card.json — A2A agent card with x-amf.mcp_endpoint
//
// Run: go run ./cmd/beigebox --port 8767 --name my-beigebox
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/grandcat/zeroconf"
	nats "github.com/nats-io/nats.go"
)

var (
	port    = flag.Int("port", 8767, "HTTP port")
	name    = flag.String("name", "beigebox", "Agent display name")
	natsURL = flag.String("nats", "nats://127.0.0.1:4222", "NATS server URL")
	trust   = flag.String("trust", "local", "Trust domain")
)

const (
	mdnsService = "_amf-agent._tcp"
	mDNSDomain  = "local."
)

var (
	nc      *nats.Conn
	agentID string
)

func main() {
	flag.Parse()
	agentID = "spiffe://local/beigebox/" + uuid.New().String()
	log.Printf("beigebox[%s]: starting on port %d", *name, *port)

	// Connect to NATS (optional — degrade gracefully if not available)
	var err error
	for i := range 5 {
		nc, err = nats.Connect(*natsURL)
		if err == nil {
			break
		}
		log.Printf("beigebox: NATS connect attempt %d: %v", i+1, err)
		time.Sleep(500 * time.Millisecond)
	}
	if err != nil {
		log.Printf("beigebox: running without NATS (mesh events disabled): %v", err)
		nc = nil
	} else {
		defer nc.Close()
		log.Printf("beigebox: connected to NATS")
	}

	endpoint := fmt.Sprintf("http://localhost:%d", *port)
	mcpEndpoint := endpoint + "/mcp"
	cardURL := endpoint + "/.well-known/agent-card.json"

	// Register on mDNS — include mcp_endpoint in TXT
	txt := []string{
		"id=" + agentID,
		"ep=" + endpoint,
		"proto=MCP/2024-11-05",
		"tags=" + strings.Join(toolNames(), ","),
		"td=" + *trust,
		"vis=local",
		"v=1.0.0",
		"status=active",
		"card=" + cardURL,
		"mcp=" + mcpEndpoint,
	}
	mdnsSrv, err := zeroconf.Register(*name, mdnsService, mDNSDomain, *port, txt, nil)
	if err != nil {
		log.Printf("beigebox: mDNS register warning: %v", err)
	} else {
		defer mdnsSrv.Shutdown()
		log.Printf("beigebox: registered on mDNS as %s (MCP at %s)", *name, mcpEndpoint)
	}

	// Publish heartbeat to mesh
	publishEvent("amf.discovery.agent.heartbeat", map[string]any{
		"status":       "online",
		"name":         *name,
		"mcp_endpoint": mcpEndpoint,
	})

	mux := http.NewServeMux()
	mux.HandleFunc("/mcp", handleMCP)
	mux.HandleFunc("/.well-known/agent-card.json", serveAgentCard(endpoint, mcpEndpoint, cardURL))
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok", "agent_id": agentID})
	})

	srv := &http.Server{Addr: fmt.Sprintf(":%d", *port), Handler: mux}
	go func() {
		log.Printf("beigebox: HTTP listening on %s", endpoint)
		srv.ListenAndServe()
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	publishEvent("amf.discovery.agent.heartbeat", map[string]any{"status": "offline", "name": *name})
	log.Printf("beigebox: shutting down")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	srv.Shutdown(ctx)
}

// --- MCP JSON-RPC types ---

type mcpRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type mcpResponse struct {
	JSONRPC string `json:"jsonrpc"`
	ID      any    `json:"id,omitempty"`
	Result  any    `json:"result,omitempty"`
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

// handleMCP is the single Streamable HTTP endpoint for all MCP traffic.
func handleMCP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Accept")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// GET /mcp — SSE channel for server-push (reserved, send keep-alive)
	if r.Method == http.MethodGet {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming not supported", 500)
			return
		}
		fmt.Fprintf(w, ": beigebox mcp stream\n\n")
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
		writeError(w, nil, -32700, "parse error")
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
				"instructions":    "Beigebox exposes local machine skills to the AMF mesh.",
			},
		})

	case "notifications/initialized":
		// Client acknowledges init — no response needed for notifications
		w.WriteHeader(http.StatusAccepted)

	case "tools/list":
		json.NewEncoder(w).Encode(mcpResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result:  map[string]any{"tools": tools()},
		})

	case "tools/call":
		var p struct {
			Name      string         `json:"name"`
			Arguments map[string]any `json:"arguments"`
		}
		if err := json.Unmarshal(req.Params, &p); err != nil {
			writeError(w, req.ID, -32602, "invalid params")
			return
		}
		result, toolErr := callTool(p.Name, p.Arguments)
		if toolErr != "" {
			json.NewEncoder(w).Encode(mcpResponse{
				JSONRPC: "2.0",
				ID:      req.ID,
				Result: map[string]any{
					"content":  []map[string]any{{"type": "text", "text": toolErr}},
					"isError":  true,
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
		writeError(w, req.ID, -32601, "method not found: "+req.Method)
	}
}

func writeError(w http.ResponseWriter, id any, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(mcpResponse{
		JSONRPC: "2.0",
		ID:      id,
		Error:   &mcpError{Code: code, Message: msg},
	})
}

// --- Tool registry ---

func tools() []mcpTool {
	return []mcpTool{
		{
			Name:        "echo",
			Description: "Echo a message back. Useful for testing connectivity.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"message": map[string]any{"type": "string", "description": "Text to echo"},
				},
				"required": []string{"message"},
			},
		},
		{
			Name:        "system_info",
			Description: "Return basic info about this machine (hostname, OS, time).",
			InputSchema: map[string]any{"type": "object", "properties": map[string]any{}},
		},
		{
			Name:        "read_file",
			Description: "Read a local file by path. Only files the process can access.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"path":      map[string]any{"type": "string", "description": "Absolute file path"},
					"max_bytes": map[string]any{"type": "integer", "description": "Max bytes to read (default 4096)"},
				},
				"required": []string{"path"},
			},
		},
		{
			Name:        "list_dir",
			Description: "List files in a local directory.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"path": map[string]any{"type": "string", "description": "Directory path"},
				},
				"required": []string{"path"},
			},
		},
		{
			Name:        "mesh_announce",
			Description: "Announce a task to the AMF mesh via NATS.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"task":                map[string]any{"type": "string", "description": "Task description"},
					"required_capability": map[string]any{"type": "string", "description": "Capability tag required"},
				},
				"required": []string{"task"},
			},
		},
	}
}

func toolNames() []string {
	ts := tools()
	names := make([]string, len(ts))
	for i, t := range ts {
		names[i] = t.Name
	}
	return names
}

func callTool(name string, args map[string]any) (result string, errMsg string) {
	switch name {
	case "echo":
		msg, _ := args["message"].(string)
		return msg, ""

	case "system_info":
		hostname, _ := os.Hostname()
		return fmt.Sprintf("hostname=%s time=%s agent_id=%s",
			hostname, time.Now().UTC().Format(time.RFC3339), agentID), ""

	case "read_file":
		path, _ := args["path"].(string)
		if path == "" {
			return "", "path is required"
		}
		maxBytes := 4096
		if mb, ok := args["max_bytes"].(float64); ok && mb > 0 {
			maxBytes = int(mb)
		}
		f, err := os.Open(path)
		if err != nil {
			return "", fmt.Sprintf("cannot open %s: %v", path, err)
		}
		defer f.Close()
		buf := make([]byte, maxBytes)
		n, _ := f.Read(buf)
		return string(buf[:n]), ""

	case "list_dir":
		path, _ := args["path"].(string)
		if path == "" {
			return "", "path is required"
		}
		entries, err := os.ReadDir(path)
		if err != nil {
			return "", fmt.Sprintf("cannot read dir %s: %v", path, err)
		}
		lines := make([]string, 0, len(entries))
		for _, e := range entries {
			kind := "f"
			if e.IsDir() {
				kind = "d"
			}
			lines = append(lines, fmt.Sprintf("[%s] %s", kind, e.Name()))
		}
		return strings.Join(lines, "\n"), ""

	case "mesh_announce":
		if nc == nil {
			return "", "NATS not connected — mesh unavailable"
		}
		task, _ := args["task"].(string)
		cap, _ := args["required_capability"].(string)
		taskID := uuid.New().String()
		traceID := uuid.New().String()
		evt := map[string]any{
			"specversion":      "1.0",
			"id":               uuid.New().String(),
			"source":           agentID,
			"type":             "amf.task.announce",
			"time":             time.Now().UTC().Format(time.RFC3339),
			"datacontenttype":  "application/json",
			"amftraceid":       traceID,
			"amftaskid":        taskID,
			"amfagentrole":     "specialist",
			"amfvisibility":    "local",
			"amfconfidence":    "1.0",
			"amfttl":           60,
			"amfschemaversion": "1.0.0",
			"amftrustdomain":   *trust,
			"data": map[string]any{
				"payload": map[string]any{
					"task_id":             taskID,
					"task":                task,
					"required_capability": cap,
				},
				"auth_context": map[string]any{
					"identity":     agentID,
					"trust_domain": *trust,
					"scopes":       []string{"dispatch:tasks"},
				},
			},
		}
		data, _ := json.Marshal(evt)
		nc.Publish("amf.task.announce", data)
		return fmt.Sprintf("task %s announced to mesh", taskID), ""

	default:
		return "", fmt.Sprintf("unknown tool: %s", name)
	}
}

// --- Agent card ---

func serveAgentCard(endpoint, mcpEndpoint, cardURL string) http.HandlerFunc {
	skills := make([]map[string]any, 0)
	for _, t := range tools() {
		skills = append(skills, map[string]any{
			"id":          t.Name,
			"name":        t.Name,
			"description": t.Description,
			"tags":        []string{t.Name},
		})
	}
	card := map[string]any{
		"name":               *name,
		"description":        "Beigebox — local machine MCP skill node",
		"url":                endpoint,
		"version":            "1.0.0",
		"capabilities":       map[string]bool{"streaming": false, "pushNotifications": false},
		"skills":             skills,
		"defaultInputModes":  []string{"application/json"},
		"defaultOutputModes": []string{"application/json"},
		"x-amf": map[string]any{
			"agent_id":     agentID,
			"trust_domain": *trust,
			"protocols":    []string{"MCP/2024-11-05"},
			"nats_url":     *natsURL,
			"mcp_endpoint": mcpEndpoint,
		},
	}
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		json.NewEncoder(w).Encode(card)
	}
}

// --- NATS helpers ---

func publishEvent(eventType string, payload any) {
	if nc == nil {
		return
	}
	evt := map[string]any{
		"specversion":      "1.0",
		"id":               uuid.New().String(),
		"source":           agentID,
		"type":             eventType,
		"time":             time.Now().UTC().Format(time.RFC3339),
		"datacontenttype":  "application/json",
		"amfagentrole":     "specialist",
		"amfvisibility":    "local",
		"amfconfidence":    "1.0",
		"amfttl":           60,
		"amfschemaversion": "1.0.0",
		"amftrustdomain":   *trust,
		"data": map[string]any{
			"payload":      payload,
			"auth_context": map[string]any{"identity": agentID, "trust_domain": *trust},
		},
	}
	data, _ := json.Marshal(evt)
	nc.Publish(eventType, data)
}
