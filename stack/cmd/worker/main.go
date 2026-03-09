// AMF Worker Agent
//
// A standalone agent that:
// 1. Registers itself on the local mDNS mesh (_amf-agent._tcp.local)
// 2. Subscribes to amf.task.announce and claims tasks it can handle
// 3. Processes tasks and publishes amf.result.final
// 4. Serves /.well-known/agent-card.json (A2A agent card)
//
// Run: go run ./cmd/worker --port 8766 --name my-worker
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
	port     = flag.Int("port", 8766, "HTTP port for agent card and A2A endpoint")
	name     = flag.String("name", "amf-worker", "Agent display name")
	natsURL  = flag.String("nats", "nats://127.0.0.1:4222", "NATS server URL")
	tags     = flag.String("tags", "text-summarize,code-review,question-answer", "Comma-separated capability tags")
	trust    = flag.String("trust", "local", "Trust domain")
)

const (
	mdnsService = "_amf-agent._tcp"
	mDNSDomain  = "local."
)

var nc *nats.Conn
var agentID string

func main() {
	flag.Parse()
	agentID = "spiffe://local/agent/" + uuid.New().String()
	log.Printf("worker[%s]: starting as %s on port %d", *name, agentID, *port)

	// Connect to NATS
	var err error
	for i := range 10 {
		nc, err = nats.Connect(*natsURL)
		if err == nil {
			break
		}
		log.Printf("worker: NATS connect attempt %d: %v", i+1, err)
		time.Sleep(500 * time.Millisecond)
	}
	if err != nil {
		log.Fatalf("worker: could not connect to NATS: %v", err)
	}
	defer nc.Close()
	log.Printf("worker: connected to NATS at %s", *natsURL)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Register on mDNS
	capTags := strings.Split(*tags, ",")
	endpoint := fmt.Sprintf("http://localhost:%d", *port)
	cardURL := fmt.Sprintf("%s/.well-known/agent-card.json", endpoint)

	txt := []string{
		"id=" + agentID,
		"ep=" + endpoint,
		"proto=" + "A2A/1.0,MCP/2024-11-05",
		"tags=" + strings.Join(capTags, ","),
		"td=" + *trust,
		"vis=local",
		"v=1.0.0",
		"status=active",
		"card=" + cardURL,
	}

	mdnsSrv, err := zeroconf.Register(*name, mdnsService, mDNSDomain, *port, txt, nil)
	if err != nil {
		log.Printf("worker: mDNS register warning: %v", err)
	} else {
		defer mdnsSrv.Shutdown()
		log.Printf("worker: registered on mDNS as %s", *name)
	}

	// Publish heartbeat
	publishEvent(TypeAgentHeartbeat, map[string]string{
		"status": "online",
		"name":   *name,
		"note":   "worker agent started",
	})

	// Subscribe to task.announce — claim tasks we can handle
	nc.Subscribe(TypeTaskAnnounce, func(msg *nats.Msg) {
		handleTaskAnnounce(msg.Data, capTags)
	})

	// HTTP server — agent card + A2A endpoint
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/agent-card.json", serveAgentCard(capTags, endpoint, cardURL))
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"status": "ok", "agent_id": agentID})
	})
	mux.HandleFunc("/tasks/send", handleA2ATaskSend)

	srv := &http.Server{Addr: fmt.Sprintf(":%d", *port), Handler: mux}
	go func() {
		log.Printf("worker: HTTP listening on %s", endpoint)
		srv.ListenAndServe()
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	// Publish offline heartbeat
	publishEvent(TypeAgentHeartbeat, map[string]string{"status": "offline", "name": *name})
	log.Printf("worker: shutting down")
	_ = ctx
}

func handleTaskAnnounce(data []byte, myTags []string) {
	// Parse the CloudEvents envelope
	var evt map[string]any
	if err := json.Unmarshal(data, &evt); err != nil {
		return
	}

	evtData, _ := evt["data"].(map[string]any)
	if evtData == nil {
		return
	}
	payload, _ := evtData["payload"].(map[string]any)
	if payload == nil {
		return
	}

	taskID, _ := evt["amftaskid"].(string)
	if taskID == "" {
		taskID = uuid.New().String()
	}
	traceID, _ := evt["amftraceid"].(string)
	requiredCap, _ := payload["required_capability"].(string)

	// Check if we can handle this task
	canHandle := requiredCap == "" // handle anything if no specific cap required
	for _, t := range myTags {
		if t == requiredCap {
			canHandle = true
			break
		}
	}
	if !canHandle {
		return
	}

	log.Printf("worker: claiming task %s (capability: %s)", taskID, requiredCap)

	// Publish task.claim
	claimEvt := newWorkerEvent(TypeTaskClaim, traceID, taskID, map[string]any{
		"task_id":    taskID,
		"worker_id":  agentID,
		"worker":     *name,
		"capability": requiredCap,
	})
	publishRaw(TypeTaskClaim, claimEvt)

	// Process task (simulated work)
	go processTask(taskID, traceID, payload, myTags)
}

func processTask(taskID, traceID string, payload map[string]any, myTags []string) {
	taskDesc, _ := payload["task"].(string)
	log.Printf("worker: processing task %s: %q", taskID, taskDesc)

	// Simulate work with progress updates
	for i, step := range []string{"analyzing", "processing", "finalizing"} {
		time.Sleep(500 * time.Millisecond)
		prog := newWorkerEvent(TypeTaskProgress, traceID, taskID, map[string]any{
			"task_id":  taskID,
			"step":     step,
			"progress": (i + 1) * 33,
		})
		publishRaw(TypeTaskProgress, prog)
	}

	// Publish result
	result := newWorkerEvent(TypeResultFinal, traceID, taskID, map[string]any{
		"task_id":    taskID,
		"worker":     *name,
		"worker_id":  agentID,
		"input":      taskDesc,
		"result":     fmt.Sprintf("[%s] processed: %q — completed by %s", time.Now().Format("15:04:05"), taskDesc, *name),
		"capabilities_used": myTags,
	})
	publishRaw(TypeResultFinal, result)
	log.Printf("worker: completed task %s", taskID)
}

func handleA2ATaskSend(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var body map[string]any
	json.NewDecoder(r.Body).Decode(&body)

	taskID := uuid.New().String()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"id":     taskID,
		"status": map[string]string{"state": "working"},
	})

	// Process via NATS
	go processTask(taskID, uuid.New().String(), body, strings.Split(*tags, ","))
}

func serveAgentCard(capTags []string, endpoint, cardURL string) http.HandlerFunc {
	skills := make([]map[string]any, 0, len(capTags))
	for _, t := range capTags {
		skills = append(skills, map[string]any{
			"id":          t,
			"name":        t,
			"description": "AMF worker capability: " + t,
			"tags":        []string{t},
		})
	}
	card := map[string]any{
		"name":               *name,
		"description":        "AMF worker agent — " + strings.Join(capTags, ", "),
		"url":                endpoint,
		"version":            "1.0.0",
		"capabilities":       map[string]bool{"streaming": false, "pushNotifications": false},
		"skills":             skills,
		"defaultInputModes":  []string{"application/json"},
		"defaultOutputModes": []string{"application/json"},
		"x-amf": map[string]any{
			"agent_id":     agentID,
			"trust_domain": *trust,
			"protocols":    []string{"A2A/1.0", "MCP/2024-11-05"},
			"nats_url":     *natsURL,
		},
	}
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		json.NewEncoder(w).Encode(card)
	}
}

// AMF event type constants (duplicated from main package for standalone binary)
const (
	TypeCapabilityAdvertise = "amf.discovery.capability.advertise"
	TypeAgentHeartbeat      = "amf.discovery.agent.heartbeat"
	TypeTaskAnnounce        = "amf.task.announce"
	TypeTaskClaim           = "amf.task.claim"
	TypeTaskProgress        = "amf.task.progress"
	TypeResultFinal         = "amf.result.final"
)

func newWorkerEvent(eventType, traceID, taskID string, payload any) map[string]any {
	return map[string]any{
		"specversion":     "1.0",
		"id":              uuid.New().String(),
		"source":          agentID,
		"type":            eventType,
		"time":            time.Now().UTC().Format(time.RFC3339),
		"datacontenttype": "application/json",
		"amftraceid":      traceID,
		"amftaskid":       taskID,
		"amfagentrole":    "specialist",
		"amfvisibility":   "local",
		"amfconfidence":   "1.0",
		"amfttl":          60,
		"amfschemaversion": "1.0.0",
		"amftrustdomain":  *trust,
		"data": map[string]any{
			"payload":      payload,
			"auth_context": map[string]any{"identity": agentID, "trust_domain": *trust, "scopes": []string{"claim:tasks"}},
		},
	}
}

func publishEvent(eventType string, payload any) {
	evt := newWorkerEvent(eventType, uuid.New().String(), "", payload)
	publishRaw(eventType, evt)
}

func publishRaw(subject string, evt any) {
	data, err := json.Marshal(evt)
	if err != nil {
		return
	}
	nc.Publish(subject, data)
}
