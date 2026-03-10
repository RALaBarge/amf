package main

// OpenAI-compatible API layer.
//
// Exposes /v1/chat/completions and /v1/models so any OpenAI-compatible
// middleware, SDK, or proxy can route to the AMF mesh without modification.
//
// chat/completions → picks best available agent by capability tag →
//   publishes amf.task.announce → waits for amf.result.final on a
//   per-request reply subject → returns OpenAI response envelope.
//
// Supports both non-streaming (default) and streaming (stream:true) modes.

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	nats "github.com/nats-io/nats.go"
)

// --- OpenAI wire types ---

type OAIMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type OAIChatRequest struct {
	Model    string       `json:"model"`
	Messages []OAIMessage `json:"messages"`
	Stream   bool         `json:"stream"`
	// passthrough fields we don't use but accept gracefully
	Temperature *float64 `json:"temperature,omitempty"`
	MaxTokens   *int     `json:"max_tokens,omitempty"`
}

type OAIChoice struct {
	Index        int        `json:"index"`
	Message      OAIMessage `json:"message"`
	FinishReason string     `json:"finish_reason"`
}

type OAIUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

type OAIChatResponse struct {
	ID      string      `json:"id"`
	Object  string      `json:"object"`
	Created int64       `json:"created"`
	Model   string      `json:"model"`
	Choices []OAIChoice `json:"choices"`
	Usage   OAIUsage    `json:"usage"`
}

// Streaming delta types
type OAIDelta struct {
	Role    string `json:"role,omitempty"`
	Content string `json:"content,omitempty"`
}

type OAIStreamChoice struct {
	Index        int      `json:"index"`
	Delta        OAIDelta `json:"delta"`
	FinishReason *string  `json:"finish_reason"`
}

type OAIStreamChunk struct {
	ID      string            `json:"id"`
	Object  string            `json:"object"`
	Created int64             `json:"created"`
	Model   string            `json:"model"`
	Choices []OAIStreamChoice `json:"choices"`
}

// --- Model list ---

type OAIModel struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int64  `json:"created"`
	OwnedBy string `json:"owned_by"`
}

type OAIModelList struct {
	Object string     `json:"object"`
	Data   []OAIModel `json:"data"`
}

// handleOAIModels returns available agents as "models".
// Static entries + dynamic mesh agents.
func handleOAIModels(w http.ResponseWriter, r *http.Request) {
	models := []OAIModel{
		{ID: "amf-mesh", Object: "model", Created: 1700000000, OwnedBy: "amf"},
		{ID: "amf-coordinator", Object: "model", Created: 1700000000, OwnedBy: "amf"},
	}

	// Add live mesh agents as addressable models
	agentRegistry.RLock()
	for _, a := range agentRegistry.agents {
		for _, tag := range a.Record.CapabilityTags {
			models = append(models, OAIModel{
				ID:      "amf/" + tag,
				Object:  "model",
				Created: a.SeenAt.Unix(),
				OwnedBy: a.Record.TrustDomain,
			})
		}
	}
	agentRegistry.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(OAIModelList{Object: "list", Data: models})
}

// handleOAIChat implements POST /v1/chat/completions.
func handleOAIChat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}

	var req OAIChatRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	if len(req.Messages) == 0 {
		http.Error(w, "messages required", http.StatusBadRequest)
		return
	}

	// Extract capability from model name: "amf/text-summarize" → "text-summarize"
	// "amf-mesh" or anything else → "" (any available agent)
	capability := ""
	if strings.HasPrefix(req.Model, "amf/") {
		capability = strings.TrimPrefix(req.Model, "amf/")
	}

	// Build task text from the last user message
	taskText := ""
	for i := len(req.Messages) - 1; i >= 0; i-- {
		if req.Messages[i].Role == "user" {
			taskText = req.Messages[i].Content
			break
		}
	}
	if taskText == "" {
		taskText = req.Messages[len(req.Messages)-1].Content
	}

	taskID := uuid.New().String()
	traceID := uuid.New().String()
	replySubject := "amf.internal.reply." + taskID

	if req.Stream {
		handleOAIChatStream(w, r, req, taskID, traceID, replySubject, capability, taskText)
		return
	}

	// Non-streaming: subscribe for result, publish task, wait
	result, err := dispatchAndWait(taskID, traceID, replySubject, capability, taskText)
	if err != nil {
		// Return a graceful error as an assistant message rather than HTTP 500
		// so middleware chains don't break
		result = fmt.Sprintf("[AMF error: %v]", err)
	}

	resp := OAIChatResponse{
		ID:      "chatcmpl-" + taskID,
		Object:  "chat.completion",
		Created: time.Now().Unix(),
		Model:   req.Model,
		Choices: []OAIChoice{{
			Index:        0,
			Message:      OAIMessage{Role: "assistant", Content: result},
			FinishReason: "stop",
		}},
		Usage: OAIUsage{
			PromptTokens:     len(taskText) / 4,
			CompletionTokens: len(result) / 4,
			TotalTokens:      (len(taskText) + len(result)) / 4,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// dispatchAndWait publishes a task and blocks until result.final arrives on the reply subject.
func dispatchAndWait(taskID, traceID, replySubject, capability, taskText string) (string, error) {
	resultCh := make(chan string, 1)

	sub, err := nc.Subscribe(replySubject, func(msg *nats.Msg) {
		var evt map[string]any
		if json.Unmarshal(msg.Data, &evt) != nil {
			return
		}
		// Extract result text from AMF envelope
		data, _ := evt["data"].(map[string]any)
		if data == nil {
			return
		}
		payload, _ := data["payload"].(map[string]any)
		if payload == nil {
			return
		}
		res, _ := payload["result"].(string)
		if res == "" {
			res, _ = payload["output"].(string)
		}
		if res == "" {
			b, _ := json.Marshal(payload)
			res = string(b)
		}
		select {
		case resultCh <- res:
		default:
		}
	})
	if err != nil {
		return "", fmt.Errorf("subscribe failed: %w", err)
	}
	defer sub.Unsubscribe()

	// Announce the task — workers subscribe to TypeTaskAnnounce
	evt := NewEvent(uuid.New().String(), traceID,
		"spiffe://local/coordinator",
		TypeTaskAnnounce,
		RoleCoordinator,
		map[string]any{
			"task_id":             taskID,
			"task":                taskText,
			"required_capability": capability,
			"reply_subject":       replySubject,
		},
	)
	evt.Data.Payload = map[string]any{
		"task_id":             taskID,
		"task":                taskText,
		"required_capability": capability,
		"reply_subject":       replySubject,
	}

	// Also set AMF extension attributes
	b := map[string]any{}
	raw, _ := json.Marshal(evt)
	json.Unmarshal(raw, &b)
	b["amftaskid"] = taskID
	b["amftraceid"] = traceID

	data, _ := json.Marshal(b)
	if err := nc.Publish(TypeTaskAnnounce, data); err != nil {
		return "", fmt.Errorf("publish failed: %w", err)
	}

	log.Printf("oai: task %s dispatched (cap=%q)", taskID, capability)

	select {
	case result := <-resultCh:
		return result, nil
	case <-time.After(30 * time.Second):
		return "", fmt.Errorf("timeout waiting for worker result")
	}
}

// handleOAIChatStream sends SSE chunks as the task progresses.
func handleOAIChatStream(w http.ResponseWriter, r *http.Request, req OAIChatRequest,
	taskID, traceID, replySubject, capability, taskText string) {

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	chunkID := "chatcmpl-" + taskID
	writeChunk := func(content string, done bool) {
		finishReason := (*string)(nil)
		if done {
			s := "stop"
			finishReason = &s
		}
		chunk := OAIStreamChunk{
			ID:      chunkID,
			Object:  "chat.completion.chunk",
			Created: time.Now().Unix(),
			Model:   req.Model,
			Choices: []OAIStreamChoice{{
				Index:        0,
				Delta:        OAIDelta{Content: content},
				FinishReason: finishReason,
			}},
		}
		b, _ := json.Marshal(chunk)
		fmt.Fprintf(w, "data: %s\n\n", b)
		flusher.Flush()
	}

	// Send role chunk first (OpenAI streaming convention)
	roleChunk := OAIStreamChunk{
		ID: chunkID, Object: "chat.completion.chunk",
		Created: time.Now().Unix(), Model: req.Model,
		Choices: []OAIStreamChoice{{Delta: OAIDelta{Role: "assistant"}}},
	}
	b, _ := json.Marshal(roleChunk)
	fmt.Fprintf(w, "data: %s\n\n", b)
	flusher.Flush()

	// Subscribe to progress events and the final result
	progressSub := "amf.task.progress"
	doneCh := make(chan struct{})

	// Watch for progress events matching our taskID
	pSub, _ := nc.Subscribe(progressSub, func(msg *nats.Msg) {
		var evt map[string]any
		if json.Unmarshal(msg.Data, &evt) != nil {
			return
		}
		if evt["amftaskid"] != taskID {
			return
		}
		data, _ := evt["data"].(map[string]any)
		if data == nil {
			return
		}
		payload, _ := data["payload"].(map[string]any)
		step, _ := payload["step"].(string)
		if step != "" {
			writeChunk(" ["+step+"]", false)
		}
	})
	if pSub != nil {
		defer pSub.Unsubscribe()
	}

	// Wait for final result
	rSub, _ := nc.Subscribe(replySubject, func(msg *nats.Msg) {
		var evt map[string]any
		if json.Unmarshal(msg.Data, &evt) != nil {
			close(doneCh)
			return
		}
		data, _ := evt["data"].(map[string]any)
		if data == nil {
			close(doneCh)
			return
		}
		payload, _ := data["payload"].(map[string]any)
		res, _ := payload["result"].(string)
		if res == "" {
			b, _ := json.Marshal(payload)
			res = string(b)
		}
		writeChunk(res, true)
		close(doneCh)
	})
	if rSub != nil {
		defer rSub.Unsubscribe()
	}

	// Publish task
	evtMap := map[string]any{
		"specversion":      "1.0",
		"id":               uuid.New().String(),
		"source":           "spiffe://local/coordinator",
		"type":             TypeTaskAnnounce,
		"time":             time.Now().UTC().Format(time.RFC3339),
		"datacontenttype":  "application/json",
		"amftraceid":       traceID,
		"amftaskid":        taskID,
		"amfagentrole":     "coordinator",
		"amfvisibility":    "local",
		"amfconfidence":    "1.0",
		"amfttl":           60,
		"amfschemaversion": "1.0.0",
		"amftrustdomain":   "local",
		"data": map[string]any{
			"payload": map[string]any{
				"task_id":             taskID,
				"task":                taskText,
				"required_capability": capability,
				"reply_subject":       replySubject,
			},
			"auth_context": map[string]any{
				"identity":     "spiffe://local/coordinator",
				"trust_domain": "local",
				"scopes":       []string{"dispatch:tasks"},
			},
		},
	}
	data, _ := json.Marshal(evtMap)
	nc.Publish(TypeTaskAnnounce, data)

	log.Printf("oai-stream: task %s dispatched (cap=%q)", taskID, capability)

	select {
	case <-doneCh:
	case <-r.Context().Done():
	case <-time.After(30 * time.Second):
		writeChunk(" [timeout]", true)
	}

	fmt.Fprintf(w, "data: [DONE]\n\n")
	flusher.Flush()
}
