package main

// OPA Policy Engine integration.
//
// Starts OPA in server mode as a subprocess, loads policies from stack/policies/.
// All AMF authorization decisions flow through OPA and are logged to the event fabric.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"log"
	"net/http"
	"os"
	"os/exec"
	"time"
)

const opaAddr = "http://127.0.0.1:8181"

var opaCmd *exec.Cmd

// StartOPA launches OPA server as a subprocess, serving policies from ./policies/.
func StartOPA(policiesDir string) {
	opaPath, err := exec.LookPath("opa")
	if err != nil {
		home, _ := os.UserHomeDir()
		opaPath = home + "/.local/bin/opa"
	}

	abs, err := exec.Command("realpath", policiesDir).Output()
	absStr := strings.TrimSpace(string(abs))
	if err != nil || absStr == "" {
		absStr = policiesDir
	}
	if _, err := os.Stat(absStr); err != nil {
		log.Printf("OPA: policies dir not found: %s", absStr)
		return
	}

	opaCmd = exec.Command(opaPath, "run", "--server",
		"--addr", "127.0.0.1:8181",
		"--log-level", "error",
		absStr,
	)
	opaCmd.Stdout = os.Stdout
	opaCmd.Stderr = os.Stderr

	if err := opaCmd.Start(); err != nil {
		log.Printf("OPA: failed to start: %v", err)
		return
	}
	log.Printf("OPA: started (pid %d) serving %s", opaCmd.Process.Pid, absStr)

	// Wait for OPA to be ready
	for i := range 10 {
		time.Sleep(200 * time.Millisecond)
		resp, err := http.Get(opaAddr + "/health")
		if err == nil && resp.StatusCode == 200 {
			log.Printf("OPA: ready")
			return
		}
		_ = i
	}
	log.Printf("OPA: did not become ready in time")
}

// StopOPA shuts down the OPA subprocess.
func StopOPA() {
	if opaCmd != nil && opaCmd.Process != nil {
		opaCmd.Process.Kill()
	}
}

// PolicyInput is the standard input structure for all AMF OPA decisions.
type PolicyInput struct {
	Agent       map[string]any `json:"agent"`
	Coordinator map[string]any `json:"coordinator"`
	Event       map[string]any `json:"event,omitempty"`
	TraceID     string         `json:"trace_id"`
}

// PolicyResult is returned by CheckPolicy.
type PolicyResult struct {
	Allow  bool   `json:"allow"`
	Reason string `json:"reason,omitempty"`
}

// CheckPolicy evaluates an OPA rule against the given input.
// rulePath is the OPA data path, e.g. "amf/policy/allow_advertisement".
func CheckPolicy(rulePath string, input PolicyInput) (PolicyResult, error) {
	body, _ := json.Marshal(map[string]any{"input": input})

	url := fmt.Sprintf("%s/v1/data/%s", opaAddr, rulePath)
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return PolicyResult{Allow: false, Reason: "OPA unreachable"}, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)

	// OPA returns {"result": <value>} where value depends on the query path.
	// Querying a package returns a map; querying a single rule returns a scalar.
	// We query the full package and extract the named fields.
	var envelope struct {
		Result any `json:"result"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return PolicyResult{Allow: false, Reason: "OPA parse error"}, err
	}

	switch v := envelope.Result.(type) {
	case bool:
		// Single-rule query returned a plain bool
		return PolicyResult{Allow: v}, nil
	case map[string]any:
		// Package-level query returned a map of all rules
		allow, _ := v["allow_advertisement"].(bool)
		reason, _ := v["reason"].(string)
		return PolicyResult{Allow: allow, Reason: reason}, nil
	default:
		return PolicyResult{Allow: false, Reason: "unexpected OPA result type"}, nil
	}
}

// EvaluateAdvertisement runs OPA policy against a WatcherSummary and returns allow/deny.
// Also publishes a policy event to the fabric.
func EvaluateAdvertisement(summary *WatcherSummary) bool {
	input := PolicyInput{
		Agent: map[string]any{
			"agent_id":     summary.OriginalAgentID,
			"trust_domain": summary.TrustDomain,
			"risk_score":   summary.RiskScore,
			"capabilities": summary.ExtractedCapabilities,
			"protocols":    summary.ProtocolsSupported,
		},
		Coordinator: map[string]any{
			"trust_domain": "local",
		},
		TraceID: summary.WatcherID,
	}

	result, err := CheckPolicy("amf/policy", input)
	if err != nil {
		log.Printf("policy: OPA error for %s: %v — defaulting to deny", summary.OriginalAgentID, err)
		return false
	}

	if !result.Allow {
		log.Printf("policy: DENY %s — %s", summary.OriginalAgentID, result.Reason)
		publishPolicyEvent("amf.policy.deny", fmt.Sprintf("agent %s denied: %s", summary.OriginalAgentID, result.Reason), summary.WatcherID)
	} else {
		log.Printf("policy: ALLOW %s", summary.OriginalAgentID)
	}

	return result.Allow
}
