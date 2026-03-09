package amf.policy

import future.keywords.if
import future.keywords.in

default allow_task_claim = false

# Allow task claim if agent is in same trust domain and has required scope
allow_task_claim if {
    input.agent.trust_domain == input.coordinator.trust_domain
    "claim:tasks" in input.agent.scopes
    input.agent.risk_score <= 0.5
}

# Allow within public trust domain with looser rules
allow_task_claim if {
    input.agent.trust_domain == "public"
    "claim:tasks" in input.agent.scopes
    input.agent.risk_score <= 0.3
}
