package amf.policy

import future.keywords.if
import future.keywords.in

# Default deny
default allow_advertisement = false
default reason = "default deny"

# Allow if risk score is acceptable and trust domain is known
allow_advertisement if {
    input.agent.risk_score <= 0.5
    input.agent.trust_domain != ""
    count(input.agent.protocols) > 0
    valid_protocols
}

# Allow with warning if risk score is elevated but under threshold
allow_advertisement if {
    input.agent.risk_score > 0.5
    input.agent.risk_score <= 0.7
    input.agent.trust_domain == input.coordinator.trust_domain
    valid_protocols
}

# Deny high-risk advertisements
reason = "risk_score too high" if {
    input.agent.risk_score > 0.7
}

reason = "no trust_domain" if {
    input.agent.risk_score <= 0.7
    input.agent.trust_domain == ""
}

reason = "no supported protocols" if {
    count(input.agent.protocols) == 0
}

reason = "unknown protocols only" if {
    count(input.agent.protocols) > 0
    not valid_protocols
}

# Only allow agents speaking A2A or MCP
valid_protocols if {
    some p in input.agent.protocols
    p in {"A2A/1.0", "MCP/2024-11-05"}
}
