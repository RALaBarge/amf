package main

// AMF Identity Provider
//
// Abstracts workload identity so any SPIFFE-compatible system can be
// plugged in without changing agent code. Two implementations:
//
//   StaticIdentity  — self-asserted SPIFFE URI strings. No crypto, no deps.
//                     Fine for local-only deployments. Default.
//
//   SpiffeIdentity  — connects to the SPIFFE Workload API gRPC socket.
//                     Works with SPIRE, Istio, Teleport, cert-manager CSI,
//                     or any other SPIFFE-compliant implementation.
//                     Wire in using github.com/spiffe/go-spiffe/v2.
//
// The SPIFFE Workload API is defined at:
//   https://github.com/spiffe/spiffe/blob/main/proto/spiffe/workload/workload.proto
//
// Key RPCs:
//   FetchX509SVID(X509SVIDRequest) → stream X509SVIDResponse
//     Returns a certificate + private key for mTLS. Auto-rotates.
//   FetchJWTSVID(JWTSVIDRequest{audience}) → JWTSVIDResponse
//     Returns a short-lived JWT for service-to-service auth.
//   FetchX509Bundles → trust bundles for validating peer certificates.
//
// The socket path is conventionally:
//   /run/spire/sockets/agent.sock   (SPIRE)
//   /run/secrets/workload-spiffe-credentials  (some k8s environments)
//   $SPIFFE_ENDPOINT_SOCKET  (env var, Workload API spec)
//
// To add a connector for a new identity system:
//   1. Implement IdentityProvider below
//   2. Fetch X.509 SVID via that system's Workload API
//   3. Return the SPIFFE ID string from AgentID()
//   4. Wire via NewIdentityProvider() based on config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/google/uuid"
)

// IdentityProvider is the single interface all AMF components use for identity.
// Implementations range from static strings (local dev) to real SVIDs (SPIRE).
type IdentityProvider interface {
	// AgentID returns the SPIFFE ID for this workload.
	// Format: spiffe://<trust-domain>/<path>
	// This is placed in event.source and amftrustdomain fields.
	AgentID() string

	// TrustDomain returns just the trust domain component.
	TrustDomain() string

	// X509SVID returns the TLS certificate for this workload.
	// Used for mTLS between agents.
	// Returns nil, nil for StaticIdentity (no crypto available).
	X509SVID() (*tls.Certificate, error)

	// JWTSVID returns a short-lived JWT asserting this workload's identity
	// to the given audience. Used for service-to-service auth headers.
	// Returns "", nil for StaticIdentity.
	JWTSVID(audience string) (string, error)

	// TrustBundle returns the CA pool for verifying peer SVIDs.
	// Returns nil for StaticIdentity.
	TrustBundle() (*x509.CertPool, error)

	// Mode returns a short string describing the implementation.
	// e.g. "static", "spiffe", "vault"
	Mode() string
}

// --- StaticIdentity ----------------------------------------------------------

// StaticIdentity is the zero-dependency default. It formats a SPIFFE URI
// but has no cryptographic backing — the ID is self-asserted.
// Appropriate for local-only deployments where all agents are trusted.
type StaticIdentity struct {
	id          string
	trustDomain string
}

// NewStaticIdentity creates a StaticIdentity with an auto-generated UUID.
// trustDomain defaults to "local" if empty.
func NewStaticIdentity(trustDomain, role, name string) *StaticIdentity {
	if trustDomain == "" {
		trustDomain = "local"
	}
	if name == "" {
		name = uuid.New().String()
	}
	return &StaticIdentity{
		id:          fmt.Sprintf("spiffe://%s/%s/%s", trustDomain, role, name),
		trustDomain: trustDomain,
	}
}

func (s *StaticIdentity) AgentID() string                          { return s.id }
func (s *StaticIdentity) TrustDomain() string                      { return s.trustDomain }
func (s *StaticIdentity) X509SVID() (*tls.Certificate, error)      { return nil, nil }
func (s *StaticIdentity) JWTSVID(_ string) (string, error)         { return "", nil }
func (s *StaticIdentity) TrustBundle() (*x509.CertPool, error)     { return nil, nil }
func (s *StaticIdentity) Mode() string                             { return "static" }

// --- SpiffeIdentity (stub) ---------------------------------------------------

// SpiffeIdentity connects to the SPIFFE Workload API socket and fetches
// real SVIDs. It works with SPIRE, Istio, Teleport, cert-manager, or any
// SPIFFE-compliant Workload API implementation.
//
// To activate, add to go.mod:
//   require github.com/spiffe/go-spiffe/v2 v2.x.x
//
// Then replace the stub body with:
//
//   import "github.com/spiffe/go-spiffe/v2/workloadapi"
//
//   func NewSpiffeIdentity(socketPath string) (*SpiffeIdentity, error) {
//       ctx := context.Background()
//       client, err := workloadapi.New(ctx,
//           workloadapi.WithAddr("unix://"+socketPath))
//       if err != nil {
//           return nil, err
//       }
//       svid, err := client.FetchX509SVID(ctx)
//       if err != nil {
//           return nil, err
//       }
//       return &SpiffeIdentity{client: client, svid: svid}, nil
//   }
//
//   func (s *SpiffeIdentity) X509SVID() (*tls.Certificate, error) {
//       return s.svid.Certificates[0], nil // auto-rotates via watcher
//   }
//
//   func (s *SpiffeIdentity) JWTSVID(audience string) (string, error) {
//       svid, err := s.client.FetchJWTSVID(context.Background(),
//           jwtsvid.Params{Audience: audience})
//       return svid.Marshal(), err
//   }

type SpiffeIdentity struct {
	socketPath  string
	trustDomain string
	// client workloadapi.Client  ← uncomment when adding go-spiffe dep
}

// NewSpiffeIdentity creates a SpiffeIdentity that will connect to the
// Workload API at socketPath. Uses $SPIFFE_ENDPOINT_SOCKET if path is empty.
func NewSpiffeIdentity(socketPath string) (*SpiffeIdentity, error) {
	if socketPath == "" {
		socketPath = os.Getenv("SPIFFE_ENDPOINT_SOCKET")
	}
	if socketPath == "" {
		socketPath = "/run/spire/sockets/agent.sock"
	}
	if _, err := os.Stat(socketPath); err != nil {
		return nil, fmt.Errorf("SPIFFE socket not found at %s: %w", socketPath, err)
	}
	// TODO: connect client and fetch initial SVID
	// For now, return stub that logs the socket path
	return &SpiffeIdentity{socketPath: socketPath, trustDomain: "local"}, nil
}

func (s *SpiffeIdentity) AgentID() string {
	// TODO: return svid.ID.String() once client is wired
	return fmt.Sprintf("spiffe://%s/agent/pending", s.trustDomain)
}
func (s *SpiffeIdentity) TrustDomain() string                      { return s.trustDomain }
func (s *SpiffeIdentity) X509SVID() (*tls.Certificate, error)      { return nil, fmt.Errorf("go-spiffe not wired: add github.com/spiffe/go-spiffe/v2") }
func (s *SpiffeIdentity) JWTSVID(_ string) (string, error)         { return "", fmt.Errorf("go-spiffe not wired") }
func (s *SpiffeIdentity) TrustBundle() (*x509.CertPool, error)     { return nil, fmt.Errorf("go-spiffe not wired") }
func (s *SpiffeIdentity) Mode() string                             { return "spiffe" }

// --- Factory -----------------------------------------------------------------

// NewIdentityProvider selects the implementation based on environment.
// Priority:
//   1. SPIFFE_ENDPOINT_SOCKET set and socket exists → SpiffeIdentity
//   2. AMF_IDENTITY_MODE=spiffe → SpiffeIdentity (error if socket missing)
//   3. Everything else → StaticIdentity
func NewIdentityProvider(role, name, trustDomain string) IdentityProvider {
	mode := os.Getenv("AMF_IDENTITY_MODE")
	socketPath := os.Getenv("SPIFFE_ENDPOINT_SOCKET")
	if socketPath == "" {
		socketPath = "/run/spire/sockets/agent.sock"
	}

	if mode == "spiffe" || (mode == "" && socketExists(socketPath)) {
		id, err := NewSpiffeIdentity(socketPath)
		if err == nil {
			return id
		}
		if mode == "spiffe" {
			// Explicit request for SPIFFE — fail hard
			panic(fmt.Sprintf("identity: AMF_IDENTITY_MODE=spiffe but socket unavailable: %v", err))
		}
		// Socket not found, fall through to static
	}

	return NewStaticIdentity(trustDomain, role, name)
}

func socketExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
