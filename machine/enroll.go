package machine

import "time"

// The enrollment's HTTP messages (docs/design/machines.md, §5), in JSON.
// A device request (RFC 8628): POST /machines/device with an EnrollRequest
// gives a DeviceGrant; the machine then polls POST /machines/device/token
// with its DeviceCode until a TokenGrant comes. From a script: POST
// /machines/enroll with an EnrollRequest carrying an enrollment token.

// EnrollRequest is what a machine says of itself when it enrolls.
type EnrollRequest struct {
	Name          string   `json:"name"`
	OS            string   `json:"os"`
	Capabilities  []string `json:"capabilities"`
	MaxDirectives int      `json:"max_directives"`
	AgentVersion  string   `json:"agent_version"`
	// EnrollmentToken: /machines/enroll only.
	EnrollmentToken string `json:"enrollment_token,omitempty"`
}

// DeviceGrant answers a device request: the user code to type in the
// front, the secret to poll with, and how long it lasts.
type DeviceGrant struct {
	DeviceCode string `json:"device_code"`
	UserCode   string `json:"user_code"`
	// VerificationPath is where the user types the code, on the server the
	// machine joins.
	VerificationPath string `json:"verification_path"`
	ExpiresIn        int    `json:"expires_in"` // seconds
	Interval         int    `json:"interval"`   // seconds between polls
}

// TokenRequest polls for a device request's token.
type TokenRequest struct {
	DeviceCode string `json:"device_code"`
}

// TokenGrant is the machine's token, given once; or, with an HTTP 400,
// Error: why not yet (ErrorPending) or never (the others).
type TokenGrant struct {
	MachineID string `json:"machine_id,omitempty"`
	Name      string `json:"name,omitempty"`
	Token     string `json:"token,omitempty"`
	Error     string `json:"error,omitempty"`
}

// Token errors, as RFC 8628 names them.
const (
	ErrorPending = "authorization_pending"
	ErrorExpired = "expired_token"
	ErrorInvalid = "invalid_grant"
	ErrorRequest = "invalid_request"
	ErrorBusy    = "slow_down"
)

// VerificationPath is the front's page where a user types a code.
const VerificationPath = "/machines/activer"

// DefaultPollInterval is how often a waiting machine asks for its token.
const DefaultPollInterval = 5 * time.Second
