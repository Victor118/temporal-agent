package machine

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"strings"
)

// Token prefixes: a leaked secret says what it is.
const (
	machineTokenPrefix    = "agm_"
	enrollmentTokenPrefix = "age_"
	deviceSecretPrefix    = "agd_"
)

func randomSecret(prefix string) string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(err) // crypto/rand does not fail on supported platforms
	}
	return prefix + base64.RawURLEncoding.EncodeToString(b)
}

// NewMachineToken is a machine's credential: long, its own, revocable. Only
// its hash is stored.
func NewMachineToken() string { return randomSecret(machineTokenPrefix) }

// NewEnrollmentToken enrolls one machine from a script, once.
func NewEnrollmentToken() string { return randomSecret(enrollmentTokenPrefix) }

// NewDeviceSecret is what a machine waiting for its approval keeps for
// itself: it alone fetches the machine token with it.
func NewDeviceSecret() string { return randomSecret(deviceSecretPrefix) }

// HashToken is what the database keeps of a token or a secret.
func HashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// userCodeAlphabet leaves out what reads alike (0 O, 1 I L): the code is
// read on one screen and typed on another.
const userCodeAlphabet = "ABCDEFGHJKMNPQRSTUVWXYZ23456789"

// UserCodeLength is the user code's length, without its dash: 31^6, about
// 9×10^8 codes, against a few tries per user and ten minutes.
const UserCodeLength = 6

// NewUserCode draws a user code, in its stored form (no dash).
func NewUserCode() string {
	b := make([]byte, UserCodeLength)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	// 256 is not a multiple of 31: draw again what would bias the code.
	out := make([]byte, 0, UserCodeLength)
	for len(out) < UserCodeLength {
		for _, c := range b {
			if int(c) < 256-256%len(userCodeAlphabet) && len(out) < UserCodeLength {
				out = append(out, userCodeAlphabet[int(c)%len(userCodeAlphabet)])
			}
		}
		if _, err := rand.Read(b); err != nil {
			panic(err)
		}
	}
	return string(out)
}

// FormatUserCode is how a user code is shown: KX4-92M.
func FormatUserCode(code string) string {
	if len(code) != UserCodeLength {
		return code
	}
	return code[:3] + "-" + code[3:]
}

// NormalizeUserCode reads a code as a user typed it (any case, dashes and
// spaces): its stored form, or "" when it cannot be one.
func NormalizeUserCode(input string) string {
	var b strings.Builder
	for _, r := range strings.ToUpper(input) {
		switch {
		case r == '-' || r == ' ' || r == '\t':
			continue
		case strings.ContainsRune(userCodeAlphabet, r):
			b.WriteRune(r)
		default:
			return ""
		}
	}
	if b.Len() != UserCodeLength {
		return ""
	}
	return b.String()
}
