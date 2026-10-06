package connect

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/victor/temporal-agent/machine"
)

// CheckServer reads the URL of the server a machine joins: https, or plain
// http to this host only (development). There is no way to skip the
// certificate's check: the server sends work for the owner's CLI.
func CheckServer(raw string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" {
		return "", fmt.Errorf("%q is not a server URL (https://…)", raw)
	}
	switch u.Scheme {
	case "https":
	case "http":
		host := u.Hostname()
		ip := net.ParseIP(host)
		if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
			return "", fmt.Errorf("%q: plain http only reaches this host; use https", raw)
		}
	default:
		return "", fmt.Errorf("%q: the URL must start with https://", raw)
	}
	if u.Path != "" && u.Path != "/" || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
		return "", fmt.Errorf("%q: give the server's address alone (https://host[:port])", raw)
	}
	return u.Scheme + "://" + u.Host, nil
}

// wsURL is the gateway's WebSocket on server.
func wsURL(server string) string {
	return "ws" + strings.TrimPrefix(server, "http") + "/machines/connect"
}

// Enroller enrolls a machine on a server.
type Enroller struct {
	Server string // CheckServer's
	Client *http.Client
	// Out is where the user is told what to do.
	Out io.Writer
}

func (e *Enroller) client() *http.Client {
	if e.Client != nil {
		return e.Client
	}
	return &http.Client{Timeout: 30 * time.Second}
}

// ErrEnrollment is an enrollment the server refused or let expire.
var ErrEnrollment = errors.New("enrollment refused")

func (e *Enroller) post(ctx context.Context, path string, body, out any) (int, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.Server+path, bytes.NewReader(b))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := e.client().Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(out); err != nil {
		return resp.StatusCode, fmt.Errorf("%s: status %d, unreadable answer: %w", path, resp.StatusCode, err)
	}
	return resp.StatusCode, nil
}

// ByCode enrolls by a code (RFC 8628): the machine asks, the user types the
// code it shows in the front, logged in, and approves; the machine polls
// until its token comes.
func (e *Enroller) ByCode(ctx context.Context, req machine.EnrollRequest) (machine.TokenGrant, error) {
	var grant machine.DeviceGrant
	status, err := e.post(ctx, "/machines/device", req, &grant)
	if err != nil {
		return machine.TokenGrant{}, err
	}
	if status != http.StatusOK || grant.DeviceCode == "" {
		return machine.TokenGrant{}, fmt.Errorf("%w: the server refused the request (status %d)", ErrEnrollment, status)
	}
	fmt.Fprintf(e.Out, "Ouvre %s%s et saisis le code %s (valable %d min).\n",
		e.Server, grant.VerificationPath, grant.UserCode, grant.ExpiresIn/60)
	interval := time.Duration(grant.Interval) * time.Second
	if interval < time.Second {
		interval = machine.DefaultPollInterval
	}
	for {
		select {
		case <-ctx.Done():
			return machine.TokenGrant{}, ctx.Err()
		case <-time.After(interval):
		}
		var tok machine.TokenGrant
		status, err := e.post(ctx, "/machines/device/token", machine.TokenRequest{DeviceCode: grant.DeviceCode}, &tok)
		switch {
		case err != nil:
			fmt.Fprintf(e.Out, "Serveur injoignable (%v), nouvel essai…\n", err)
			continue
		case status == http.StatusOK && tok.Token != "":
			return tok, nil
		case tok.Error == machine.ErrorPending:
			continue
		case tok.Error == machine.ErrorBusy:
			interval += time.Second
			continue
		case tok.Error == machine.ErrorExpired:
			return machine.TokenGrant{}, fmt.Errorf("%w: the code expired before it was approved; run the command again", ErrEnrollment)
		default:
			return machine.TokenGrant{}, fmt.Errorf("%w: %s (status %d)", ErrEnrollment, tok.Error, status)
		}
	}
}

// ByToken enrolls with an enrollment token a logged-in user created (a
// script's way).
func (e *Enroller) ByToken(ctx context.Context, req machine.EnrollRequest) (machine.TokenGrant, error) {
	var tok machine.TokenGrant
	status, err := e.post(ctx, "/machines/enroll", req, &tok)
	if err != nil {
		return tok, err
	}
	if status != http.StatusOK || tok.Token == "" {
		return machine.TokenGrant{}, fmt.Errorf("%w: %s (status %d)", ErrEnrollment, tok.Error, status)
	}
	return tok, nil
}
