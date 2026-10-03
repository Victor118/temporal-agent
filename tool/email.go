package tool

import (
	"context"
	"encoding/json"
	"fmt"
	"mime"
	"net"
	"net/mail"
	"net/smtp"
	"strings"
)

type SMTPConfig struct {
	Host     string
	Port     string
	Username string
	Password string
	From     string // Default sender
}

func RegisterEmailTool(r *Registry, cfg SMTPConfig) {
	if cfg.Host == "" {
		return
	}

	r.Register(&Tool{
		Name:        "send_email",
		Description: "Send an email via SMTP. Use this to send reports, notifications, or any content by email.",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"to":      {"type": "array", "items": {"type": "string"}, "description": "List of recipient email addresses"},
				"subject": {"type": "string", "description": "Email subject line"},
				"body":    {"type": "string", "description": "Email body (plain text)"},
				"cc":      {"type": "array", "items": {"type": "string"}, "description": "CC recipients (optional)"},
				"reply_to": {"type": "string", "description": "Reply-To address (optional)"}
			},
			"required": ["to", "subject", "body"]
		}`),
		Kind:      ToolKindActivity,
		Sensitive: true,
		Execute: func(ctx context.Context, input json.RawMessage) (string, error) {
			var params emailParams
			if err := json.Unmarshal(input, &params); err != nil {
				return "", fmt.Errorf("send_email: %w", err)
			}
			msg, recipients, err := buildEmail(cfg.From, params)
			if err != nil {
				return "", fmt.Errorf("send_email: %w", err)
			}

			addr := net.JoinHostPort(cfg.Host, cfg.Port)
			auth := smtp.PlainAuth("", cfg.Username, cfg.Password, cfg.Host)

			if err := smtp.SendMail(addr, auth, cfg.From, recipients, msg); err != nil {
				return "", fmt.Errorf("send_email: %w", err)
			}

			return fmt.Sprintf("Email sent to %s (subject: %s)", strings.Join(params.To, ", "), params.Subject), nil
		},
	})
}

// emailParams is the model's input to send_email.
type emailParams struct {
	To      []string `json:"to"`
	Subject string   `json:"subject"`
	Body    string   `json:"body"`
	CC      []string `json:"cc"`
	ReplyTo string   `json:"reply_to"`
}

// buildEmail writes the message and lists its recipients (To and Cc). Every
// header value comes from the model: a line break in one would add headers
// of its choosing, or start the body early, so it is refused, and each
// address must parse as one. net/smtp guards only the SMTP commands, not the
// message.
func buildEmail(from string, p emailParams) ([]byte, []string, error) {
	if len(p.To) == 0 {
		return nil, nil, fmt.Errorf("at least one recipient required")
	}
	if p.Subject == "" {
		return nil, nil, fmt.Errorf("subject is required")
	}
	if strings.ContainsAny(p.Subject, "\r\n") {
		return nil, nil, fmt.Errorf("the subject must hold on one line")
	}
	to, err := parseAddresses("to", p.To)
	if err != nil {
		return nil, nil, err
	}
	cc, err := parseAddresses("cc", p.CC)
	if err != nil {
		return nil, nil, err
	}
	var replyTo []*mail.Address
	if p.ReplyTo != "" {
		if replyTo, err = parseAddresses("reply_to", []string{p.ReplyTo}); err != nil {
			return nil, nil, err
		}
	}

	var msg strings.Builder
	fmt.Fprintf(&msg, "From: %s\r\n", from)
	fmt.Fprintf(&msg, "To: %s\r\n", joinAddresses(to))
	if len(cc) > 0 {
		fmt.Fprintf(&msg, "Cc: %s\r\n", joinAddresses(cc))
	}
	if len(replyTo) > 0 {
		fmt.Fprintf(&msg, "Reply-To: %s\r\n", joinAddresses(replyTo))
	}
	// A header is ASCII: a subject that is not goes as RFC 2047 encoded
	// words. An ASCII one is left as it is.
	fmt.Fprintf(&msg, "Subject: %s\r\n", mime.QEncoding.Encode("utf-8", p.Subject))
	msg.WriteString("MIME-Version: 1.0\r\n")
	msg.WriteString("Content-Type: text/plain; charset=\"utf-8\"\r\n")
	msg.WriteString("\r\n")
	msg.WriteString(p.Body)

	recipients := make([]string, 0, len(to)+len(cc))
	for _, list := range [][]*mail.Address{to, cc} {
		for _, a := range list {
			recipients = append(recipients, a.Address)
		}
	}
	return []byte(msg.String()), recipients, nil
}

// parseAddresses parses one address per value, refusing a line break even
// where the address syntax would fold it away.
func parseAddresses(field string, values []string) ([]*mail.Address, error) {
	out := make([]*mail.Address, 0, len(values))
	for _, v := range values {
		if strings.ContainsAny(v, "\r\n") {
			return nil, fmt.Errorf("%s: %q holds a line break", field, v)
		}
		a, err := mail.ParseAddress(v)
		if err != nil {
			return nil, fmt.Errorf("%s: %q is not one email address: %w", field, v, err)
		}
		out = append(out, a)
	}
	return out, nil
}

// joinAddresses writes addresses for a header; a name that is not ASCII is
// encoded.
func joinAddresses(addrs []*mail.Address) string {
	s := make([]string, len(addrs))
	for i, a := range addrs {
		s[i] = a.String()
	}
	return strings.Join(s, ", ")
}
