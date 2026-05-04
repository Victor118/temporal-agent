package tool

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
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
		Kind: ToolKindActivity,
		Execute: func(ctx context.Context, input json.RawMessage) (string, error) {
			var params struct {
				To      []string `json:"to"`
				Subject string   `json:"subject"`
				Body    string   `json:"body"`
				CC      []string `json:"cc"`
				ReplyTo string   `json:"reply_to"`
			}
			if err := json.Unmarshal(input, &params); err != nil {
				return "", fmt.Errorf("send_email: %w", err)
			}

			if len(params.To) == 0 {
				return "", fmt.Errorf("send_email: at least one recipient required")
			}
			if params.Subject == "" {
				return "", fmt.Errorf("send_email: subject is required")
			}

			// Build message
			var msg strings.Builder
			msg.WriteString(fmt.Sprintf("From: %s\r\n", cfg.From))
			msg.WriteString(fmt.Sprintf("To: %s\r\n", strings.Join(params.To, ", ")))
			if len(params.CC) > 0 {
				msg.WriteString(fmt.Sprintf("Cc: %s\r\n", strings.Join(params.CC, ", ")))
			}
			if params.ReplyTo != "" {
				msg.WriteString(fmt.Sprintf("Reply-To: %s\r\n", params.ReplyTo))
			}
			msg.WriteString(fmt.Sprintf("Subject: %s\r\n", params.Subject))
			msg.WriteString("MIME-Version: 1.0\r\n")
			msg.WriteString("Content-Type: text/plain; charset=\"utf-8\"\r\n")
			msg.WriteString("\r\n")
			msg.WriteString(params.Body)

			// All recipients (To + CC)
			recipients := make([]string, 0, len(params.To)+len(params.CC))
			recipients = append(recipients, params.To...)
			recipients = append(recipients, params.CC...)

			addr := net.JoinHostPort(cfg.Host, cfg.Port)
			auth := smtp.PlainAuth("", cfg.Username, cfg.Password, cfg.Host)

			if err := smtp.SendMail(addr, auth, cfg.From, recipients, []byte(msg.String())); err != nil {
				return "", fmt.Errorf("send_email: %w", err)
			}

			return fmt.Sprintf("Email sent to %s (subject: %s)", strings.Join(params.To, ", "), params.Subject), nil
		},
	})
}
