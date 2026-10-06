package connect

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/victor/temporal-agent/machine"
)

type directiveKey struct{}

// withDirective tells an executor the directive it runs: what it publishes
// goes to it (Client.Upload).
func withDirective(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, directiveKey{}, id)
}

// DirectiveOf is the directive ctx runs; "" outside of one.
func DirectiveOf(ctx context.Context) string {
	id, _ := ctx.Value(directiveKey{}).(string)
	return id
}

// UploadRefused is a file the server refused (too large, a name taken with
// other content, the directive over): trying again would not help. Its
// words are for the model.
type UploadRefused struct{ Reason string }

func (e *UploadRefused) Error() string { return e.Reason }

// uploadTries is how many times an upload is tried when the server cannot be
// reached or fails: it is idempotent (the same name and content is the file
// stored first).
const uploadTries = 3

// Upload publishes content as a file named name for the directive ctx runs
// (PUT /machines/files): the server attaches it to the directive's turn and
// returns its reference. The machine's token is read again at each try: a
// rotation may have replaced it since the connection began, and the server
// refuses a replaced one on this route (401: tried again with the new one).
func (c *Client) Upload(ctx context.Context, name string, content []byte) (machine.FileRef, error) {
	id := DirectiveOf(ctx)
	if id == "" {
		return machine.FileRef{}, errors.New("no directive to publish for")
	}
	var last error
	for i, wait := 0, time.Second; i < uploadTries; i, wait = i+1, wait*2 {
		if i > 0 {
			select {
			case <-ctx.Done():
				return machine.FileRef{}, ctx.Err()
			case <-time.After(wait):
			}
		}
		ref, err := c.upload(ctx, id, name, content)
		var refused *UploadRefused
		if err == nil || errors.As(err, &refused) || ctx.Err() != nil {
			return ref, err
		}
		last = err
	}
	return machine.FileRef{}, last
}

func (c *Client) upload(ctx context.Context, id, name string, content []byte) (machine.FileRef, error) {
	cfg, err := c.State.Load()
	if err != nil {
		return machine.FileRef{}, err
	}
	q := url.Values{"directive": {id}, "name": {name}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, cfg.Server+machine.FilesPath+"?"+q.Encode(), bytes.NewReader(content))
	if err != nil {
		return machine.FileRef{}, err
	}
	req.Header.Set("Authorization", "Bearer "+cfg.Token)
	req.Header.Set("Content-Type", "application/octet-stream")
	hc := c.HTTPClient
	if hc == nil {
		hc = http.DefaultClient
	}
	resp, err := hc.Do(req)
	if err != nil {
		return machine.FileRef{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return machine.FileRef{}, err
	}
	if resp.StatusCode == http.StatusOK {
		var ref machine.FileRef
		if err := json.Unmarshal(body, &ref); err != nil || ref.ID == "" {
			return machine.FileRef{}, fmt.Errorf("the server's answer: %q", machine.Cut(string(body), 200))
		}
		return ref, nil
	}
	var refusal machine.UploadError
	json.Unmarshal(body, &refusal)
	msg := machine.Cut(refusal.Error, 1024)
	if msg == "" {
		msg = fmt.Sprintf("status %d", resp.StatusCode)
	}
	switch {
	case resp.StatusCode == http.StatusUnauthorized, resp.StatusCode/100 == 5, resp.StatusCode == http.StatusTooManyRequests:
		return machine.FileRef{}, errors.New(msg)
	}
	return machine.FileRef{}, &UploadRefused{Reason: msg}
}
