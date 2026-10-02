package tool

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
)

func TestIsPublicAddr(t *testing.T) {
	for addr, want := range map[string]bool{
		"93.184.216.34":   true,
		"2606:4700::1111": true,
		"127.0.0.1":       false,
		"::1":             false,
		"10.1.2.3":        false,
		"172.18.0.5":      false, // a Docker network
		"192.168.1.1":     false,
		"169.254.169.254": false, // cloud metadata
		"fe80::1":         false,
		"fd00::1":         false,
		"0.0.0.0":         false,
		"::":              false,
		"100.64.0.1":      false,
		"224.0.0.1":       false,
		"255.255.255.255": false,
		"::ffff:10.0.0.1": false, // IPv4-mapped
		"64:ff9b::a00:1":  false, // NAT64 to 10.0.0.1
	} {
		if got := isPublicAddr(netip.MustParseAddr(addr)); got != want {
			t.Errorf("isPublicAddr(%s) = %v, want %v", addr, got, want)
		}
	}
}

// The model picks the URL: a local service must stay out of its reach, by
// name, by address or through a redirect.
func TestWebFetch_RefusesPrivateAddresses(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("internal secret"))
	}))
	defer srv.Close()

	client := newFetchClient(isPublicAddr)
	localhost := strings.Replace(srv.URL, "127.0.0.1", "localhost", 1)
	for _, u := range []string{srv.URL, localhost} {
		out, err := fetchPage(context.Background(), client, u)
		if err == nil || !strings.Contains(err.Error(), "not a public address") {
			t.Errorf("fetch %s: %q, %v; want a refusal", u, out, err)
		}
	}
}

func TestWebFetch_RefusesOtherSchemes(t *testing.T) {
	client := newFetchClient(isPublicAddr)
	for _, u := range []string{"file:///etc/passwd", "gopher://example.com", "//example.com/x", "http://"} {
		if _, err := fetchPage(context.Background(), client, u); err == nil {
			t.Errorf("fetch %s: no error", u)
		}
	}
}

func TestWebFetch_Redirects(t *testing.T) {
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/file":
			http.Redirect(w, r, "file:///etc/passwd", http.StatusFound)
		case "/loop":
			http.Redirect(w, r, srv.URL+"/loop", http.StatusFound)
		case "/hop":
			http.Redirect(w, r, srv.URL+"/page", http.StatusFound)
		default:
			w.Write([]byte("<p>hello</p>"))
		}
	}))
	defer srv.Close()

	// The test server is on loopback: allow it, the redirect rules are what
	// is under test here.
	client := newFetchClient(func(netip.Addr) bool { return true })
	if out, err := fetchPage(context.Background(), client, srv.URL+"/hop"); err != nil || !strings.Contains(out, "hello") {
		t.Errorf("a plain redirect: %q, %v", out, err)
	}
	if _, err := fetchPage(context.Background(), client, srv.URL+"/file"); err == nil {
		t.Error("followed a redirect to file://")
	}
	if _, err := fetchPage(context.Background(), client, srv.URL+"/loop"); err == nil || !strings.Contains(err.Error(), "redirects") {
		t.Errorf("redirect loop: %v", err)
	}
}
