package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/victor/temporal-agent/config"
)

// writeCert writes a self-signed certificate and its key as PEM files.
func writeCert(t *testing.T) (certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "temporal"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certFile, keyFile = filepath.Join(dir, "client.pem"), filepath.Join(dir, "client.key")
	os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600)
	os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600)
	return certFile, keyFile
}

func TestTemporalTLS(t *testing.T) {
	cert, key := writeCert(t)
	notPEM := filepath.Join(t.TempDir(), "ca.pem")
	os.WriteFile(notPEM, []byte("not a certificate"), 0o600)

	// Nothing set: plaintext, as before.
	if got, err := temporalTLS(&config.Config{}); got != nil || err != nil {
		t.Errorf("no TLS settings = %v, %v; want plaintext", got, err)
	}

	// A client certificate, a private CA and a server name.
	got, err := temporalTLS(&config.Config{TemporalTLSCert: cert, TemporalTLSKey: key, TemporalTLSCA: cert, TemporalTLSServerName: "temporal.internal"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Certificates) != 1 || got.RootCAs == nil || got.ServerName != "temporal.internal" {
		t.Errorf("TLS config %+v", got)
	}

	// A CA alone: TLS without a client certificate.
	if got, err := temporalTLS(&config.Config{TemporalTLSCA: cert}); err != nil || got == nil || len(got.Certificates) != 0 || got.RootCAs == nil {
		t.Errorf("CA alone = %+v, %v", got, err)
	}

	// TEMPORAL_TLS alone: the system's CAs, and a server name may go with it.
	if got, err := temporalTLS(&config.Config{TemporalTLS: "true", TemporalTLSServerName: "temporal.example.com"}); err != nil || got == nil ||
		len(got.Certificates) != 0 || got.RootCAs != nil || got.ServerName != "temporal.example.com" {
		t.Errorf("TEMPORAL_TLS=true = %+v, %v; want TLS with the system's CAs", got, err)
	}
	if got, err := temporalTLS(&config.Config{TemporalTLS: "false"}); got != nil || err != nil {
		t.Errorf("TEMPORAL_TLS=false = %v, %v; want plaintext", got, err)
	}

	for name, cfg := range map[string]config.Config{
		"TEMPORAL_TLS not a boolean":   {TemporalTLS: "yes please"},
		"TEMPORAL_TLS=false with cert": {TemporalTLS: "false", TemporalTLSCert: cert, TemporalTLSKey: key},
		"TEMPORAL_TLS=false with CA":   {TemporalTLS: "false", TemporalTLSCA: cert},
		"cert without key":             {TemporalTLSCert: cert},
		"key without cert":             {TemporalTLSKey: key},
		"server name alone":            {TemporalTLSServerName: "temporal.internal"},
		"missing files":                {TemporalTLSCert: cert + ".missing", TemporalTLSKey: key},
		"CA that is not PEM":           {TemporalTLSCA: notPEM},
		"cert and key swapped":         {TemporalTLSCert: key, TemporalTLSKey: cert},
		"CA file that is absent":       {TemporalTLSCA: notPEM + ".missing"},
	} {
		if got, err := temporalTLS(&cfg); err == nil {
			t.Errorf("%s: accepted, %+v", name, got)
		}
	}
}
