package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"

	"github.com/victor/temporal-agent/config"
)

// temporalTLS is the TLS configuration of the connection to Temporal, or nil
// for plaintext. TLS is on with a client certificate (mTLS) or a CA: a
// certificate without its key, or the reverse, is a mistake reported here
// rather than a plaintext connection the server refuses, or worse accepts.
func temporalTLS(cfg *config.Config) (*tls.Config, error) {
	if (cfg.TemporalTLSCert == "") != (cfg.TemporalTLSKey == "") {
		return nil, errors.New("TEMPORAL_TLS_CERT and TEMPORAL_TLS_KEY go together: set both or neither")
	}
	if cfg.TemporalTLSCert == "" && cfg.TemporalTLSCA == "" {
		if cfg.TemporalTLSServerName != "" {
			return nil, errors.New("TEMPORAL_TLS_SERVER_NAME needs TLS: set TEMPORAL_TLS_CERT and TEMPORAL_TLS_KEY, or TEMPORAL_TLS_CA")
		}
		return nil, nil
	}

	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: cfg.TemporalTLSServerName}
	if cfg.TemporalTLSCert != "" {
		cert, err := tls.LoadX509KeyPair(cfg.TemporalTLSCert, cfg.TemporalTLSKey)
		if err != nil {
			return nil, fmt.Errorf("load client certificate: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}
	if cfg.TemporalTLSCA != "" {
		pem, err := os.ReadFile(cfg.TemporalTLSCA)
		if err != nil {
			return nil, fmt.Errorf("read TEMPORAL_TLS_CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("TEMPORAL_TLS_CA %s holds no PEM certificate", cfg.TemporalTLSCA)
		}
		tlsCfg.RootCAs = pool
	}
	return tlsCfg, nil
}
