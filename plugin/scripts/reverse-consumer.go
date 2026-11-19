// Run the copied reverse examples as independent processes with temporary trust.
// This helper intentionally imports only the standard library.
package main

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run() error {
	if len(os.Args) != 3 {
		return fmt.Errorf("host and provider executable paths required")
	}
	dir, e := os.MkdirTemp("", "reverse-consumer-")
	if e != nil {
		return e
	}
	defer os.RemoveAll(dir)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	caKey, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if e != nil {
		return e
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "example CA"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, e := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if e != nil {
		return e
	}
	write := func(name string, b []byte) error { return os.WriteFile(filepath.Join(dir, name), b, 0600) }
	if e = write("ca.crt", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})); e != nil {
		return e
	}
	for i, name := range []string{"collector.test", "alpha", "beta"} {
		key, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if e != nil {
			return e
		}
		usage := x509.ExtKeyUsageClientAuth
		if i == 0 {
			usage = x509.ExtKeyUsageServerAuth
		}
		cert := &x509.Certificate{SerialNumber: big.NewInt(int64(i + 2)), Subject: pkix.Name{CommonName: name}, DNSNames: []string{name}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), ExtKeyUsage: []x509.ExtKeyUsage{usage}, KeyUsage: x509.KeyUsageDigitalSignature}
		der, e := x509.CreateCertificate(rand.Reader, cert, ca, &key.PublicKey, caKey)
		if e != nil {
			return e
		}
		pk, e := x509.MarshalPKCS8PrivateKey(key)
		if e != nil {
			return e
		}
		if e = write(name+".crt", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})); e != nil {
			return e
		}
		if e = write(name+".key", pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pk})); e != nil {
			return e
		}
	}
	if e = write("config.yaml", []byte("base: example\n")); e != nil {
		return e
	}
	tlsArgs := func(name string) []string {
		return []string{"-ca", filepath.Join(dir, "ca.crt"), "-cert", filepath.Join(dir, name+".crt"), "-key", filepath.Join(dir, name+".key")}
	}
	host := exec.CommandContext(ctx, os.Args[1], append(tlsArgs("collector.test"), "-identities", "alpha,beta")...)
	host.Stderr = os.Stderr
	input, e := host.StdinPipe()
	if e != nil {
		return e
	}
	output, e := host.StdoutPipe()
	if e != nil {
		return e
	}
	if e = host.Start(); e != nil {
		return e
	}
	defer func() { _ = host.Process.Kill(); _ = host.Wait() }()
	scanner := bufio.NewScanner(output)
	if !scanner.Scan() || !strings.HasPrefix(scanner.Text(), "listening=") {
		return fmt.Errorf("collector did not start")
	}
	address := strings.TrimPrefix(scanner.Text(), "listening=")
	var changes []string
	recordChange := func(line string) bool {
		if strings.HasPrefix(line, "change ") {
			changes = append(changes, line)
			return true
		}
		return false
	}
	invocation := func(expected string) error {
		for {
			if _, e := io.WriteString(input, "invoke\n"); e != nil {
				return e
			}
			got := false
			for scanner.Scan() {
				if !recordChange(scanner.Text()) {
					got = true
					break
				}
			}
			line := scanner.Text()
			if !got {
				return fmt.Errorf("collector invocation ended: %v", scanner.Err())
			}
			if line == "invocation failed" {
				// Departure can race discovery on a captured snapshot. A failed
				// read proves no membership state; retry a fresh invocation below.
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(10 * time.Millisecond):
				}
				continue
			}
			parts := strings.SplitN(line, " providers=", 2)
			if len(parts) != 2 {
				return fmt.Errorf("invalid invocation result: %s", line)
			}
			ids := []string{}
			for _, entry := range strings.Split(parts[1], ",") {
				if entry != "" {
					ids = append(ids, strings.SplitN(entry, "/", 2)[0])
				}
			}
			if strings.Join(ids, ",") == expected {
				fmt.Println(line)
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
	start := func(name string) (*exec.Cmd, error) {
		args := append(tlsArgs(name), "-name", name, "-address", address, "-server-name", "collector.test", "-config", filepath.Join(dir, "config.yaml"))
		p := exec.CommandContext(ctx, os.Args[2], args...)
		p.Stderr = os.Stderr
		if e := p.Start(); e != nil {
			return nil, e
		}
		return p, nil
	}
	stop := func(p *exec.Cmd) error {
		if e := p.Process.Signal(os.Interrupt); e != nil {
			return e
		}
		return p.Wait()
	}
	if e = invocation(""); e != nil {
		return e
	}
	alpha, e := start("alpha")
	if e != nil {
		return e
	}
	defer func() { _ = alpha.Process.Kill(); _ = alpha.Wait() }()
	if e = invocation("alpha"); e != nil {
		return e
	}
	beta, e := start("beta")
	if e != nil {
		return e
	}
	defer func() { _ = beta.Process.Kill(); _ = beta.Wait() }()
	if e = invocation("alpha,beta"); e != nil {
		return e
	}
	if e = stop(alpha); e != nil {
		return e
	}
	if e = invocation("beta"); e != nil {
		return e
	}
	if e = stop(beta); e != nil {
		return e
	}
	if e = invocation(""); e != nil {
		return e
	}
	if _, e = io.WriteString(input, "quit\n"); e != nil {
		return e
	}
	for scanner.Scan() {
		if !recordChange(scanner.Text()) {
			return fmt.Errorf("unexpected shutdown output: %s", scanner.Text())
		}
	}
	if e = scanner.Err(); e != nil {
		return e
	}
	if e = host.Wait(); e != nil {
		return e
	}
	if len(changes) != 6 {
		return fmt.Errorf("expected initial, four deltas and stopped; got %v", changes)
	}
	for i, line := range changes {
		fields := strings.Fields(line)
		values := map[string]string{}
		for _, f := range fields[1:] {
			k, v, ok := strings.Cut(f, "=")
			if !ok {
				return fmt.Errorf("invalid callback: %s", line)
			}
			values[k] = v
		}
		kind, reason, joined, left := "membership", "ready", "", ""
		switch i {
		case 0:
			kind, reason = "initial", "initial"
		case 1:
			joined = "alpha/1"
		case 2:
			joined = "beta/2"
		case 3:
			left = "alpha/1"
			reason = values["reason"]
		case 4:
			left = "beta/2"
			reason = values["reason"]
		case 5:
			kind, reason = "stopped", "collector_closed"
		}
		if (i == 3 || i == 4) && reason != "connection_lost" && reason != "probe_failed" {
			return fmt.Errorf("invalid departure reason: %s", line)
		}
		if len(fields) != 6 || values["revision"] != fmt.Sprint(i+1) || values["kind"] != kind || values["reason"] != reason || values["joined"] != joined || values["left"] != left {
			return fmt.Errorf("unexpected callback: %s", line)
		}
		fmt.Println(line)
	}
	fmt.Println("independent callback baseline, ordered deltas and stopped delivery passed")
	fmt.Println("independent reverse collector join/leave verification passed")
	return nil
}
