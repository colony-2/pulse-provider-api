package plugin

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"unicode"
	"unicode/utf8"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ReverseALPN distinguishes reverse provider sessions from ordinary gRPC.
const ReverseALPN = "pulse-provider-reverse/1"

const reverseALPN = ReverseALPN

func validIdentity(id string) bool {
	if len(id) == 0 || len(id) > 256 || !utf8.ValidString(id) {
		return false
	}
	for _, r := range id {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

// Dynamic TLS configurations could bypass the required verification settings.
// Callers can rotate credentials by supplying fresh options to a new lifecycle.
func reverseTLS(in *tls.Config, host bool) (*tls.Config, error) {
	if in == nil || in.InsecureSkipVerify || in.GetConfigForClient != nil || in.MinVersion < tls.VersionTLS13 || (in.MaxVersion != 0 && in.MaxVersion < tls.VersionTLS13) {
		return nil, ErrInvalid
	}
	if len(in.NextProtos) != 1 || in.NextProtos[0] != reverseALPN {
		return nil, ErrInvalid
	}
	if host {
		if in.ClientAuth != tls.RequireAndVerifyClientCert || in.ClientCAs == nil || (len(in.Certificates) == 0 && in.GetCertificate == nil) {
			return nil, ErrInvalid
		}
	} else if in.RootCAs == nil || in.ServerName == "" || (len(in.Certificates) == 0 && in.GetClientCertificate == nil) {
		return nil, ErrInvalid
	}
	c := in.Clone()
	c.NextProtos = append([]string(nil), in.NextProtos...)
	c.Certificates = append([]tls.Certificate(nil), in.Certificates...)
	for i := range c.Certificates {
		c.Certificates[i].Certificate = make([][]byte, len(in.Certificates[i].Certificate))
		for j, b := range in.Certificates[i].Certificate {
			c.Certificates[i].Certificate[j] = append([]byte(nil), b...)
		}
		c.Certificates[i].OCSPStaple = append([]byte(nil), in.Certificates[i].OCSPStaple...)
	}
	if in.RootCAs != nil {
		c.RootCAs = in.RootCAs.Clone()
	}
	if in.ClientCAs != nil {
		c.ClientCAs = in.ClientCAs.Clone()
	}
	return c, nil
}

func writeAdmission(c net.Conn, status byte, id string) error {
	b := make([]byte, 7+len(id))
	copy(b, "PPR1")
	b[4] = status
	binary.BigEndian.PutUint16(b[5:7], uint16(len(id)))
	copy(b[7:], id)
	for len(b) > 0 {
		n, e := c.Write(b)
		if e != nil {
			return e
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		b = b[n:]
	}
	return nil
}
func readAdmission(c net.Conn) (string, error) {
	var h [7]byte
	if _, e := io.ReadFull(c, h[:]); e != nil {
		return "", e
	}
	n := int(binary.BigEndian.Uint16(h[5:]))
	if string(h[:4]) != "PPR1" || h[4] > 4 || n > 256 {
		return "", ErrProtocol
	}
	if h[4] != 0 {
		if n != 0 {
			return "", ErrProtocol
		}
		return "", ErrProtocol
	}
	if n == 0 {
		return "", ErrProtocol
	}
	b := make([]byte, n)
	if _, e := io.ReadFull(c, b); e != nil {
		return "", e
	}
	if !validIdentity(string(b)) {
		return "", ErrProtocol
	}
	return string(b), nil
}

type sessionSocket struct {
	net.Conn
	once sync.Once
	done chan struct{}
}

func newSessionSocket(c net.Conn) *sessionSocket {
	return &sessionSocket{Conn: c, done: make(chan struct{})}
}
func (c *sessionSocket) Close() error {
	e := c.Conn.Close()
	c.once.Do(func() { close(c.done) })
	return e
}

type socketListener struct {
	socket       *sessionSocket
	mu           sync.Mutex
	used, closed bool
	done         chan struct{}
}

func newSocketListener(c *sessionSocket) *socketListener {
	return &socketListener{socket: c, done: make(chan struct{})}
}
func (l *socketListener) Accept() (net.Conn, error) {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil, net.ErrClosed
	}
	if !l.used {
		l.used = true
		l.mu.Unlock()
		return l.socket, nil
	}
	l.mu.Unlock()
	select {
	case <-l.done:
	case <-l.socket.done:
	}
	return nil, net.ErrClosed
}
func (l *socketListener) Addr() net.Addr { return l.socket.LocalAddr() }
func (l *socketListener) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.closed {
		l.closed = true
		close(l.done)
		if !l.used {
			return l.socket.Close()
		}
	}
	return nil
}

func adoptSocket(c *sessionSocket, limit, metadata int) (*grpc.ClientConn, error) {
	var mu sync.Mutex
	used := false
	dial := func(context.Context, string) (net.Conn, error) {
		mu.Lock()
		defer mu.Unlock()
		if used {
			return nil, net.ErrClosed
		}
		used = true
		return c, nil
	}
	return grpc.NewClient("passthrough:///reverse-session", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(dial), grpc.WithDisableRetry(), grpc.WithDisableServiceConfig(), grpc.WithMaxHeaderListSize(uint32(metadata)), grpc.WithDefaultCallOptions(grpc.WaitForReady(false), grpc.MaxCallRecvMsgSize(limit), grpc.MaxCallSendMsgSize(limit)))
}
