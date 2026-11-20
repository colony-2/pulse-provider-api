package plugin

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"time"
)

// MaxMutualKeyBytes bounds both input acquisition and the admission frame.
const MaxMutualKeyBytes = 4096

// MutualKeySource selects exactly one explicit source. The same buffered reader
// must be used for any commands following a stdin key. LookupEnv is injected;
// this package never reads the process environment or stdin implicitly.
type MutualKeySource struct {
	Stdin     *bufio.Reader
	EnvName   string
	LookupEnv func(string) (string, bool)
}

func validMutualKey(key []byte) bool {
	return len(key) > 0 && len(key) <= MaxMutualKeyBytes && !bytes.ContainsAny(key, "\r\n")
}

// LoadMutualKey reads one bounded line (stripping only LF or CRLF), or an exact
// environment value. Errors never include input bytes or underlying errors.
// It is synchronous; callers must interrupt an owned blocking input themselves.
func LoadMutualKey(source MutualKeySource) ([]byte, error) {
	if (source.Stdin == nil) == (source.EnvName == "") {
		return nil, ErrInvalid
	}
	var key []byte
	if source.Stdin != nil {
		// ReadByte bounds allocation and consumption even with a small Reader buffer.
		for i := 0; i < MaxMutualKeyBytes+2; i++ {
			b, e := source.Stdin.ReadByte()
			if e == io.EOF {
				break
			}
			if e != nil {
				return nil, ErrInvalid
			}
			key = append(key, b)
			if b == '\n' {
				break
			}
		}
		if len(key) > 0 && key[len(key)-1] == '\n' {
			key = key[:len(key)-1]
			if len(key) > 0 && key[len(key)-1] == '\r' {
				key = key[:len(key)-1]
			}
		}
	} else {
		if source.LookupEnv == nil {
			return nil, ErrInvalid
		}
		value, ok := source.LookupEnv(source.EnvName)
		if !ok || len(value) > MaxMutualKeyBytes {
			return nil, ErrInvalid
		}
		key = []byte(value)
	}
	if !validMutualKey(key) {
		return nil, ErrInvalid
	}
	return key, nil
}

// LoadMutualKeyInput bounds acquisition from an explicitly owned input file.
// Pipes support deadlines on supported platforms. Unsupported inputs (including
// terminals without deadline support) fail before reading; use an env source.
// reader must wrap input and have no concurrent reader. This helper owns the
// read deadline and clears it before returning, preserving later command reads.
func LoadMutualKeyInput(ctx context.Context, input *os.File, reader *bufio.Reader) ([]byte, error) {
	unavailable := errors.New("key input unavailable; use an environment source")
	if ctx == nil || input == nil || reader == nil {
		return nil, ErrInvalid
	}
	if ctx.Err() != nil {
		return nil, unavailable
	}
	acquisition, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if input.SetReadDeadline(deadlineOf(acquisition)) != nil {
		return nil, unavailable
	}
	interrupted := make(chan struct{})
	stop := context.AfterFunc(acquisition, func() {
		_ = input.SetReadDeadline(time.Now())
		close(interrupted)
	})
	key, e := LoadMutualKey(MutualKeySource{Stdin: reader})
	if !stop() {
		<-interrupted
	}
	_ = input.SetReadDeadline(time.Time{})
	// File deadlines can fire just before the context timer is scheduled.
	if acquisition.Err() != nil || !time.Now().Before(deadlineOf(acquisition)) {
		return nil, unavailable
	}
	return key, e
}

func (o *CollectorOptions) matchesKey(id string, key []byte) bool {
	actual := sha256.Sum256(key)
	expected, exists := o.keyDigests[id] // Zero digest is the unknown-ID dummy.
	equal := subtle.ConstantTimeCompare(actual[:], expected[:])
	return exists && equal == 1
}

func writeKeyRequest(w io.Writer, id string, key []byte) error {
	if !validIdentity(id) || !validMutualKey(key) {
		return ErrInvalid
	}
	b := make([]byte, 9+len(id)+len(key))
	copy(b, "PPK1")
	b[4] = 1
	binary.BigEndian.PutUint16(b[5:7], uint16(len(id)))
	binary.BigEndian.PutUint16(b[7:9], uint16(len(key)))
	copy(b[9:], id)
	copy(b[9+len(id):], key)
	for len(b) > 0 {
		n, e := w.Write(b)
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

// deny is true only for a recognizable, bounded envelope. No buffered reader:
// bytes belonging to the following gRPC session must remain on the socket.
func readKeyRequest(r io.Reader) (id string, key []byte, deny bool, err error) {
	var h [9]byte
	if _, e := io.ReadFull(r, h[:]); e != nil {
		return "", nil, false, e
	}
	if string(h[:3]) != "PPK" {
		return "", nil, false, ErrProtocol
	}
	ni, nk := int(binary.BigEndian.Uint16(h[5:7])), int(binary.BigEndian.Uint16(h[7:9]))
	if ni > 256 || nk > MaxMutualKeyBytes {
		return "", nil, false, ErrProtocol
	}
	if h[3] != '1' || h[4] != 1 || ni == 0 || nk == 0 {
		return "", nil, true, ErrProtocol
	}
	payload := make([]byte, ni+nk)
	if _, e := io.ReadFull(r, payload); e != nil {
		return "", nil, false, e
	}
	id, key = string(payload[:ni]), payload[ni:]
	if !validIdentity(id) || !validMutualKey(key) {
		return "", nil, true, ErrProtocol
	}
	return id, key, false, nil
}
