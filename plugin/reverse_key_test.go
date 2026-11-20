package plugin

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

type privateReader struct{ reads int }

func (r *privateReader) Read([]byte) (int, error) {
	r.reads++
	return 0, errors.New("sensitive underlying failure")
}

type limitedWriter struct{ bytes.Buffer }

func (w *limitedWriter) Write(b []byte) (int, error) {
	if len(b) > 3 {
		b = b[:3]
	}
	return w.Buffer.Write(b)
}

type countedReader struct {
	*bytes.Reader
	consumed int
}

func (r *countedReader) Read(b []byte) (int, error) {
	n, e := r.Reader.Read(b)
	r.consumed += n
	return n, e
}

func TestReverseKeySources(t *testing.T) {
	for _, selection := range []string{"none", "both"} {
		r := &privateReader{}
		calls := 0
		source := MutualKeySource{LookupEnv: func(string) (string, bool) { calls++; return "secret", true }}
		if selection == "both" {
			source.Stdin = bufio.NewReader(r)
			source.EnvName = "KEY"
		}
		if _, e := LoadMutualKey(source); !errors.Is(e, ErrInvalid) || r.reads != 0 || calls != 0 {
			t.Fatal(e, r.reads, calls)
		}
	}
	for _, ending := range []string{"\n", "\r\n", ""} {
		for _, value := range []string{" spaces \x00\xff ", strings.Repeat("x", MaxMutualKeyBytes)} {
			t.Run("stdin/"+fmtEnding(ending)+"/"+strconv.Itoa(len(value)), func(t *testing.T) {
				input := value + ending
				if ending != "" {
					input += "invoke\nquit\n"
				}
				reader := bufio.NewReaderSize(strings.NewReader(input), 16)
				got, e := LoadMutualKey(MutualKeySource{Stdin: reader})
				if e != nil || string(got) != value {
					t.Fatal("key bytes changed", e)
				}
				rest, _ := io.ReadAll(reader)
				if ending != "" && string(rest) != "invoke\nquit\n" {
					t.Fatal("commands consumed")
				}
			})
		}
	}
	for _, value := range []string{"", "\n", "a\rb\n", strings.Repeat("x", 4097) + "\n", strings.Repeat("x", 1<<20)} {
		reader := &countedReader{Reader: bytes.NewReader([]byte(value))}
		if _, e := LoadMutualKey(MutualKeySource{Stdin: bufio.NewReaderSize(reader, 16)}); !errors.Is(e, ErrInvalid) {
			t.Fatal(e)
		}
		if reader.consumed > MaxMutualKeyBytes+32 {
			t.Fatal("unbounded input", reader.consumed)
		}
	}
	for _, value := range []string{" leading and trailing ", strings.Repeat("k", 4096), "", "a\nb", "a\rb", strings.Repeat("x", 4097)} {
		got, e := LoadMutualKey(MutualKeySource{EnvName: "KEY", LookupEnv: func(name string) (string, bool) {
			if name != "KEY" {
				t.Fatal(name)
			}
			return value, true
		}})
		if validMutualKey([]byte(value)) {
			if e != nil || string(got) != value {
				t.Fatal("environment changed", e)
			}
		} else if !errors.Is(e, ErrInvalid) {
			t.Fatal(e)
		}
	}
	for _, source := range []MutualKeySource{
		{Stdin: bufio.NewReader(&privateReader{})},
		{EnvName: "KEY", LookupEnv: func(string) (string, bool) { return "secret", false }},
		{EnvName: "KEY"},
	} {
		_, e := LoadMutualKey(source)
		if !errors.Is(e, ErrInvalid) || strings.Contains(e.Error(), "sensitive") || strings.Contains(e.Error(), "secret") {
			t.Fatal(e)
		}
	}
}
func fmtEnding(s string) string {
	if s == "\n" {
		return "LF"
	}
	if s == "\r\n" {
		return "CRLF"
	}
	return "EOF"
}

func TestReverseKeyOwnedInput(t *testing.T) {
	for _, kind := range []string{"timeout", "cancel", "active-cancel", "success"} {
		t.Run(kind, func(t *testing.T) {
			input, writer, e := os.Pipe()
			if e != nil {
				t.Fatal(e)
			}
			defer input.Close()
			defer writer.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
			defer cancel()
			reader := bufio.NewReader(input)
			if kind == "success" {
				_, _ = io.WriteString(writer, " disposable \ninvoke\n")
			} else if kind == "cancel" {
				cancel()
			} else if kind == "active-cancel" {
				timer := time.AfterFunc(10*time.Millisecond, cancel)
				defer timer.Stop()
			}
			got, e := LoadMutualKeyInput(ctx, input, reader)
			if kind != "success" {
				if e == nil {
					t.Fatal("blocked input accepted")
				}
				return
			}
			if e != nil || string(got) != " disposable " {
				t.Fatal(e)
			}
			// After the acquisition timeout has elapsed the command stream still works.
			<-ctx.Done()
			line, e := reader.ReadString('\n')
			if line != "invoke\n" || e != nil {
				t.Fatal(line, e)
			}
		})
	}
}

func keyFrame(id string, key []byte) []byte {
	b := make([]byte, 9+len(id)+len(key))
	copy(b, "PPK1")
	b[4] = 1
	binary.BigEndian.PutUint16(b[5:7], uint16(len(id)))
	binary.BigEndian.PutUint16(b[7:9], uint16(len(key)))
	copy(b[9:], id)
	copy(b[9+len(id):], key)
	return b
}
func TestReverseKeyFraming(t *testing.T) {
	for _, id := range []string{"alpha", strings.Repeat("é", 128)} {
		key := bytes.Repeat([]byte{'k'}, 4096)
		var writer limitedWriter
		if e := writeKeyRequest(&writer, id, key); e != nil {
			t.Fatal(e)
		}
		if !bytes.Equal(writer.Bytes(), keyFrame(id, key)) {
			t.Fatal("short write damaged frame")
		}
		// A one-byte reader fragments every header and payload read.
		r := io.MultiReader(bytes.NewReader(writer.Bytes()), strings.NewReader("NEXT"))
		got, actual, deny, e := readKeyRequest(&oneByteReader{r})
		if e != nil || deny || got != id || !bytes.Equal(actual, key) {
			t.Fatal(got, deny, e)
		}
		rest, _ := io.ReadAll(r)
		if string(rest) != "NEXT" {
			t.Fatal("consumed next protocol", rest)
		}
	}
	for _, kind := range []string{"magic", "version", "type", "zero-id", "zero-key", "big-id", "big-key", "utf8", "control", "CR", "truncated-header", "truncated-payload"} {
		t.Run(kind, func(t *testing.T) {
			b := keyFrame("alpha", []byte("key"))
			deny := true
			switch kind {
			case "magic":
				b[0] = 'X'
				deny = false
			case "version":
				b[3] = '2'
			case "type":
				b[4] = 2
			case "zero-id":
				binary.BigEndian.PutUint16(b[5:7], 0)
			case "zero-key":
				binary.BigEndian.PutUint16(b[7:9], 0)
			case "big-id":
				binary.BigEndian.PutUint16(b[5:7], 257)
				deny = false
			case "big-key":
				binary.BigEndian.PutUint16(b[7:9], 4097)
				deny = false
			case "utf8":
				b[9] = 255
			case "control":
				b[9] = '\n'
			case "CR":
				b[len(b)-1] = '\r'
			case "truncated-header":
				b = b[:8]
				deny = false
			case "truncated-payload":
				b = b[:len(b)-1]
				deny = false
			}
			reader := &countedReader{Reader: bytes.NewReader(b)}
			_, _, got, e := readKeyRequest(reader)
			if e == nil || got != deny {
				t.Fatal(got, e)
			}
			if (kind == "big-id" || kind == "big-key") && reader.consumed != 9 {
				t.Fatal("read oversized payload")
			}
		})
	}
	for _, id := range []string{"", "bad\n", strings.Repeat("a", 257)} {
		if writeKeyRequest(io.Discard, id, []byte("key")) == nil {
			t.Fatal("invalid identity written")
		}
	}
	if writeKeyRequest(io.Discard, "alpha", nil) == nil {
		t.Fatal("empty key written")
	}
}

type oneByteReader struct{ io.Reader }

func (r *oneByteReader) Read(b []byte) (int, error) {
	if len(b) > 1 {
		b = b[:1]
	}
	return r.Reader.Read(b)
}
