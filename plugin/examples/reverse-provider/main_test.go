package main

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/pulse-provider-api/plugin"
)

func TestProviderKeyInputStopsBeforeDialing(t *testing.T) {
	file := filepath.Join(t.TempDir(), "config.yaml")
	if e := os.WriteFile(file, []byte("base: example\n"), 0600); e != nil {
		t.Fatal(e)
	}
	for _, kind := range []string{"cancel", "timeout"} {
		t.Run(kind, func(t *testing.T) {
			input, writer, e := os.Pipe()
			if e != nil {
				t.Fatal(e)
			}
			defer input.Close()
			defer writer.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			if kind == "cancel" {
				cancel()
			}
			listener, e := net.Listen("tcp", "127.0.0.1:0")
			if e != nil {
				t.Fatal(e)
			}
			accepted := make(chan bool, 1)
			go func() {
				conn, e := listener.Accept()
				if e == nil {
					conn.Close()
				}
				accepted <- e == nil
			}()
			if e = runProvider(ctx, []string{"-auth", "mutual-key", "-transport", "http", "-auth-key-stdin", "-name", "alpha", "-address", listener.Addr().String(), "-config", file}, input); e == nil || !strings.Contains(e.Error(), "key input unavailable") {
				listener.Close()
				<-accepted
				t.Fatal("acquired blocked input", e)
			}
			listener.Close()
			if <-accepted {
				t.Fatal("provider dialed before acquiring a key")
			}
			_, _ = io.WriteString(writer, "next\n")
			line, e := bufio.NewReader(input).ReadString('\n')
			if e != nil || line != "next\n" {
				t.Fatal(line, e)
			}
		})
	}
}
func TestProviderInvalidKeyFlags(t *testing.T) {
	for _, args := range [][]string{
		{"-auth", "unknown"}, {"-transport", "unknown"}, {"-transport", "http"},
		{"-auth", "mutual-key", "-transport", "http"},
		{"-auth", "mutual-key", "-transport", "http", "-auth-key-stdin", "-auth-key-env", "KEY"},
		{"-auth-key-env", "KEY"},
	} {
		_, e := providerOptions(context.Background(), args, nil)
		if !errors.Is(e, plugin.ErrInvalid) {
			t.Fatal(args, e)
		}
	}
}
