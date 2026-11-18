// Command host needs only SDK options; transport and supervision stay in the SDK.
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/colony-2/pulse-provider-api/plugin"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "provider hosting failed")
		os.Exit(1)
	}
}
func run() error {
	if len(os.Args) != 2 {
		return plugin.ErrInvalid
	}
	executable, err := filepath.Abs(os.Args[1])
	if err != nil {
		return err
	}
	base, err := os.Getwd()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	process, err := plugin.Start(ctx, plugin.StartOptions{Executable: executable, Init: plugin.Initialization{Name: "example", BaseDir: base, ConfigYAML: []byte("base: local\n")}})
	if err != nil {
		return err
	}
	defer process.Close()
	capabilities, err := process.GetCapabilities(context.Background())
	if err != nil {
		return err
	}
	fmt.Printf("%s protocol=%d bases=%d\n", process.Health().State, process.Health().Protocol, len(capabilities.Bases))
	return process.Close()
}
