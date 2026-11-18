//go:build linux

package plugin

import (
	"os"
	"syscall"
)

func platformSupported() bool { return true }
func parentFile() *os.File    { syscall.CloseOnExec(3); return os.NewFile(3, "plugin-parent") }
func killSelf()               { _ = syscall.Kill(os.Getpid(), syscall.SIGKILL) }
