//go:build !linux

package plugin

import "os"

func platformSupported() bool { return false }
func parentFile() *os.File    { return nil }
func killSelf()               {}
