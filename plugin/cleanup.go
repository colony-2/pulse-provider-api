package plugin

import (
	"os"
	"path/filepath"
	"strings"
)

// Remove only the SDK socket directory, then its empty containing directory.
// Do not recursively remove unrelated provider/native execution state.
func removeTransport(socketDir, root string) error {
	if socketDir != "" && filepath.Dir(socketDir) == root && strings.HasPrefix(filepath.Base(socketDir), "plugin-dir") && strings.HasPrefix(filepath.Base(root), "pulse-plugin-") {
		if e := os.RemoveAll(socketDir); e != nil {
			return e
		}
	}
	if strings.HasPrefix(filepath.Base(root), "pulse-plugin-") {
		if e := os.Remove(root); e != nil && !os.IsNotExist(e) {
			return e
		}
	}
	return nil
}
