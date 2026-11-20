package provider

import "testing"

func TestLogSourcePrefixes(t *testing.T) {
	for src, want := range map[LogSource]string{LogSourceLaunch: "[launch]", LogSourceStdout: "[out]", LogSourceStderr: "[err]", LogSourceCombined: "[out+err]", "": "", "other": ""} {
		if got := src.Prefix(); got != want {
			t.Errorf("%q: %q != %q", src, got, want)
		}
	}
}
