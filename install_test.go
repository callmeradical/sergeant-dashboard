package dashboard_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestServiceAndLifecycleScriptsAreSafeAndComplete(t *testing.T) {
	unit := readProjectFile(t, "deploy/sergeant-dashboard.service")
	for _, required := range []string{"ExecStart=%h/.local/bin/sergeant-dashboard", "127.0.0.1:8992", "Restart=on-failure", "NoNewPrivileges=true"} {
		if !strings.Contains(unit, required) {
			t.Errorf("service unit lacks %q", required)
		}
	}
	for _, script := range []string{"scripts/install.sh", "scripts/uninstall.sh", "scripts/validate.sh"} {
		command := exec.Command("sh", "-n", script)
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("%s syntax: %v: %s", script, err, output)
		}
	}
	install := readProjectFile(t, "scripts/install.sh")
	uninstall := readProjectFile(t, "scripts/uninstall.sh")
	if !strings.Contains(install, "systemctl --user enable --now") || !strings.Contains(uninstall, "systemctl --user disable --now") {
		t.Fatal("lifecycle scripts do not enable/disable the user service")
	}
}

func TestDocumentationPinsTailscaleSubpathAndHealthValidation(t *testing.T) {
	readme := readProjectFile(t, "README.md")
	for _, required := range []string{
		"127.0.0.1:8992",
		"tailscale serve --bg --set-path /sergeant http://127.0.0.1:8992/sergeant",
		"https://cleanthes.taila4fb6a.ts.net/sergeant/",
		"/healthz",
	} {
		if !strings.Contains(readme, required) {
			t.Errorf("README lacks %q", required)
		}
	}
	for _, path := range []string{"deploy/sergeant-dashboard.service", "scripts/install.sh", "scripts/validate.sh"} {
		contents := readProjectFile(t, path)
		if !strings.Contains(contents, "127.0.0.1:8992") {
			t.Errorf("%s lacks the fixed listener", path)
		}
		if strings.Contains(contents, "127.0.0.1:8991") {
			t.Errorf("%s retains the conflicting listener", path)
		}
	}
	validate := readProjectFile(t, "scripts/validate.sh")
	for _, required := range []string{"tailscale serve status --json", "127.0.0.1:8992/sergeant", "'/sergeant'"} {
		if !strings.Contains(validate, required) {
			t.Errorf("validation script lacks Serve assertion %q", required)
		}
	}
}

func readProjectFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
