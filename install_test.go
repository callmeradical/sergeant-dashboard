package dashboard_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestInstallAndUninstallManageUserService(t *testing.T) {
	home := t.TempDir()
	mocks := filepath.Join(t.TempDir(), "bin")
	mustMkdir(t, mocks)
	logPath := filepath.Join(t.TempDir(), "systemctl.log")
	writeExecutable(t, filepath.Join(mocks, "systemctl"), `#!/bin/sh
printf '%s\n' "$*" >> "$SYSTEMCTL_LOG"
if [ "$*" = "--user is-active --quiet sergeant-dashboard.service" ]; then
  exit 3
fi
`)
	writeExecutable(t, filepath.Join(mocks, "go"), `#!/bin/sh
while [ "$#" -gt 0 ]; do
  if [ "$1" = "-o" ]; then
    shift
    mkdir -p "$(dirname "$1")"
    printf '#!/bin/sh\n' > "$1"
    chmod +x "$1"
    exit 0
  fi
  shift
done
exit 1
`)
	env := append(os.Environ(),
		"HOME="+home,
		"XDG_CONFIG_HOME="+filepath.Join(home, "config"),
		"PATH="+mocks+":/usr/bin:/bin",
		"SYSTEMCTL_LOG="+logPath,
	)

	runScript(t, env, "scripts/install.sh")
	binary := filepath.Join(home, ".local", "bin", "sergeant-dashboard")
	unit := filepath.Join(home, "config", "systemd", "user", "sergeant-dashboard.service")
	for _, path := range []string{binary, unit} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("installed artifact %s: %v", path, err)
		}
	}
	unitData, err := os.ReadFile(unit)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"ExecStart=%h/.local/bin/sergeant-dashboard", "127.0.0.1:8992", "Restart=on-failure", "NoNewPrivileges=true", "ProtectSystem=strict", "ProtectHome=read-only"} {
		if !strings.Contains(string(unitData), required) {
			t.Errorf("installed unit lacks %q: %s", required, unitData)
		}
	}

	runScript(t, env, "scripts/uninstall.sh")
	for _, path := range []string{binary, unit} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("uninstalled artifact still exists %s: %v", path, err)
		}
	}
	logData, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	wantLog := "--user daemon-reload\n--user enable --now sergeant-dashboard.service\n--user disable --now sergeant-dashboard.service\n--user is-active --quiet sergeant-dashboard.service\n--user daemon-reload\n"
	if string(logData) != wantLog {
		t.Fatalf("systemctl calls = %q, want %q", logData, wantLog)
	}
}

func TestUninstallPreservesArtifactsWhenServiceCannotStop(t *testing.T) {
	home := t.TempDir()
	mocks := filepath.Join(t.TempDir(), "bin")
	mustMkdir(t, mocks)
	writeExecutable(t, filepath.Join(mocks, "systemctl"), `#!/bin/sh
case "$*" in
  "--user disable --now sergeant-dashboard.service") exit 1 ;;
  "--user is-active --quiet sergeant-dashboard.service") exit 0 ;;
esac
exit 0
`)

	binary := filepath.Join(home, ".local", "bin", "sergeant-dashboard")
	unit := filepath.Join(home, "config", "systemd", "user", "sergeant-dashboard.service")
	mustMkdir(t, filepath.Dir(binary))
	mustMkdir(t, filepath.Dir(unit))
	writeExecutable(t, binary, "#!/bin/sh\n")
	if err := os.WriteFile(unit, []byte("[Service]\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	command := exec.Command("sh", "scripts/uninstall.sh")
	command.Env = append(os.Environ(),
		"HOME="+home,
		"XDG_CONFIG_HOME="+filepath.Join(home, "config"),
		"PATH="+mocks+":/usr/bin:/bin",
	)
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("uninstall succeeded while service remained active: %s", output)
	}
	if strings.Contains(string(output), "Uninstalled") {
		t.Fatalf("failed uninstall printed success: %s", output)
	}
	for _, path := range []string{binary, unit} {
		if _, statErr := os.Stat(path); statErr != nil {
			t.Errorf("failed uninstall removed recovery artifact %s: %v", path, statErr)
		}
	}
}

func TestUninstallToleratesMissingInactiveService(t *testing.T) {
	for name, status := range map[string]string{"inactive": "3", "missing": "4"} {
		t.Run(name, func(t *testing.T) {
			home := t.TempDir()
			mocks := filepath.Join(t.TempDir(), "bin")
			mustMkdir(t, mocks)
			writeExecutable(t, filepath.Join(mocks, "systemctl"), `#!/bin/sh
case "$*" in
  "--user disable --now sergeant-dashboard.service") exit 1 ;;
  "--user is-active --quiet sergeant-dashboard.service") exit `+status+` ;;
esac
exit 0
`)
			env := append(os.Environ(),
				"HOME="+home,
				"XDG_CONFIG_HOME="+filepath.Join(home, "config"),
				"PATH="+mocks+":/usr/bin:/bin",
			)

			runScript(t, env, "scripts/uninstall.sh")
		})
	}
}

func TestUninstallPreservesArtifactsWhenInactivityCannotBeVerified(t *testing.T) {
	home := t.TempDir()
	mocks := filepath.Join(t.TempDir(), "bin")
	mustMkdir(t, mocks)
	writeExecutable(t, filepath.Join(mocks, "systemctl"), `#!/bin/sh
case "$*" in
  "--user disable --now sergeant-dashboard.service") exit 1 ;;
  "--user is-active --quiet sergeant-dashboard.service") exit 1 ;;
esac
exit 0
`)
	binary := filepath.Join(home, ".local", "bin", "sergeant-dashboard")
	unit := filepath.Join(home, "config", "systemd", "user", "sergeant-dashboard.service")
	mustMkdir(t, filepath.Dir(binary))
	mustMkdir(t, filepath.Dir(unit))
	writeExecutable(t, binary, "#!/bin/sh\n")
	if err := os.WriteFile(unit, []byte("[Service]\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	command := exec.Command("sh", "scripts/uninstall.sh")
	command.Env = append(os.Environ(),
		"HOME="+home,
		"XDG_CONFIG_HOME="+filepath.Join(home, "config"),
		"PATH="+mocks+":/usr/bin:/bin",
	)
	output, err := command.CombinedOutput()
	if err == nil || strings.Contains(string(output), "Uninstalled") {
		t.Fatalf("uninstall did not fail truthfully when inactivity was unknown: err=%v output=%s", err, output)
	}
	for _, path := range []string{binary, unit} {
		if _, statErr := os.Stat(path); statErr != nil {
			t.Errorf("uninstall removed recovery artifact %s: %v", path, statErr)
		}
	}
}

func TestDocumentationCoversServeValidationAndRollback(t *testing.T) {
	readme, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"tailscale serve --bg --set-path /sergeant http://127.0.0.1:8992/sergeant",
		"https://cleanthes.taila4fb6a.ts.net/sergeant/",
		"./scripts/validate.sh",
		"## Rollback",
		"tailscale serve --https=443 --set-path=/sergeant off",
		"./scripts/uninstall.sh",
		"If the service cannot be stopped, uninstall exits without removing the unit or binary",
	} {
		if !strings.Contains(string(readme), required) {
			t.Errorf("README lacks %q", required)
		}
	}
}

func TestDocumentationDefinesLinuxAndMacOSServiceManagement(t *testing.T) {
	readme, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"systemd lifecycle scripts support Linux only",
		"macOS does not include launchd integration",
		"go build -o sergeant-dashboard ./cmd/sergeant-dashboard",
		"./sergeant-dashboard",
		"Control-C",
	} {
		if !strings.Contains(string(readme), required) {
			t.Errorf("README lacks %q", required)
		}
	}
}

func TestLifecycleScriptsRejectNonLinuxHosts(t *testing.T) {
	mocks := filepath.Join(t.TempDir(), "bin")
	mustMkdir(t, mocks)
	writeExecutable(t, filepath.Join(mocks, "uname"), "#!/bin/sh\nprintf '%s\\n' Darwin\n")
	env := append(os.Environ(), "PATH="+mocks+":/usr/bin:/bin")
	for _, script := range []string{"scripts/install.sh", "scripts/uninstall.sh"} {
		command := exec.Command("sh", script)
		command.Env = env
		output, err := command.CombinedOutput()
		if err == nil || !strings.Contains(string(output), "supports Linux only") {
			t.Errorf("%s on macOS: err=%v output=%q", script, err, output)
		}
	}
}

func TestValidationRequiresServePathAndBackendAssociation(t *testing.T) {
	home := t.TempDir()
	binDir := filepath.Join(home, ".local", "bin")
	mocks := filepath.Join(t.TempDir(), "bin")
	mustMkdir(t, binDir)
	mustMkdir(t, mocks)
	writeExecutable(t, filepath.Join(mocks, "curl"), "#!/bin/sh\nexit 0\n")
	writeExecutable(t, filepath.Join(mocks, "tailscale"), "#!/bin/sh\nprintf '%s\\n' \"$TAILSCALE_STATUS\"\n")

	build := exec.Command("go", "build", "-o", filepath.Join(binDir, "sergeant-dashboard"), "./cmd/sergeant-dashboard")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build validator: %v: %s", err, output)
	}
	baseEnv := append(os.Environ(), "HOME="+home, "PATH="+mocks+":/usr/bin:/bin")
	misassociated := `{"Web":{"cleanthes.taila4fb6a.ts.net:443":{"Handlers":{"/sergeant":{"Proxy":"http://127.0.0.1:9999/sergeant"},"/other":{"Proxy":"http://127.0.0.1:8992/sergeant"}}},"other-host:443":{"Handlers":{"/sergeant":{"Proxy":"http://127.0.0.1:8992/sergeant"}}}}}`
	command := exec.Command("sh", "scripts/validate.sh")
	command.Env = append(baseEnv, "TAILSCALE_STATUS="+misassociated)
	if output, err := command.CombinedOutput(); err == nil {
		t.Fatalf("validation accepted unrelated Serve path/backend values: %s", output)
	}

	associated := `{"Web":{"cleanthes.taila4fb6a.ts.net:443":{"Handlers":{"/sergeant":{"Proxy":"http://127.0.0.1:8992/sergeant"}}}}}`
	command = exec.Command("sh", "scripts/validate.sh")
	command.Env = append(baseEnv, "TAILSCALE_STATUS="+associated)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("validation rejected associated Serve path/backend: %v: %s", err, output)
	}

	command = exec.Command("sh", "scripts/validate.sh")
	command.Env = append(baseEnv,
		"TAILSCALE_STATUS="+associated,
		"SERGEANT_TAILNET_URL=https://other-host.ts.net/sergeant/",
		"SERGEANT_TAILNET_HOST=cleanthes.taila4fb6a.ts.net:443",
	)
	if output, err := command.CombinedOutput(); err == nil {
		t.Fatalf("validation accepted independently mismatched URL and host: %s", output)
	}
}

func runScript(t *testing.T, env []string, path string) {
	t.Helper()
	command := exec.Command("sh", path)
	command.Env = env
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("%s: %v: %s", path, err, output)
	}
}

func mustMkdir(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatal(err)
	}
}

func writeExecutable(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o755); err != nil {
		t.Fatal(err)
	}
}
