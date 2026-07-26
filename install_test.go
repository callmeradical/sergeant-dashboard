package dashboard_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestInstallAndUninstallManageUserService(t *testing.T) {
	requireLinux(t)
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
	requireLinux(t)
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
	requireLinux(t)
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
	requireLinux(t)
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

func TestDocumentationCoversTailnetProxyValidationAndRollback(t *testing.T) {
	readme, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"http://cleanthes:8992/",
		"sergeant-dashboard-tailnet-proxy.service",
		"./scripts/validate.sh",
		"## Rollback",
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

func TestValidationRequiresDashboardAndTailnetProxyServicesAndExactRoute(t *testing.T) {
	home := t.TempDir()
	mocks := filepath.Join(t.TempDir(), "bin")
	mustMkdir(t, mocks)
	logPath := filepath.Join(t.TempDir(), "validation.log")
	writeExecutable(t, filepath.Join(mocks, "curl"), "#!/bin/sh\nprintf 'curl %s\\n' \"$*\" >> \"$VALIDATION_LOG\"\nprintf '%s\\n' \"${CURL_BODY:-<title>Sergeant | Fleet command</title>}\"\n")
	writeExecutable(t, filepath.Join(mocks, "systemctl"), "#!/bin/sh\nprintf 'systemctl %s\\n' \"$*\" >> \"$VALIDATION_LOG\"\n")
	writeExecutable(t, filepath.Join(mocks, "tailscale"), "#!/bin/sh\nprintf '100.122.151.88\\n'\n")

	command := exec.Command("sh", "scripts/validate.sh")
	command.Env = append(os.Environ(), "HOME="+home, "PATH="+mocks+":/usr/bin:/bin", "VALIDATION_LOG="+logPath)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("validation rejected proxy contract: %v: %s", err, output)
	}
	logData, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"systemctl --user is-active --quiet sergeant-dashboard.service",
		"systemctl --user is-active --quiet sergeant-dashboard-tailnet-proxy.service",
		"curl --fail --silent --show-error http://127.0.0.1:8992/healthz",
		"curl --fail --silent --show-error http://127.0.0.1:8992/sergeant/",
		"curl --fail --silent --show-error --location --resolve cleanthes:8992:100.122.151.88 http://cleanthes:8992/",
	} {
		if !strings.Contains(string(logData), required) {
			t.Errorf("validation log lacks %q: %s", required, logData)
		}
	}
	for _, forbidden := range []string{"tailscale serve", "https://cleanthes"} {
		if strings.Contains(string(logData), forbidden) {
			t.Errorf("validation retained obsolete route %q: %s", forbidden, logData)
		}
	}

	command = exec.Command("sh", "scripts/validate.sh")
	command.Env = append(os.Environ(), "HOME="+home, "PATH="+mocks+":/usr/bin:/bin", "VALIDATION_LOG="+logPath, "CURL_BODY=Fleet command but unrelated")
	if output, err := command.CombinedOutput(); err == nil {
		t.Fatalf("validation accepted unrelated tailnet content: %s", output)
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

func requireLinux(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("systemd lifecycle scripts support Linux only")
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
