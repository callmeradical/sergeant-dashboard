package dashboard_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/callmeradical/sergeant-dashboard/internal/dashboard"
)

func TestConfigUsesLoopbackAndPortableDataHome(t *testing.T) {
	values := map[string]string{
		"HOME":                 "/home/operator",
		"XDG_DATA_HOME":        "/var/operator-data",
		"SERGEANT_CONFIG":      "/etc/sergeant",
		"SERGEANT_STALE_AFTER": "45m",
	}
	config := dashboard.ConfigFromEnv(func(key string) string { return values[key] })
	if config.Address != "127.0.0.1:8992" {
		t.Fatalf("address = %q", config.Address)
	}
	if config.FleetRoot != filepath.Join("/var/operator-data", "sergeant", "fleet") {
		t.Fatalf("fleet root = %q", config.FleetRoot)
	}
	if config.ConfigRoot != "/etc/sergeant" {
		t.Fatalf("config root = %q", config.ConfigRoot)
	}
	if config.StaleAfter != 45*time.Minute {
		t.Fatalf("stale threshold = %s", config.StaleAfter)
	}
}

// Regression tests for td-9782c5: Config must expose Limit so the production
// binary can pass it to Collector and bound the fleet scan.

func TestConfigReadsWorkerLimitFromEnv(t *testing.T) {
	config := dashboard.ConfigFromEnv(func(key string) string {
		if key == "HOME" {
			return "/home/operator"
		}
		if key == "SERGEANT_WORKER_LIMIT" {
			return "500"
		}
		return ""
	})
	if config.Limit != 500 {
		t.Fatalf("worker limit = %d, want 500", config.Limit)
	}
}

func TestConfigDefaultsWorkerLimitToZero(t *testing.T) {
	config := dashboard.ConfigFromEnv(func(key string) string {
		if key == "HOME" {
			return "/home/operator"
		}
		return ""
	})
	if config.Limit != 0 {
		t.Fatalf("default worker limit = %d, want 0 (unlimited)", config.Limit)
	}
}

func TestConfigFallsBackForInvalidOptionalValues(t *testing.T) {
	config := dashboard.ConfigFromEnv(func(key string) string {
		if key == "HOME" {
			return "/Users/operator"
		}
		if key == "SERGEANT_STALE_AFTER" {
			return "not-a-duration"
		}
		return ""
	})
	if config.FleetRoot != filepath.Join("/Users/operator", ".local", "share", "sergeant", "fleet") || config.ConfigRoot != filepath.Join("/Users/operator", ".config", "sergeant") || config.StaleAfter != 30*time.Minute {
		t.Fatalf("fallback config = %#v", config)
	}
}
