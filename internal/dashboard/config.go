package dashboard

import (
	"path/filepath"
	"strconv"
	"time"
)

const ListenAddress = "127.0.0.1:8992"

type Config struct {
	Address    string
	FleetRoot  string
	ConfigRoot string
	StaleAfter time.Duration
	// Limit caps the number of workers the Collector materialises during the
	// fleet scan. Zero means unlimited. Set SERGEANT_WORKER_LIMIT to configure.
	Limit int
}

func ConfigFromEnv(getenv func(string) string) Config {
	dataHome := getenv("XDG_DATA_HOME")
	if dataHome == "" {
		dataHome = filepath.Join(getenv("HOME"), ".local", "share")
	}
	fleetRoot := getenv("SERGEANT_FLEET_ROOT")
	if fleetRoot == "" {
		fleetRoot = filepath.Join(dataHome, "sergeant", "fleet")
	}
	configRoot := getenv("SERGEANT_CONFIG")
	if configRoot == "" {
		configRoot = filepath.Join(getenv("HOME"), ".config", "sergeant")
	}
	staleAfter, err := time.ParseDuration(getenv("SERGEANT_STALE_AFTER"))
	if err != nil || staleAfter <= 0 {
		staleAfter = 30 * time.Minute
	}
	limit, _ := strconv.Atoi(getenv("SERGEANT_WORKER_LIMIT"))
	if limit < 0 {
		limit = 0
	}
	return Config{Address: ListenAddress, FleetRoot: fleetRoot, ConfigRoot: configRoot, StaleAfter: staleAfter, Limit: limit}
}
