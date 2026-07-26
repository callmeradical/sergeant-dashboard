package dashboard

import (
	"path/filepath"
	"time"
)

const ListenAddress = "127.0.0.1:8992"

type Config struct {
	Address    string
	FleetRoot  string
	ConfigRoot string
	StaleAfter time.Duration
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
	return Config{Address: ListenAddress, FleetRoot: fleetRoot, ConfigRoot: configRoot, StaleAfter: staleAfter}
}
