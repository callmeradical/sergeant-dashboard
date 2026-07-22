package dashboard

import (
	"encoding/json"
	"fmt"
	"io"
)

func ValidateServeStatus(input io.Reader, host, path, backend string) error {
	decoder := json.NewDecoder(input)
	var status struct {
		Web map[string]struct {
			Handlers map[string]struct {
				Proxy string `json:"Proxy"`
			} `json:"Handlers"`
		} `json:"Web"`
	}
	if err := decoder.Decode(&status); err != nil {
		return fmt.Errorf("decode tailscale Serve status: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("decode tailscale Serve status: trailing input")
	}
	server, ok := status.Web[host]
	if ok && server.Handlers[path].Proxy == backend {
		return nil
	}
	return fmt.Errorf("tailscale Serve host %s path %s does not proxy to %s", host, path, backend)
}
