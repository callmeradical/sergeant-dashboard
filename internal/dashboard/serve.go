package dashboard

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
)

func ValidateServeURL(input io.Reader, rawURL, backend string) error {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Scheme != "https" || parsed.Hostname() == "" || parsed.User != nil || parsed.Opaque != "" {
		return fmt.Errorf("tailnet URL must be an HTTPS URL without userinfo")
	}
	if parsed.Port() != "" && parsed.Port() != "443" {
		return fmt.Errorf("tailnet URL must use HTTPS port 443")
	}
	if parsed.Path != "/sergeant/" || parsed.RawPath != "" || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" {
		return fmt.Errorf("tailnet URL must use exactly /sergeant/ without query or fragment")
	}
	host := net.JoinHostPort(strings.ToLower(parsed.Hostname()), "443")
	return ValidateServeStatus(input, host, strings.TrimSuffix(parsed.Path, "/"), backend)
}

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
