package truenas

import (
	"fmt"
	"strings"
)

// NFSShare represents an NFS share from the TrueNAS API.
type NFSShare struct {
	ID           int      `json:"id"`
	Path         string   `json:"path"`
	Paths        []string `json:"paths"`
	Comment      string   `json:"comment"`
	Networks     []string `json:"networks"`
	Hosts        []string `json:"hosts"`
	Ro           bool     `json:"ro"`
	MaprootUser  string   `json:"maproot_user"`
	MaprootGroup string   `json:"maproot_group"`
	MapallUser   string   `json:"mapall_user"`
	MapallGroup  string   `json:"mapall_group"`
	Security     []string `json:"security"`
	Enabled      bool     `json:"enabled"`
}

// NFSShareCreateParams holds parameters for creating an NFS share.
type NFSShareCreateParams struct {
	Path         string   `json:"path"`
	Comment      string   `json:"comment,omitempty"`
	Networks     []string `json:"networks,omitempty"`
	Hosts        []string `json:"hosts,omitempty"`
	Ro           bool     `json:"ro,omitempty"`
	MaprootUser  string   `json:"maproot_user,omitempty"`
	MaprootGroup string   `json:"maproot_group,omitempty"`
	MapallUser   string   `json:"mapall_user,omitempty"`
	MapallGroup  string   `json:"mapall_group,omitempty"`
	Security     []string `json:"security,omitempty"`
	Enabled      bool     `json:"enabled"`
}

// NFSShareCreate creates a new NFS share.
func (c *Client) NFSShareCreate(params *NFSShareCreateParams) (*NFSShare, error) {
	// Set default enabled to true
	params.Enabled = true

	result, err := c.Call("sharing.nfs.create", params)
	if err != nil {
		// Handle "already exports" error by finding existing share
		if strings.Contains(err.Error(), "already exports") ||
			strings.Contains(err.Error(), "already shared") {
			existing, findErr := c.NFSShareFindByPath(params.Path)
			if findErr == nil && existing != nil {
				return existing, nil
			}
		}
		return nil, fmt.Errorf("failed to create NFS share: %w", err)
	}

	return parseNFSShare(result)
}

// NFSShareDelete deletes an NFS share by ID.
func (c *Client) NFSShareDelete(id int) error {
	_, err := c.Call("sharing.nfs.delete", id)
	if err != nil {
		// Ignore "does not exist" errors
		if strings.Contains(err.Error(), "does not exist") ||
			strings.Contains(err.Error(), "not found") {
			return nil
		}
		return fmt.Errorf("failed to delete NFS share: %w", err)
	}

	return nil
}

// NFSShareGet retrieves an NFS share by ID.
func (c *Client) NFSShareGet(id int) (*NFSShare, error) {
	filters := [][]interface{}{{"id", "=", id}}

	result, err := c.Call("sharing.nfs.query", filters, map[string]interface{}{})
	if err != nil {
		return nil, fmt.Errorf("failed to get NFS share: %w", err)
	}

	shares, ok := result.([]interface{})
	if !ok || len(shares) == 0 {
		return nil, fmt.Errorf("NFS share not found: %d", id)
	}

	return parseNFSShare(shares[0])
}

// NFSShareFindByPath finds an NFS share by path.
func (c *Client) NFSShareFindByPath(path string) (*NFSShare, error) {
	result, err := c.Call("sharing.nfs.query", []interface{}{}, map[string]interface{}{})
	if err != nil {
		return nil, fmt.Errorf("failed to query NFS shares: %w", err)
	}

	shares, ok := result.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected response format")
	}

	for _, item := range shares {
		share, err := parseNFSShare(item)
		if err != nil {
			continue
		}

		// Check primary path
		if share.Path == path {
			return share, nil
		}

		// Check paths array (for multi-path shares)
		for _, p := range share.Paths {
			if p == path {
				return share, nil
			}
		}
	}

	return nil, nil // Not found, not an error
}

// NFSShareList lists all NFS shares.
func (c *Client) NFSShareList() ([]*NFSShare, error) {
	result, err := c.Call("sharing.nfs.query", []interface{}{}, map[string]interface{}{})
	if err != nil {
		return nil, fmt.Errorf("failed to list NFS shares: %w", err)
	}

	items, ok := result.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected response format")
	}

	shares := make([]*NFSShare, 0, len(items))
	for _, item := range items {
		share, err := parseNFSShare(item)
		if err != nil {
			continue
		}
		shares = append(shares, share)
	}

	return shares, nil
}

// NFSShareUpdate updates an NFS share.
func (c *Client) NFSShareUpdate(id int, params map[string]interface{}) (*NFSShare, error) {
	result, err := c.Call("sharing.nfs.update", id, params)
	if err != nil {
		return nil, fmt.Errorf("failed to update NFS share: %w", err)
	}

	return parseNFSShare(result)
}

// parseNFSShare converts a raw API response to an NFSShare struct.
func parseNFSShare(data interface{}) (*NFSShare, error) {
	m, ok := data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected NFS share format")
	}

	share := &NFSShare{}

	if v, ok := m["id"].(float64); ok {
		share.ID = int(v)
	}
	if v, ok := m["path"].(string); ok {
		share.Path = v
	}
	if v, ok := m["paths"].([]interface{}); ok {
		for _, p := range v {
			if s, ok := p.(string); ok {
				share.Paths = append(share.Paths, s)
			}
		}
	}
	if v, ok := m["comment"].(string); ok {
		share.Comment = v
	}
	if v, ok := m["networks"].([]interface{}); ok {
		for _, n := range v {
			if s, ok := n.(string); ok {
				share.Networks = append(share.Networks, s)
			}
		}
	}
	if v, ok := m["hosts"].([]interface{}); ok {
		for _, h := range v {
			if s, ok := h.(string); ok {
				share.Hosts = append(share.Hosts, s)
			}
		}
	}
	if v, ok := m["ro"].(bool); ok {
		share.Ro = v
	}
	if v, ok := m["maproot_user"].(string); ok {
		share.MaprootUser = v
	}
	if v, ok := m["maproot_group"].(string); ok {
		share.MaprootGroup = v
	}
	if v, ok := m["mapall_user"].(string); ok {
		share.MapallUser = v
	}
	if v, ok := m["mapall_group"].(string); ok {
		share.MapallGroup = v
	}
	if v, ok := m["security"].([]interface{}); ok {
		for _, s := range v {
			if str, ok := s.(string); ok {
				share.Security = append(share.Security, str)
			}
		}
	}
	if v, ok := m["enabled"].(bool); ok {
		share.Enabled = v
	}

	return share, nil
}
