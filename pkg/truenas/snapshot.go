package truenas

import (
	"fmt"
	"strings"
)

// Snapshot represents a ZFS snapshot from the TrueNAS API.
type Snapshot struct {
	ID             string                  `json:"id"`
	Name           string                  `json:"name"`
	Dataset        string                  `json:"dataset"`
	Pool           string                  `json:"pool"`
	Type           string                  `json:"type"`
	Properties     map[string]interface{}  `json:"properties"`
	UserProperties map[string]UserProperty `json:"user_properties"`
}

// SnapshotCreateParams holds parameters for creating a snapshot.
type SnapshotCreateParams struct {
	Dataset   string `json:"dataset"`
	Name      string `json:"name"`
	Recursive bool   `json:"recursive,omitempty"`
}

// SnapshotCreate creates a new ZFS snapshot.
func (c *Client) SnapshotCreate(dataset string, name string) (*Snapshot, error) {
	params := &SnapshotCreateParams{
		Dataset: dataset,
		Name:    name,
	}

	result, err := c.Call("zfs.snapshot.create", params)
	if err != nil {
		// Ignore "already exists" errors
		if strings.Contains(err.Error(), "already exists") {
			return c.SnapshotGet(dataset + "@" + name)
		}
		return nil, fmt.Errorf("failed to create snapshot: %w", err)
	}

	return parseSnapshot(result)
}

// SnapshotDelete deletes a ZFS snapshot.
func (c *Client) SnapshotDelete(snapshotID string, defer_ bool, recursive bool) error {
	options := map[string]interface{}{
		"defer":     defer_,
		"recursive": recursive,
	}

	_, err := c.Call("zfs.snapshot.delete", snapshotID, options)
	if err != nil {
		// Ignore "does not exist" errors
		if strings.Contains(err.Error(), "does not exist") ||
			strings.Contains(err.Error(), "not found") {
			return nil
		}
		return fmt.Errorf("failed to delete snapshot: %w", err)
	}

	return nil
}

// SnapshotGet retrieves a snapshot by ID (dataset@snapshot format).
func (c *Client) SnapshotGet(snapshotID string) (*Snapshot, error) {
	filters := [][]interface{}{{"id", "=", snapshotID}}

	result, err := c.Call("zfs.snapshot.query", filters, map[string]interface{}{})
	if err != nil {
		return nil, fmt.Errorf("failed to get snapshot: %w", err)
	}

	snapshots, ok := result.([]interface{})
	if !ok || len(snapshots) == 0 {
		return nil, fmt.Errorf("snapshot not found: %s", snapshotID)
	}

	return parseSnapshot(snapshots[0])
}

// SnapshotList lists snapshots for a dataset.
func (c *Client) SnapshotList(dataset string) ([]*Snapshot, error) {
	filters := [][]interface{}{{"dataset", "=", dataset}}

	result, err := c.Call("zfs.snapshot.query", filters, map[string]interface{}{})
	if err != nil {
		return nil, fmt.Errorf("failed to list snapshots: %w", err)
	}

	items, ok := result.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected response type")
	}

	snapshots := make([]*Snapshot, 0, len(items))
	for _, item := range items {
		snap, err := parseSnapshot(item)
		if err != nil {
			continue
		}
		snapshots = append(snapshots, snap)
	}

	return snapshots, nil
}

// SnapshotListAll lists all snapshots under a parent dataset (recursive).
func (c *Client) SnapshotListAll(parentDataset string) ([]*Snapshot, error) {
	filters := [][]interface{}{{"dataset", "^", parentDataset}}

	result, err := c.Call("zfs.snapshot.query", filters, map[string]interface{}{})
	if err != nil {
		return nil, fmt.Errorf("failed to list snapshots: %w", err)
	}

	items, ok := result.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected response type")
	}

	snapshots := make([]*Snapshot, 0, len(items))
	for _, item := range items {
		snap, err := parseSnapshot(item)
		if err != nil {
			continue
		}
		snapshots = append(snapshots, snap)
	}

	return snapshots, nil
}

// SnapshotSetUserProperty sets a user property on a snapshot.
func (c *Client) SnapshotSetUserProperty(snapshotID string, key string, value string) error {
	params := map[string]interface{}{
		"user_properties_update": []map[string]interface{}{
			{"key": key, "value": value},
		},
	}

	_, err := c.Call("zfs.snapshot.update", snapshotID, params)
	return err
}

// SnapshotClone clones a snapshot to create a new dataset.
func (c *Client) SnapshotClone(snapshotID string, newDatasetName string) error {
	params := map[string]interface{}{
		"snapshot":    snapshotID,
		"dataset_dst": newDatasetName,
	}

	_, err := c.Call("zfs.snapshot.clone", params)
	if err != nil {
		// Ignore "already exists" errors
		if strings.Contains(err.Error(), "already exists") {
			return nil
		}
		return fmt.Errorf("failed to clone snapshot: %w", err)
	}

	return nil
}

// SnapshotRollback rolls back a dataset to a snapshot.
func (c *Client) SnapshotRollback(snapshotID string, force bool, recursive bool, recursiveClones bool) error {
	options := map[string]interface{}{
		"force":            force,
		"recursive":        recursive,
		"recursive_clones": recursiveClones,
	}

	_, err := c.Call("zfs.snapshot.rollback", snapshotID, options)
	if err != nil {
		return fmt.Errorf("failed to rollback snapshot: %w", err)
	}

	return nil
}

// parseSnapshot converts a raw API response to a Snapshot struct.
func parseSnapshot(data interface{}) (*Snapshot, error) {
	m, ok := data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected snapshot format")
	}

	snap := &Snapshot{
		Properties:     make(map[string]interface{}),
		UserProperties: make(map[string]UserProperty),
	}

	if v, ok := m["id"].(string); ok {
		snap.ID = v
	}
	if v, ok := m["name"].(string); ok {
		snap.Name = v
	}
	if v, ok := m["dataset"].(string); ok {
		snap.Dataset = v
	}
	if v, ok := m["pool"].(string); ok {
		snap.Pool = v
	}
	if v, ok := m["type"].(string); ok {
		snap.Type = v
	}

	// Parse properties
	if props, ok := m["properties"].(map[string]interface{}); ok {
		snap.Properties = props
	}

	// Parse user properties
	if userProps, ok := m["user_properties"].(map[string]interface{}); ok {
		for key, val := range userProps {
			if propMap, ok := val.(map[string]interface{}); ok {
				prop := UserProperty{}
				if v, ok := propMap["value"].(string); ok {
					prop.Value = v
				}
				if v, ok := propMap["source"].(string); ok {
					prop.Source = v
				}
				snap.UserProperties[key] = prop
			}
		}
	}

	return snap, nil
}

// GetSnapshotSize returns the size of a snapshot in bytes.
func (snap *Snapshot) GetSnapshotSize() int64 {
	if used, ok := snap.Properties["used"]; ok {
		if usedMap, ok := used.(map[string]interface{}); ok {
			if parsed, ok := usedMap["parsed"].(float64); ok {
				return int64(parsed)
			}
		}
	}
	return 0
}

// GetCreationTime returns the creation timestamp of a snapshot.
func (snap *Snapshot) GetCreationTime() int64 {
	if creation, ok := snap.Properties["creation"]; ok {
		if creationMap, ok := creation.(map[string]interface{}); ok {
			if parsed, ok := creationMap["parsed"].(float64); ok {
				return int64(parsed)
			}
		}
	}
	return 0
}
