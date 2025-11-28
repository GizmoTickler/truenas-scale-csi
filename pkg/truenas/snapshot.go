package truenas

import (
	"context"
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
func (c *Client) SnapshotCreate(ctx context.Context, dataset string, name string) (*Snapshot, error) {
	params := &SnapshotCreateParams{
		Dataset: dataset,
		Name:    name,
	}

	result, err := c.Call(ctx, "zfs.snapshot.create", params)
	if err != nil {
		// Handle "already exists" errors for idempotency
		if IsAlreadyExistsError(err) {
			return c.SnapshotGet(ctx, dataset+"@"+name)
		}
		// TrueNAS may return "Invalid params" when snapshot already exists
		if IsInvalidParamsError(err) {
			existing, getErr := c.SnapshotGet(ctx, dataset+"@"+name)
			if getErr == nil && existing != nil {
				return existing, nil
			}
		}
		return nil, fmt.Errorf("failed to create snapshot: %w", err)
	}

	return parseSnapshot(result)
}

// SnapshotDelete deletes a ZFS snapshot.
// If the snapshot has orphaned clones (from failed volume deletions), it will attempt
// to delete those clones first before deleting the snapshot.
func (c *Client) SnapshotDelete(ctx context.Context, snapshotID string, defer_ bool, recursive bool) error {
	options := map[string]interface{}{
		"defer":     defer_,
		"recursive": recursive,
	}

	_, err := c.Call(ctx, "zfs.snapshot.delete", snapshotID, options)
	if err != nil {
		// Handle "not found" errors as success for idempotency
		if IsNotFoundError(err) {
			return nil
		}

		// Check if this is a "has clones" error
		if IsHasClonesError(err) {
			return c.handleSnapshotDeleteWithClones(ctx, snapshotID, options, err)
		}

		// TrueNAS returns "Invalid params" for multiple conditions:
		// 1. Snapshot doesn't exist
		// 2. Snapshot has clones and can't be deleted
		// 3. Actual validation errors
		// Check if snapshot exists to distinguish between these cases
		if IsInvalidParamsError(err) {
			// Try to get the snapshot - if it doesn't exist, treat as success
			snap, getErr := c.SnapshotGet(ctx, snapshotID)
			if getErr != nil {
				// Snapshot doesn't exist, treat delete as successful
				return nil
			}

			// Snapshot exists - check if it has clones we can clean up
			clones := snap.GetClones()
			if len(clones) > 0 {
				return c.handleSnapshotDeleteWithClones(ctx, snapshotID, options, err)
			}

			// Snapshot exists but can't be deleted for unknown reason
			return fmt.Errorf("failed to delete snapshot (may have clones): %w", err)
		}
		return fmt.Errorf("failed to delete snapshot: %w", err)
	}

	return nil
}

// handleSnapshotDeleteWithClones attempts to delete orphaned clones and retry snapshot deletion.
func (c *Client) handleSnapshotDeleteWithClones(ctx context.Context, snapshotID string, options map[string]interface{}, originalErr error) error {
	// Get the snapshot to find its clones
	snap, getErr := c.SnapshotGet(ctx, snapshotID)
	if getErr != nil {
		// Snapshot doesn't exist anymore, treat as success
		return nil
	}

	clones := snap.GetClones()
	if len(clones) == 0 {
		// No clones found but error indicated clones - return original error
		return fmt.Errorf("failed to delete snapshot (may have clones): %w", originalErr)
	}

	// Attempt to delete orphaned clones
	for _, cloneDataset := range clones {
		// Try to delete the clone dataset - these are likely orphaned
		// from failed volume deletions during TrueNAS overload
		if delErr := c.DatasetDelete(ctx, cloneDataset, false, true); delErr != nil {
			// Log but continue - clone might still be in use
			continue
		}
	}

	// Retry snapshot deletion after cleaning up clones
	_, retryErr := c.Call(ctx, "zfs.snapshot.delete", snapshotID, options)
	if retryErr == nil {
		return nil
	}

	// Check if retry error is "not found" (snapshot was deleted by another process)
	if IsNotFoundError(retryErr) {
		return nil
	}

	// Still failed - return the original error with context
	return fmt.Errorf("failed to delete snapshot (has clones: %v): %w", clones, originalErr)
}

// SnapshotGet retrieves a snapshot by ID (dataset@snapshot format).
func (c *Client) SnapshotGet(ctx context.Context, snapshotID string) (*Snapshot, error) {
	result, err := c.Call(ctx, "zfs.snapshot.get_instance", snapshotID)
	if err != nil {
		// Check for "Invalid params" which indicates not found for get_instance
		if apiErr, ok := err.(*APIError); ok && apiErr.Code == -32602 {
			return nil, fmt.Errorf("snapshot not found: %s", snapshotID)
		}
		// Also check standard IsNotFoundError
		if IsNotFoundError(err) {
			return nil, fmt.Errorf("snapshot not found: %s", snapshotID)
		}
		return nil, fmt.Errorf("failed to get snapshot: %w", err)
	}

	return parseSnapshot(result)
}

// SnapshotList lists snapshots for a dataset.
func (c *Client) SnapshotList(ctx context.Context, dataset string) ([]*Snapshot, error) {
	filters := [][]interface{}{{"dataset", "=", dataset}}

	result, err := c.Call(ctx, "zfs.snapshot.query", filters, map[string]interface{}{})
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
func (c *Client) SnapshotListAll(ctx context.Context, parentDataset string, limit int, offset int) ([]*Snapshot, error) {
	filters := [][]interface{}{{"dataset", "^", parentDataset}}

	options := map[string]interface{}{}
	if limit > 0 {
		options["limit"] = limit
	}
	if offset > 0 {
		options["offset"] = offset
	}

	result, err := c.Call(ctx, "zfs.snapshot.query", filters, options)
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

// SnapshotFindByName finds a snapshot by its short name under a parent dataset.
// This is more efficient than SnapshotListAll + iteration (PERF-001 fix).
// The name parameter is the snapshot name without the dataset prefix (e.g., "my-snapshot" not "pool/dataset@my-snapshot").
func (c *Client) SnapshotFindByName(ctx context.Context, parentDataset string, name string) (*Snapshot, error) {
	// Build the full snapshot ID pattern to match: any dataset under parentDataset + @ + name
	// We use "id" filter with regex match to find the snapshot regardless of its parent dataset
	// The pattern matches any string ending with "@" + name
	filters := [][]interface{}{
		{"dataset", "^", parentDataset},
		{"id", "~", fmt.Sprintf(".*@%s$", name)},
	}

	result, err := c.Call(ctx, "zfs.snapshot.query", filters, map[string]interface{}{})
	if err != nil {
		return nil, fmt.Errorf("failed to query snapshots: %w", err)
	}

	items, ok := result.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected response type")
	}

	if len(items) == 0 {
		return nil, nil // Not found, not an error
	}

	return parseSnapshot(items[0])
}

// SnapshotSetUserProperty sets a user property on a snapshot.
func (c *Client) SnapshotSetUserProperty(ctx context.Context, snapshotID string, key string, value string) error {
	params := map[string]interface{}{
		"user_properties_update": []map[string]interface{}{
			{"key": key, "value": value},
		},
	}

	_, err := c.Call(ctx, "zfs.snapshot.update", snapshotID, params)
	return err
}

// SnapshotClone clones a snapshot to create a new dataset.
func (c *Client) SnapshotClone(ctx context.Context, snapshotID string, newDatasetName string) error {
	params := map[string]interface{}{
		"snapshot":    snapshotID,
		"dataset_dst": newDatasetName,
	}

	_, err := c.Call(ctx, "zfs.snapshot.clone", params)
	if err != nil {
		// Handle "already exists" errors for idempotency
		if IsAlreadyExistsError(err) {
			return nil
		}
		// TrueNAS may return "Invalid params" when clone already exists
		if IsInvalidParamsError(err) {
			// Check if the target dataset already exists
			_, getErr := c.DatasetGet(ctx, newDatasetName)
			if getErr == nil {
				// Dataset exists, treat as success
				return nil
			}
		}
		return fmt.Errorf("failed to clone snapshot: %w", err)
	}

	return nil
}

// SnapshotRollback rolls back a dataset to a snapshot.
func (c *Client) SnapshotRollback(ctx context.Context, snapshotID string, force bool, recursive bool, recursiveClones bool) error {
	options := map[string]interface{}{
		"force":            force,
		"recursive":        recursive,
		"recursive_clones": recursiveClones,
	}

	_, err := c.Call(ctx, "zfs.snapshot.rollback", snapshotID, options)
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
		// Also look for user properties in properties map (keys with :)
		for key, val := range props {
			if strings.Contains(key, ":") {
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
	}

	// Parse user properties (if explicitly returned in separate field)
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

// GetClones returns a list of clone dataset names that were created from this snapshot.
func (snap *Snapshot) GetClones() []string {
	if clones, ok := snap.Properties["clones"]; ok {
		if clonesMap, ok := clones.(map[string]interface{}); ok {
			if value, ok := clonesMap["value"].(string); ok && value != "" {
				// Clones are comma-separated
				return strings.Split(value, ",")
			}
		}
	}
	return nil
}
