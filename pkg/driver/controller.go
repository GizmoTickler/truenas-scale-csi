package driver

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/klog/v2"

	"github.com/GizmoTickler/truenas-scale-csi/pkg/truenas"
)

// ZFS property names for tracking CSI resources
const (
	PropManagedResource           = "truenas-csi:managed_resource"
	PropProvisionSuccess          = "truenas-csi:provision_success"
	PropCSIVolumeName             = "truenas-csi:csi_volume_name"
	PropShareVolumeContext        = "truenas-csi:csi_share_volume_context"
	PropVolumeContentSourceType   = "truenas-csi:csi_volume_content_source_type"
	PropVolumeContentSourceID     = "truenas-csi:csi_volume_content_source_id"
	PropCSISnapshotName           = "truenas-csi:csi_snapshot_name"
	PropCSISnapshotSourceVolumeID = "truenas-csi:csi_snapshot_source_volume_id"
	PropNFSShareID                = "truenas-csi:truenas_nfs_share_id"
	PropISCSITargetID             = "truenas-csi:truenas_iscsi_target_id"
	PropISCSIExtentID             = "truenas-csi:truenas_iscsi_extent_id"
	PropISCSITargetExtentID       = "truenas-csi:truenas_iscsi_targetextent_id"
	PropNVMeoFSubsystemID         = "truenas-csi:truenas_nvmeof_subsystem_id"
	PropNVMeoFNamespaceID         = "truenas-csi:truenas_nvmeof_namespace_id"
)

// ControllerGetCapabilities returns the capabilities of the controller.
func (d *Driver) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	klog.V(4).Info("ControllerGetCapabilities called")

	caps := []*csi.ControllerServiceCapability{
		{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
				},
			},
		},
		{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_LIST_VOLUMES,
				},
			},
		},
		{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_GET_CAPACITY,
				},
			},
		},
		{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
				},
			},
		},
		{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS,
				},
			},
		},
		{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_CLONE_VOLUME,
				},
			},
		},
		{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
				},
			},
		},
		{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_GET_VOLUME,
				},
			},
		},
		{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_SINGLE_NODE_MULTI_WRITER,
				},
			},
		},
	}

	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: caps,
	}, nil
}

// CreateVolume creates a new volume.
func (d *Driver) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	name := req.GetName()
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "volume name is required")
	}

	klog.Infof("CreateVolume: name=%s", name)

	// Lock on volume name to prevent concurrent creates
	lockKey := "volume:" + name
	if !d.acquireOperationLock(lockKey) {
		return nil, status.Error(codes.Aborted, "operation already in progress for this volume")
	}
	defer d.releaseOperationLock(lockKey)

	// Calculate capacity
	capacityBytes := int64(0)
	if req.GetCapacityRange() != nil {
		capacityBytes = req.GetCapacityRange().GetRequiredBytes()
	}
	if capacityBytes == 0 {
		capacityBytes = 1024 * 1024 * 1024 // Default 1GiB
	}

	// Get volume ID from name
	volumeID := d.sanitizeVolumeID(name)
	datasetName := path.Join(d.config.ZFS.DatasetParentName, volumeID)

	// Check if volume already exists
	existingDS, err := d.truenasClient.DatasetGet(datasetName)
	if err == nil && existingDS != nil {
		// Volume exists - return existing volume context
		klog.Infof("Volume %s already exists", volumeID)
		volumeContext, err := d.getVolumeContext(datasetName)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to get volume context: %v", err)
		}
		return &csi.CreateVolumeResponse{
			Volume: &csi.Volume{
				VolumeId:      volumeID,
				CapacityBytes: d.getDatasetCapacity(existingDS),
				VolumeContext: volumeContext,
			},
		}, nil
	}

	// Handle volume content source (clone from snapshot or volume)
	var contentSource *csi.VolumeContentSource
	if req.GetVolumeContentSource() != nil {
		contentSource = req.GetVolumeContentSource()
		if err := d.handleVolumeContentSource(ctx, datasetName, contentSource, capacityBytes); err != nil {
			return nil, err
		}
	} else {
		// Create new dataset
		if err := d.createDataset(ctx, datasetName, capacityBytes); err != nil {
			return nil, err
		}
	}

	// Create share (NFS, iSCSI, or NVMe-oF)
	if err := d.createShare(ctx, datasetName, name); err != nil {
		// Cleanup on failure
		if delErr := d.truenasClient.DatasetDelete(datasetName, false, false); delErr != nil {
			klog.Warningf("Failed to cleanup dataset after share creation failure: %v", delErr)
		}
		return nil, err
	}

	// Mark as managed and successful
	if err := d.truenasClient.DatasetSetUserProperty(datasetName, PropManagedResource, "true"); err != nil {
		klog.Warningf("Failed to set managed resource property: %v", err)
	}
	if err := d.truenasClient.DatasetSetUserProperty(datasetName, PropProvisionSuccess, "true"); err != nil {
		klog.Warningf("Failed to set provision success property: %v", err)
	}
	if err := d.truenasClient.DatasetSetUserProperty(datasetName, PropCSIVolumeName, name); err != nil {
		klog.Warningf("Failed to set CSI volume name property: %v", err)
	}

	// Get volume context for response
	volumeContext, err := d.getVolumeContext(datasetName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get volume context: %v", err)
	}

	klog.Infof("Volume %s created successfully", volumeID)

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      volumeID,
			CapacityBytes: capacityBytes,
			VolumeContext: volumeContext,
			ContentSource: contentSource,
		},
	}, nil
}

// DeleteVolume deletes a volume.
func (d *Driver) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}

	klog.Infof("DeleteVolume: volumeID=%s", volumeID)

	// Lock on volume ID
	lockKey := "volume:" + volumeID
	if !d.acquireOperationLock(lockKey) {
		return nil, status.Error(codes.Aborted, "operation already in progress for this volume")
	}
	defer d.releaseOperationLock(lockKey)

	datasetName := path.Join(d.config.ZFS.DatasetParentName, volumeID)

	// Delete share first
	if err := d.deleteShare(ctx, datasetName); err != nil {
		klog.Warningf("Failed to delete share for %s: %v", volumeID, err)
	}

	// Delete dataset
	if err := d.truenasClient.DatasetDelete(datasetName, true, true); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete volume: %v", err)
	}

	klog.Infof("Volume %s deleted successfully", volumeID)

	return &csi.DeleteVolumeResponse{}, nil
}

// ControllerPublishVolume attaches a volume to a node (not used for NFS).
func (d *Driver) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	// Not implemented - NFS doesn't require controller publish
	return &csi.ControllerPublishVolumeResponse{}, nil
}

// ControllerUnpublishVolume detaches a volume from a node (not used for NFS).
func (d *Driver) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	// Not implemented - NFS doesn't require controller unpublish
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

// ValidateVolumeCapabilities validates volume capabilities.
func (d *Driver) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}

	caps := req.GetVolumeCapabilities()
	if len(caps) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume capabilities are required")
	}

	// Check volume exists
	datasetName := path.Join(d.config.ZFS.DatasetParentName, volumeID)
	_, err := d.truenasClient.DatasetGet(datasetName)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "volume not found: %s", volumeID)
	}

	// Validate capabilities
	confirmed := &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
		VolumeCapabilities: caps,
	}

	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: confirmed,
	}, nil
}

// ListVolumes lists all volumes.
func (d *Driver) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	klog.V(4).Info("ListVolumes called")

	datasets, err := d.truenasClient.DatasetList(d.config.ZFS.DatasetParentName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list volumes: %v", err)
	}

	entries := make([]*csi.ListVolumesResponse_Entry, 0)
	for _, ds := range datasets {
		// Skip if not managed by CSI
		if prop, ok := ds.UserProperties[PropManagedResource]; !ok || prop.Value != "true" {
			continue
		}

		volumeID := path.Base(ds.Name)
		capacity := d.getDatasetCapacity(ds)

		entries = append(entries, &csi.ListVolumesResponse_Entry{
			Volume: &csi.Volume{
				VolumeId:      volumeID,
				CapacityBytes: capacity,
			},
		})
	}

	return &csi.ListVolumesResponse{
		Entries: entries,
	}, nil
}

// GetCapacity returns the available capacity.
func (d *Driver) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	klog.V(4).Info("GetCapacity called")

	available, err := d.truenasClient.GetPoolAvailable(d.config.ZFS.DatasetParentName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get capacity: %v", err)
	}

	return &csi.GetCapacityResponse{
		AvailableCapacity: available,
	}, nil
}

// CreateSnapshot creates a snapshot.
func (d *Driver) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	sourceVolumeID := req.GetSourceVolumeId()
	if sourceVolumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "source volume ID is required")
	}

	name := req.GetName()
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot name is required")
	}

	klog.Infof("CreateSnapshot: name=%s, sourceVolumeID=%s", name, sourceVolumeID)

	// Lock on snapshot name
	lockKey := "snapshot:" + name
	if !d.acquireOperationLock(lockKey) {
		return nil, status.Error(codes.Aborted, "operation already in progress for this snapshot")
	}
	defer d.releaseOperationLock(lockKey)

	datasetName := path.Join(d.config.ZFS.DatasetParentName, sourceVolumeID)
	snapshotID := d.sanitizeVolumeID(name)

	// Create snapshot
	snap, err := d.truenasClient.SnapshotCreate(datasetName, snapshotID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create snapshot: %v", err)
	}

	// Set snapshot properties
	if err := d.truenasClient.SnapshotSetUserProperty(snap.ID, PropManagedResource, "true"); err != nil {
		klog.Warningf("Failed to set managed resource property on snapshot: %v", err)
	}
	if err := d.truenasClient.SnapshotSetUserProperty(snap.ID, PropCSISnapshotName, name); err != nil {
		klog.Warningf("Failed to set CSI snapshot name property: %v", err)
	}
	if err := d.truenasClient.SnapshotSetUserProperty(snap.ID, PropCSISnapshotSourceVolumeID, sourceVolumeID); err != nil {
		klog.Warningf("Failed to set CSI snapshot source volume ID property: %v", err)
	}

	klog.Infof("Snapshot %s created successfully", snapshotID)

	return &csi.CreateSnapshotResponse{
		Snapshot: &csi.Snapshot{
			SnapshotId:     snapshotID,
			SourceVolumeId: sourceVolumeID,
			SizeBytes:      snap.GetSnapshotSize(),
			CreationTime:   timestampProto(snap.GetCreationTime()),
			ReadyToUse:     true,
		},
	}, nil
}

// DeleteSnapshot deletes a snapshot.
func (d *Driver) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	snapshotID := req.GetSnapshotId()
	if snapshotID == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot ID is required")
	}

	klog.Infof("DeleteSnapshot: snapshotID=%s", snapshotID)

	// Lock on snapshot ID
	lockKey := "snapshot:" + snapshotID
	if !d.acquireOperationLock(lockKey) {
		return nil, status.Error(codes.Aborted, "operation already in progress for this snapshot")
	}
	defer d.releaseOperationLock(lockKey)

	// Find and delete the snapshot
	// Snapshot ID format: dataset@snapshotname
	snapshots, err := d.truenasClient.SnapshotListAll(d.config.ZFS.DatasetParentName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list snapshots: %v", err)
	}

	for _, snap := range snapshots {
		snapName := path.Base(strings.Split(snap.ID, "@")[1])
		if snapName == snapshotID {
			if err := d.truenasClient.SnapshotDelete(snap.ID, false, false); err != nil {
				return nil, status.Errorf(codes.Internal, "failed to delete snapshot: %v", err)
			}
			break
		}
	}

	klog.Infof("Snapshot %s deleted successfully", snapshotID)

	return &csi.DeleteSnapshotResponse{}, nil
}

// ListSnapshots lists snapshots.
func (d *Driver) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	klog.V(4).Info("ListSnapshots called")

	snapshots, err := d.truenasClient.SnapshotListAll(d.config.ZFS.DatasetParentName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list snapshots: %v", err)
	}

	entries := make([]*csi.ListSnapshotsResponse_Entry, 0)
	for _, snap := range snapshots {
		// Skip if not managed by CSI
		if prop, ok := snap.UserProperties[PropManagedResource]; !ok || prop.Value != "true" {
			continue
		}

		// Filter by snapshot ID if specified
		if req.GetSnapshotId() != "" {
			snapName := path.Base(strings.Split(snap.ID, "@")[1])
			if snapName != req.GetSnapshotId() {
				continue
			}
		}

		// Filter by source volume if specified
		sourceVolumeID := ""
		if prop, ok := snap.UserProperties[PropCSISnapshotSourceVolumeID]; ok {
			sourceVolumeID = prop.Value
		}
		if req.GetSourceVolumeId() != "" && sourceVolumeID != req.GetSourceVolumeId() {
			continue
		}

		snapshotID := path.Base(strings.Split(snap.ID, "@")[1])
		entries = append(entries, &csi.ListSnapshotsResponse_Entry{
			Snapshot: &csi.Snapshot{
				SnapshotId:     snapshotID,
				SourceVolumeId: sourceVolumeID,
				SizeBytes:      snap.GetSnapshotSize(),
				CreationTime:   timestampProto(snap.GetCreationTime()),
				ReadyToUse:     true,
			},
		})
	}

	return &csi.ListSnapshotsResponse{
		Entries: entries,
	}, nil
}

// ControllerExpandVolume expands a volume.
func (d *Driver) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}

	capacityBytes := req.GetCapacityRange().GetRequiredBytes()
	if capacityBytes == 0 {
		return nil, status.Error(codes.InvalidArgument, "capacity is required")
	}

	klog.Infof("ControllerExpandVolume: volumeID=%s, capacity=%d", volumeID, capacityBytes)

	datasetName := path.Join(d.config.ZFS.DatasetParentName, volumeID)

	// For zvols (iSCSI/NVMe-oF), expand the volsize
	if d.config.GetZFSResourceType() == "volume" {
		if err := d.truenasClient.DatasetExpand(datasetName, capacityBytes); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to expand volume: %v", err)
		}
	}

	// For filesystems (NFS), update quota if enabled
	if d.config.GetZFSResourceType() == "filesystem" && d.config.ZFS.DatasetEnableQuotas {
		params := &truenas.DatasetUpdateParams{
			Refquota: capacityBytes,
		}
		if _, err := d.truenasClient.DatasetUpdate(datasetName, params); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to update quota: %v", err)
		}
	}

	// Node expansion may be required for filesystems
	nodeExpansionRequired := d.config.GetZFSResourceType() == "volume"

	klog.Infof("Volume %s expanded successfully", volumeID)

	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         capacityBytes,
		NodeExpansionRequired: nodeExpansionRequired,
	}, nil
}

// ControllerGetVolume gets information about a volume.
func (d *Driver) ControllerGetVolume(ctx context.Context, req *csi.ControllerGetVolumeRequest) (*csi.ControllerGetVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}

	datasetName := path.Join(d.config.ZFS.DatasetParentName, volumeID)
	ds, err := d.truenasClient.DatasetGet(datasetName)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "volume not found: %s", volumeID)
	}

	return &csi.ControllerGetVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      volumeID,
			CapacityBytes: d.getDatasetCapacity(ds),
		},
		Status: &csi.ControllerGetVolumeResponse_VolumeStatus{
			VolumeCondition: &csi.VolumeCondition{
				Abnormal: false,
				Message:  "volume is healthy",
			},
		},
	}, nil
}

// ControllerModifyVolume modifies a volume (not implemented).
func (d *Driver) ControllerModifyVolume(ctx context.Context, req *csi.ControllerModifyVolumeRequest) (*csi.ControllerModifyVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "ControllerModifyVolume not implemented")
}

// Helper functions

func (d *Driver) sanitizeVolumeID(name string) string {
	// Remove invalid characters for ZFS dataset names
	name = strings.ReplaceAll(name, "/", "-")
	name = strings.ReplaceAll(name, " ", "-")
	// Ensure it starts with alphanumeric
	if len(name) > 0 && !isAlphanumeric(name[0]) {
		name = "v" + name
	}
	// Limit length (ZFS has a 256 char limit, CSI has 128)
	if len(name) > 128 {
		name = name[:128]
	}
	return name
}

func isAlphanumeric(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

func (d *Driver) getDatasetCapacity(ds *truenas.Dataset) int64 {
	// For zvols, use volsize
	if ds.Type == "VOLUME" {
		if parsed, ok := ds.Volsize.Parsed.(float64); ok {
			return int64(parsed)
		}
	}
	// For filesystems, use quota or available
	if parsed, ok := ds.Refquota.Parsed.(float64); ok && parsed > 0 {
		return int64(parsed)
	}
	if parsed, ok := ds.Available.Parsed.(float64); ok {
		return int64(parsed)
	}
	return 0
}

func (d *Driver) createDataset(ctx context.Context, datasetName string, capacityBytes int64) error {
	shareType := d.config.GetDriverShareType()

	params := &truenas.DatasetCreateParams{
		Name: datasetName,
	}

	if shareType == "nfs" {
		// Create filesystem for NFS
		params.Type = "FILESYSTEM"
		if d.config.ZFS.DatasetEnableQuotas {
			params.Refquota = capacityBytes
		}
		if d.config.ZFS.DatasetEnableReservation {
			params.Refreservation = capacityBytes
		}
	} else {
		// Create zvol for iSCSI/NVMe-oF
		params.Type = "VOLUME"
		params.Volsize = capacityBytes
		params.Volblocksize = d.config.ZFS.ZvolBlocksize
		params.Sparse = true
	}

	_, err := d.truenasClient.DatasetCreate(params)
	return err
}

func (d *Driver) handleVolumeContentSource(ctx context.Context, datasetName string, source *csi.VolumeContentSource, capacityBytes int64) error {
	if snapshot := source.GetSnapshot(); snapshot != nil {
		// Clone from snapshot
		snapshotID := snapshot.GetSnapshotId()

		// Find the snapshot
		snapshots, err := d.truenasClient.SnapshotListAll(d.config.ZFS.DatasetParentName)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to find snapshot: %v", err)
		}

		var sourceSnapshot string
		for _, snap := range snapshots {
			snapName := path.Base(strings.Split(snap.ID, "@")[1])
			if snapName == snapshotID {
				sourceSnapshot = snap.ID
				break
			}
		}

		if sourceSnapshot == "" {
			return status.Errorf(codes.NotFound, "snapshot not found: %s", snapshotID)
		}

		if err := d.truenasClient.SnapshotClone(sourceSnapshot, datasetName); err != nil {
			return status.Errorf(codes.Internal, "failed to clone snapshot: %v", err)
		}

		// Set content source properties
		if err := d.truenasClient.DatasetSetUserProperty(datasetName, PropVolumeContentSourceType, "snapshot"); err != nil {
			klog.Warningf("Failed to set volume content source type property: %v", err)
		}
		if err := d.truenasClient.DatasetSetUserProperty(datasetName, PropVolumeContentSourceID, snapshotID); err != nil {
			klog.Warningf("Failed to set volume content source ID property: %v", err)
		}

	} else if volume := source.GetVolume(); volume != nil {
		// Clone from volume
		sourceVolumeID := volume.GetVolumeId()
		sourceDataset := path.Join(d.config.ZFS.DatasetParentName, sourceVolumeID)

		// Create a snapshot of source volume, then clone it
		tempSnapshotName := fmt.Sprintf("clone-source-%s", d.sanitizeVolumeID(path.Base(datasetName)))
		snap, err := d.truenasClient.SnapshotCreate(sourceDataset, tempSnapshotName)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to create source snapshot: %v", err)
		}

		if err := d.truenasClient.SnapshotClone(snap.ID, datasetName); err != nil {
			if delErr := d.truenasClient.SnapshotDelete(snap.ID, false, false); delErr != nil {
				klog.Warningf("Failed to cleanup snapshot after clone failure: %v", delErr)
			}
			return status.Errorf(codes.Internal, "failed to clone volume: %v", err)
		}

		// Set content source properties
		if err := d.truenasClient.DatasetSetUserProperty(datasetName, PropVolumeContentSourceType, "volume"); err != nil {
			klog.Warningf("Failed to set volume content source type property: %v", err)
		}
		if err := d.truenasClient.DatasetSetUserProperty(datasetName, PropVolumeContentSourceID, sourceVolumeID); err != nil {
			klog.Warningf("Failed to set volume content source ID property: %v", err)
		}
	}

	return nil
}

func (d *Driver) getVolumeContext(datasetName string) (map[string]string, error) {
	shareType := d.config.GetDriverShareType()

	context := map[string]string{
		"node_attach_driver": shareType,
	}

	ds, err := d.truenasClient.DatasetGet(datasetName)
	if err != nil {
		return nil, err
	}

	switch shareType {
	case "nfs":
		context["server"] = d.config.NFS.ShareHost
		context["share"] = ds.Mountpoint

	case "iscsi":
		// Get target info from dataset properties
		if prop, ok := ds.UserProperties[PropISCSITargetID]; ok {
			targetID, _ := strconv.Atoi(prop.Value)
			target, err := d.truenasClient.ISCSITargetGet(targetID)
			if err == nil {
				globalCfg, _ := d.truenasClient.ISCSIGlobalConfigGet()
				if globalCfg != nil {
					context["iqn"] = fmt.Sprintf("%s:%s", globalCfg.Basename, target.Name)
				}
			}
		}
		context["portal"] = d.config.ISCSI.TargetPortal
		context["lun"] = "0"
		context["interface"] = d.config.ISCSI.Interface

	case "nvmeof":
		// Get subsystem info from dataset properties
		if prop, ok := ds.UserProperties[PropNVMeoFSubsystemID]; ok {
			subsysID, _ := strconv.Atoi(prop.Value)
			subsys, err := d.truenasClient.NVMeoFSubsystemGet(subsysID)
			if err == nil {
				context["nqn"] = subsys.NQN
			}
		}
		context["transport"] = d.config.NVMeoF.Transport
		context["address"] = d.config.NVMeoF.TransportAddress
		context["port"] = strconv.Itoa(d.config.NVMeoF.TransportServiceID)
	}

	return context, nil
}

func timestampProto(unixSeconds int64) *timestamppb.Timestamp {
	return &timestamppb.Timestamp{
		Seconds: unixSeconds,
	}
}
