package driver

import (
	"context"
	"fmt"
	"path"
	"strconv"

	"github.com/GizmoTickler/truenas-scale-csi/pkg/truenas"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
)

// createShare creates the appropriate share type (NFS, iSCSI, or NVMe-oF) for a dataset.
func (d *Driver) createShare(ctx context.Context, datasetName string, volumeName string) error {
	shareType := d.config.GetDriverShareType()

	klog.Infof("Creating %s share for dataset: %s", shareType, datasetName)

	switch shareType {
	case "nfs":
		return d.createNFSShare(ctx, datasetName, volumeName)
	case "iscsi":
		return d.createISCSIShare(ctx, datasetName, volumeName)
	case "nvmeof":
		return d.createNVMeoFShare(ctx, datasetName, volumeName)
	default:
		return status.Errorf(codes.InvalidArgument, "unsupported share type: %s", shareType)
	}
}

// deleteShare deletes the share for a dataset.
func (d *Driver) deleteShare(ctx context.Context, datasetName string) error {
	shareType := d.config.GetDriverShareType()

	klog.Infof("Deleting %s share for dataset: %s", shareType, datasetName)

	switch shareType {
	case "nfs":
		return d.deleteNFSShare(ctx, datasetName)
	case "iscsi":
		return d.deleteISCSIShare(ctx, datasetName)
	case "nvmeof":
		return d.deleteNVMeoFShare(ctx, datasetName)
	default:
		return nil
	}
}

// createNFSShare creates an NFS share for a dataset.
func (d *Driver) createNFSShare(ctx context.Context, datasetName string, volumeName string) error {
	// Get dataset to find mountpoint
	ds, err := d.truenasClient.DatasetGet(datasetName)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to get dataset: %v", err)
	}

	// Check if share already exists
	existingProp, _ := d.truenasClient.DatasetGetUserProperty(datasetName, PropNFSShareID)
	if existingProp != "" && existingProp != "-" {
		klog.Infof("NFS share already exists for %s", datasetName)
		return nil
	}

	// Create NFS share
	comment := fmt.Sprintf("truenas-csi (%s): %s", d.name, datasetName)

	params := &truenas.NFSShareCreateParams{
		Path:         ds.Mountpoint,
		Comment:      comment,
		Networks:     d.config.NFS.ShareAllowedNetworks,
		Hosts:        d.config.NFS.ShareAllowedHosts,
		Ro:           false,
		MaprootUser:  d.config.NFS.ShareMaprootUser,
		MaprootGroup: d.config.NFS.ShareMaprootGroup,
		MapallUser:   d.config.NFS.ShareMapallUser,
		MapallGroup:  d.config.NFS.ShareMapallGroup,
	}

	share, err := d.truenasClient.NFSShareCreate(params)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create NFS share: %v", err)
	}

	// Store share ID in dataset property
	if err := d.truenasClient.DatasetSetUserProperty(datasetName, PropNFSShareID, strconv.Itoa(share.ID)); err != nil {
		klog.Warningf("Failed to store NFS share ID: %v", err)
	}

	klog.Infof("Created NFS share ID %d for %s", share.ID, datasetName)
	return nil
}

// deleteNFSShare deletes the NFS share for a dataset.
func (d *Driver) deleteNFSShare(ctx context.Context, datasetName string) error {
	// Get share ID from dataset property
	shareIDStr, err := d.truenasClient.DatasetGetUserProperty(datasetName, PropNFSShareID)
	if err != nil || shareIDStr == "" || shareIDStr == "-" {
		return nil // No share to delete
	}

	shareID, err := strconv.Atoi(shareIDStr)
	if err != nil {
		return nil
	}

	if err := d.truenasClient.NFSShareDelete(shareID); err != nil {
		klog.Warningf("Failed to delete NFS share %d: %v", shareID, err)
	}

	klog.Infof("Deleted NFS share ID %d", shareID)
	return nil
}

// createISCSIShare creates iSCSI target, extent, and target-extent association.
func (d *Driver) createISCSIShare(ctx context.Context, datasetName string, volumeName string) error {
	// Check if already configured
	existingProp, _ := d.truenasClient.DatasetGetUserProperty(datasetName, PropISCSITargetExtentID)
	if existingProp != "" && existingProp != "-" {
		klog.Infof("iSCSI share already exists for %s", datasetName)
		return nil
	}

	// Generate iSCSI name
	iscsiName := path.Base(datasetName)
	if d.config.ISCSI.NamePrefix != "" {
		iscsiName = d.config.ISCSI.NamePrefix + iscsiName
	}
	if d.config.ISCSI.NameSuffix != "" {
		iscsiName = iscsiName + d.config.ISCSI.NameSuffix
	}

	// Create target
	target, err := d.truenasClient.ISCSITargetCreate(iscsiName, "", "ISCSI", nil)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create iSCSI target: %v", err)
	}
	if err := d.truenasClient.DatasetSetUserProperty(datasetName, PropISCSITargetID, strconv.Itoa(target.ID)); err != nil {
		klog.Warningf("Failed to store iSCSI target ID: %v", err)
	}

	// Create extent
	diskPath := fmt.Sprintf("zvol/%s", datasetName)
	comment := fmt.Sprintf("truenas-csi: %s", datasetName)

	extent, err := d.truenasClient.ISCSIExtentCreate(
		iscsiName,
		diskPath,
		comment,
		d.config.ISCSI.ExtentBlocksize,
		d.config.ISCSI.ExtentRpm,
	)
	if err != nil {
		if delErr := d.truenasClient.ISCSITargetDelete(target.ID, true); delErr != nil {
			klog.Warningf("Failed to cleanup iSCSI target: %v", delErr)
		}
		return status.Errorf(codes.Internal, "failed to create iSCSI extent: %v", err)
	}
	if err := d.truenasClient.DatasetSetUserProperty(datasetName, PropISCSIExtentID, strconv.Itoa(extent.ID)); err != nil {
		klog.Warningf("Failed to store iSCSI extent ID: %v", err)
	}

	// Create target-extent association
	targetExtent, err := d.truenasClient.ISCSITargetExtentCreate(target.ID, extent.ID, 0)
	if err != nil {
		if delErr := d.truenasClient.ISCSIExtentDelete(extent.ID, false, true); delErr != nil {
			klog.Warningf("Failed to cleanup iSCSI extent: %v", delErr)
		}
		if delErr := d.truenasClient.ISCSITargetDelete(target.ID, true); delErr != nil {
			klog.Warningf("Failed to cleanup iSCSI target: %v", delErr)
		}
		return status.Errorf(codes.Internal, "failed to create target-extent association: %v", err)
	}
	if err := d.truenasClient.DatasetSetUserProperty(datasetName, PropISCSITargetExtentID, strconv.Itoa(targetExtent.ID)); err != nil {
		klog.Warningf("Failed to store iSCSI target-extent ID: %v", err)
	}

	klog.Infof("Created iSCSI target=%d, extent=%d, targetextent=%d for %s", target.ID, extent.ID, targetExtent.ID, datasetName)
	return nil
}

// deleteISCSIShare deletes iSCSI resources for a dataset.
func (d *Driver) deleteISCSIShare(ctx context.Context, datasetName string) error {
	// Delete target-extent association
	if teIDStr, _ := d.truenasClient.DatasetGetUserProperty(datasetName, PropISCSITargetExtentID); teIDStr != "" && teIDStr != "-" {
		if teID, err := strconv.Atoi(teIDStr); err == nil {
			if err := d.truenasClient.ISCSITargetExtentDelete(teID, true); err != nil {
				klog.Warningf("Failed to delete iSCSI target-extent %d: %v", teID, err)
			}
		}
	}

	// Delete extent
	if extIDStr, _ := d.truenasClient.DatasetGetUserProperty(datasetName, PropISCSIExtentID); extIDStr != "" && extIDStr != "-" {
		if extID, err := strconv.Atoi(extIDStr); err == nil {
			if err := d.truenasClient.ISCSIExtentDelete(extID, false, true); err != nil {
				klog.Warningf("Failed to delete iSCSI extent %d: %v", extID, err)
			}
		}
	}

	// Delete target
	if tgtIDStr, _ := d.truenasClient.DatasetGetUserProperty(datasetName, PropISCSITargetID); tgtIDStr != "" && tgtIDStr != "-" {
		if tgtID, err := strconv.Atoi(tgtIDStr); err == nil {
			if err := d.truenasClient.ISCSITargetDelete(tgtID, true); err != nil {
				klog.Warningf("Failed to delete iSCSI target %d: %v", tgtID, err)
			}
		}
	}

	klog.Infof("Deleted iSCSI resources for %s", datasetName)
	return nil
}

// createNVMeoFShare creates NVMe-oF subsystem and namespace.
func (d *Driver) createNVMeoFShare(ctx context.Context, datasetName string, volumeName string) error {
	// Check if already configured
	existingProp, _ := d.truenasClient.DatasetGetUserProperty(datasetName, PropNVMeoFNamespaceID)
	if existingProp != "" && existingProp != "-" {
		klog.Infof("NVMe-oF share already exists for %s", datasetName)
		return nil
	}

	// Generate NVMe-oF NQN
	nqn := path.Base(datasetName)
	if d.config.NVMeoF.NamePrefix != "" {
		nqn = d.config.NVMeoF.NamePrefix + nqn
	}
	if d.config.NVMeoF.NameSuffix != "" {
		nqn = nqn + d.config.NVMeoF.NameSuffix
	}

	// Generate serial (max 20 chars)
	serial := d.sanitizeVolumeID(volumeName)
	if len(serial) > 20 {
		serial = serial[:20]
	}

	// Create subsystem
	subsys, err := d.truenasClient.NVMeoFSubsystemCreate(
		nqn,
		serial,
		d.config.NVMeoF.SubsystemAllowAnyHost,
		d.config.NVMeoF.SubsystemHosts,
	)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create NVMe-oF subsystem: %v", err)
	}
	if err := d.truenasClient.DatasetSetUserProperty(datasetName, PropNVMeoFSubsystemID, strconv.Itoa(subsys.ID)); err != nil {
		klog.Warningf("Failed to store NVMe-oF subsystem ID: %v", err)
	}

	// Create namespace
	devicePath := fmt.Sprintf("/dev/zvol/%s", datasetName)
	namespace, err := d.truenasClient.NVMeoFNamespaceCreate(subsys.ID, devicePath)
	if err != nil {
		if delErr := d.truenasClient.NVMeoFSubsystemDelete(subsys.ID); delErr != nil {
			klog.Warningf("Failed to cleanup NVMe-oF subsystem: %v", delErr)
		}
		return status.Errorf(codes.Internal, "failed to create NVMe-oF namespace: %v", err)
	}
	if err := d.truenasClient.DatasetSetUserProperty(datasetName, PropNVMeoFNamespaceID, strconv.Itoa(namespace.ID)); err != nil {
		klog.Warningf("Failed to store NVMe-oF namespace ID: %v", err)
	}

	klog.Infof("Created NVMe-oF subsystem=%d, namespace=%d for %s", subsys.ID, namespace.ID, datasetName)
	return nil
}

// deleteNVMeoFShare deletes NVMe-oF resources for a dataset.
func (d *Driver) deleteNVMeoFShare(ctx context.Context, datasetName string) error {
	// Delete namespace
	if nsIDStr, _ := d.truenasClient.DatasetGetUserProperty(datasetName, PropNVMeoFNamespaceID); nsIDStr != "" && nsIDStr != "-" {
		if nsID, err := strconv.Atoi(nsIDStr); err == nil {
			if err := d.truenasClient.NVMeoFNamespaceDelete(nsID); err != nil {
				klog.Warningf("Failed to delete NVMe-oF namespace %d: %v", nsID, err)
			}
		}
	}

	// Delete subsystem
	if ssIDStr, _ := d.truenasClient.DatasetGetUserProperty(datasetName, PropNVMeoFSubsystemID); ssIDStr != "" && ssIDStr != "-" {
		if ssID, err := strconv.Atoi(ssIDStr); err == nil {
			if err := d.truenasClient.NVMeoFSubsystemDelete(ssID); err != nil {
				klog.Warningf("Failed to delete NVMe-oF subsystem %d: %v", ssID, err)
			}
		}
	}

	klog.Infof("Deleted NVMe-oF resources for %s", datasetName)
	return nil
}
