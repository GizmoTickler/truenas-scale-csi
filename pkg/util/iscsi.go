// Package util provides utility functions for iSCSI operations.
package util

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"k8s.io/klog/v2"
)

// ISCSISession represents an active iSCSI session.
type ISCSISession struct {
	TargetPortal string
	IQN          string
	SessionID    string
}

// ISCSIConnect connects to an iSCSI target and returns the device path.
func ISCSIConnect(portal, iqn string, lun int) (string, error) {
	klog.V(4).Infof("ISCSIConnect: portal=%s, iqn=%s, lun=%d", portal, iqn, lun)

	// Discover targets
	if err := iscsiDiscovery(portal); err != nil {
		return "", fmt.Errorf("discovery failed: %w", err)
	}

	// Login to target
	if err := iscsiLogin(portal, iqn); err != nil {
		return "", fmt.Errorf("login failed: %w", err)
	}

	// Wait for device to appear
	devicePath, err := waitForISCSIDevice(portal, iqn, lun, 30*time.Second)
	if err != nil {
		return "", fmt.Errorf("device not found: %w", err)
	}

	return devicePath, nil
}

// ISCSIDisconnect disconnects from an iSCSI target.
func ISCSIDisconnect(portal, iqn string) error {
	klog.V(4).Infof("ISCSIDisconnect: portal=%s, iqn=%s", portal, iqn)

	// Logout from target
	cmd := exec.Command("iscsiadm", "-m", "node", "-T", iqn, "-p", portal, "--logout")
	output, err := cmd.CombinedOutput()
	if err != nil {
		// Check if already logged out
		if strings.Contains(string(output), "No matching sessions") ||
			strings.Contains(string(output), "not logged in") {
			klog.V(4).Infof("Target already logged out: %s", iqn)
			return nil
		}
		return fmt.Errorf("logout failed: %v, output: %s", err, string(output))
	}

	// Delete the node record
	cmd = exec.Command("iscsiadm", "-m", "node", "-T", iqn, "-p", portal, "-o", "delete")
	output, err = cmd.CombinedOutput()
	if err != nil {
		// Not critical if delete fails
		klog.Warningf("Failed to delete node record: %v, output: %s", err, string(output))
	}

	return nil
}

// iscsiDiscovery performs iSCSI discovery on the target portal.
func iscsiDiscovery(portal string) error {
	cmd := exec.Command("iscsiadm", "-m", "discovery", "-t", "sendtargets", "-p", portal)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("discovery command failed: %v, output: %s", err, string(output))
	}
	klog.V(4).Infof("Discovery output: %s", string(output))
	return nil
}

// iscsiLogin logs into an iSCSI target.
func iscsiLogin(portal, iqn string) error {
	// Check if already logged in
	sessions, err := getISCSISessions()
	if err != nil {
		klog.Warningf("Failed to get iSCSI sessions: %v", err)
	} else {
		for _, session := range sessions {
			if session.IQN == iqn {
				klog.V(4).Infof("Already logged in to target: %s", iqn)
				return nil
			}
		}
	}

	// Login to target
	cmd := exec.Command("iscsiadm", "-m", "node", "-T", iqn, "-p", portal, "--login")
	output, err := cmd.CombinedOutput()
	if err != nil {
		// Check if already logged in
		if strings.Contains(string(output), "already present") {
			klog.V(4).Infof("Target already logged in: %s", iqn)
			return nil
		}
		return fmt.Errorf("login command failed: %v, output: %s", err, string(output))
	}
	klog.V(4).Infof("Login output: %s", string(output))
	return nil
}

// getISCSISessions returns the list of active iSCSI sessions.
func getISCSISessions() ([]ISCSISession, error) {
	cmd := exec.Command("iscsiadm", "-m", "session")
	output, err := cmd.Output()
	if err != nil {
		// No sessions is not an error
		if strings.Contains(string(output), "No active sessions") {
			return nil, nil
		}
		return nil, err
	}

	var sessions []ISCSISession
	lines := strings.Split(string(output), "\n")
	// Format: tcp: [session_id] portal:port,target_portal_group_tag iqn
	re := regexp.MustCompile(`^tcp:\s+\[(\d+)\]\s+([^,]+),\d+\s+(.+)$`)

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		matches := re.FindStringSubmatch(line)
		if len(matches) == 4 {
			sessions = append(sessions, ISCSISession{
				SessionID:    matches[1],
				TargetPortal: matches[2],
				IQN:          matches[3],
			})
		}
	}

	return sessions, nil
}

// waitForISCSIDevice waits for the iSCSI device to appear in /dev.
func waitForISCSIDevice(portal, iqn string, lun int, timeout time.Duration) (string, error) {
	start := time.Now()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		devicePath, err := findISCSIDevice(iqn, lun)
		if err == nil && devicePath != "" {
			return devicePath, nil
		}

		if time.Since(start) > timeout {
			return "", fmt.Errorf("timeout waiting for device (iqn=%s, lun=%d)", iqn, lun)
		}
	}
	return "", fmt.Errorf("ticker stopped unexpectedly")
}

// findISCSIDevice finds the device path for an iSCSI LUN.
func findISCSIDevice(iqn string, lun int) (string, error) {
	// Look in /sys/class/iscsi_session for the session
	sessionDirs, err := filepath.Glob("/sys/class/iscsi_session/session*")
	if err != nil {
		return "", err
	}

	for _, sessionDir := range sessionDirs {
		// Read the targetname file
		targetNamePath := filepath.Join(sessionDir, "targetname")
		targetNameBytes, err := os.ReadFile(targetNamePath)
		if err != nil {
			continue
		}
		targetName := strings.TrimSpace(string(targetNameBytes))

		if targetName != iqn {
			continue
		}

		// Found the session, now find the device
		// Session directory contains device subdirectory
		sessionName := filepath.Base(sessionDir)
		devicePath, err := findDeviceForSession(sessionName, lun)
		if err == nil && devicePath != "" {
			return devicePath, nil
		}
	}

	return "", fmt.Errorf("device not found for iqn=%s, lun=%d", iqn, lun)
}

// findDeviceForSession finds the block device for a specific session and LUN.
func findDeviceForSession(sessionName string, lun int) (string, error) {
	// Extract session number
	var sessionNum int
	if _, err := fmt.Sscanf(sessionName, "session%d", &sessionNum); err != nil {
		return "", fmt.Errorf("failed to parse session name: %w", err)
	}

	// Look for the device in /sys/class/scsi_device
	// Format: host:bus:target:lun
	pattern := fmt.Sprintf("/sys/class/scsi_device/*:0:0:%d/device/block/*", lun)
	devices, err := filepath.Glob(pattern)
	if err != nil {
		return "", err
	}

	// Also check specific host pattern based on session
	hostPattern := fmt.Sprintf("/sys/class/iscsi_host/host*/device/session%d", sessionNum)
	hostDirs, _ := filepath.Glob(hostPattern)
	if len(hostDirs) > 0 {
		// Extract host number
		hostDir := filepath.Dir(filepath.Dir(hostDirs[0]))
		hostName := filepath.Base(hostDir)
		var hostNum int
		if _, err := fmt.Sscanf(hostName, "host%d", &hostNum); err != nil {
			return "", fmt.Errorf("failed to parse host name: %w", err)
		}

		// Look for device with this host
		pattern = fmt.Sprintf("/sys/class/scsi_device/%d:0:0:%d/device/block/*", hostNum, lun)
		devices, err = filepath.Glob(pattern)
		if err == nil && len(devices) > 0 {
			deviceName := filepath.Base(devices[0])
			return "/dev/" + deviceName, nil
		}
	}

	// Fallback: look through all scsi devices
	for _, device := range devices {
		deviceName := filepath.Base(device)
		devicePath := "/dev/" + deviceName
		if _, err := os.Stat(devicePath); err == nil {
			return devicePath, nil
		}
	}

	return "", fmt.Errorf("device not found")
}

// GetISCSIDevicePath returns the device path for an iSCSI target/LUN combination.
func GetISCSIDevicePath(iqn string, lun int) (string, error) {
	return findISCSIDevice(iqn, lun)
}

// ISCSIRescanSession rescans an iSCSI session to detect new LUNs.
func ISCSIRescanSession(portal, iqn string) error {
	cmd := exec.Command("iscsiadm", "-m", "node", "-T", iqn, "-p", portal, "--rescan")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("rescan failed: %v, output: %s", err, string(output))
	}
	return nil
}

// ISCSIGetSessionStats returns session statistics for an iSCSI target.
func ISCSIGetSessionStats(iqn string) (map[string]string, error) {
	cmd := exec.Command("iscsiadm", "-m", "session", "-s")
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to get session stats: %v", err)
	}

	stats := make(map[string]string)
	lines := strings.Split(string(output), "\n")
	inTargetSection := false

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.Contains(line, iqn) {
			inTargetSection = true
			continue
		}
		if inTargetSection {
			if strings.HasPrefix(line, "Target:") {
				break // Next target
			}
			if strings.Contains(line, ":") {
				parts := strings.SplitN(line, ":", 2)
				if len(parts) == 2 {
					key := strings.TrimSpace(parts[0])
					value := strings.TrimSpace(parts[1])
					stats[key] = value
				}
			}
		}
	}

	return stats, nil
}

// SetISCSINodeParam sets a parameter on an iSCSI node.
func SetISCSINodeParam(portal, iqn, name, value string) error {
	cmd := exec.Command("iscsiadm", "-m", "node", "-T", iqn, "-p", portal,
		"-o", "update", "-n", name, "-v", value)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to set node param: %v, output: %s", err, string(output))
	}
	return nil
}

// ConfigureISCSICHAP configures CHAP authentication for an iSCSI target.
func ConfigureISCSICHAP(portal, iqn, username, password string) error {
	// Set auth method to CHAP
	if err := SetISCSINodeParam(portal, iqn, "node.session.auth.authmethod", "CHAP"); err != nil {
		return err
	}

	// Set username
	if err := SetISCSINodeParam(portal, iqn, "node.session.auth.username", username); err != nil {
		return err
	}

	// Set password
	if err := SetISCSINodeParam(portal, iqn, "node.session.auth.password", password); err != nil {
		return err
	}

	return nil
}

// GetDeviceWWN returns the WWN (World Wide Name) for a device.
func GetDeviceWWN(devicePath string) (string, error) {
	// Get the device name without /dev/
	deviceName := filepath.Base(devicePath)

	// Read the WWN from sysfs
	wwnPath := fmt.Sprintf("/sys/block/%s/device/wwid", deviceName)
	wwn, err := os.ReadFile(wwnPath)
	if err != nil {
		// Try alternative path
		wwnPath = fmt.Sprintf("/sys/block/%s/device/vpd_pg83", deviceName)
		wwn, err = os.ReadFile(wwnPath)
		if err != nil {
			return "", fmt.Errorf("failed to read WWN: %v", err)
		}
	}

	return strings.TrimSpace(string(wwn)), nil
}

// GetDeviceSize returns the size of a block device in bytes.
func GetDeviceSize(devicePath string) (int64, error) {
	deviceName := filepath.Base(devicePath)
	sizePath := fmt.Sprintf("/sys/block/%s/size", deviceName)

	sizeBytes, err := os.ReadFile(sizePath)
	if err != nil {
		return 0, fmt.Errorf("failed to read device size: %v", err)
	}

	// Size is in 512-byte sectors
	sectors, err := strconv.ParseInt(strings.TrimSpace(string(sizeBytes)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse device size: %v", err)
	}

	return sectors * 512, nil
}

// FlushDeviceBuffers flushes buffers for a block device.
func FlushDeviceBuffers(devicePath string) error {
	cmd := exec.Command("blockdev", "--flushbufs", devicePath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to flush buffers: %v, output: %s", err, string(output))
	}
	return nil
}
