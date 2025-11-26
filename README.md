# TrueNAS Scale CSI Driver

[![Build Status](https://img.shields.io/github/actions/workflow/status/GizmoTickler/truenas-scale-csi/ci.yml?branch=main&style=flat-square)](https://github.com/GizmoTickler/truenas-scale-csi/actions)
[![License](https://img.shields.io/github/license/GizmoTickler/truenas-scale-csi?style=flat-square)](LICENSE)

A Kubernetes CSI (Container Storage Interface) driver for TrueNAS SCALE, providing
dynamic storage provisioning via NFS, iSCSI, and NVMe-oF protocols.

This driver focuses exclusively on TrueNAS SCALE 25.04+ with the modern WebSocket JSON-RPC 2.0 API,
providing a streamlined codebase specifically optimized for TrueNAS SCALE deployments.

## Key Features

- **WebSocket JSON-RPC 2.0 API**: No SSH required - all operations via WebSocket
- **Modern TrueNAS SCALE 25.04+**: Uses the latest versioned API (`/api/current`)
- **Three Storage Protocols**: NFS and iSCSI (25.04+), NVMe-oF (25.10+)
- **Full CSI Spec**: Volume resizing, snapshots, clones, and more
- **Persistent Connection**: Auto-reconnecting WebSocket with authentication
- **API Key Auth**: Secure authentication via TrueNAS API keys

## Supported Drivers

| Driver | Protocol | TrueNAS Version | Description |
|--------|----------|-----------------|-------------|
| `truenas-nfs` | NFS | SCALE 25.04+ | ZFS datasets shared over NFS |
| `truenas-iscsi` | iSCSI | SCALE 25.04+ | ZFS zvols shared over iSCSI |
| `truenas-nvmeof` | NVMe-oF | SCALE 25.10+ | ZFS zvols shared over NVMe-oF |

## Installation

### Prerequisites

1. **TrueNAS SCALE 25.04+** with API access enabled
2. **Kubernetes cluster** with CSI support
3. **Node packages** installed based on storage protocol (see [Node Prep](#node-prep))

### Node Prep

Install the required packages on your Kubernetes cluster nodes based on which storage protocol(s) you plan to use.

#### NFS

```bash
# RHEL / CentOS
sudo yum install -y nfs-utils

# Ubuntu / Debian
sudo apt-get install -y nfs-common
```

#### iSCSI

Note that `multipath` is supported for the `iscsi`-based drivers. Simply setup
multipath to your liking and set multiple portals in the config as appropriate.

If you are running Kubernetes with rancher/rke please see the following:
- https://github.com/rancher/rke/issues/1846

##### RHEL / CentOS

```bash
# Install the following system packages
sudo yum install -y lsscsi iscsi-initiator-utils sg3_utils device-mapper-multipath

# Enable multipathing
sudo mpathconf --enable --with_multipathd y

# Ensure that iscsid and multipathd are running
sudo systemctl enable iscsid multipathd
sudo systemctl start iscsid multipathd

# Start and enable iscsi
sudo systemctl enable iscsi
sudo systemctl start iscsi
```

##### Ubuntu / Debian

```bash
# Install the following system packages
sudo apt-get install -y open-iscsi lsscsi sg3-utils multipath-tools scsitools

# Enable multipathing
sudo tee /etc/multipath.conf <<-'EOF'
defaults {
    user_friendly_names yes
    find_multipaths yes
}
EOF

sudo systemctl enable multipath-tools.service
sudo service multipath-tools restart

# Ensure that open-iscsi and multipath-tools are enabled and running
sudo systemctl status multipath-tools
sudo systemctl enable open-iscsi.service
sudo service open-iscsi start
sudo systemctl status open-iscsi
```

##### Talos

To use iSCSI storage in a Kubernetes cluster with [Talos](https://www.talos.dev/),
the iscsi extension is needed. Create a `patch.yaml` file:

```yaml
- op: add
  path: /machine/install/extensions
  value:
    - image: ghcr.io/siderolabs/iscsi-tools:v0.1.1
```

Apply the patch and upgrade your nodes:

```bash
talosctl -e <endpoint ip/hostname> -n <node ip/hostname> patch mc -p @patch.yaml
talosctl -e <endpoint ip/hostname> -n <node ip/hostname> upgrade --image=ghcr.io/siderolabs/installer:v1.1.1
```

In your `values.yaml` file, enable these settings:

```yaml
node:
  hostPID: true
  driver:
    extraEnv:
      - name: ISCSIADM_HOST_STRATEGY
        value: nsenter
      - name: ISCSIADM_HOST_PATH
        value: /usr/local/sbin/iscsiadm
    iscsiDirHostPath: /usr/local/etc/iscsi
    iscsiDirHostPathType: ""
```

##### Privileged Namespace

The CSI driver requires privileged access to nodes. Add the following label to your installation namespace:

```bash
kubectl label --overwrite namespace truenas-csi pod-security.kubernetes.io/enforce=privileged
```

#### NVMe-oF

```bash
# Install nvme-cli tools (optional - tools are included in container images)
apt-get install -y nvme-cli

# Install kernel modules
apt-get install linux-generic

# Ensure NVMe-oF modules load at boot
cat <<EOF > /etc/modules-load.d/nvme.conf
nvme
nvme-tcp
nvme-fc
nvme-rdma
EOF

# Load modules immediately
modprobe nvme
modprobe nvme-tcp
modprobe nvme-fc
modprobe nvme-rdma

# Check multipath configuration
# NVMe supports native multipath or DM multipath
# RedHat recommends DM multipath (nvme_core.multipath=N)
cat /sys/module/nvme_core/parameters/multipath

# Set kernel arg to enable/disable native multipath
nvme_core.multipath=N
```

### TrueNAS SCALE Configuration

**Required**: TrueNAS SCALE 25.04 or later

These drivers use the WebSocket JSON-RPC 2.0 API exclusively - **no SSH required**.
All operations are performed via a persistent WebSocket connection to the
TrueNAS API endpoint (`wss://host/api/current`).

#### TrueNAS Setup

1. **Enable API Access**
   - Navigate to **Settings → API Keys**
   - Click **Add** to create a new API key
   - Copy the API key (format: `1-xxxxxxxxxxxxxxxxxxxxx`)
   - Store securely - this is used for authentication

2. **Configure Storage Pools**
   - Ensure you have a ZFS pool created (e.g., `tank`)
   - Create parent datasets for volumes and snapshots:
     ```bash
     # Example: Create parent datasets
     zfs create tank/k8s
     zfs create tank/k8s/volumes
     zfs create tank/k8s/snapshots
     ```
   - **Important**: Volume and snapshot datasets should be siblings, not nested

3. **Configure Services**

   **For NFS (`truenas-nfs`)**:
   - Navigate to **Sharing → NFS**
   - Ensure NFS service is enabled
   - Shares are created dynamically by the CSI driver

   **For iSCSI (`truenas-iscsi`)**:
   - Navigate to **Sharing → iSCSI**
   - Create Portal (default port 3260)
   - Create Initiator Group
   - Targets and extents are created dynamically by the CSI driver

   **For NVMe-oF (`truenas-nvmeof`)**:
   - Navigate to **Sharing → NVMe-oF**
   - Ensure NVMe-oF service is configured
   - Subsystems and namespaces are created dynamically by the CSI driver

4. **Network Configuration**
   - Ensure the TrueNAS system is reachable from your Kubernetes cluster
   - Open required ports:
     - **WebSocket API**: 443 (HTTPS) or 80 (HTTP)
     - **NFS**: 2049, 111, 20048
     - **iSCSI**: 3260 (default)
     - **NVMe-oF**: 4420 (TCP default)

## Documentation

- **[Architecture Guide](docs/architecture.md)**: Learn how the driver works, component responsibilities, and storage workflows.
- **[Deployment & Configuration Guide](docs/deployment.md)**: Detailed instructions for Helm and Flux deployments, plus a complete configuration reference.

## Quick Start (Helm)

```bash
helm install truenas-csi \
  oci://ghcr.io/gizmotickler/charts/truenas-csi \
  --namespace truenas-csi \
  --create-namespace \
  --values values.yaml \
  --version 2.2.8
```

See [Deployment Guide](docs/deployment.md) for full details.

## Credits

This project is a fork of [democratic-csi](https://github.com/democratic-csi/democratic-csi) by [Travis Glenn Hansen](https://github.com/travisghansen). The original project provides CSI drivers for multiple storage backends including TrueNAS, Synology, and generic ZFS systems.

This fork focuses exclusively on TrueNAS SCALE 25.04+ with the WebSocket JSON-RPC 2.0 API, removing legacy drivers and SSH-based operations for a streamlined codebase.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
