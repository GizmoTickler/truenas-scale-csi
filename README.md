![Image](https://img.shields.io/docker/pulls/democraticcsi/democratic-csi.svg)
![Image](https://img.shields.io/github/actions/workflow/status/democratic-csi/democratic-csi/main.yml?branch=master&style=flat-square)
[![Artifact Hub](https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/democratic-csi)](https://artifacthub.io/packages/search?repo=democratic-csi)

# Introduction

`democratic-csi` implements the `csi` (container storage interface) spec
providing storage for various container orchestration systems (ie: Kubernetes).

This version focuses exclusively on providing storage via iSCSI/NFS from
**TrueNAS SCALE 25.04+** and NVMe-oF from **TrueNAS SCALE 25.10+** using the modern WebSocket JSON-RPC 2.0 API.

The drivers implement the depth and breadth of the `csi` spec, so you
have access to resizing, snapshots, clones, etc functionality.

`democratic-csi` is 2 things:

- TrueNAS SCALE 25.04+ CSI driver implementations
  - `truenas-nfs` (manages ZFS datasets to share over NFS)
  - `truenas-iscsi` (manages ZFS zvols to share over iSCSI)
  - `truenas-nvmeof` (manages ZFS zvols to share over NVMe-oF)
- framework for developing `csi` drivers

## Key Features

- **WebSocket JSON-RPC 2.0 API**: No SSH required - all operations via WebSocket
- **Modern TrueNAS SCALE 25.04+**: Uses the latest versioned API (`/api/current`)
- **Three Storage Protocols**: NFS and iSCSI (25.04+), NVMe-oF (25.10+)
- **Full CSI Spec**: Volume resizing, snapshots, clones, and more
- **Persistent Connection**: Auto-reconnecting WebSocket with authentication
- **API Key Auth**: Secure authentication via TrueNAS API keys

If you have any interest in providing a `csi` driver, simply open an issue to
discuss. The project provides an extensive framework to build from making it
relatively easy to implement new drivers.

# Installation

Predominantly 3 things are needed:

- node prep (ie: your kubernetes cluster nodes)
- server prep (ie: your storage server)
- deploy the driver into the cluster (`helm` chart provided with sample
  `values.yaml`)

## Node Prep

Install the required packages on your Kubernetes cluster nodes based on which storage protocol(s) you plan to use.

### NFS

```bash
# RHEL / CentOS
sudo yum install -y nfs-utils

# Ubuntu / Debian
sudo apt-get install -y nfs-common
```

### iscsi

Note that `multipath` is supported for the `iscsi`-based drivers. Simply setup
multipath to your liking and set multiple portals in the config as appropriate.

If you are running Kubernetes with rancher/rke please see the following:

- https://github.com/rancher/rke/issues/1846

#### RHEL / CentOS

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

#### Ubuntu / Debian

```
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

#### [Talos](https://www.talos.dev/)

To use iscsi storage in kubernetes cluster in talos these steps are needed which are similar to the ones explained in https://www.talos.dev/v1.1/kubernetes-guides/configuration/replicated-local-storage-with-openebs-jiva/#patching-the-jiva-installation

##### Patch nodes

since talos does not have iscsi support by default, the iscsi extension is needed
create a `patch.yaml` file with

```yaml
- op: add
  path: /machine/install/extensions
  value:
    - image: ghcr.io/siderolabs/iscsi-tools:v0.1.1
```

and apply the patch across all of your nodes

```bash
talosctl -e <endpoint ip/hostname> -n <node ip/hostname> patch mc -p @patch.yaml
```

the extension will not activate until you "upgrade" the nodes, even if there is no update, use the latest version of talos installer.
VERIFY THE TALOS VERSION IN THIS COMMAND BEFORE RUNNING IT AND READ THE [OpenEBS Jiva](https://www.talos.dev/v1.1/kubernetes-guides/configuration/replicated-local-storage-with-openebs-jiva/#patching-the-jiva-installation).
upgrade all of the nodes in the cluster to get the extension

```bash
talosctl -e <endpoint ip/hostname> -n <node ip/hostname> upgrade --image=ghcr.io/siderolabs/installer:v1.1.1
```

in your `values.yaml` file make sure to enable these settings

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

and continue your democratic installation as usuall with other iscsi drivers.

#### Privileged Namespace

democratic-csi requires privileged access to the nodes, so the namespace should allow for privileged pods. One way of doing it is via [namespace labels](https://kubernetes.io/docs/tasks/configure-pod-container/enforce-standards-namespace-labels/).
Add the followin label to the democratic-csi installation namespace `pod-security.kubernetes.io/enforce=privileged`

```
kubectl label --overwrite namespace democratic-csi pod-security.kubernetes.io/enforce=privileged
```

### NVMe-oF

```bash
# Install nvme-cli tools (optional - tools are included in democratic-csi images)
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

## Server Prep

### TrueNAS SCALE 25.04+ (truenas-nfs, truenas-iscsi, truenas-nvmeof)

**Required**: TrueNAS SCALE 25.04 or later

These drivers use the WebSocket JSON-RPC 2.0 API exclusively - **no SSH required**.
All operations are performed via a persistent WebSocket connection to the
TrueNAS API endpoint (`wss://host/api/current`).

#### TrueNAS Configuration

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
   Ensure the appropriate services are enabled and running:

   **For NFS (`truenas-nfs`)**:
   - Navigate to **Sharing → NFS**
   - Ensure NFS service is enabled (will be started automatically when shares are created)
   - No pre-configuration needed - shares are created dynamically by the CSI driver

   **For iSCSI (`truenas-iscsi`)**:
   - Navigate to **Sharing → iSCSI**
   - Create Portal (default port 3260)
   - Create Initiator Group (allow appropriate initiators or leave empty for all)
   - Optionally configure CHAP authentication
   - Note the Portal Group ID and Initiator Group ID for configuration
   - Use TrueNAS UI or API to get IDs:
     ```bash
     # Get Portal IDs
     curl -H "Authorization: Bearer YOUR_API_KEY" \
       https://truenas.example.com/api/v2.0/iscsi/portal

     # Get Initiator Group IDs
     curl -H "Authorization: Bearer YOUR_API_KEY" \
       https://truenas.example.com/api/v2.0/iscsi/initiator
     ```
   - Targets and extents are created dynamically by the CSI driver

   **For NVMe-oF (`truenas-nvmeof`)**:
   - Navigate to **Sharing → NVMe-oF**
   - Ensure NVMe-oF service is configured
   - Subsystems and namespaces are created dynamically by the CSI driver
   - Configure transport (TCP recommended, port 4420)

4. **Network Configuration**
   - Ensure the TrueNAS system is reachable from your Kubernetes cluster
   - Open required ports in firewall:
     - **WebSocket API**: 443 (HTTPS) or 80 (HTTP)
     - **NFS**: 2049, 111, 20048
     - **iSCSI**: 3260 (default)
     - **NVMe-oF**: 4420 (TCP default)

5. **TLS/SSL Configuration**
   - For production use, configure a valid TLS certificate
   - For testing, you can use self-signed certificates with `allowInsecure: true`
   - Navigate to **Settings → Certificates** to manage certificates

#### Example Configuration

See the `examples/` directory for complete configuration examples:
- `examples/truenas-nfs.yaml` - NFS driver configuration
- `examples/truenas-iscsi.yaml` - iSCSI driver configuration
- `examples/truenas-nvmeof.yaml` - NVMe-oF driver configuration

Each example includes detailed comments explaining all configuration options.

## Helm Installation

```bash
# Add the democratic-csi helm repository
helm repo add democratic-csi https://democratic-csi.github.io/charts/
helm repo update

# Search for available charts
helm search repo democratic-csi/

# Copy and edit the appropriate values file from examples/
# - examples/truenas-nfs.yaml
# - examples/truenas-iscsi.yaml
# - examples/truenas-nvmeof.yaml

# Install NFS driver
helm upgrade --install \
  --values truenas-nfs.yaml \
  --namespace democratic-csi \
  --create-namespace \
  truenas-nfs democratic-csi/democratic-csi

# Install iSCSI driver
helm upgrade --install \
  --values truenas-iscsi.yaml \
  --namespace democratic-csi \
  --create-namespace \
  truenas-iscsi democratic-csi/democratic-csi

# Install NVMe-oF driver
helm upgrade --install \
  --values truenas-nvmeof.yaml \
  --namespace democratic-csi \
  --create-namespace \
  truenas-nvmeof democratic-csi/democratic-csi
```

### Non-Standard Kubelet Paths

Some distributions, such as `minikube` and `microk8s`, use non-standard
kubelet paths. In such cases, specify the kubelet host path during installation:

```bash
# microk8s example
microk8s helm upgrade --install \
  --values truenas-nfs.yaml \
  --set node.kubeletHostPath="/var/snap/microk8s/common/var/lib/kubelet" \
  --namespace democratic-csi \
  --create-namespace \
  truenas-nfs democratic-csi/democratic-csi
```

Common non-standard kubelet paths:
- **microk8s**: `/var/snap/microk8s/common/var/lib/kubelet`
- **pivotal**: `/var/vcap/data/kubelet`
- **k0s**: `/var/lib/k0s/kubelet`

### OpenShift

`democratic-csi` works with OpenShift. Set these parameters during helm installation:

```bash
# Required parameters
--set node.rbac.openshift.privileged=true
--set node.driver.localtimeHostPath=false

# Rarely needed, but may be required in special circumstances
--set controller.rbac.openshift.privileged=true
```

### Nomad

`democratic-csi` works with Nomad in a limited capacity. See the [Nomad docs](docs/nomad.md) for details.

## Multiple Deployments

You can install multiple deployments of any driver. Requirements:

- Use a unique helm release name for each deployment
- Set a unique `csiDriver.name` in the values file (per cluster)
- Use unique storage class names (per cluster)
- Use a unique parent dataset for each deployment
- For `iscsi` and `nvmeof`, asset/share names are global - use `nameTemplate`, `namePrefix`, and `nameSuffix` to avoid collisions

# Snapshot Support

Install the snapshot controller once per cluster:

**Option 1**: Use the democratic-csi chart
- https://github.com/democratic-csi/charts/tree/master/stable/snapshot-controller

**Option 2**: Use the upstream kubernetes-csi snapshotter
- https://github.com/kubernetes-csi/external-snapshotter/tree/master/client/config/crd
- https://github.com/kubernetes-csi/external-snapshotter/tree/master/deploy/kubernetes/snapshot-controller

Then install `democratic-csi` with `volumeSnapshotClasses` defined in your values file.

**Resources:**
- https://kubernetes.io/docs/concepts/storage/volume-snapshots/
- https://github.com/kubernetes-csi/external-snapshotter#usage

# Additional Resources

- [TrueNAS SCALE 25.04 API Documentation](https://api.truenas.com/v25.04.2/jsonrpc.html)
- [TrueNAS API Client Reference](https://github.com/truenas/api_client)
- [Kubernetes CSI Documentation](https://kubernetes-csi.github.io/docs/)
- [democratic-csi Charts Repository](https://github.com/democratic-csi/charts)
