# Build stage
FROM golang:1.22-bookworm AS builder

WORKDIR /build

# Copy go mod files first for better caching
COPY go.mod go.sum* ./
RUN go mod download || true

# Copy source code
COPY . .

# Build the binary
ARG TARGETARCH
ARG VERSION=dev
ARG COMMIT=unknown
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build \
    -ldflags="-w -s -X main.Version=${VERSION} -X main.GitCommit=${COMMIT}" \
    -o truenas-csi ./cmd/truenas-csi

######################
# Runtime image
######################
FROM debian:12-slim

LABEL org.opencontainers.image.source="https://github.com/GizmoTickler/truenas-scale-csi"
LABEL org.opencontainers.image.url="https://github.com/GizmoTickler/truenas-scale-csi"
LABEL org.opencontainers.image.licenses="MIT"
LABEL org.opencontainers.image.title="TrueNAS Scale CSI Driver"
LABEL org.opencontainers.image.description="Kubernetes CSI driver for TrueNAS SCALE"

ENV DEBIAN_FRONTEND=noninteractive

# Install runtime dependencies
# - nfs-common: NFS client for mounting NFS shares
# - open-iscsi: iSCSI initiator for block storage
# - nvme-cli: NVMe-oF client for NVMe over fabrics
# - e2fsprogs, xfsprogs, btrfs-progs: filesystem tools for formatting
# - util-linux: mount, findmnt, blkid utilities
# - ca-certificates: for HTTPS connections to TrueNAS API
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    nfs-common \
    open-iscsi \
    nvme-cli \
    e2fsprogs \
    xfsprogs \
    btrfs-progs \
    util-linux \
    udev \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /var/cache/apt/archives/*

# Copy binary from builder
COPY --from=builder /build/truenas-csi /usr/local/bin/truenas-csi

# Copy helper scripts for host command execution (iSCSI, mount operations)
COPY docker/iscsiadm /usr/local/bin/iscsiadm-wrapper
COPY docker/mount /usr/local/bin/mount-wrapper
COPY docker/umount /usr/local/bin/umount-wrapper

# Create directories for CSI socket and config
RUN mkdir -p /csi /etc/truenas-csi

WORKDIR /

ENTRYPOINT ["/usr/local/bin/truenas-csi"]
