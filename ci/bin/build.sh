#!/bin/bash

set -e
set -x

# Build TrueNAS CSI driver

go version

# Build the binary
CGO_ENABLED=0 go build -o bin/truenas-csi ./cmd/truenas-csi

echo "Build complete: bin/truenas-csi"
