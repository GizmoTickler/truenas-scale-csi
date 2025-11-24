#!/bin/bash

# TrueNAS Scale CSI Driver - Windows Container Release Script

set -e

echo "$GHCR_PASSWORD" | docker login ghcr.io -u "$GHCR_USERNAME" --password-stdin

export GHCR_ORG="gizmotickler"
export GHCR_PROJECT="truenas-scale-csi"
export GHCR_REPO="ghcr.io/${GHCR_ORG}/${GHCR_PROJECT}"

export MANIFEST_NAME="truenas-csi-combined:${IMAGE_TAG}"

if [[ -n "${IMAGE_TAG}" ]]; then
  # create local manifest to work with
  buildah manifest rm "${MANIFEST_NAME}" || true
  buildah manifest create "${MANIFEST_NAME}"

  # add all the existing linux data to the manifest
  buildah manifest add "${MANIFEST_NAME}" --all "${GHCR_REPO}:${IMAGE_TAG}"
  buildah manifest inspect "${MANIFEST_NAME}"

  # import pre-built images
  buildah pull docker-archive:truenas-csi-windows-ltsc2019.tar
  buildah pull docker-archive:truenas-csi-windows-ltsc2022.tar

  # add pre-built images to manifest
  buildah manifest add "${MANIFEST_NAME}" truenas-csi-windows:${GITHUB_RUN_ID}-ltsc2019
  buildah manifest add "${MANIFEST_NAME}" truenas-csi-windows:${GITHUB_RUN_ID}-ltsc2022
  buildah manifest inspect "${MANIFEST_NAME}"

  # push manifest
  buildah manifest push --all "${MANIFEST_NAME}" docker://${GHCR_REPO}:${IMAGE_TAG}

  # cleanup
  buildah manifest rm "${MANIFEST_NAME}" || true
else
  :
fi
