#!/usr/bin/env bash
# Phase 3 §8 (SSD / tiered cache): one-time device layout for experiments.
#
#   root EBS (nvme0n1)         OS, jars, results, Spark scratch (unchanged, so the
#                              baseline stays comparable with earlier results)
#   instance store #1          /mnt/ssd-cache   dedicated to the cache tier
#   instance store #2          /mnt/ssd-spare   second device: replicate the device
#                              microbenchmark; becomes Spark scratch only if the
#                              per-run disk evidence shows scratch I/O is large
#
# Usage:  tools/track2/phase3_ssd_env.sh prepare | status | teardown
# Instance-store contents are lost on stop/terminate; nothing here is durable.
set -euo pipefail

CACHE_DEV="${CACHE_DEV:-/dev/nvme1n1}"
SPARE_DEV="${SPARE_DEV:-/dev/nvme2n1}"
CACHE_MNT="${CACHE_MNT:-/mnt/ssd-cache}"
SPARE_MNT="${SPARE_MNT:-/mnt/ssd-spare}"
OWNER="${SUDO_USER:-$(id -un)}"

root_disk() { lsblk -no pkname "$(findmnt -n -o SOURCE /)" | head -1; }

prep_one() {
  local dev="$1" mnt="$2"
  [[ -b "$dev" ]] || { echo "skip: $dev not found"; return 0; }
  if findmnt -rn -S "$dev" >/dev/null; then echo "ok: $dev already mounted"; return 0; fi
  if [[ "$(basename "$dev")" == "$(root_disk)" ]]; then
    echo "refuse: $dev is the root disk" >&2; return 1
  fi
  if [[ "$(lsblk -dno MODEL "$dev" | xargs)" != *"Instance Storage"* ]]; then
    echo "refuse: $dev is not an EC2 instance-store device" >&2; return 1
  fi
  if blkid "$dev" >/dev/null 2>&1; then
    echo "refuse: $dev already has a filesystem signature; wipe it by hand first" >&2; return 1
  fi
  mkfs.xfs -q "$dev"
  mkdir -p "$mnt"
  mount -o noatime "$dev" "$mnt"
  chown "$OWNER":"$OWNER" "$mnt"
  echo "ready: $dev -> $mnt"
}

case "${1:-}" in
  prepare)
    command -v fio >/dev/null || dnf install -y fio
    prep_one "$CACHE_DEV" "$CACHE_MNT"
    prep_one "$SPARE_DEV" "$SPARE_MNT"
    ;;
  status)
    lsblk -o NAME,SIZE,MODEL,FSTYPE,MOUNTPOINT
    df -h "$CACHE_MNT" "$SPARE_MNT" / 2>/dev/null || true
    ;;
  teardown)
    umount "$CACHE_MNT" "$SPARE_MNT" 2>/dev/null || true
    ;;
  *)
    echo "usage: $0 prepare|status|teardown" >&2; exit 2 ;;
esac
