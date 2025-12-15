#!/usr/bin/env bash
set -euo pipefail

: "${URL:?ERROR: URL is required. Usage: URL=<zip_url> make download-landing}"
DEST="${DEST:-data}"

mkdir -p "$DEST"

tmp="$(mktemp -t landing.XXXXXX.zip)"
trap 'rm -f "$tmp"' EXIT

echo "[download_landing] Downloading -> $tmp"
if command -v curl >/dev/null 2>&1; then
  curl -fL -C - --retry 20 --retry-delay 2 --retry-all-errors -o "$tmp" "$URL"
elif command -v wget >/dev/null 2>&1; then
  wget -c -O "$tmp" "$URL"
else
  echo "ERROR: need curl or wget" >&2
  exit 1
fi

echo "[download_landing] Extracting -> $DEST"
if command -v unzip >/dev/null 2>&1; then
  unzip -q "$tmp" -d "$DEST"
else
  echo "ERROR: need unzip" >&2
  exit 1
fi

echo "[download_landing] Done."
