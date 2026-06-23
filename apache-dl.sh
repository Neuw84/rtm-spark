#!/usr/bin/env bash
#
# Download an Apache dist artifact from the fastest available source.
#
# Usage: apache-dl <apache-dist-relative-path> <output-file>
#   e.g. apache-dl spark/spark-4.1.0/spark-4.1.0-bin-hadoop3.tgz /tmp/spark.tgz
#
# Strategy (first success wins):
#   1. Ask Apache's mirror selector (closer.lua) for the geographically closest
#      mirror and download from there  -> usually the fastest.
#   2. Fall back to the Apache CDN (dlcdn.apache.org) -> fast for current releases.
#   3. Fall back to archive.apache.org -> authoritative but slow; always has it.
set -euo pipefail

REL="${1:?usage: apache-dl <dist-relative-path> <output-file>}"
OUT="${2:?usage: apache-dl <dist-relative-path> <output-file>}"

fetch() {
  curl -fSL --retry 3 --retry-delay 2 --connect-timeout 15 -o "${OUT}" "$1"
}

# Ask the mirror selector for the preferred (closest) mirror base URL.
PREFERRED="$(curl -fsSL --connect-timeout 10 \
  "https://www.apache.org/dyn/closer.lua?path=${REL}&as_json=1" 2>/dev/null \
  | grep -o '"preferred"[^,]*' \
  | sed -E 's/.*:[[:space:]]*"([^"]*)".*/\1/' || true)"

URLS=()
[ -n "${PREFERRED:-}" ] && URLS+=("${PREFERRED%/}/${REL}")
URLS+=("https://dlcdn.apache.org/${REL}")
URLS+=("https://archive.apache.org/dist/${REL}")

for url in "${URLS[@]}"; do
  echo ">> apache-dl: trying ${url}"
  if fetch "${url}"; then
    echo ">> apache-dl: downloaded ${OUT} from ${url}"
    exit 0
  fi
  echo ">> apache-dl: failed ${url}"
done

echo "apache-dl: ERROR — could not download ${REL} from any source" >&2
exit 1
