#!/usr/bin/env bash
# Self-check for start-tsindex.sh dev-catalog mode: an existing config.toml
# keeps its [server] tuning, repos are appended once (even when already
# registered by an equivalent relative path), and concurrent starts do not
# duplicate entries. Set START_TSINDEX to test the goal-tsindex copy.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
script="${START_TSINDEX:-$here/start-tsindex.sh}"
tmp="$(cd "$(mktemp -d)" && pwd -P)"
trap 'rm -rf "$tmp"' EXIT

mkdir -p "$tmp/dev/alpha/.git" "$tmp/dev/beta/.git" "$tmp/dev/.tsindex" "$tmp/bin"
printf '#!/bin/sh\necho "stub $*"\n' > "$tmp/bin/tsindex"
chmod +x "$tmp/bin/tsindex"
# alpha is pre-registered with a *relative* path: the start script must
# recognise it as the same repo and not append an absolute duplicate.
printf '[root]\npath = "."\n\n[server]\nmax_response_chars = 1000\n\n[[repos]]\nname = "alpha"\npath = "alpha"\n' > "$tmp/dev/.tsindex/config.toml"

run() {
  (cd "$tmp/dev" && TSINDEX_BIN="$tmp/bin/tsindex" TSINDEX_DEV_ROOT="$tmp/dev" \
    TSINDEX_START_MODE=none bash "$script" >/dev/null)
}

run
run
# Concurrent starts must serialize on the lock, not double-append.
run & run & wait
config="$tmp/dev/.tsindex/config.toml"
grep -q 'max_response_chars = 1000' "$config" || { echo "FAIL: [server] tuning lost"; cat "$config"; exit 1; }
[[ "$(grep -c '^\[\[repos\]\]' "$config")" == 2 ]] || { echo "FAIL: repos duplicated or missing"; cat "$config"; exit 1; }
grep -q 'name = "alpha"' "$config" && grep -q 'name = "beta"' "$config" || { echo "FAIL: repo names"; cat "$config"; exit 1; }
grep -q '^path = "alpha"$' "$config" || { echo "FAIL: relative alpha entry rewritten"; cat "$config"; exit 1; }
grep -q "^path = \"$tmp/dev/beta\"$" "$config" || { echo "FAIL: beta not registered by absolute path"; cat "$config"; exit 1; }
[[ ! -e "$tmp/dev/.tsindex/config.lock" ]] || { echo "FAIL: lock dir left behind"; exit 1; }
echo "ok"
