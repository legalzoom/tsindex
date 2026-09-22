#!/usr/bin/env bash
set -euo pipefail

tsindex_bin="${TSINDEX_BIN:-}"
if [[ -z "$tsindex_bin" ]]; then
  if command -v tsindex >/dev/null 2>&1; then
    tsindex_bin="$(command -v tsindex)"
  elif [[ -x "$HOME/.cargo/bin/tsindex" ]]; then
    tsindex_bin="$HOME/.cargo/bin/tsindex"
  else
    echo "tsindex binary not found. Install it or set TSINDEX_BIN=/path/to/tsindex." >&2
    exit 127
  fi
fi

dev_root="$(cd "${TSINDEX_DEV_ROOT:-$HOME/dev}" 2>/dev/null && pwd -P || true)"
cwd="$(pwd -P)"
mode="single"
target_root=""

if [[ -n "$dev_root" && "$cwd" == "$dev_root" ]]; then
  mode="dev-catalog"
  target_root="$dev_root"
else
  if git_root="$(git rev-parse --show-toplevel 2>/dev/null)"; then
    target_root="$(cd "$git_root" && pwd -P)"
  else
    target_root="$cwd"
  fi
fi

tsindex_dir="$target_root/.tsindex"
mkdir -p "$tsindex_dir"

# Escape a value for use inside a TOML basic (double-quoted) string.
toml_escape() {
  local s="$1"
  s="${s//\\/\\\\}"
  s="${s//\"/\\\"}"
  printf '%s' "$s"
}

# Canonical absolute form of a config.toml path entry (relative entries are
# relative to the dev root); empty when the directory no longer exists.
canonical_path() {
  local p="$1"
  # Undo toml_escape: \" -> " and \\ -> \ (a basic-string value).
  p="${p//\\\"/\"}"
  p="${p//\\\\/\\}"
  [[ "$p" == /* ]] || p="$target_root/$p"
  (cd "$p" 2>/dev/null && pwd -P) || true
}

# Create config.toml if missing, then append any immediate child git repo
# that is not yet registered. Existing content is copied byte-for-byte and
# only new [[repos]] blocks are added, so user tuning under
# [server]/[languages]/[ignore] survives reruns. Registration is
# serialized with a mkdir lock and written via temp file + mv so two
# concurrent starts cannot interleave or duplicate entries.
configure_dev_catalog() {
  local config="$tsindex_dir/config.toml"
  local lock="$tsindex_dir/config.lock"

  local tries=0
  until mkdir "$lock" 2>/dev/null; do
    tries=$((tries + 1))
    if (( tries > 100 )); then
      echo "could not acquire $lock after 10s (stale lock? rmdir it and retry)" >&2
      exit 1
    fi
    sleep 0.1
  done
  trap 'rmdir "$lock" 2>/dev/null' EXIT

  if [[ ! -f "$config" ]]; then
    {
      printf '[root]\n'
      printf 'path = "."\n\n'
      printf '[server]\n'
      printf 'http_port = 7337\n'
      printf 'query_timeout_ms = 2000\n'
      printf 'default_result_limit = 200\n'
    } > "$config"
  fi

  # Newline-delimited set of already-registered canonical paths, so a
  # relative or differently-spelled entry still dedupes.
  local registered=$'\n' raw
  while IFS= read -r raw; do
    registered+="$(canonical_path "$raw")"$'\n'
  done < <(sed -n 's/^path = "\(.*\)"[[:space:]]*$/\1/p' "$config")

  local found=0 additions=""
  while IFS= read -r repo_git_dir; do
    local repo_path repo_name
    repo_path="$(cd "$(dirname "$repo_git_dir")" && pwd -P)"
    repo_name="$(basename "$repo_path")"
    found=1
    if [[ "$registered" != *$'\n'"$repo_path"$'\n'* ]]; then
      additions+="$(printf '\n[[repos]]\nname = "%s"\npath = "%s"\n' \
        "$(toml_escape "$repo_name")" "$(toml_escape "$repo_path")")"$'\n'
      registered+="$repo_path"$'\n'
    fi
  done < <(find "$target_root" -mindepth 2 -maxdepth 2 -type d -name .git | sort)

  if [[ -n "$additions" ]]; then
    local tmp
    tmp="$(mktemp "$tsindex_dir/config.toml.XXXXXX")"
    cat "$config" > "$tmp"
    printf '%s' "$additions" >> "$tmp"
    mv -f "$tmp" "$config"
  fi

  # Release explicitly: `exec` in watch mode would skip the EXIT trap.
  rmdir "$lock" 2>/dev/null || true
  trap - EXIT

  if [[ "$found" -eq 0 ]]; then
    echo "No immediate git repos found under $target_root." >&2
    exit 1
  fi
}

configure_single_repo() {
  if [[ ! -f "$tsindex_dir/config.toml" ]]; then
    "$tsindex_bin" --root "$target_root" init "$target_root" >/dev/null
  fi
}

run_refresh() {
  local refresh_mode="${TSINDEX_START_MODE:-update}"

  case "$refresh_mode" in
    update)
      "$tsindex_bin" --root "$target_root" update
      ;;
    build)
      "$tsindex_bin" --root "$target_root" build
      ;;
    watch)
      echo "starting foreground watcher; stop it with Ctrl-C"
      exec "$tsindex_bin" --root "$target_root" watch
      ;;
    none|skip)
      echo "index refresh skipped"
      ;;
    *)
      echo "unsupported TSINDEX_START_MODE: $refresh_mode (expected update, build, watch, none, or skip)" >&2
      exit 2
      ;;
  esac
}

if [[ "$mode" == "dev-catalog" ]]; then
  configure_dev_catalog
else
  configure_single_repo
fi

echo
echo "tsindex target: $target_root"
echo "mode: $mode"
echo "refresh mode: ${TSINDEX_START_MODE:-update}"
echo "binary: $tsindex_bin"
echo "config: $tsindex_dir/config.toml"
echo "mcp command: $tsindex_bin --root $target_root serve --mcp"
printf 'watch command: TSINDEX_START_MODE=watch bash %q\n' "${BASH_SOURCE[0]}"

if [[ "$mode" == "dev-catalog" ]]; then
  echo
  echo "repos:"
  "$tsindex_bin" --root "$target_root" repos list
fi

echo
run_refresh
