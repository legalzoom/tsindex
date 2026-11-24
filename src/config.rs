use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RootConfig {
    #[serde(default = "default_root_path")]
    pub path: String,
}

fn default_root_path() -> String {
    ".".to_string()
}

impl Default for RootConfig {
    fn default() -> Self {
        Self {
            path: default_root_path(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LanguagesConfig {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub include: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IgnoreConfig {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extra: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RepoConfig {
    pub name: String,
    pub path: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub languages: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ignore: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    #[serde(default)]
    pub mcp_port: u16,
    #[serde(default = "default_http_port")]
    pub http_port: u16,
    #[serde(default = "default_query_timeout_ms")]
    pub query_timeout_ms: u64,
    #[serde(default = "default_worktree_refresh_ms")]
    pub worktree_refresh_ms: u64,
    #[serde(default = "default_result_limit")]
    pub default_result_limit: usize,
    #[serde(default = "default_max_result_limit")]
    pub max_result_limit: usize,
    #[serde(default = "default_body_line_limit")]
    pub default_body_line_limit: usize,
    #[serde(default = "default_max_body_line_limit")]
    pub max_body_line_limit: usize,
    #[serde(default = "default_capture_char_limit")]
    pub capture_char_limit: usize,
    /// Hard ceiling on the serialized byte length of a `find_references`
    /// response. When the page selected by `limit`/`offset` would serialize
    /// larger than this, the server shrinks the kept reference count until
    /// it fits (adjusting `returned`/`next_offset` accordingly) so the
    /// payload can never be rejected by the MCP client's tool-result cap.
    /// `0` disables the budget — page size is then governed by `limit` alone.
    #[serde(default = "default_max_response_chars")]
    pub max_response_chars: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            mcp_port: 0,
            http_port: default_http_port(),
            query_timeout_ms: default_query_timeout_ms(),
            worktree_refresh_ms: default_worktree_refresh_ms(),
            default_result_limit: default_result_limit(),
            max_result_limit: default_max_result_limit(),
            default_body_line_limit: default_body_line_limit(),
            max_body_line_limit: default_max_body_line_limit(),
            capture_char_limit: default_capture_char_limit(),
            max_response_chars: default_max_response_chars(),
        }
    }
}

fn default_http_port() -> u16 {
    7337
}

fn default_query_timeout_ms() -> u64 {
    2_000
}

fn default_worktree_refresh_ms() -> u64 {
    2_000
}

// Page sizes are bounded by the MCP client's tool-result cap, not by taste.
// Measured against Claude Code on a 1,607-reference identifier at
// `snippet_lines: 0` (~134 chars/ref): 200 refs = 28 KB accepted, 300 = 45 KB
// accepted, 500 = 67 KB HARD-REJECTED ("exceeds maximum allowed tokens") and
// spilled to a file, which makes the call useless. Keep the default well
// inside the accepted range and the ceiling at the largest proven-safe page.
// NOTE: `limit` alone is not a guarantee — `snippet_lines: 3` roughly doubles
// bytes per reference, so 300 refs can still exceed the cap. A byte budget on
// the serialized response is the only real fix.
fn default_result_limit() -> usize {
    200
}

fn default_max_result_limit() -> usize {
    300
}

fn default_body_line_limit() -> usize {
    120
}

fn default_max_body_line_limit() -> usize {
    1_000
}

fn default_capture_char_limit() -> usize {
    2_000
}

// Byte budget for a serialized `find_references` response. `limit` alone is
// not a sufficient guard against the MCP client's tool-result cap because
// `snippet_lines` roughly doubles chars per reference, so even a 300-ref page
// can exceed it. 45,000 sits just above the largest proven-accepted payload
// (39,325 chars at 300 refs / snippet_lines=0) and well under the rejected one
// (67,278 at 500 refs). A configured value of `0` means UNLIMITED — the budget
// is disabled and page size is governed by `limit` alone.
fn default_max_response_chars() -> usize {
    45_000
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TsIndexConfig {
    #[serde(default)]
    pub root: RootConfig,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub repos: Vec<RepoConfig>,
    #[serde(default)]
    pub languages: LanguagesConfig,
    #[serde(default)]
    pub ignore: IgnoreConfig,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub grammars: BTreeMap<String, String>,
    #[serde(default)]
    pub server: ServerConfig,
}

impl TsIndexConfig {
    pub fn load(root: &Path) -> Result<Self> {
        let path = config_path(root);
        let raw = fs::read_to_string(&path)
            .with_context(|| format!("failed to read config {}", path.display()))?;
        let config: TsIndexConfig = toml::from_str(&raw)
            .with_context(|| format!("failed to parse config {}", path.display()))?;
        Ok(config)
    }

    pub fn load_or_default(root: &Path) -> Result<Self> {
        let path = config_path(root);
        if path.exists() {
            Self::load(root)
        } else {
            Ok(Self::default())
        }
    }

    pub fn write(&self, root: &Path) -> Result<PathBuf> {
        use std::io::Write;
        use std::sync::atomic::{AtomicU64, Ordering};

        static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

        let config_dir = root.join(".tsindex");
        fs::create_dir_all(&config_dir)
            .with_context(|| format!("failed to create {}", config_dir.display()))?;
        let path = config_dir.join("config.toml");
        let target = match fs::symlink_metadata(&path) {
            Ok(meta) if meta.file_type().is_symlink() => {
                let resolved = crate::maintenance::canonical_target(&path)
                    .with_context(|| format!("failed to resolve {}", path.display()))?;
                if fs::symlink_metadata(&resolved)
                    .is_ok_and(|target_meta| target_meta.file_type().is_symlink())
                {
                    anyhow::bail!(
                        "failed to resolve {}: too many levels of symbolic links",
                        path.display()
                    );
                }
                resolved
            }
            Ok(_) => path.clone(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => path.clone(),
            Err(error) => {
                return Err(error).with_context(|| format!("failed to write {}", path.display()));
            }
        };
        let existing_permissions = match fs::metadata(&target) {
            Ok(meta) if meta.is_dir() => {
                anyhow::bail!("failed to write {}: target is a directory", path.display());
            }
            Ok(meta) => Some(meta.permissions()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => {
                return Err(error).with_context(|| format!("failed to write {}", path.display()));
            }
        };
        let target_dir = target
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .ok_or_else(|| anyhow::anyhow!("failed to write {}: missing parent", path.display()))?;
        let target_name = target
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .ok_or_else(|| {
                anyhow::anyhow!("failed to write {}: missing file name", path.display())
            })?;
        let raw = toml::to_string_pretty(self).context("failed to serialize config")?;
        // Publish atomically so concurrent readers (e.g. managed clients joining
        // a linked checkout during preparation) never see an empty or partial file.
        let pid = std::process::id();
        let (temporary, mut file) = loop {
            let seq = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
            let candidate = target_dir.join(format!(".{target_name}.{pid}.{seq}.tmp"));
            let mut options = fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            match options.open(&candidate) {
                Ok(file) => break (candidate, file),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("failed to write {}", path.display()));
                }
            }
        };
        let result = (|| -> Result<()> {
            file.write_all(raw.as_bytes())?;
            file.sync_all()?;
            if let Some(permissions) = existing_permissions {
                file.set_permissions(permissions)?;
            }
            drop(file);
            match fs::rename(&temporary, &target) {
                Ok(()) => Ok(()),
                #[cfg(windows)]
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    let _ = fs::remove_file(&target);
                    fs::rename(&temporary, &target)?;
                    Ok(())
                }
                Err(error) => Err(error.into()),
            }
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result.with_context(|| format!("failed to write {}", path.display()))?;
        Ok(path)
    }

    /// Any configured ignore file (`[ignore].extra` or `[[repos]].ignore`) is
    /// passed to `ignore::WalkBuilder::add_ignore`, which opens relative paths
    /// and anchors patterns against the process working directory.
    pub fn has_explicit_ignore_files(&self) -> bool {
        !self.ignore.extra.is_empty() || self.repos.iter().any(|repo| !repo.ignore.is_empty())
    }
}

pub fn config_path(root: &Path) -> PathBuf {
    root.join(".tsindex").join("config.toml")
}

pub fn db_path(root: &Path) -> PathBuf {
    root.join(".tsindex").join("index.db")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use std::thread;

    #[test]
    fn config_write_publishes_atomically_under_concurrent_reads() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let root = dir.path().to_path_buf();
        let mut config = TsIndexConfig::default();
        config.repos.push(RepoConfig {
            name: "alpha".into(),
            path: ".".into(),
            languages: vec!["rust".into(), "python".into()],
            ignore: vec!["custom.ignore".into()],
        });
        config.write(&root)?;

        let done = Arc::new(AtomicBool::new(false));
        let reader_done = Arc::clone(&done);
        let reader_root = root.clone();
        let reader = thread::spawn(move || -> Result<usize> {
            let mut reads = 0usize;
            while !reader_done.load(Ordering::Relaxed) || reads < 50 {
                let loaded = TsIndexConfig::load(&reader_root)?;
                anyhow::ensure!(
                    loaded.repos.len() == 1 && loaded.repos[0].name == "alpha",
                    "observed partial or default config during concurrent write"
                );
                reads += 1;
            }
            Ok(reads)
        });

        for _ in 0..80 {
            config.write(&root)?;
        }
        done.store(true, Ordering::Relaxed);
        let reads = reader.join().expect("reader thread panicked")?;
        assert!(reads >= 50);

        let leftovers: Vec<_> = fs::read_dir(root.join(".tsindex"))?
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_name().to_string_lossy().ends_with(".tmp"))
            .collect();
        assert!(
            leftovers.is_empty(),
            "unexpected temporary config files left behind: {leftovers:?}"
        );
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn config_write_preserves_permissions_and_follows_symlinks() -> Result<()> {
        use std::os::unix::fs::{PermissionsExt, symlink};

        // 1. New config files are created privately and existing modes are preserved.
        let dir = tempfile::tempdir()?;
        let root = dir.path().to_path_buf();
        let mut config = TsIndexConfig::default();
        config.repos.push(RepoConfig {
            name: "alpha".into(),
            path: ".".into(),
            languages: vec!["rust".into()],
            ignore: vec![],
        });
        let path = config.write(&root)?;
        assert_eq!(fs::metadata(&path)?.permissions().mode() & 0o777, 0o600);

        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
        config.repos.push(RepoConfig {
            name: "beta".into(),
            path: "beta".into(),
            languages: vec![],
            ignore: vec![],
        });
        config.write(&root)?;
        assert_eq!(fs::metadata(&path)?.permissions().mode() & 0o777, 0o600);

        fs::set_permissions(&path, fs::Permissions::from_mode(0o640))?;
        config.write(&root)?;
        assert_eq!(fs::metadata(&path)?.permissions().mode() & 0o777, 0o640);

        // 2. Existing config symlink updates the resolved target and preserves
        // both the symlink and the target's mode.
        let linked_root = dir.path().join("linked-root");
        let shared_dir = dir.path().join("shared");
        fs::create_dir_all(linked_root.join(".tsindex"))?;
        fs::create_dir_all(&shared_dir)?;
        let shared_target = shared_dir.join("team-config.toml");
        fs::write(&shared_target, "")?;
        fs::set_permissions(&shared_target, fs::Permissions::from_mode(0o600))?;
        let link_path = linked_root.join(".tsindex").join("config.toml");
        symlink(&shared_target, &link_path)?;

        let written = config.write(&linked_root)?;
        assert_eq!(written, link_path);
        assert!(
            fs::symlink_metadata(&link_path)?.file_type().is_symlink(),
            "config.toml symlink was replaced instead of updating its target"
        );
        assert_eq!(fs::read_link(&link_path)?, shared_target);
        assert_eq!(
            fs::metadata(&shared_target)?.permissions().mode() & 0o777,
            0o600
        );
        let target_raw = fs::read_to_string(&shared_target)?;
        let target_parsed: TsIndexConfig = toml::from_str(&target_raw)?;
        assert_eq!(target_parsed.repos.len(), 2);

        // 3. Dangling config symlink publishes to its resolved target without
        // replacing the symlink.
        let dangling_root = dir.path().join("dangling-root");
        fs::create_dir_all(dangling_root.join(".tsindex"))?;
        let later_target = shared_dir.join("later-config.toml");
        let dangling_link = dangling_root.join(".tsindex").join("config.toml");
        symlink(&later_target, &dangling_link)?;
        config.write(&dangling_root)?;
        assert!(
            fs::symlink_metadata(&dangling_link)?
                .file_type()
                .is_symlink()
        );
        assert!(later_target.is_file());
        assert_eq!(
            fs::metadata(&later_target)?.permissions().mode() & 0o777,
            0o600
        );

        // 4. Symlink loop fails cleanly without replacing the symlink.
        let loop_root = dir.path().join("loop-root");
        fs::create_dir_all(loop_root.join(".tsindex"))?;
        let loop_a = loop_root.join(".tsindex").join("config.toml");
        let loop_b = loop_root.join(".tsindex").join("loop-b.toml");
        symlink(&loop_b, &loop_a)?;
        symlink(&loop_a, &loop_b)?;
        assert!(config.write(&loop_root).is_err());
        assert!(fs::symlink_metadata(&loop_a)?.file_type().is_symlink());

        Ok(())
    }
}
