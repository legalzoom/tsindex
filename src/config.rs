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
        let config_dir = root.join(".tsindex");
        fs::create_dir_all(&config_dir)
            .with_context(|| format!("failed to create {}", config_dir.display()))?;
        let path = config_dir.join("config.toml");
        let raw = toml::to_string_pretty(self).context("failed to serialize config")?;
        fs::write(&path, raw).with_context(|| format!("failed to write {}", path.display()))?;
        Ok(path)
    }
}

pub fn config_path(root: &Path) -> PathBuf {
    root.join(".tsindex").join("config.toml")
}

pub fn db_path(root: &Path) -> PathBuf {
    root.join(".tsindex").join("index.db")
}
