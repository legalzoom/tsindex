//! Ignore inputs that live outside the source-directory registration walk.
//!
//! Watching their parents observes edits, atomic replacement, and removal
//! without recursively registering Git metadata or the user's home directory.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use crate::index::Workspace;

#[derive(Default)]
pub(crate) struct IgnoreInputs {
    repositories: HashSet<PathBuf>,
    files: HashMap<PathBuf, HashSet<PathBuf>>,
}

impl IgnoreInputs {
    /// Reload indirections as well as rule paths: worktrees can share excludes,
    /// and global configuration can select a different file during a session.
    pub fn refresh(&mut self, workspaces: &[Workspace]) {
        self.repositories.retain(|dir| {
            dir.is_dir()
                && workspaces
                    .iter()
                    .any(|workspace| dir.starts_with(&workspace.root))
        });
        self.files.clear();
        let global = global_inputs();
        for workspace in workspaces {
            for file in &global {
                self.add(file, &workspace.root);
            }
            for file in &workspace.ignore {
                // Match WalkBuilder::add_ignore: relative paths use process cwd.
                self.add(Path::new(file), &workspace.root);
            }
            for dir in workspace.root.ancestors() {
                for file in repository_inputs(dir) {
                    self.add(&file, &workspace.root);
                }
            }
        }
        for dir in self.repositories.clone() {
            for file in repository_inputs(&dir) {
                self.add(&file, &dir);
            }
        }
    }

    /// Called only for eligible source directories. This includes unconfigured
    /// nested Git repositories without adding another full-tree discovery walk.
    pub fn discover_repository(&mut self, dir: &Path) -> HashSet<PathBuf> {
        if self.repositories.contains(dir) {
            return HashSet::new();
        }
        let files = repository_inputs(dir);
        if files.is_empty() {
            return HashSet::new();
        }
        self.repositories.insert(dir.to_path_buf());
        let mut probes = HashSet::new();
        for file in files {
            self.add(&file, dir);
            if let Some(parent) = existing_parent(&file) {
                probes.insert(parent);
            }
            if let Ok(real) = file.canonicalize()
                && let Some(parent) = existing_parent(&real)
            {
                probes.insert(parent);
            }
        }
        probes
    }

    pub fn retain_repositories(&mut self, keep: impl FnMut(&PathBuf) -> bool) {
        self.repositories.retain(keep);
    }

    pub fn directories(&self) -> HashSet<PathBuf> {
        self.files
            .keys()
            .filter_map(|file| existing_parent(file))
            .collect()
    }

    pub fn changed_targets(&self, changed: &[PathBuf]) -> HashSet<PathBuf> {
        let mut targets = HashSet::new();
        let changed: HashSet<_> = changed.iter().map(PathBuf::as_path).collect();
        for file in self
            .files
            .keys()
            .filter(|file| file.ancestors().any(|ancestor| changed.contains(ancestor)))
        {
            targets.extend(self.files[file].iter().cloned());
        }
        targets
    }

    pub fn observe_repository_markers(&mut self, changed: &[PathBuf]) {
        // Creating Git metadata in an already-watched source directory does
        // not trigger a source registration pass: `.git` is normally filtered.
        // Remember its owner before refresh so its new rules acquire probes.
        self.repositories.extend(changed.iter().filter_map(|path| {
            path.ancestors()
                .find(|path| path.file_name().is_some_and(|name| name == ".git"))
                .and_then(Path::parent)
                .map(Path::to_path_buf)
        }));
    }

    fn add(&mut self, file: &Path, target: &Path) {
        let Some(file) = normalized_path(file) else {
            return;
        };
        // A symlinked rule file is read through its target. Observe both the
        // target's edits and replacement of the original link itself.
        if let Ok(real) = file.canonicalize() {
            self.files
                .entry(real)
                .or_default()
                .insert(target.to_path_buf());
        }
        self.files
            .entry(file)
            .or_default()
            .insert(target.to_path_buf());
    }
}

pub(crate) fn existing_parent(path: &Path) -> Option<PathBuf> {
    path.parent()?
        .ancestors()
        .find_map(|parent| parent.canonicalize().ok())
}

fn normalized_path(path: &Path) -> Option<PathBuf> {
    let path = std::path::absolute(path).ok()?;
    Some(
        path.parent()
            .and_then(|parent| parent.canonicalize().ok())
            .zip(path.file_name())
            .map_or_else(|| path.clone(), |(parent, name)| parent.join(name)),
    )
}

fn first_line(path: &Path) -> Option<String> {
    BufReader::new(fs::File::open(path).ok()?)
        .lines()
        .next()?
        .ok()
}

/// Follow the same `.git`/`commondir` inputs as pinned ignore 0.4.25. In
/// particular, it does not use GIT_DIR/GIT_COMMON_DIR or run the Git CLI.
fn repository_inputs(dir: &Path) -> Vec<PathBuf> {
    let marker = dir.join(".git");
    let Ok(kind) = fs::metadata(&marker) else {
        return Vec::new();
    };
    let mut files = vec![marker.clone()];
    if kind.is_dir() {
        files.push(marker.join("info/exclude"));
    } else if kind.is_file()
        && let Some(line) = first_line(&marker)
        && let Some(gitdir) = line.strip_prefix("gitdir: ")
    {
        let gitdir = Path::new(gitdir);
        let common_file = gitdir.join("commondir");
        files.push(common_file.clone());
        if let Some(common) = first_line(&common_file) {
            let common = if common.starts_with('.') {
                gitdir.join(common)
            } else {
                PathBuf::from(common)
            };
            files.push(common.join("info/exclude"));
        }
    }
    files
}

#[allow(deprecated)] // Match the pinned ignore crate's home-directory lookup.
fn global_inputs() -> Vec<PathBuf> {
    let home = std::env::home_dir();
    let config = std::env::var_os("XDG_CONFIG_HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .or_else(|| home.as_ref().map(|home| home.join(".config")));
    let mut files = Vec::new();
    if let Some(home) = home {
        files.push(home.join(".gitconfig"));
    }
    if let Some(config) = config {
        files.push(config.join("git/config"));
    }
    if let Some(excludes) = ignore::gitignore::gitconfig_excludes_path() {
        files.push(excludes);
    }
    files
}
