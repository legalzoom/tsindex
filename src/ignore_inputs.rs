//! Ignore inputs that live outside the source-directory registration walk.
//!
//! Watching their parents observes edits, atomic replacement, and removal
//! without recursively registering Git metadata or the user's home directory.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use crate::index::{IGNORE_FILE_NAMES, Workspace};

#[derive(Default)]
pub(crate) struct IgnoreInputs {
    source_directories: HashSet<PathBuf>,
    files: HashMap<PathBuf, HashSet<PathBuf>>,
}

impl IgnoreInputs {
    /// Reload indirections as well as rule paths: worktrees can share excludes,
    /// and global configuration can select a different file during a session.
    pub fn refresh(&mut self, workspaces: &[Workspace]) {
        self.source_directories.retain(|dir| {
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
                // The walker reads directory-scoped rules above its root too.
                // Keep absent paths so creating/removing an inherited rule
                // refreshes every governed workspace through parent probes.
                for name in IGNORE_FILE_NAMES {
                    self.add(&dir.join(name), &workspace.root);
                }
                for file in repository_inputs(dir) {
                    self.add(&file, &workspace.root);
                }
            }
        }
        for dir in self.source_directories.clone() {
            let files = directory_inputs(&dir);
            // Ordinary source directories are already watched. Once their
            // last special input disappears, a later rule creation there
            // can rediscover its inputs without retaining a stale input set.
            if files.is_empty() {
                self.source_directories.remove(&dir);
            }
            for file in files {
                self.add(&file, &dir);
            }
        }
    }

    /// Called only for eligible source directories. Discover Git metadata and
    /// rule-file targets without traversing directory symlinks or another tree.
    pub fn discover_directory(&mut self, dir: &Path) -> HashSet<PathBuf> {
        // Reconciliation passes reload these local inputs: a rule link may
        // have changed before this directory's first probe was installed.
        // This avoids refreshing every workspace's metadata per directory.
        let files = directory_inputs(dir);
        if files.is_empty() {
            return HashSet::new();
        }
        self.source_directories.insert(dir.to_path_buf());
        let mut probes = HashSet::new();
        for file in files {
            for input in self.add(&file, dir) {
                if let Some(parent) = existing_parent(&input) {
                    probes.insert(parent);
                }
            }
        }
        probes
    }

    pub fn retain_directories(&mut self, keep: impl FnMut(&PathBuf) -> bool) {
        self.source_directories.retain(keep);
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
        self.source_directories
            .extend(changed.iter().filter_map(|path| {
                path.ancestors()
                    .find(|path| path.file_name().is_some_and(|name| name == ".git"))
                    .and_then(Path::parent)
                    .map(Path::to_path_buf)
            }));
    }

    fn add(&mut self, file: &Path, target: &Path) -> Vec<PathBuf> {
        let paths = input_paths(file);
        for file in &paths {
            self.files
                .entry(file.clone())
                .or_default()
                .insert(target.to_path_buf());
        }
        paths
    }
}

pub(crate) fn existing_parent(path: &Path) -> Option<PathBuf> {
    path.parent()?
        .ancestors()
        .find_map(|parent| parent.canonicalize().ok())
}

fn input_paths(path: &Path) -> Vec<PathBuf> {
    let Ok(mut path) = std::path::absolute(path) else {
        return Vec::new();
    };
    let mut paths = Vec::new();
    let mut seen = HashSet::new();
    // Resolve one link at a time, including directory components. A single
    // parent canonicalization would erase routes such as current/rules, so
    // repointing current would never refresh its governed sources. Inspect
    // only path components, never recurse through a symlinked directory tree.
    // Include the final file after at most 40 links, the Linux hop limit.
    for _ in 0..=40 {
        // The same link can appear twice in a finite route with different
        // remaining components. Only a repeated complete resolution state is
        // a cycle; the hop bound also covers cycles whose paths keep growing.
        if !seen.insert(path.clone()) {
            break;
        }
        let mut resolved = PathBuf::new();
        let mut components = path.components();
        let mut indirection = None;
        while let Some(component) = components.next() {
            resolved.push(component);
            if component == std::path::Component::ParentDir {
                // Resolve '..' only when its prefix exists. A dangling route
                // must retain its missing ancestor's recreation notification.
                if let Ok(parent) = resolved.canonicalize() {
                    resolved = parent;
                }
            }
            if let Ok(link) = fs::read_link(&resolved) {
                let next = if link.is_absolute() {
                    link
                } else {
                    resolved.parent().unwrap().join(link)
                };
                indirection = Some(next.join(components.as_path()));
                break;
            }
        }
        paths.push(resolved);
        let Some(next) = indirection else {
            break;
        };
        path = next;
    }
    paths
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

fn directory_inputs(dir: &Path) -> Vec<PathBuf> {
    let mut files = repository_inputs(dir);
    files.extend(
        IGNORE_FILE_NAMES
            .iter()
            .map(|name| dir.join(name))
            .filter(|file| fs::symlink_metadata(file).is_ok()),
    );
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

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;

    #[test]
    fn directory_rule_symlinks_track_targets_and_link_replacements() -> anyhow::Result<()> {
        for filename in IGNORE_FILE_NAMES {
            let dir = tempfile::tempdir()?;
            let base = dir.path().canonicalize()?;
            let root = base.join("repo");
            let governed = root.join("nested");
            let external = base.join("rules");
            fs::create_dir_all(&governed)?;
            fs::create_dir_all(&external)?;
            let target = external.join("first");
            fs::write(&target, "later/\n")?;
            let rules = governed.join(filename);
            symlink("../../rules/first", &rules)?;
            let workspace = Workspace {
                name: "repo".to_string(),
                root,
                languages: Vec::new(),
                ignore: Vec::new(),
            };
            let mut inputs = IgnoreInputs::default();
            inputs.refresh(std::slice::from_ref(&workspace));
            assert!(inputs.discover_directory(&governed).contains(&external));
            assert_eq!(
                inputs.changed_targets(std::slice::from_ref(&target)),
                HashSet::from([governed.clone()])
            );
            // Refresh reloads link indirections rather than keeping a startup
            // snapshot, and retains a missing target's future creation route.
            fs::remove_file(&target)?;
            inputs.refresh(std::slice::from_ref(&workspace));
            assert!(inputs.directories().contains(&external));
            assert_eq!(
                inputs.changed_targets(std::slice::from_ref(&target)),
                HashSet::from([governed.clone()])
            );
            fs::remove_file(&rules)?;
            let new_target = external.join("second");
            symlink("../../rules/second", &rules)?;
            inputs.refresh(std::slice::from_ref(&workspace));
            assert!(inputs.changed_targets(&[target]).is_empty());
            assert_eq!(
                inputs.changed_targets(&[new_target]),
                HashSet::from([governed])
            );
        }
        Ok(())
    }

    #[test]
    fn symlink_rule_chains_track_indirections_without_looping() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let base = dir.path().canonicalize()?;
        let root = base.join("repo");
        let links = base.join("links");
        let targets = base.join("targets");
        for path in [&root, &links, &targets] {
            fs::create_dir(path)?;
        }
        let rules = root.join(".ignore");
        let intermediate = links.join("first");
        let missing = targets.join("rules");
        symlink("../links/first", &rules)?;
        symlink("../targets/rules", &intermediate)?;
        let workspace = Workspace {
            name: "repo".to_string(),
            root: root.clone(),
            languages: Vec::new(),
            ignore: Vec::new(),
        };
        let mut inputs = IgnoreInputs::default();
        inputs.refresh(std::slice::from_ref(&workspace));
        assert!(inputs.directories().contains(&links));
        assert!(inputs.directories().contains(&targets));
        for path in [&intermediate, &missing] {
            assert_eq!(
                inputs.changed_targets(std::slice::from_ref(path)),
                HashSet::from([root.clone()])
            );
        }
        // A cyclic rule file must not hang input discovery or retain a target
        // that its link chain no longer reaches.
        fs::remove_file(&intermediate)?;
        symlink("first", &intermediate)?;
        inputs.refresh(std::slice::from_ref(&workspace));
        assert!(inputs.changed_targets(&[missing]).is_empty());
        assert_eq!(
            inputs.changed_targets(&[intermediate]),
            HashSet::from([root])
        );
        Ok(())
    }

    #[test]
    fn rule_directory_components_preserve_nested_links_and_dangling_routes() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let base = dir.path().canonicalize()?;
        let root = base.join("repo");
        let external = base.join("external");
        let middle = external.join("middle");
        let old = external.join("old");
        let new = external.join("new");
        for path in [&root, &middle, &old.join("sub"), &new.join("sub")] {
            fs::create_dir_all(path)?;
        }
        fs::write(old.join("rules"), "later/\n")?;
        let route = external.join("current");
        let intermediate = middle.join("current");
        symlink("middle/current", &route)?;
        symlink("../old", &intermediate)?;
        let rules = root.join(".ignore");
        symlink(route.join("sub/../rules"), &rules)?;
        let workspace = Workspace {
            name: "repo".to_string(),
            root: root.clone(),
            languages: Vec::new(),
            ignore: Vec::new(),
        };
        let mut inputs = IgnoreInputs::default();
        inputs.refresh(std::slice::from_ref(&workspace));
        for path in [&route, &intermediate, &old.join("rules")] {
            assert_eq!(
                inputs.changed_targets(std::slice::from_ref(path)),
                HashSet::from([root.clone()])
            );
        }
        for path in [&external, &middle, &old] {
            assert!(inputs.directories().contains(path));
        }
        fs::remove_file(&intermediate)?;
        symlink("../new", &intermediate)?;
        inputs.refresh(std::slice::from_ref(&workspace));
        assert!(inputs.changed_targets(&[old.join("rules")]).is_empty());
        assert_eq!(
            inputs.changed_targets(&[new.join("rules")]),
            HashSet::from([root.clone()])
        );
        assert!(inputs.directories().contains(&new));

        // Cyclic directory routes retain the replaceable link's parent, but
        // must not hang discovery or keep a target they no longer reach.
        fs::remove_file(&route)?;
        symlink("current", &route)?;
        inputs.refresh(std::slice::from_ref(&workspace));
        assert!(inputs.changed_targets(&[new.join("rules")]).is_empty());
        assert_eq!(inputs.changed_targets(&[route]), HashSet::from([root]));
        assert!(inputs.directories().contains(&external));
        Ok(())
    }

    #[test]
    fn rule_route_can_visit_the_same_directory_link_with_different_tails() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let base = dir.path().canonicalize()?;
        let root = base.join("repo");
        let external = base.join("external");
        let old = external.join("old");
        fs::create_dir(&root)?;
        fs::create_dir_all(&old)?;
        let route = external.join("current");
        let target = old.join("rules");
        fs::write(&target, "later/\n")?;
        symlink("old", &route)?;
        symlink(route.join("../current/rules"), root.join(".ignore"))?;
        let workspace = Workspace {
            name: "repo".to_string(),
            root: root.clone(),
            languages: Vec::new(),
            ignore: Vec::new(),
        };
        let mut inputs = IgnoreInputs::default();
        inputs.refresh(&[workspace]);
        assert!(inputs.directories().contains(&old));
        assert_eq!(inputs.changed_targets(&[target]), HashSet::from([root]));
        Ok(())
    }
}
