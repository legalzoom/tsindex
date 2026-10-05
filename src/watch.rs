//! Filesystem notifications reduced to the paths the indexer needs.
//!
//! Renames do not need file-ID correlation: the indexer resolves both paths
//! against the current tree. Keeping a hash registry here avoids the full
//! debouncer's linear root lookup for every nonrecursive Linux registration.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use notify::{Event, EventKind, RecursiveMode, Watcher};

#[derive(Default)]
pub(crate) struct ChangeBatch {
    pub paths: HashSet<PathBuf>,
    pub invalidated: HashSet<PathBuf>,
    pub rescan: bool,
}

pub(crate) struct ChangeReceiver {
    pending: Arc<Mutex<ChangeBatch>>,
    rx: mpsc::Receiver<()>,
    window: Duration,
}

/// Coalesce before queueing, including while an index update is running.
/// One wakeup and one entry per distinct path bound repeated-event memory;
/// fixed windows also ensure continuous writes cannot postpone updates forever.
pub(crate) fn changes(
    window: Duration,
) -> (
    impl FnMut(notify::Result<Event>) + Send + 'static,
    ChangeReceiver,
) {
    let pending = Arc::new(Mutex::new(ChangeBatch::default()));
    let (tx, rx) = mpsc::sync_channel(1);
    let receiver = ChangeReceiver {
        pending: Arc::clone(&pending),
        rx,
        window,
    };
    let handler = move |result: notify::Result<Event>| {
        let event = match result {
            Ok(event) => event,
            Err(error) => {
                eprintln!("watch error: {error}");
                return;
            }
        };
        // Our own walks produce opens and reads. Writable closes are retained:
        // a shared mmap write may have no other modification notification.
        if is_read_only_access(&event.kind) && !event.need_rescan() {
            return;
        }
        let mut batch = pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        batch.rescan |= event.need_rescan();
        // Delete/recreate and moves can invalidate a kernel watch even when
        // the same pathname exists by the time this batch is processed.
        if matches!(
            event.kind,
            EventKind::Remove(_) | EventKind::Modify(notify::event::ModifyKind::Name(_))
        ) {
            batch.invalidated.extend(event.paths.iter().cloned());
        }
        batch.paths.extend(event.paths);
        if !batch.paths.is_empty() || batch.rescan {
            // Sending under the lock prevents a wakeup arriving after the
            // receiver has taken the batch that caused it.
            let _ = tx.try_send(());
        }
    };
    (handler, receiver)
}

impl ChangeReceiver {
    pub fn recv(&self) -> Option<ChangeBatch> {
        self.rx.recv().ok()?;
        std::thread::sleep(self.window);
        let mut pending = self
            .pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // A second notification may have filled the slot during the window.
        // It belongs to the batch being taken, not the next update.
        let _ = self.rx.try_recv();
        Some(std::mem::take(&mut *pending))
    }
}

fn is_read_only_access(kind: &EventKind) -> bool {
    use notify::event::{AccessKind, AccessMode};
    matches!(
        kind,
        EventKind::Access(access) if !matches!(access, AccessKind::Close(AccessMode::Write))
    )
}

pub(crate) fn under_any(path: &Path, roots: &HashSet<PathBuf>) -> bool {
    path.ancestors().any(|ancestor| roots.contains(ancestor))
}

/// Registrations scale with distinct active directories, rather than all
/// directories ever seen. Unchanged paths never call the backend again.
pub(crate) struct WatchRegistry<W> {
    watcher: W,
    paths: HashMap<PathBuf, RecursiveMode>,
}

impl<W: Watcher> WatchRegistry<W> {
    pub fn new(watcher: W) -> Self {
        Self {
            watcher,
            paths: HashMap::new(),
        }
    }

    pub fn watch(&mut self, path: &Path, mode: RecursiveMode) -> notify::Result<()> {
        if self
            .paths
            .get(path)
            .is_some_and(|existing| *existing == mode || *existing == RecursiveMode::Recursive)
        {
            // A parent-directory probe must not downgrade a native recursive
            // workspace watch when both happen to use the same path.
            return Ok(());
        }
        self.watcher.watch(path, mode)?;
        self.paths.insert(path.to_path_buf(), mode);
        Ok(())
    }

    pub fn invalidate(&mut self, changed: &HashSet<PathBuf>) {
        // File removals need no registry walk; every registered directory has
        // its own key, so only a removed/moved watched root invalidates a subtree.
        let roots: HashSet<_> = changed
            .iter()
            .filter(|path| self.paths.contains_key(*path))
            .cloned()
            .collect();
        if !roots.is_empty() {
            self.retain(|path| !under_any(path, &roots));
        }
    }

    pub fn retain(&mut self, mut keep: impl FnMut(&Path) -> bool) {
        let obsolete: Vec<_> = self
            .paths
            .keys()
            .filter(|path| !keep(path))
            .cloned()
            .collect();
        for path in obsolete {
            match self.watcher.unwatch(&path) {
                Ok(()) => {}
                // Inotify automatically removes deleted directory watches.
                Err(error)
                    if matches!(error.kind, notify::ErrorKind::WatchNotFound)
                        || matches!(&error.kind, notify::ErrorKind::Io(io)
                        if io.kind() == std::io::ErrorKind::NotFound
                            || io.raw_os_error() == Some(libc::EINVAL)) => {}
                Err(error) => {
                    eprintln!("watch removal failed {}: {error}", path.display());
                    continue;
                }
            }
            self.paths.remove(&path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use notify::event::{AccessKind, AccessMode, Flag, ModifyKind, RenameMode};

    #[test]
    fn batches_repeated_events_while_updates_are_busy_and_retains_rename_paths() {
        let (mut handler, receiver) = changes(Duration::ZERO);
        for _ in 0..10_000 {
            handler(Ok(
                Event::new(EventKind::Modify(ModifyKind::Any)).add_path("src/a.py".into())
            ));
        }
        handler(Ok(Event::new(EventKind::Modify(ModifyKind::Name(
            RenameMode::Both,
        )))
        .add_path("src/old".into())
        .add_path("src/new".into())));
        let batch = receiver.recv().unwrap();
        assert_eq!(
            batch.paths,
            HashSet::from(["src/a.py".into(), "src/old".into(), "src/new".into()])
        );
        assert_eq!(
            batch.invalidated,
            HashSet::from(["src/old".into(), "src/new".into()])
        );
        drop(handler);
        assert!(
            receiver.recv().is_none(),
            "duplicate wakeups must not cause empty updates"
        );
    }

    #[test]
    fn drops_reads_but_keeps_writable_closes_and_pathless_overflow() {
        let (mut handler, receiver) = changes(Duration::ZERO);
        handler(Ok(Event::new(EventKind::Access(AccessKind::Open(
            AccessMode::Read,
        )))
        .add_path("read".into())));
        handler(Ok(Event::new(EventKind::Access(AccessKind::Close(
            AccessMode::Write,
        )))
        .add_path("write".into())));
        handler(Ok(Event::new(EventKind::Other).set_flag(Flag::Rescan)));
        let batch = receiver.recv().unwrap();
        assert_eq!(batch.paths, HashSet::from(["write".into()]));
        assert!(batch.rescan);
    }

    struct CountingWatcher {
        adds: usize,
        removes: usize,
    }
    impl Watcher for CountingWatcher {
        fn new<F: notify::EventHandler>(_: F, _: notify::Config) -> notify::Result<Self> {
            Ok(Self {
                adds: 0,
                removes: 0,
            })
        }
        fn watch(&mut self, _: &Path, _: RecursiveMode) -> notify::Result<()> {
            self.adds += 1;
            Ok(())
        }
        fn unwatch(&mut self, _: &Path) -> notify::Result<()> {
            self.removes += 1;
            Ok(())
        }
        fn kind() -> notify::WatcherKind {
            notify::WatcherKind::NullWatcher
        }
    }

    #[test]
    fn registry_skips_unchanged_paths_and_forgets_removed_subtrees() -> notify::Result<()> {
        let mut registry =
            WatchRegistry::new(CountingWatcher::new(|_| {}, notify::Config::default())?);
        let paths: Vec<_> = (0..40_000)
            .map(|i| PathBuf::from(format!("repo/{i}/src")))
            .collect();
        for _ in 0..2 {
            for path in &paths {
                registry.watch(path, RecursiveMode::NonRecursive)?;
            }
        }
        assert_eq!(
            registry.watcher.adds,
            paths.len(),
            "a refresh must not re-register unchanged paths"
        );
        registry.invalidate(&HashSet::from([paths[0].clone()]));
        registry.watch(&paths[0], RecursiveMode::NonRecursive)?;
        assert_eq!(registry.watcher.adds, paths.len() + 1);
        assert_eq!(registry.watcher.removes, 1);
        registry.retain(|_| false);
        assert!(
            registry.paths.is_empty(),
            "removed paths must not accumulate across refreshes"
        );
        Ok(())
    }
}
