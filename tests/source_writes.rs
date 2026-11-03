use std::fs;
use std::path::Path;
use std::sync::{Barrier, mpsc};
use std::time::Duration;

use anyhow::Result;
use tsindex::config::{TsIndexConfig, db_path};
use tsindex::index::{ReplaceSymbolArgs, Runtime};
use tsindex::maintenance::{FileLease, writer_lease_path};

#[test]
fn parallel_replacements_wait_for_the_checkout_writer_and_preserve_both_edits() -> Result<()> {
    let artifacts = Path::new(env!("CARGO_MANIFEST_DIR")).join("target/source-write-fixtures");
    fs::create_dir_all(&artifacts)?;
    let directory = tempfile::tempdir_in(artifacts)?;
    let root = directory.path().canonicalize()?;
    let source = root.join("module.py");
    fs::write(
        &source,
        "def first():\n    return 1\n\ndef second():\n    return 2\n",
    )?;
    let runtime = Runtime::new(
        root.clone(),
        db_path(&root),
        TsIndexConfig::default(),
        vec![],
    );
    let held = FileLease::acquire(writer_lease_path(&runtime.db_path), Duration::from_secs(1))?;
    let ready = Barrier::new(3);
    let (send, receive) = mpsc::channel();
    let (escaped, outcomes) = std::thread::scope(|scope| {
        let mut workers = Vec::new();
        for (name, value) in [("first", 10), ("second", 20)] {
            let send = send.clone();
            let runtime = &runtime;
            let ready = &ready;
            workers.push(scope.spawn(move || {
                ready.wait();
                let result = runtime.replace_symbol(ReplaceSymbolArgs {
                    file: "module.py".into(),
                    name: name.into(),
                    new_body: format!("def {name}():\n    return {value}"),
                    repo: None,
                    kind: None,
                    qualified: None,
                    row: None,
                });
                send.send(()).unwrap();
                result
            }));
        }
        ready.wait();
        let escaped = receive.recv_timeout(Duration::from_millis(200)).is_ok();
        drop(held);
        let outcomes = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>();
        (escaped, outcomes)
    });
    assert!(
        !escaped,
        "a source replacement bypassed the held checkout writer lease"
    );
    for outcome in outcomes {
        outcome?;
    }
    assert_eq!(
        fs::read_to_string(source)?,
        "def first():\n    return 10\n\ndef second():\n    return 20\n"
    );
    Ok(())
}
