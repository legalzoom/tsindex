use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Result;
use tempfile::TempDir;
use tsindex::config::{RepoConfig, TsIndexConfig, db_path};
use tsindex::index::{GetSymbolArgs, QueryArgs, Runtime};
use tsindex::overlay::{read_connection, storage_stats};
use tsindex::worktrees::{discover, open_checkout};

struct Fixture {
    _directory: TempDir,
    main: PathBuf,
    alpha: PathBuf,
    beta: PathBuf,
    config: TsIndexConfig,
}

impl Fixture {
    fn new() -> Result<Self> {
        let artifacts = Path::new(env!("CARGO_MANIFEST_DIR")).join("target/overlay-fixtures");
        fs::create_dir_all(&artifacts)?;
        let directory = tempfile::tempdir_in(artifacts)?;
        let root = directory.path().canonicalize()?;
        let main = root.join("main");
        let alpha = root.join("alpha");
        let beta = root.join("beta");
        fs::create_dir_all(main.join(".git/worktrees"))?;
        for (checkout, name) in [(&alpha, "alpha"), (&beta, "beta")] {
            fs::create_dir(checkout)?;
            let gitdir = main.join(".git/worktrees").join(name);
            fs::create_dir(&gitdir)?;
            fs::write(
                checkout.join(".git"),
                format!("gitdir: {}\n", gitdir.display()),
            )?;
            fs::write(
                gitdir.join("gitdir"),
                format!("{}\n", checkout.join(".git").display()),
            )?;
            fs::write(gitdir.join("commondir"), "../..\n")?;
        }
        for checkout in [&main, &alpha, &beta] {
            fs::write(
                checkout.join("shared.py"),
                "def shared():\n    return common()\n",
            )?;
            fs::write(
                checkout.join("change.py"),
                "def original():\n    return before()\n",
            )?;
            fs::write(
                checkout.join("remove.py"),
                "def removed():\n    return other()\n",
            )?;
        }
        let mut config = TsIndexConfig::default();
        config.languages.include = vec!["python".into()];
        config.write(&main)?;
        Runtime::new(main.clone(), db_path(&main), config.clone(), vec![]).build(false, None)?;
        Ok(Self {
            _directory: directory,
            main,
            alpha,
            beta,
            config,
        })
    }

    fn runtime(&self, root: &Path) -> Result<Runtime> {
        let checkout = discover(root)?
            .into_iter()
            .find(|c| c.path == root)
            .unwrap();
        let runtime = open_checkout(&checkout, &self.config, vec![])?;
        runtime.build(true, None)?;
        Ok(runtime)
    }
}

fn symbol(runtime: &Runtime, name: &str) -> Result<usize> {
    Ok(runtime
        .get_symbol(GetSymbolArgs {
            name: Some(name.into()),
            names: None,
            repo: None,
            kind: None,
            file_glob: None,
            include_body: false,
            context_lines: 0,
            max_body_lines: None,
            limit: None,
            offset: None,
        })?
        .total)
}

#[test]
fn two_checkouts_share_one_base_and_store_only_changed_extractions() -> Result<()> {
    let fixture = Fixture::new()?;
    fs::write(
        fixture.alpha.join("change.py"),
        "def alpha_only():\n    return alpha()\n",
    )?;
    fs::write(
        fixture.beta.join("change.py"),
        "def beta_only():\n    return beta()\n",
    )?;
    let alpha = fixture.runtime(&fixture.alpha)?;
    let beta = fixture.runtime(&fixture.beta)?;
    assert_eq!(symbol(&alpha, "shared")?, 1);
    assert_eq!(symbol(&alpha, "alpha_only")?, 1);
    assert_eq!(symbol(&alpha, "original")?, 0);
    assert_eq!(symbol(&beta, "beta_only")?, 1);
    let a = storage_stats(&alpha.db_path)?;
    let b = storage_stats(&beta.db_path)?;
    assert!(
        a.base_path.is_some(),
        "checkout still stores a complete independent index: {a:?}"
    );
    assert_eq!(a.base_path, b.base_path);
    assert_eq!(a.base_files, 3);
    assert_eq!(a.private_files, 1);
    assert_eq!(b.private_files, 1);
    Ok(())
}

#[test]
fn a_missing_base_is_recovered_by_rebuilding_the_selected_checkout() -> Result<()> {
    let fixture = Fixture::new()?;
    let alpha = fixture.runtime(&fixture.alpha)?;
    let pinned = read_connection(&alpha.db_path)?;
    let original: i64 = pinned.query_row(
        "SELECT COUNT(*) FROM symbols WHERE name='original'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(original, 1);
    let base = storage_stats(&alpha.db_path)?.base_path.unwrap();
    fs::remove_file(base)?;
    fs::write(
        fixture.alpha.join("change.py"),
        "def recovered():\n    return local_only()\n",
    )?;
    alpha.build(true, None)?;
    assert_eq!(symbol(&alpha, "recovered")?, 1);
    assert_eq!(symbol(&alpha, "original")?, 0);
    assert_eq!(symbol(&alpha, "shared")?, 1);
    assert_eq!(storage_stats(&alpha.db_path)?.base_path, None);
    assert_eq!(
        pinned.query_row(
            "SELECT COUNT(*) FROM symbols WHERE name='original'",
            [],
            |row| row.get::<_, i64>(0),
        )?,
        1,
        "recovery replaced the generation underneath an existing logical reader"
    );
    Ok(())
}

#[test]
fn advancing_main_keeps_existing_and_new_checkout_bases_immutable() -> Result<()> {
    let fixture = Fixture::new()?;
    let alpha = fixture.runtime(&fixture.alpha)?;
    let base = storage_stats(&alpha.db_path)?.base_path.unwrap();
    let immutable = fs::read(&base)?;
    fs::write(
        fixture.main.join("shared.py"),
        "def advanced_main():\n    pass\n",
    )?;
    let main = Runtime::new(
        fixture.main.clone(),
        db_path(&fixture.main),
        fixture.config.clone(),
        vec![],
    );
    main.build(true, None)?;
    let beta = fixture.runtime(&fixture.beta)?;
    assert_eq!(symbol(&main, "advanced_main")?, 1);
    assert_eq!(symbol(&alpha, "shared")?, 1);
    assert_eq!(symbol(&alpha, "advanced_main")?, 0);
    assert_eq!(symbol(&beta, "shared")?, 1);
    assert_eq!(
        storage_stats(&beta.db_path)?.base_path.as_ref(),
        Some(&base)
    );
    assert_eq!(fs::read(base)?, immutable);
    Ok(())
}

#[test]
fn recreating_the_base_path_cannot_change_an_existing_checkouts_generation() -> Result<()> {
    let fixture = Fixture::new()?;
    let alpha = fixture.runtime(&fixture.alpha)?;
    let pinned = read_connection(&alpha.db_path)?;
    assert_eq!(
        pinned.query_row(
            "SELECT COUNT(*) FROM symbols WHERE name='original'",
            [],
            |row| row.get::<_, i64>(0),
        )?,
        1
    );
    let base = storage_stats(&alpha.db_path)?.base_path.unwrap();
    fs::remove_file(&base)?;
    fs::write(
        fixture.main.join("change.py"),
        "def intruder():\n    return advanced_main()\n",
    )?;
    Runtime::new(
        fixture.main.clone(),
        db_path(&fixture.main),
        fixture.config.clone(),
        vec![],
    )
    .build(true, None)?;
    let beta = fixture.runtime(&fixture.beta)?;
    assert_eq!(
        storage_stats(&beta.db_path)?.base_path.as_ref(),
        Some(&base)
    );
    assert_eq!(symbol(&beta, "original")?, 1);
    let error = symbol(&alpha, "intruder").unwrap_err();
    assert!(error.to_string().contains("identity"), "{error:#}");
    assert_eq!(
        pinned.query_row(
            "SELECT COUNT(*) FROM symbols WHERE name='original'",
            [],
            |row| row.get::<_, i64>(0),
        )?,
        1,
        "a pinned connection must retain its original immutable base inode"
    );
    alpha.build(true, None)?;
    assert_eq!(symbol(&alpha, "original")?, 1);
    assert_eq!(symbol(&alpha, "intruder")?, 0);
    assert_eq!(storage_stats(&alpha.db_path)?.base_path, None);
    Ok(())
}

#[test]
fn bases_without_identity_are_not_rewritten_or_reused() -> Result<()> {
    let fixture = Fixture::new()?;
    let alpha = fixture.runtime(&fixture.alpha)?;
    let base = storage_stats(&alpha.db_path)?.base_path.unwrap();
    rusqlite::Connection::open(&base)?.execute_batch("DROP TABLE overlay_base_identity")?;
    let legacy_bytes = fs::read(&base)?;
    assert!(
        symbol(&alpha, "shared")
            .unwrap_err()
            .to_string()
            .contains("identity")
    );
    let beta = fixture.runtime(&fixture.beta)?;
    assert_eq!(storage_stats(&beta.db_path)?.base_path, None);
    assert_eq!(symbol(&beta, "shared")?, 1);
    assert_eq!(
        fs::read(base)?,
        legacy_bytes,
        "a shared legacy base was rewritten"
    );
    alpha.build(true, None)?;
    assert_eq!(storage_stats(&alpha.db_path)?.base_path, None);
    assert_eq!(symbol(&alpha, "shared")?, 1);
    Ok(())
}

#[test]
fn overlays_without_an_identity_pin_require_recovery_before_reads() -> Result<()> {
    let fixture = Fixture::new()?;
    let alpha = fixture.runtime(&fixture.alpha)?;
    rusqlite::Connection::open(&alpha.db_path)?
        .execute_batch("ALTER TABLE overlay_meta DROP COLUMN base_identity")?;
    assert!(
        symbol(&alpha, "shared")
            .unwrap_err()
            .to_string()
            .contains("identity")
    );
    alpha.build(true, None)?;
    assert_eq!(symbol(&alpha, "shared")?, 1);
    assert_eq!(storage_stats(&alpha.db_path)?.base_path, None);
    Ok(())
}

#[test]
fn no_op_refresh_does_not_publish_another_generation() -> Result<()> {
    let fixture = Fixture::new()?;
    let alpha = fixture.runtime(&fixture.alpha)?;
    let before = storage_stats(&alpha.db_path)?;
    let stats = alpha.build(true, None)?;
    let after = storage_stats(&alpha.db_path)?;
    assert_eq!(stats.indexed, 0);
    assert_eq!(after.generation, before.generation);
    assert_eq!(after.private_files, 0);
    assert_eq!(symbol(&alpha, "shared")?, 1);
    Ok(())
}

#[test]
fn base_shadow_checks_use_the_private_repository_and_path_index() -> Result<()> {
    let fixture = Fixture::new()?;
    fs::write(
        fixture.alpha.join("change.py"),
        "def private():\n    pass\n",
    )?;
    let alpha = fixture.runtime(&fixture.alpha)?;
    let reader = read_connection(&alpha.db_path)?;
    let details = reader
        .prepare("EXPLAIN QUERY PLAN SELECT id FROM symbols WHERE name=?1")?
        .query_map(["shared"], |row| row.get::<_, String>(3))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    assert!(
        details
            .iter()
            .any(|detail| detail.contains("SEARCH p") && detail.contains("repo_id=? AND path=?")),
        "base shadowing must not scan the delta once per base file: {details:#?}"
    );
    Ok(())
}

fn assert_named_base_lookup_plan(sql: &str, name: &str, index: &str) -> Result<()> {
    let fixture = Fixture::new()?;
    fs::write(
        fixture.alpha.join("change.py"),
        "def private():\n    return private_call()\n",
    )?;
    let alpha = fixture.runtime(&fixture.alpha)?;
    let reader = read_connection(&alpha.db_path)?;
    let details = reader
        .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))?
        .query_map(rusqlite::params![name, None::<&str>, None::<&str>], |row| {
            row.get::<_, String>(3)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    assert!(
        !details
            .iter()
            .any(|detail| detail.contains("CO-ROUTINE temp.files")),
        "named base facts must not build an inner effective-file catalog: {details:#?}"
    );
    assert!(
        details
            .iter()
            .any(|detail| detail.contains(&format!("SEARCH s USING INDEX {index} (name=?)"))),
        "named base facts must use their name index: {details:#?}"
    );
    assert!(
        details
            .iter()
            .any(|detail| detail.contains("SEARCH b USING INTEGER PRIMARY KEY (rowid=?)")),
        "base fact visibility must look up its file by row id: {details:#?}"
    );
    let rows = reader
        .prepare(sql)?
        .query_map(rusqlite::params![name, None::<&str>, None::<&str>], |_| {
            Ok(())
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    assert_eq!(rows.len(), 1);
    Ok(())
}

#[test]
fn named_symbol_lookup_uses_the_base_name_index_without_an_inner_file_catalog() -> Result<()> {
    assert_named_base_lookup_plan(
        "SELECT s.id, s.file_id, r.name, f.path, f.language, s.kind, s.name, s.qualified,
                s.start_row, s.start_col, s.end_row, s.end_col, s.signature, s.docstring,
                f.sha, f.partial
         FROM symbols s JOIN files f ON f.id=s.file_id JOIN repos r ON r.id=f.repo_id
         WHERE s.name IN (?1) AND (?2 IS NULL OR r.name=?3)
         ORDER BY s.name, r.name, f.path, s.start_row, s.start_col",
        "shared",
        "idx_symbols_name",
    )
}

#[test]
fn named_reference_lookup_uses_the_base_name_index_without_an_inner_file_catalog() -> Result<()> {
    assert_named_base_lookup_plan(
        "SELECT rf.name, rp.name, f.path, rf.start_row, rf.start_col, rf.end_row,
                rf.end_col, rf.context, f.sha, f.partial
         FROM refs rf JOIN files f ON f.id=rf.file_id JOIN repos rp ON rp.id=f.repo_id
         WHERE rf.name IN (?1) AND (?2 IS NULL OR rp.name=?3)
         ORDER BY rf.name, rp.name, f.path, rf.start_row, rf.start_col",
        "common",
        "idx_refs_name",
    )
}

#[test]
fn selective_lookups_seek_effective_files_without_materializing_the_catalog() -> Result<()> {
    let fixture = Fixture::new()?;
    let alpha = fixture.runtime(&fixture.alpha)?;
    let reader = read_connection(&alpha.db_path)?;
    for (sql, name) in [
        (
            "SELECT s.id, s.file_id, r.name, f.path, f.language, s.kind, s.name,
                    s.qualified, s.start_row, s.start_col, s.end_row, s.end_col,
                    s.signature, s.docstring, f.sha, f.partial
             FROM symbols s JOIN files f ON f.id=s.file_id JOIN repos r ON r.id=f.repo_id
             WHERE s.name IN (?1) AND (?2 IS NULL OR r.name=?3)
             ORDER BY s.name, r.name, f.path, s.start_row, s.start_col",
            "shared",
        ),
        (
            "SELECT rf.name, rp.name, f.path, rf.start_row, rf.start_col, rf.end_row,
                    rf.end_col, rf.context, f.sha, f.partial
             FROM refs rf JOIN files f ON f.id=rf.file_id JOIN repos rp ON rp.id=f.repo_id
             WHERE rf.name IN (?1) AND (?2 IS NULL OR rp.name=?3)
             ORDER BY rf.name, rp.name, f.path, rf.start_row, rf.start_col",
            "common",
        ),
    ] {
        let plan = reader
            .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))?
            .query_map(rusqlite::params![name, None::<&str>, None::<&str>], |row| {
                row.get::<_, String>(3)
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        assert!(
            !plan.iter().any(|step| step.contains("MATERIALIZE files")),
            "a selective lookup rebuilt the effective file catalog: {plan:#?}"
        );
        assert!(
            plan.iter().any(|step| {
                step.contains("overlay_file_directory")
                    && step.contains("SEARCH")
                    && step.contains("INTEGER PRIMARY KEY")
            }),
            "the effective file directory must be searched by logical file id: {plan:#?}"
        );
        let paths = reader
            .prepare(sql)?
            .query_map(rusqlite::params![name, None::<&str>, None::<&str>], |row| {
                row.get::<_, String>("path")
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        assert_eq!(paths, ["shared.py"]);
    }
    Ok(())
}

#[test]
fn an_existing_overlay_adopts_the_directory_without_reparsing() -> Result<()> {
    let fixture = Fixture::new()?;
    fs::write(
        fixture.alpha.join("change.py"),
        "def private():\n    pass\n",
    )?;
    let alpha = fixture.runtime(&fixture.alpha)?;
    // Removing only derived metadata reproduces the existing overlay schema.
    // Extraction and authority records remain the independent source of truth.
    rusqlite::Connection::open(&alpha.db_path)?
        .execute_batch("DROP TABLE overlay_file_directory; DROP TABLE overlay_directory_state;")?;
    assert_eq!(symbol(&alpha, "private")?, 1);
    assert_eq!(symbol(&alpha, "original")?, 0);
    let pinned = read_connection(&alpha.db_path)?;
    let rebuilt = alpha.build(true, None)?;
    assert_eq!(rebuilt.indexed, 0);
    assert_eq!(symbol(&alpha, "private")?, 1);
    assert_eq!(symbol(&alpha, "shared")?, 1);
    assert_eq!(
        pinned.query_row(
            "SELECT COUNT(*) FROM symbols WHERE name='private'",
            [],
            |r| { r.get::<_, i64>(0) }
        )?,
        1
    );
    let reader = read_connection(&alpha.db_path)?;
    let plan = reader
        .prepare("EXPLAIN QUERY PLAN SELECT path FROM files WHERE id=?1")?
        .query_map([-1], |row| row.get::<_, String>(3))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    assert!(
        plan.iter()
            .any(|step| step.contains("overlay_file_directory"))
    );
    Ok(())
}

#[test]
fn a_stale_directory_falls_back_to_facts_and_is_rebuilt_before_publication() -> Result<()> {
    let fixture = Fixture::new()?;
    let alpha = fixture.runtime(&fixture.alpha)?;
    let pinned = read_connection(&alpha.db_path)?;
    // An older writer can update extraction without knowing about this cache.
    // Its generation change is the signal that the directory is not usable.
    rusqlite::Connection::open(&alpha.db_path)?.execute_batch(
        "BEGIN;
         DELETE FROM overlay_file_directory WHERE path='shared.py';
         UPDATE index_state SET generation=generation+1 WHERE id=1;
         COMMIT;",
    )?;
    assert_eq!(symbol(&alpha, "shared")?, 1);
    let refreshed = alpha.build(true, None)?;
    assert_eq!(refreshed.indexed, 0);
    assert_eq!(symbol(&alpha, "shared")?, 1);
    assert_eq!(
        pinned.query_row(
            "SELECT COUNT(*) FROM files WHERE path='shared.py'",
            [],
            |row| { row.get::<_, i64>(0) }
        )?,
        1
    );
    let reader = read_connection(&alpha.db_path)?;
    assert_eq!(
        reader.query_row(
            "SELECT COUNT(*) FROM files WHERE path='shared.py'",
            [],
            |row| { row.get::<_, i64>(0) }
        )?,
        1
    );
    let plan = reader
        .prepare("EXPLAIN QUERY PLAN SELECT path FROM files WHERE id=?1")?
        .query_map([-1], |row| row.get::<_, String>(3))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    assert!(
        plan.iter()
            .any(|step| step.contains("overlay_file_directory"))
    );
    Ok(())
}

#[test]
fn restoring_changed_and_deleted_files_returns_to_shared_extractions() -> Result<()> {
    let fixture = Fixture::new()?;
    fs::write(
        fixture.alpha.join("change.py"),
        "# no symbols or references\n",
    )?;
    fs::remove_file(fixture.alpha.join("remove.py"))?;
    let alpha = fixture.runtime(&fixture.alpha)?;
    assert_eq!(symbol(&alpha, "original")?, 0);
    assert_eq!(symbol(&alpha, "removed")?, 0);
    assert_eq!(storage_stats(&alpha.db_path)?.private_files, 1);
    fs::copy(
        fixture.main.join("change.py"),
        fixture.alpha.join("change.py"),
    )?;
    fs::copy(
        fixture.main.join("remove.py"),
        fixture.alpha.join("remove.py"),
    )?;
    let restored = alpha.build(true, None)?;
    assert_eq!(restored.indexed, 0, "restored base bytes were parsed again");
    assert_eq!(symbol(&alpha, "original")?, 1);
    assert_eq!(symbol(&alpha, "removed")?, 1);
    assert_eq!(storage_stats(&alpha.db_path)?.private_files, 0);
    Ok(())
}

#[test]
fn a_deleted_base_file_restored_with_epoch_mtime_reuses_its_extraction() -> Result<()> {
    let fixture = Fixture::new()?;
    let alpha = fixture.runtime(&fixture.alpha)?;
    let source = fixture.alpha.join("remove.py");
    fs::remove_file(&source)?;
    alpha.build(true, None)?;
    assert_eq!(symbol(&alpha, "removed")?, 0);
    fs::copy(fixture.main.join("remove.py"), &source)?;
    fs::File::options()
        .write(true)
        .open(&source)?
        .set_times(fs::FileTimes::new().set_modified(std::time::UNIX_EPOCH))?;
    assert_eq!(fs::metadata(&source)?.modified()?, std::time::UNIX_EPOCH);
    let restored = alpha.build(true, None)?;
    assert_eq!(restored.indexed, 0, "restored base bytes were parsed again");
    assert_eq!(symbol(&alpha, "removed")?, 1);
    assert_eq!(storage_stats(&alpha.db_path)?.private_files, 0);
    Ok(())
}

#[test]
fn a_base_backed_file_with_pre_epoch_mtime_remains_indexable() -> Result<()> {
    let fixture = Fixture::new()?;
    let alpha = fixture.runtime(&fixture.alpha)?;
    let source = fixture.alpha.join("shared.py");
    let modified = std::time::UNIX_EPOCH - std::time::Duration::from_secs(1);
    fs::File::options()
        .write(true)
        .open(&source)?
        .set_times(fs::FileTimes::new().set_modified(modified))?;
    assert_eq!(fs::metadata(&source)?.modified()?, modified);
    let refreshed = alpha.build(true, None)?;
    assert_eq!(
        refreshed.indexed, 0,
        "unchanged base bytes were parsed again"
    );
    assert_eq!(symbol(&alpha, "shared")?, 1);
    assert_eq!(storage_stats(&alpha.db_path)?.private_files, 0);
    Ok(())
}

#[test]
fn a_pinned_logical_reader_retains_its_entire_generation() -> Result<()> {
    let fixture = Fixture::new()?;
    let alpha = fixture.runtime(&fixture.alpha)?;
    let reader = read_connection(&alpha.db_path)?;
    let names = |conn: &rusqlite::Connection| -> Result<Vec<String>> {
        Ok(conn
            .prepare("SELECT name FROM symbols ORDER BY name")?
            .query_map([], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?)
    };
    let before = names(&reader)?;
    fs::write(
        fixture.alpha.join("change.py"),
        "def published_later():\n    pass\n",
    )?;
    fs::remove_file(fixture.alpha.join("remove.py"))?;
    alpha.build(true, None)?;
    assert_eq!(names(&reader)?, before);
    assert_eq!(symbol(&alpha, "published_later")?, 1);
    assert_eq!(symbol(&alpha, "removed")?, 0);
    assert_ne!(names(&read_connection(&alpha.db_path)?)?, before);
    Ok(())
}

#[test]
fn incompatible_base_recovers_and_a_failed_recovery_preserves_published_data() -> Result<()> {
    let fixture = Fixture::new()?;
    fs::write(
        fixture.alpha.join("change.py"),
        "def private_before_failure():\n    pass\n",
    )?;
    let alpha = fixture.runtime(&fixture.alpha)?;
    let base = storage_stats(&alpha.db_path)?.base_path.unwrap();
    let saved_base = fs::read(&base)?;
    fs::remove_file(&base)?;
    let mut invalid = alpha.clone();
    invalid.config.languages.include = vec!["not-a-supported-language".into()];
    assert!(invalid.build(true, None).is_err());
    assert!(
        fs::read_dir(alpha.db_path.parent().unwrap())?.all(|entry| !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains("overlay-stage")),
        "failed recovery left a private stage behind"
    );
    fs::write(&base, saved_base)?;
    assert_eq!(symbol(&alpha, "private_before_failure")?, 1);
    assert_eq!(symbol(&alpha, "shared")?, 1);
    assert_eq!(
        storage_stats(&alpha.db_path)?.base_path.as_ref(),
        Some(&base)
    );
    rusqlite::Connection::open(&base)?.pragma_update(None, "user_version", 999)?;
    alpha.build(true, None)?;
    assert_eq!(symbol(&alpha, "private_before_failure")?, 1);
    assert_eq!(symbol(&alpha, "shared")?, 1);
    assert_eq!(storage_stats(&alpha.db_path)?.base_path, None);
    Ok(())
}

#[test]
fn invalid_scoped_selectors_still_error_when_an_overlay_requires_cold_recovery() -> Result<()> {
    let fixture = Fixture::new()?;
    let alpha = fixture.runtime(&fixture.alpha)?;
    let base = storage_stats(&alpha.db_path)?.base_path.unwrap();
    fs::remove_file(base)?;
    let invalid_repo = alpha.build(true, Some("not-configured")).unwrap_err();
    assert!(invalid_repo.to_string().contains("is not configured"));
    let mut invalid_language = alpha.clone();
    invalid_language.languages = vec!["not-a-supported-language".into()];
    let error = invalid_language.build(true, None).unwrap_err();
    assert!(error.to_string().contains("unsupported language"));
    assert!(
        fs::read_dir(alpha.db_path.parent().unwrap())?.all(|entry| !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains("overlay-stage")),
        "failed refresh left a private stage behind"
    );
    Ok(())
}

#[test]
fn equal_mtime_and_size_do_not_alias_distinct_checkout_contents() -> Result<()> {
    let fixture = Fixture::new()?;
    let alpha = fixture.runtime(&fixture.alpha)?;
    let source = fixture.alpha.join("change.py");
    let modified = fs::metadata(&source)?.modified()?;
    fs::write(&source, "def replaced():\n    return before()\n")?;
    fs::File::options()
        .write(true)
        .open(&source)?
        .set_times(fs::FileTimes::new().set_modified(modified))?;
    alpha.build(true, None)?;
    assert_eq!(symbol(&alpha, "replaced")?, 1);
    assert_eq!(symbol(&alpha, "original")?, 0);
    assert_eq!(storage_stats(&alpha.db_path)?.private_files, 1);
    Ok(())
}

#[test]
fn scoped_first_build_validates_all_visible_configured_base_languages() -> Result<()> {
    let mut fixture = Fixture::new()?;
    fixture.config.languages.include.push("javascript".into());
    for root in [&fixture.main, &fixture.alpha] {
        fs::write(root.join("extra.js"), "function extra() { return 1; }\n")?;
    }
    Runtime::new(
        fixture.main.clone(),
        db_path(&fixture.main),
        fixture.config.clone(),
        vec![],
    )
    .build(true, None)?;
    let checkout = discover(&fixture.alpha)?
        .into_iter()
        .find(|checkout| checkout.path == fixture.alpha)
        .unwrap();
    let runtime = open_checkout(&checkout, &fixture.config, vec!["python".into()])?;
    runtime.build(true, None)?;
    assert_eq!(symbol(&runtime, "shared")?, 1);
    assert_eq!(symbol(&runtime, "extra")?, 1);
    assert_eq!(storage_stats(&runtime.db_path)?.private_files, 0);
    Ok(())
}

#[test]
fn overlay_queries_merge_candidates_and_hide_private_tables_from_old_readers() -> Result<()> {
    let fixture = Fixture::new()?;
    fs::write(
        fixture.alpha.join("change.py"),
        "def only_private():\n    pass\n",
    )?;
    let alpha = fixture.runtime(&fixture.alpha)?;
    fs::write(
        fixture.alpha.join("not_indexed_yet.py"),
        "def not_indexed_yet():\n    pass\n",
    )?;
    let query: QueryArgs = serde_json::from_value(serde_json::json!({
        "language": "python",
        "query": "(function_definition name: (identifier) @name)"
    }))?;
    let result = serde_json::to_value(alpha.query(query)?)?;
    assert_eq!(
        result["captures"].as_array().unwrap().len(),
        3,
        "{result:#}"
    );
    let disk = rusqlite::Connection::open(&alpha.db_path)?;
    // The parent raw-query implementation checks this exact physical-table
    // condition before choosing indexed candidates. It must take its complete
    // live-walk fallback rather than serve the one private file as the catalog.
    let has_parent_candidates: bool = disk.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='files')",
        [],
        |r| r.get(0),
    )?;
    assert!(!has_parent_candidates);
    Ok(())
}

#[test]
fn changing_to_a_multi_repo_catalog_materializes_disjoint_file_identities() -> Result<()> {
    let fixture = Fixture::new()?;
    let mut alpha = fixture.runtime(&fixture.alpha)?;
    fs::write(
        fixture.beta.join("change.py"),
        "def other_repo():\n    pass\n",
    )?;
    alpha.config.repos = vec![
        RepoConfig {
            name: "alpha".into(),
            path: ".".into(),
            ..Default::default()
        },
        RepoConfig {
            name: "beta".into(),
            path: "../beta".into(),
            ..Default::default()
        },
    ];
    alpha.build(true, None)?;
    assert_eq!(symbol(&alpha, "shared")?, 2);
    assert_eq!(symbol(&alpha, "original")?, 1);
    assert_eq!(symbol(&alpha, "other_repo")?, 1);
    assert_eq!(storage_stats(&alpha.db_path)?.base_path, None);
    assert_eq!(storage_stats(&alpha.db_path)?.private_files, 6);
    Ok(())
}

#[test]
fn changing_to_a_root_outside_the_checkout_materializes_that_repository() -> Result<()> {
    let fixture = Fixture::new()?;
    let mut alpha = fixture.runtime(&fixture.alpha)?;
    fs::write(
        fixture.beta.join("change.py"),
        "def outside_root():\n    pass\n",
    )?;
    alpha.config.repos[0].path = "../beta".into();
    alpha.build(true, None)?;
    assert_eq!(symbol(&alpha, "outside_root")?, 1);
    assert_eq!(symbol(&alpha, "original")?, 0);
    assert_eq!(symbol(&alpha, "shared")?, 1);
    assert_eq!(storage_stats(&alpha.db_path)?.base_path, None);
    assert_eq!(storage_stats(&alpha.db_path)?.private_files, 3);
    Ok(())
}

#[test]
fn simultaneous_first_opens_publish_one_complete_checkout() -> Result<()> {
    let fixture = Fixture::new()?;
    fs::write(
        fixture.alpha.join("change.py"),
        "def first_generation():\n    pass\n",
    )?;
    let checkout = discover(&fixture.alpha)?
        .into_iter()
        .find(|checkout| checkout.path == fixture.alpha)
        .unwrap();
    let barrier = std::sync::Barrier::new(6);
    let runtimes = std::thread::scope(|scope| -> Result<Vec<Runtime>> {
        let mut workers = Vec::new();
        for _ in 0..6 {
            workers.push(scope.spawn(|| -> Result<Runtime> {
                barrier.wait();
                let runtime = open_checkout(&checkout, &fixture.config, vec![])?;
                runtime.build(true, None)?;
                assert_eq!(symbol(&runtime, "shared")?, 1);
                assert_eq!(symbol(&runtime, "first_generation")?, 1);
                assert_eq!(symbol(&runtime, "original")?, 0);
                Ok(runtime)
            }));
        }
        workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect()
    })?;
    let stats = storage_stats(&runtimes[0].db_path)?;
    assert_eq!(stats.private_files, 1);
    assert_eq!(
        stats.generation, 1,
        "first-use waiters republished identical generations"
    );
    assert!(stats.base_path.is_some());
    Ok(())
}

#[test]
fn separate_checkouts_ingest_their_deltas_in_parallel() -> Result<()> {
    let fixture = Fixture::new()?;
    let barrier = std::sync::Barrier::new(2);
    let ingest = |root: &Path, name: &str| -> Result<Runtime> {
        fs::write(root.join("change.py"), format!("def {name}():\n    pass\n"))?;
        barrier.wait();
        fixture.runtime(root)
    };
    let (alpha, beta) = std::thread::scope(|scope| -> Result<(Runtime, Runtime)> {
        let alpha = scope.spawn(|| ingest(&fixture.alpha, "parallel_alpha"));
        let beta = scope.spawn(|| ingest(&fixture.beta, "parallel_beta"));
        Ok((alpha.join().unwrap()?, beta.join().unwrap()?))
    })?;
    assert_eq!(symbol(&alpha, "parallel_alpha")?, 1);
    assert_eq!(symbol(&alpha, "parallel_beta")?, 0);
    assert_eq!(symbol(&beta, "parallel_beta")?, 1);
    assert_eq!(symbol(&beta, "parallel_alpha")?, 0);
    let a = storage_stats(&alpha.db_path)?;
    let b = storage_stats(&beta.db_path)?;
    assert_eq!(a.base_path, b.base_path);
    assert_eq!(a.private_files, 1);
    assert_eq!(b.private_files, 1);
    Ok(())
}
