//! Regression tests for the read/query surface: pagination warnings, the
//! `query` byte budget and timeout, declaration filtering across languages,
//! and outline body degradation.

use std::fs;
use std::path::Path;

use anyhow::Result;
use tempfile::{TempDir, tempdir};

use tsindex::config::{ServerConfig, TsIndexConfig, db_path};
use tsindex::index::{
    EnclosingSymbolArgs, FindReferencesArgs, GetSymbolArgs, OutlineArgs, QueryArgs, Runtime,
};

fn runtime_with(root: &Path, languages: &[&str], server: ServerConfig) -> Result<Runtime> {
    let config = TsIndexConfig {
        server,
        ..Default::default()
    };
    config.write(root)?;
    Ok(Runtime::new(
        root.to_path_buf(),
        db_path(root),
        config,
        languages.iter().map(|l| l.to_string()).collect(),
    ))
}

fn python_fixture(files: &[(&str, &str)], server: ServerConfig) -> Result<(TempDir, Runtime)> {
    let dir = tempdir()?;
    for (name, body) in files {
        let path = dir.path().join(name);
        fs::create_dir_all(path.parent().unwrap())?;
        fs::write(path, body)?;
    }
    let runtime = runtime_with(dir.path(), &["python"], server)?;
    runtime.build(false, None)?;
    Ok((dir, runtime))
}

fn symbol_args(name: &str, limit: usize, offset: usize) -> GetSymbolArgs {
    GetSymbolArgs {
        name: Some(name.to_string()),
        names: None,
        repo: None,
        kind: None,
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: Some(limit),
        offset: Some(offset),
    }
}

fn refs_args(name: &str, include_declarations: bool) -> FindReferencesArgs {
    FindReferencesArgs {
        name: Some(name.to_string()),
        names: None,
        repo: None,
        scope: None,
        exclude_tests: false,
        include_declarations,
        group_by_file: false,
        snippet_lines: 1,
        limit: None,
        offset: None,
        counts_only: false,
    }
}

fn outline_args(file: &str, limit: Option<usize>, offset: Option<usize>) -> OutlineArgs {
    OutlineArgs {
        file: file.to_string(),
        repo: None,
        depth: 2,
        include_signatures: true,
        include_docstrings: true,
        include_imports: false,
        include_bodies_for: None,
        max_body_lines: None,
        limit,
        offset,
    }
}

fn query_args(query: &str, limit: Option<usize>, offset: Option<usize>) -> QueryArgs {
    QueryArgs {
        language: "python".to_string(),
        repo: None,
        query: query.to_string(),
        file_glob: None,
        capture: None,
        limit,
        offset,
    }
}

// ---- F04 --------------------------------------------------------------------

#[test]
fn paginated_final_page_has_no_warning() -> Result<()> {
    let (_dir, runtime) = python_fixture(
        &[
            ("a.py", "def dup():\n    return 1\n"),
            ("b.py", "def dup():\n    return 2\n"),
            ("use.py", "from a import dup\n\ndup()\ndup()\n"),
            (
                "many.py",
                "def f1():\n    pass\n\ndef f2():\n    pass\n\ndef f3():\n    pass\n\ndef f4():\n    pass\n",
            ),
        ],
        ServerConfig::default(),
    )?;

    // get_symbol: two matches, ask for the last one.
    let symbols = runtime.get_symbol(symbol_args("dup", 1, 1))?;
    let json = serde_json::to_value(&symbols)?;
    assert_eq!(json["total"], 2);
    assert_eq!(json["truncated"], false);
    assert!(json.get("next_offset").is_none());
    assert!(
        json.get("warning").is_none(),
        "final get_symbol page must not warn: {json}"
    );

    // find_references: import + two call sites, ask for the last one.
    let total = runtime.find_references(refs_args("dup", false))?.total;
    assert_eq!(total, 3);
    let mut args = refs_args("dup", false);
    args.limit = Some(1);
    args.offset = Some(total - 1);
    let refs = runtime.find_references(args)?;
    let json = serde_json::to_value(&refs)?;
    assert_eq!(json["total"], 3);
    assert_eq!(json["truncated"], false);
    assert!(json.get("next_offset").is_none());
    assert!(
        json.get("warning").is_none(),
        "final find_references page must not warn: {json}"
    );

    // list_file_outline: four roots, ask for the last one.
    let outline = runtime.list_file_outline(outline_args("many.py", Some(1), Some(3)))?;
    let json = serde_json::to_value(&outline)?;
    assert_eq!(json["total"], 4);
    assert_eq!(json["truncated"], false);
    assert!(json.get("next_offset").is_none());
    assert!(
        json.get("warning").is_none(),
        "final outline page must not warn: {json}"
    );

    // enclosing_symbol batch: three rows, ask for the last one.
    let enclosing = runtime.enclosing_symbol(EnclosingSymbolArgs {
        file: "many.py".to_string(),
        row: None,
        rows: Some(vec![2, 5, 8]),
        col: None,
        repo: None,
        include_body: false,
        context_lines: 0,
        depth: 1,
        max_body_lines: None,
        limit: Some(1),
        offset: Some(2),
    })?;
    let json = serde_json::to_value(&enclosing)?;
    assert!(json.get("next_offset").is_none());
    assert!(
        json.get("warning").is_none(),
        "final enclosing page must not warn: {json}"
    );

    // Sanity: a genuinely truncated first page still warns.
    let first = runtime.get_symbol(symbol_args("dup", 1, 0))?;
    let json = serde_json::to_value(&first)?;
    assert_eq!(json["truncated"], true);
    assert_eq!(json["next_offset"], 1);
    assert!(json["warning"].as_str().unwrap().contains("offset: 1"));
    Ok(())
}

// ---- F03 --------------------------------------------------------------------

fn query_budget_fixture(max_response_chars: usize) -> Result<(TempDir, Runtime)> {
    let files: Vec<(String, String)> = (0..30)
        .map(|i| {
            (
                format!("src/m{i:02}.py"),
                format!(
                    "def fn_{i:02}():\n    value = \"{}\"\n    return value\n",
                    "x".repeat(150)
                ),
            )
        })
        .collect();
    let borrowed: Vec<(&str, &str)> = files
        .iter()
        .map(|(a, b)| (a.as_str(), b.as_str()))
        .collect();
    python_fixture(
        &borrowed,
        ServerConfig {
            max_response_chars,
            ..Default::default()
        },
    )
}

#[test]
fn query_budget_shrinks_page_to_fit() -> Result<()> {
    const BUDGET: usize = 2_000;
    let (_dir, budgeted) = query_budget_fixture(BUDGET)?;
    let (_dir2, unbounded) = query_budget_fixture(0)?;
    let pattern = "(function_definition) @f";

    let full = unbounded.query(query_args(pattern, Some(100), None))?;
    assert_eq!(full.captures.len(), 30);
    assert!(!full.truncated);
    assert!(
        serde_json::to_string(&full)?.len() > BUDGET,
        "fixture must overflow the budget to be meaningful"
    );

    let page = budgeted.query(query_args(pattern, Some(100), None))?;
    let serialized = serde_json::to_string(&page)?;
    assert!(
        serialized.len() <= BUDGET,
        "query payload {} must fit the {BUDGET} budget",
        serialized.len()
    );
    assert!(page.captures.len() < 30 && !page.captures.is_empty());
    assert!(page.truncated);
    assert_eq!(page.next_offset, Some(page.captures.len()));
    assert!(page.warning.is_some());

    // Walk every page; the union must equal the unbudgeted set exactly.
    let mut seen: Vec<(String, usize)> = Vec::new();
    let mut offset = None;
    loop {
        let page = budgeted.query(query_args(pattern, Some(100), offset))?;
        assert!(serde_json::to_string(&page)?.len() <= BUDGET);
        for capture in &page.captures {
            seen.push((capture.file.clone(), capture.range.start.0));
        }
        match page.next_offset {
            Some(next) => {
                assert!(next > offset.unwrap_or(0), "offset must progress");
                offset = Some(next);
            }
            None => break,
        }
    }
    let expected: Vec<(String, usize)> = full
        .captures
        .iter()
        .map(|c| (c.file.clone(), c.range.start.0))
        .collect();
    assert_eq!(seen, expected, "walk must have no gaps or duplicates");
    Ok(())
}

// ---- F10 --------------------------------------------------------------------

#[test]
fn query_timeout_before_offset_does_not_return_same_offset() -> Result<()> {
    let (_dir, runtime) = python_fixture(
        &[("a.py", "x = 1\ny = 2\nz = x + y\n")],
        ServerConfig {
            query_timeout_ms: 0,
            ..Default::default()
        },
    )?;
    let result = runtime.query(query_args("(identifier) @id", None, Some(5)));
    match result {
        Err(error) => {
            let message = format!("{error:#}");
            assert!(message.contains("timed out"), "{message}");
            assert!(message.contains("query_timeout_ms"), "{message}");
        }
        Ok(response) => assert_ne!(
            response.next_offset,
            Some(5),
            "a timed-out empty page must not hand back the same offset"
        ),
    }
    Ok(())
}

// ---- F31 --------------------------------------------------------------------

#[test]
fn query_skips_unreadable_and_oversized_files() -> Result<()> {
    let dir = tempdir()?;
    fs::write(dir.path().join("ok.py"), "def visible():\n    return 1\n")?;
    // Just over the 2 MiB build gate.
    let big = "def big_fn():\n    return 1\n".repeat(2 * 1024 * 1024 / 26 + 10);
    assert!(big.len() > 2 * 1024 * 1024);
    fs::write(dir.path().join("big.py"), &big)?;
    let locked = dir.path().join("locked.py");
    fs::write(&locked, "def hidden():\n    return 1\n")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o000))?;
    }
    let unreadable = fs::read(&locked).is_err();

    let runtime = runtime_with(dir.path(), &["python"], ServerConfig::default())?;
    let response = runtime.query(query_args(
        "(function_definition name: (identifier) @n)",
        None,
        None,
    ))?;
    let names: Vec<&str> = response.captures.iter().map(|c| c.text.as_str()).collect();
    assert_eq!(names, vec!["visible"], "only the readable, in-size file");
    assert_eq!(
        response.partial, unreadable,
        "unreadable file flags partial"
    );

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o644))?;
    }
    Ok(())
}

// ---- F08 --------------------------------------------------------------------

#[test]
fn find_references_excludes_declarations_in_every_language() -> Result<()> {
    // (language, file, source, symbol, 0-based declaration row). Every use
    // sits on a different row than the declaration.
    let cases: &[(&str, &str, &str, &str, usize)] = &[
        (
            "c",
            "lib.c",
            "int foo(int x) { return x; }\nint bar(void) { return foo(1); }\n",
            "foo",
            0,
        ),
        (
            "cpp",
            "lib.cpp",
            "int foo(int x) { return x; }\nint bar() { return foo(1); }\n",
            "foo",
            0,
        ),
        (
            "go",
            "main.go",
            "package main\ntype Thing struct{}\nfunc use() Thing {\n\tvar t Thing\n\treturn t\n}\n",
            "Thing",
            1,
        ),
        ("ruby", "lib.rb", "def rbfun\n  1\nend\nrbfun\n", "rbfun", 0),
        (
            "kotlin",
            "A.kt",
            "class Foo\nfun make(): Foo = Foo()\n",
            "Foo",
            0,
        ),
        (
            "swift",
            "A.swift",
            "struct Foo {}\nfunc make() -> Foo { return Foo() }\n",
            "Foo",
            0,
        ),
        (
            "php",
            "a.php",
            "<?php\nfunction foo() { return 1; }\nfoo();\n",
            "foo",
            1,
        ),
        (
            "javascript",
            "a.js",
            "function foo() { return 1; }\nfoo();\n",
            "foo",
            0,
        ),
        (
            "java",
            "A.java",
            "class A {\n    void jm() {}\n    void run() { jm(); }\n}\n",
            "jm",
            1,
        ),
    ];

    let dir = tempdir()?;
    for (_, file, source, _, _) in cases {
        fs::write(dir.path().join(file), source)?;
    }
    let languages: Vec<&str> = cases.iter().map(|c| c.0).collect();
    let runtime = runtime_with(dir.path(), &languages, ServerConfig::default())?;
    runtime.build(false, None)?;

    for (language, file, _, symbol, decl_row) in cases {
        let mut default = refs_args(symbol, false);
        default.scope = Some(file.to_string());
        let default = runtime.find_references(default)?;
        let mut with_decl = refs_args(symbol, true);
        with_decl.scope = Some(file.to_string());
        let with_decl = runtime.find_references(with_decl)?;

        assert!(
            default.total >= 1,
            "{language}: expected at least one use of {symbol}, got none"
        );
        assert!(
            default.refs.iter().all(|r| r.range.start.0 != *decl_row),
            "{language}: declaration row leaked into default references: {:?}",
            default
                .refs
                .iter()
                .map(|r| r.range.start.0)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            with_decl.total,
            default.total + 1,
            "{language}: include_declarations must add exactly the declaration"
        );
        assert_eq!(
            with_decl
                .refs
                .iter()
                .filter(|r| r.range.start.0 == *decl_row && r.context == "declaration")
                .count(),
            1,
            "{language}: the declaration row must be tagged `declaration`"
        );
    }

    // Java call sites are calls, not bare identifiers.
    let java = runtime.find_references(refs_args("jm", false))?;
    assert!(
        java.refs.iter().all(|r| r.context == "call"),
        "java call site context: {:?}",
        java.refs
            .iter()
            .map(|r| r.context.as_str())
            .collect::<Vec<_>>()
    );
    Ok(())
}

// ---- F28 --------------------------------------------------------------------

#[test]
fn outline_budget_includes_warning_in_measurement() -> Result<()> {
    let source = (0..8)
        .map(|i| {
            format!("def function_number_{i}(argument_one, argument_two):\n    return {i}\n\n")
        })
        .collect::<String>();
    let (dir, _) = python_fixture(&[("wide.py", &source)], ServerConfig::default())?;

    let mut checked = 0;
    for budget in (300..=900).step_by(20) {
        let runtime = Runtime::new(
            dir.path().to_path_buf(),
            db_path(dir.path()),
            TsIndexConfig {
                server: ServerConfig {
                    max_response_chars: budget,
                    ..Default::default()
                },
                ..Default::default()
            },
            vec!["python".to_string()],
        );
        if let Ok(response) = runtime.list_file_outline(outline_args("wide.py", None, None)) {
            let serialized = serde_json::to_string(&response)?;
            assert!(
                serialized.len() <= budget,
                "budget {budget}: payload is {} chars (warning must be measured too)",
                serialized.len()
            );
            if response.truncated {
                assert!(response.warning.is_some());
                checked += 1;
            }
        }
    }
    assert!(checked > 0, "some budget must produce a truncated page");
    Ok(())
}

// ---- F29 --------------------------------------------------------------------

#[test]
fn outline_body_inline_survives_missing_source() -> Result<()> {
    let (dir, runtime) = python_fixture(
        &[(
            "gone.py",
            "def keep():\n    return 1\n\ndef other():\n    return 2\n",
        )],
        ServerConfig::default(),
    )?;
    fs::remove_file(dir.path().join("gone.py"))?;

    let mut args = outline_args("gone.py", None, None);
    args.include_bodies_for = Some(vec!["keep".to_string()]);
    let response = runtime.list_file_outline(args)?;
    assert!(response.partial, "missing source degrades to partial");
    assert_eq!(response.symbols.len(), 2, "outline itself is intact");
    let keep = response.symbols.iter().find(|s| s.name == "keep").unwrap();
    assert!(keep.body.is_none());
    assert!(
        keep.body_unavailable
            .as_deref()
            .is_some_and(|reason| reason.contains("body unavailable")),
        "{:?}",
        keep.body_unavailable
    );
    let other = response.symbols.iter().find(|s| s.name == "other").unwrap();
    assert!(
        other.body_unavailable.is_none(),
        "only requested names are marked"
    );
    Ok(())
}
