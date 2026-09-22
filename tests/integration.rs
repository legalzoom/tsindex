use std::fs;

use anyhow::Result;
use rusqlite::Connection;
use tempfile::tempdir;

use tsindex::config::{RepoConfig, ServerConfig, TsIndexConfig, db_path};
use tsindex::detect::detect_languages;
use tsindex::index::{
    EnclosingSymbolArgs, FindReferencesArgs, GetSymbolArgs, OutlineArgs, QueryArgs, Runtime,
};

#[test]
fn detects_languages_in_fixture_repo() -> Result<()> {
    let dir = tempdir()?;
    fs::create_dir_all(dir.path().join("src"))?;
    fs::write(
        dir.path().join("pyproject.toml"),
        "[project]\nname = 'fixture'\n",
    )?;
    fs::write(
        dir.path().join("src").join("login.py"),
        "def authenticate_user(email, password):\n    return email\n",
    )?;

    let detected = detect_languages(dir.path())?;
    assert_eq!(
        detected.first().map(|item| item.language.as_str()),
        Some("python")
    );
    Ok(())
}

#[test]
fn detects_csharp_from_project_manifest() -> Result<()> {
    let dir = tempdir()?;
    fs::write(
        dir.path().join("BillingService.csproj"),
        r#"<Project Sdk="Microsoft.NET.Sdk"></Project>"#,
    )?;

    let detected = detect_languages(dir.path())?;
    assert_eq!(
        detected.first().map(|item| item.language.as_str()),
        Some("csharp")
    );
    assert_eq!(detected.first().map(|item| item.files), Some(0));
    Ok(())
}

#[test]
fn detects_java_gradle_kts_project_as_java_when_java_sources_exist() -> Result<()> {
    let dir = tempdir()?;
    fs::create_dir_all(dir.path().join("src").join("main").join("java"))?;
    fs::write(dir.path().join("build.gradle.kts"), "plugins { java }\n")?;
    fs::write(
        dir.path()
            .join("src")
            .join("main")
            .join("java")
            .join("OrderController.java"),
        "class OrderController {}\n",
    )?;

    let detected = detect_languages(dir.path())?;
    assert_eq!(
        detected.first().map(|item| item.language.as_str()),
        Some("java")
    );
    assert!(
        detected.iter().all(|item| item.language != "kotlin"),
        "build.gradle.kts should not add Kotlin by itself when Java sources disambiguate it"
    );
    Ok(())
}

#[test]
fn detects_kotlin_gradle_kts_project_as_kotlin_when_kotlin_sources_exist() -> Result<()> {
    let dir = tempdir()?;
    fs::create_dir_all(dir.path().join("src").join("main").join("kotlin"))?;
    fs::write(
        dir.path().join("build.gradle.kts"),
        "plugins { kotlin(\"jvm\") version \"1.9.0\" }\n",
    )?;
    fs::write(
        dir.path()
            .join("src")
            .join("main")
            .join("kotlin")
            .join("OrderService.kt"),
        "class OrderService\n",
    )?;

    let detected = detect_languages(dir.path())?;
    assert_eq!(
        detected.first().map(|item| item.language.as_str()),
        Some("kotlin")
    );
    assert!(
        detected.iter().all(|item| item.language != "java"),
        "build.gradle.kts should not add Java by itself when Kotlin sources disambiguate it"
    );
    Ok(())
}

#[test]
fn detects_mixed_java_kotlin_gradle_kts_project_as_both() -> Result<()> {
    let dir = tempdir()?;
    fs::create_dir_all(dir.path().join("src").join("main").join("java"))?;
    fs::create_dir_all(dir.path().join("src").join("main").join("kotlin"))?;
    fs::write(
        dir.path().join("build.gradle.kts"),
        "plugins { kotlin(\"jvm\") version \"1.9.0\" }\n",
    )?;
    fs::write(
        dir.path()
            .join("src")
            .join("main")
            .join("java")
            .join("OrderController.java"),
        "class OrderController {}\n",
    )?;
    fs::write(
        dir.path()
            .join("src")
            .join("main")
            .join("kotlin")
            .join("OrderService.kt"),
        "class OrderService\n",
    )?;

    let detected = detect_languages(dir.path())?;
    assert!(detected.iter().any(|item| item.language == "java"));
    assert!(detected.iter().any(|item| item.language == "kotlin"));
    Ok(())
}

#[test]
fn detects_kotlin_from_settings_gradle_kts() -> Result<()> {
    let dir = tempdir()?;
    fs::create_dir_all(dir.path().join("src").join("main").join("kotlin"))?;
    fs::write(
        dir.path().join("settings.gradle.kts"),
        "rootProject.name = \"orders\"\n",
    )?;
    fs::write(
        dir.path()
            .join("src")
            .join("main")
            .join("kotlin")
            .join("OrderService.kt"),
        "class OrderService\n",
    )?;

    let detected = detect_languages(dir.path())?;
    assert_eq!(
        detected.first().map(|item| item.language.as_str()),
        Some("kotlin")
    );
    Ok(())
}

#[test]
fn detects_extensionless_python_shebang_file() -> Result<()> {
    let dir = tempdir()?;
    fs::write(
        dir.path().join("tool"),
        "#!/usr/bin/env python3\ndef main():\n    return 1\n",
    )?;
    let detected = detect_languages(dir.path())?;
    assert!(detected.iter().any(|item| item.language == "python"));
    Ok(())
}

#[test]
fn indexes_kotlin_script_extension() -> Result<()> {
    let dir = tempdir()?;
    fs::write(
        dir.path().join("script.kts"),
        "fun scriptedValue(): Int = 42\n",
    )?;
    let config = TsIndexConfig::default();
    config.write(dir.path())?;
    let runtime = Runtime::new(
        dir.path().to_path_buf(),
        db_path(dir.path()),
        config,
        vec!["kotlin".to_string()],
    );
    runtime.build(false, None)?;
    let found = runtime.get_symbol(GetSymbolArgs {
        name: Some("scriptedValue".into()),
        names: None,
        repo: None,
        kind: None,
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(found.matches.len(), 1);
    assert_eq!(found.matches[0].file, "script.kts");
    Ok(())
}

#[test]
fn cmake_manifest_is_disambiguated_by_c_sources() -> Result<()> {
    let dir = tempdir()?;
    fs::create_dir_all(dir.path().join("src"))?;
    fs::write(
        dir.path().join("CMakeLists.txt"),
        "project(order_service C)\n",
    )?;
    fs::write(
        dir.path().join("src").join("orders.c"),
        "int submit_order(void) { return 1; }\n",
    )?;

    let detected = detect_languages(dir.path())?;
    assert_eq!(
        detected.first().map(|item| item.language.as_str()),
        Some("c")
    );
    assert!(
        detected.iter().all(|item| item.language != "cpp"),
        "CMakeLists.txt should not add C++ by itself when C sources disambiguate it"
    );
    Ok(())
}

#[test]
fn builds_index_and_answers_queries() -> Result<()> {
    let dir = tempdir()?;
    fs::create_dir_all(dir.path().join("src"))?;
    fs::create_dir_all(dir.path().join("tests"))?;
    fs::write(
        dir.path().join("pyproject.toml"),
        "[project]\nname = 'fixture'\n",
    )?;
    fs::write(
        dir.path().join("src").join("login.py"),
        r#"
class LoginManager:
    """Coordinates session + credential checks."""
    def authenticate_user(self, email: str, password: str):
        return email

def login(email: str, password: str):
    mgr = LoginManager()
    return mgr.authenticate_user(email, password)
"#,
    )?;
    fs::write(
        dir.path().join("tests").join("test_login.py"),
        r#"
from src.login import authenticate_user

def test_authenticate_user():
    return authenticate_user("a@b.c", "hunter2")
"#,
    )?;

    let config = TsIndexConfig::default();
    config.write(dir.path())?;
    let runtime = Runtime::new(
        dir.path().to_path_buf(),
        db_path(dir.path()),
        config,
        vec!["python".to_string()],
    );
    runtime.build(false, None)?;

    let symbol = runtime.get_symbol(GetSymbolArgs {
        name: Some("authenticate_user".to_string()),
        names: None,
        repo: None,
        kind: None,
        file_glob: None,
        include_body: true,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert!(!symbol.matches.is_empty());
    assert!(
        symbol.matches[0]
            .body
            .as_deref()
            .unwrap_or_default()
            .contains("def authenticate_user")
    );

    let outline = runtime.list_file_outline(OutlineArgs {
        file: "src/login.py".to_string(),
        repo: None,
        depth: 2,
        include_signatures: true,
        include_docstrings: true,
        include_imports: false,
        include_bodies_for: None,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert!(!outline.symbols.is_empty());

    let refs = runtime.find_references(FindReferencesArgs {
        name: Some("authenticate_user".to_string()),
        names: None,
        repo: None,
        scope: None,
        exclude_tests: false,
        include_declarations: false,
        group_by_file: true,
        snippet_lines: 1,
        limit: None,
        offset: None,
        counts_only: false,
    })?;
    assert!(refs.total >= 2);

    let query = runtime.query(QueryArgs {
        language: "python".to_string(),
        repo: None,
        query: "(function_definition name: (identifier) @fn)".to_string(),
        file_glob: Some("src/**/*.py".to_string()),
        capture: Some("fn".to_string()),
        limit: Some(10),
        offset: None,
    })?;
    assert!(
        query
            .captures
            .iter()
            .any(|capture| capture.text == "authenticate_user")
    );
    Ok(())
}

#[test]
fn build_indexes_all_files_across_chunk_boundaries() -> Result<()> {
    // The streaming build processes files in fixed-size batches (BUILD_CHUNK_SIZE
    // = 128) and flushes a trailing partial batch after the walk. Use a count
    // that is not a multiple of the chunk size so a dropped final batch or an
    // off-by-one at a chunk boundary would surface as a missing file.
    const FILE_COUNT: usize = 200;

    let dir = tempdir()?;
    fs::create_dir_all(dir.path().join("src"))?;
    fs::write(
        dir.path().join("pyproject.toml"),
        "[project]\nname = 'fixture'\n",
    )?;
    for i in 0..FILE_COUNT {
        fs::write(
            dir.path().join("src").join(format!("mod_{i}.py")),
            format!("def func_{i}():\n    return {i}\n"),
        )?;
    }

    let config = TsIndexConfig::default();
    config.write(dir.path())?;
    let runtime = Runtime::new(
        dir.path().to_path_buf(),
        db_path(dir.path()),
        config,
        vec!["python".to_string()],
    );

    let stats = runtime.build(false, None)?;
    assert_eq!(
        stats.indexed, FILE_COUNT,
        "every file across all chunks must be indexed"
    );
    assert_eq!(stats.failed, 0);

    // A symbol from the trailing partial batch (file index past the last full
    // chunk boundary) must be retrievable, not merely counted.
    let last = runtime.get_symbol(GetSymbolArgs {
        name: Some(format!("func_{}", FILE_COUNT - 1)),
        names: None,
        repo: None,
        kind: None,
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert!(
        !last.matches.is_empty(),
        "symbol from the trailing batch must be indexed and queryable"
    );
    Ok(())
}

#[test]
fn outlines_symbol_free_indexed_file() -> Result<()> {
    let dir = tempdir()?;
    fs::create_dir_all(dir.path().join("src"))?;
    fs::write(
        dir.path().join("src").join("empty.rs"),
        "// no symbols here\n",
    )?;

    let config = TsIndexConfig::default();
    config.write(dir.path())?;
    let runtime = Runtime::new(
        dir.path().to_path_buf(),
        db_path(dir.path()),
        config,
        vec!["rust".to_string()],
    );
    runtime.build(false, None)?;

    let outline = runtime.list_file_outline(OutlineArgs {
        file: "src/empty.rs".to_string(),
        repo: None,
        depth: 2,
        include_signatures: true,
        include_docstrings: true,
        include_imports: false,
        include_bodies_for: None,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(outline.file, "src/empty.rs");
    assert!(outline.symbols.is_empty());

    Ok(())
}

#[test]
fn indexes_tsx_files_with_tsx_parser() -> Result<()> {
    let dir = tempdir()?;
    fs::create_dir_all(dir.path().join("src"))?;
    fs::write(
        dir.path().join("src").join("App.tsx"),
        r#"
export function App() {
    return <main>Hello</main>;
}
"#,
    )?;

    let config = TsIndexConfig::default();
    config.write(dir.path())?;
    let runtime = Runtime::new(
        dir.path().to_path_buf(),
        db_path(dir.path()),
        config,
        vec!["tsx".to_string()],
    );
    runtime.build(false, None)?;

    let symbol = runtime.get_symbol(GetSymbolArgs {
        name: Some("App".to_string()),
        names: None,
        repo: None,
        kind: Some("function".to_string()),
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(symbol.matches.len(), 1);
    assert_eq!(symbol.matches[0].language, "tsx");

    Ok(())
}

#[test]
fn incremental_build_reindexes_when_detected_language_changes() -> Result<()> {
    let dir = tempdir()?;
    fs::create_dir_all(dir.path().join("src"))?;
    fs::write(
        dir.path().join("src").join("App.tsx"),
        r#"
export function App() {
    return <main>Hello</main>;
}
"#,
    )?;

    let config = TsIndexConfig::default();
    config.write(dir.path())?;
    let runtime = Runtime::new(
        dir.path().to_path_buf(),
        db_path(dir.path()),
        config,
        vec!["tsx".to_string()],
    );
    runtime.build(false, None)?;

    let conn = Connection::open(db_path(dir.path()))?;
    conn.execute(
        "UPDATE files SET language = 'typescript' WHERE path = 'src/App.tsx'",
        [],
    )?;
    drop(conn);

    runtime.build(true, None)?;

    let symbol = runtime.get_symbol(GetSymbolArgs {
        name: Some("App".to_string()),
        names: None,
        repo: None,
        kind: Some("function".to_string()),
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(symbol.matches.len(), 1);
    assert_eq!(symbol.matches[0].language, "tsx");

    Ok(())
}

#[test]
fn exclude_tests_filters_top_level_tests_dir() -> Result<()> {
    let dir = tempdir()?;
    fs::create_dir_all(dir.path().join("tests"))?;
    fs::write(
        dir.path().join("tests").join("test_login.py"),
        "def helper():\n    return helper()\n",
    )?;

    let config = TsIndexConfig::default();
    config.write(dir.path())?;
    let runtime = Runtime::new(
        dir.path().to_path_buf(),
        db_path(dir.path()),
        config,
        vec!["python".to_string()],
    );
    runtime.build(false, None)?;

    let refs = runtime.find_references(FindReferencesArgs {
        name: Some("helper".to_string()),
        names: None,
        repo: None,
        scope: None,
        exclude_tests: true,
        include_declarations: false,
        group_by_file: true,
        snippet_lines: 1,
        limit: None,
        offset: None,
        counts_only: false,
    })?;
    assert_eq!(refs.total, 0);

    Ok(())
}

#[test]
fn incremental_build_purges_ignored_file() -> Result<()> {
    let dir = tempdir()?;
    fs::create_dir_all(dir.path().join("src"))?;
    fs::write(
        dir.path().join("src").join("login.py"),
        "def authenticate_user():\n    return True\n",
    )?;

    let config = TsIndexConfig::default();
    config.write(dir.path())?;
    let runtime = Runtime::new(
        dir.path().to_path_buf(),
        db_path(dir.path()),
        config,
        Vec::new(),
    );
    runtime.build(false, None)?;
    let before = runtime.get_symbol(GetSymbolArgs {
        name: Some("authenticate_user".to_string()),
        names: None,
        repo: None,
        kind: Some("function".to_string()),
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(before.matches.len(), 1);

    fs::write(dir.path().join(".tsindexignore"), "src/login.py\n")?;
    runtime.build(true, None)?;

    let after = runtime.get_symbol(GetSymbolArgs {
        name: Some("authenticate_user".to_string()),
        names: None,
        repo: None,
        kind: Some("function".to_string()),
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert!(after.matches.is_empty());

    Ok(())
}

#[test]
fn language_filtered_update_preserves_other_language_entries() -> Result<()> {
    let dir = tempdir()?;
    fs::create_dir_all(dir.path().join("src"))?;
    fs::write(
        dir.path().join("src").join("login.py"),
        "def authenticate_user():\n    return True\n",
    )?;
    fs::write(
        dir.path().join("src").join("lib.rs"),
        "pub fn rust_symbol() -> bool { true }\n",
    )?;

    let config = TsIndexConfig::default();
    config.write(dir.path())?;
    let full_runtime = Runtime::new(
        dir.path().to_path_buf(),
        db_path(dir.path()),
        config.clone(),
        vec!["python".to_string(), "rust".to_string()],
    );
    full_runtime.build(false, None)?;

    let before = full_runtime.get_symbol(GetSymbolArgs {
        name: Some("rust_symbol".to_string()),
        names: None,
        repo: None,
        kind: Some("function".to_string()),
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(before.matches.len(), 1);

    let python_runtime = Runtime::new(
        dir.path().to_path_buf(),
        db_path(dir.path()),
        config,
        vec!["python".to_string()],
    );
    python_runtime.build(true, None)?;

    let after = python_runtime.get_symbol(GetSymbolArgs {
        name: Some("rust_symbol".to_string()),
        names: None,
        repo: None,
        kind: Some("function".to_string()),
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(after.matches.len(), 1);

    Ok(())
}

#[test]
fn query_is_not_truncated_when_result_count_equals_limit() -> Result<()> {
    let dir = tempdir()?;
    fs::create_dir_all(dir.path().join("src"))?;
    fs::write(
        dir.path().join("src").join("mod.py"),
        "def one():\n    pass\n\ndef two():\n    pass\n",
    )?;

    let config = TsIndexConfig::default();
    config.write(dir.path())?;
    let runtime = Runtime::new(
        dir.path().to_path_buf(),
        db_path(dir.path()),
        config,
        vec!["python".to_string()],
    );
    runtime.build(false, None)?;

    let query = runtime.query(QueryArgs {
        language: "python".to_string(),
        repo: None,
        query: "(function_definition name: (identifier) @fn)".to_string(),
        file_glob: Some("src/**/*.py".to_string()),
        capture: Some("fn".to_string()),
        limit: Some(2),
        offset: None,
    })?;
    assert_eq!(query.captures.len(), 2);
    assert!(!query.truncated);
    assert!(!query.timed_out);

    Ok(())
}

#[test]
fn builds_multi_repo_catalog_and_filters_by_repo() -> Result<()> {
    let catalog = tempdir()?;
    let repo_a = tempdir()?;
    let repo_b = tempdir()?;

    fs::create_dir_all(repo_a.path().join("src"))?;
    fs::create_dir_all(repo_b.path().join("src"))?;
    fs::write(
        repo_a.path().join("pyproject.toml"),
        "[project]\nname = 'repo-a'\n",
    )?;
    fs::write(
        repo_b.path().join("Cargo.toml"),
        "[package]\nname = 'repo-b'\nversion = '0.1.0'\nedition = '2024'\n",
    )?;
    fs::write(
        repo_a.path().join("src").join("login.py"),
        "def authenticate_user(email, password):\n    return email\n",
    )?;
    fs::write(
        repo_b.path().join("src").join("lib.rs"),
        "pub fn authenticate_user(email: &str, password: &str) -> &str { email }\n",
    )?;

    let config = TsIndexConfig {
        repos: vec![
            RepoConfig {
                name: "python-app".to_string(),
                path: repo_a.path().to_string_lossy().to_string(),
                languages: Vec::new(),
                ignore: Vec::new(),
            },
            RepoConfig {
                name: "rust-lib".to_string(),
                path: repo_b.path().to_string_lossy().to_string(),
                languages: Vec::new(),
                ignore: Vec::new(),
            },
        ],
        ..TsIndexConfig::default()
    };
    config.write(catalog.path())?;

    let runtime = Runtime::new(
        catalog.path().to_path_buf(),
        db_path(catalog.path()),
        config,
        Vec::new(),
    );
    runtime.build(false, None)?;

    let languages = runtime.detected_languages()?;
    assert_eq!(languages.len(), 2);

    let symbols = runtime.get_symbol(GetSymbolArgs {
        name: Some("authenticate_user".to_string()),
        names: None,
        repo: None,
        kind: None,
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(symbols.matches.len(), 2);

    let rust_only = runtime.get_symbol(GetSymbolArgs {
        name: Some("authenticate_user".to_string()),
        names: None,
        repo: Some("rust-lib".to_string()),
        kind: None,
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(rust_only.matches.len(), 1);
    assert_eq!(rust_only.matches[0].repo, "rust-lib");

    Ok(())
}

#[test]
fn enclosing_symbol_errors_when_path_is_ambiguous_across_repos() -> Result<()> {
    // Regression test: when the same repo-relative path exists in two
    // configured repos and the caller omits `repo`, enclosing_symbol
    // used to silently return symbols from whichever repo's smallest
    // range happened to win the ORDER BY tie-break. list_file_outline
    // already errored with "file X exists in multiple repos; rerun
    // with --repo" — this test pins that enclosing_symbol now matches.
    let catalog = tempdir()?;
    let repo_a = tempdir()?;
    let repo_b = tempdir()?;

    fs::create_dir_all(repo_a.path().join("src"))?;
    fs::create_dir_all(repo_b.path().join("src"))?;
    fs::write(
        repo_a.path().join("pyproject.toml"),
        "[project]\nname = 'app-a'\n",
    )?;
    fs::write(
        repo_b.path().join("pyproject.toml"),
        "[project]\nname = 'app-b'\n",
    )?;
    // Same path in both repos. Different bodies so we can distinguish
    // which repo a successful lookup actually returned.
    fs::write(
        repo_a.path().join("src").join("util.py"),
        "def helper():\n    return 'from-a'\n",
    )?;
    fs::write(
        repo_b.path().join("src").join("util.py"),
        "def helper():\n    return 'from-b'\n",
    )?;

    let config = TsIndexConfig {
        repos: vec![
            RepoConfig {
                name: "app-a".to_string(),
                path: repo_a.path().to_string_lossy().to_string(),
                languages: Vec::new(),
                ignore: Vec::new(),
            },
            RepoConfig {
                name: "app-b".to_string(),
                path: repo_b.path().to_string_lossy().to_string(),
                languages: Vec::new(),
                ignore: Vec::new(),
            },
        ],
        ..TsIndexConfig::default()
    };
    config.write(catalog.path())?;

    let runtime = Runtime::new(
        catalog.path().to_path_buf(),
        db_path(catalog.path()),
        config,
        Vec::new(),
    );
    runtime.build(false, None)?;

    // Without --repo: must error rather than silently picking a side.
    let ambiguous = runtime.enclosing_symbol(EnclosingSymbolArgs {
        file: "src/util.py".to_string(),
        row: Some(1),
        rows: None,
        col: None,
        repo: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        depth: 1,
        limit: None,
        offset: None,
    });
    let error = ambiguous.expect_err("expected ambiguous-repo lookup to error");
    assert!(
        error
            .to_string()
            .contains("exists in multiple repos; rerun with --repo"),
        "expected ambiguity error mentioning multi-repo guidance; got: {error}"
    );

    // With --repo: must return only that repo's symbol.
    let only_a = runtime.enclosing_symbol(EnclosingSymbolArgs {
        file: "src/util.py".to_string(),
        row: Some(1),
        rows: None,
        col: None,
        repo: Some("app-a".to_string()),
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        depth: 1,
        limit: None,
        offset: None,
    })?;
    assert_eq!(only_a.repo, "app-a");
    assert_eq!(only_a.matches.len(), 1);
    assert_eq!(only_a.matches[0].name, "helper");

    Ok(())
}

#[test]
fn enclosing_symbol_populates_repo_even_when_no_match() -> Result<()> {
    // Empty-match responses used to set repo: "" — but the file lookup
    // already proved which repo owns the file, so the response can
    // honestly carry the resolved name. Pin this so consumers stop
    // having to special-case empty-string sentinels.
    let dir = tempdir()?;
    fs::create_dir_all(dir.path().join("src"))?;
    fs::write(
        dir.path().join("pyproject.toml"),
        "[project]\nname = 'fixture'\n",
    )?;
    fs::write(
        dir.path().join("src").join("noop.py"),
        "# top-level comment, no symbol captures\n",
    )?;

    let config = TsIndexConfig::default();
    config.write(dir.path())?;
    let runtime = Runtime::new(
        dir.path().to_path_buf(),
        db_path(dir.path()),
        config,
        vec!["python".to_string()],
    );
    runtime.build(false, None)?;

    let response = runtime.enclosing_symbol(EnclosingSymbolArgs {
        file: "src/noop.py".to_string(),
        row: Some(1),
        rows: None,
        col: None,
        repo: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        depth: 1,
        limit: None,
        offset: None,
    })?;
    assert!(response.matches.is_empty());
    assert_eq!(response.file, "src/noop.py");
    assert!(
        !response.repo.is_empty(),
        "expected resolved repo name to be populated even on empty match; got empty string"
    );

    Ok(())
}

#[test]
fn captured_python_class_range_includes_decorators() -> Result<()> {
    // Regression test: tree-sitter-python wraps decorated definitions in a
    // `decorated_definition` node. The bare class_definition /
    // function_definition node range starts at the `class` / `def`
    // keyword, so without the decorator-wrapper expansion in
    // extract_symbols a captured @dataclass class would have a range that
    // begins below the decorator. This test pins the expected post-fix
    // behavior.
    let dir = tempdir()?;
    fs::create_dir_all(dir.path().join("src"))?;
    fs::write(
        dir.path().join("pyproject.toml"),
        "[project]\nname = 'fixture'\n",
    )?;
    // Line numbering for the expectations below (0-based rows):
    //   row 0: @dataclass
    //   row 1: class User:
    //   row 2:     name: str
    //   row 3: (blank)
    //   row 4: @cached
    //   row 5: @property
    //   row 6: def status(self):
    //   row 7:     return "ok"
    fs::write(
        dir.path().join("src").join("user.py"),
        "@dataclass\nclass User:\n    name: str\n\n@cached\n@property\ndef status(self):\n    return \"ok\"\n",
    )?;

    let config = TsIndexConfig::default();
    config.write(dir.path())?;
    let runtime = Runtime::new(
        dir.path().to_path_buf(),
        db_path(dir.path()),
        config,
        vec!["python".to_string()],
    );
    runtime.build(false, None)?;

    let class_match = runtime.get_symbol(GetSymbolArgs {
        name: Some("User".to_string()),
        names: None,
        repo: None,
        kind: None,
        file_glob: None,
        include_body: true,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(class_match.matches.len(), 1);
    let class_symbol = &class_match.matches[0];
    assert_eq!(
        class_symbol.range.start.0, 0,
        "expected User class range to start at the @dataclass line (row 0), \
         got row {}; the decorator-wrapper expansion in extract_symbols may \
         be missing",
        class_symbol.range.start.0
    );
    let class_body = class_symbol.body.as_deref().unwrap_or_default();
    assert!(
        class_body.starts_with("@dataclass"),
        "expected captured body to start with the @dataclass decorator; got: {class_body:?}"
    );

    let fn_match = runtime.get_symbol(GetSymbolArgs {
        name: Some("status".to_string()),
        names: None,
        repo: None,
        kind: None,
        file_glob: None,
        include_body: true,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(fn_match.matches.len(), 1);
    let fn_symbol = &fn_match.matches[0];
    assert_eq!(
        fn_symbol.range.start.0, 4,
        "expected status function range to start at the first @cached \
         decorator (row 4), got row {}",
        fn_symbol.range.start.0
    );
    let fn_body = fn_symbol.body.as_deref().unwrap_or_default();
    assert!(
        fn_body.starts_with("@cached"),
        "expected captured body to start with the @cached decorator; got: {fn_body:?}"
    );

    Ok(())
}

#[test]
fn stale_schema_version_dirties_files_so_incremental_build_reextracts() -> Result<()> {
    // Regression test for the v2->v3 stale-data scenario.
    //
    // Before the fix, `migrate_schema` only emitted a stderr warning
    // when on-disk version was older than the binary, and `tsindex
    // build` defaulted to incremental — so a v2 user upgrading the
    // binary saw a one-line warning, ran the recommended `tsindex
    // build`, got "skipped: N changed=0", and kept stale Python
    // decorator ranges indefinitely.
    //
    // The fix dirties every file's mtime_ns when stale data is
    // detected, guaranteeing that the next incremental build
    // re-extracts every row.
    let dir = tempdir()?;
    fs::create_dir_all(dir.path().join("src"))?;
    fs::write(
        dir.path().join("pyproject.toml"),
        "[project]\nname = 'fixture'\n",
    )?;
    fs::write(
        dir.path().join("src").join("user.py"),
        "@dataclass\nclass User:\n    name: str\n",
    )?;

    let config = TsIndexConfig::default();
    config.write(dir.path())?;
    // No CLI `--languages` filter: a scoped build deliberately never stamps
    // SCHEMA_VERSION, and this test asserts the stamp after the rebuild.
    let runtime = Runtime::new(
        dir.path().to_path_buf(),
        db_path(dir.path()),
        config.clone(),
        Vec::new(),
    );
    runtime.build(false, None)?;

    let db = db_path(dir.path());

    // Capture pre-state: every indexed file has a positive mtime_ns
    // from the real on-disk stat() call.
    {
        let conn = Connection::open(&db)?;
        let positive_mtime_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM files WHERE mtime_ns >= 0",
            [],
            |row| row.get(0),
        )?;
        assert!(
            positive_mtime_count > 0,
            "expected the initial build to populate at least one file row"
        );
    }

    // Simulate a stale upgrade: bump schema_version on disk down to 2
    // (the value before this PR's decorator-aware extraction landed).
    {
        let conn = Connection::open(&db)?;
        conn.pragma_update(None, "user_version", 2_i64)?;
    }

    // Opening a fresh Runtime and triggering migrate_schema (via
    // initialize → create_schema) should now mark every files row as
    // dirty so the next incremental build will re-extract it.
    let stale_runtime = Runtime::new(
        dir.path().to_path_buf(),
        db_path(dir.path()),
        config,
        Vec::new(),
    );
    stale_runtime.initialize()?;

    let conn = Connection::open(&db)?;
    let dirty_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM files WHERE mtime_ns = -1",
        [],
        |row| row.get(0),
    )?;
    assert!(
        dirty_count > 0,
        "expected migrate_schema to mark every files row stale (mtime_ns=-1) \
         when on-disk version was older than SCHEMA_VERSION; got dirty_count={dirty_count}"
    );

    let still_clean: i64 = conn.query_row(
        "SELECT COUNT(*) FROM files WHERE mtime_ns >= 0",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(
        still_clean, 0,
        "expected ALL files to be marked stale, but {still_clean} rows still \
         have a non-negative mtime_ns; the next incremental build would skip those \
         and the user would silently keep stale ranges"
    );

    // The on-disk version must stay OLD until a build has re-extracted the
    // dirtied files: stamping it in create_schema would silence the stale
    // warning for every later process while the data is still stale (F05).
    let post_version: i64 = conn.pragma_query_value(None, "user_version", |row| row.get(0))?;
    assert_eq!(
        post_version, 2,
        "initialize must not claim the data is current; got {post_version}"
    );
    drop(conn);

    stale_runtime.build(true, None)?;
    let conn = Connection::open(&db)?;
    let rebuilt_version: i64 = conn.pragma_query_value(None, "user_version", |row| row.get(0))?;
    assert_eq!(
        rebuilt_version, 5,
        "a completed build stamps SCHEMA_VERSION; got {rebuilt_version}"
    );

    Ok(())
}

#[test]
fn captured_java_method_range_includes_annotations() -> Result<()> {
    // Tree-sitter-java's `method_declaration` rule already includes an
    // optional `(modifiers ...)` child covering annotations, so the
    // captured node range should include `@Override` without any
    // decorator-wrapper expansion. This test guards that behavior so a
    // future grammar change or query refactor can't silently drop
    // annotations from method ranges.
    let dir = tempdir()?;
    fs::create_dir_all(dir.path().join("src"))?;
    fs::write(
        dir.path().join("pom.xml"),
        "<project><modelVersion>4.0.0</modelVersion></project>\n",
    )?;
    // Line numbering (0-based):
    //   row 0: public class Service {
    //   row 1:     @Override
    //   row 2:     public void doWork() {
    //   row 3:         // ...
    //   row 4:     }
    //   row 5: }
    fs::write(
        dir.path().join("src").join("Service.java"),
        "public class Service {\n    @Override\n    public void doWork() {\n        // ...\n    }\n}\n",
    )?;

    let config = TsIndexConfig::default();
    config.write(dir.path())?;
    let runtime = Runtime::new(
        dir.path().to_path_buf(),
        db_path(dir.path()),
        config,
        vec!["java".to_string()],
    );
    runtime.build(false, None)?;

    let method_match = runtime.get_symbol(GetSymbolArgs {
        name: Some("doWork".to_string()),
        names: None,
        repo: None,
        kind: None,
        file_glob: None,
        include_body: true,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(method_match.matches.len(), 1);
    let method_symbol = &method_match.matches[0];
    assert_eq!(
        method_symbol.range.start.0, 1,
        "expected doWork method range to start at @Override (row 1), got \
         row {}; tree-sitter-java should already include annotations as \
         part of method_declaration via the modifiers field",
        method_symbol.range.start.0
    );

    Ok(())
}

/// Build a small Python fixture with clear nesting and return a runtime
/// pointed at it. Used by the enclosing_symbol tests below.
///
/// Row layout (0-based):
///   0: # top-level comment
///   1: class Outer:
///   2:     def method_a(self):
///   3:         x = 1
///   4:         return x
///   5: (blank)
///   6:     class Inner:
///   7:         def method_b(self):
///   8:             return 2
fn build_nested_python_fixture() -> Result<(tempfile::TempDir, Runtime)> {
    let dir = tempdir()?;
    fs::create_dir_all(dir.path().join("src"))?;
    fs::write(
        dir.path().join("pyproject.toml"),
        "[project]\nname = 'fixture'\n",
    )?;
    fs::write(
        dir.path().join("src").join("nested.py"),
        "# top-level comment\n\
         class Outer:\n\
         \x20\x20\x20\x20def method_a(self):\n\
         \x20\x20\x20\x20\x20\x20\x20\x20x = 1\n\
         \x20\x20\x20\x20\x20\x20\x20\x20return x\n\
         \n\
         \x20\x20\x20\x20class Inner:\n\
         \x20\x20\x20\x20\x20\x20\x20\x20def method_b(self):\n\
         \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20return 2\n",
    )?;

    let config = TsIndexConfig::default();
    config.write(dir.path())?;
    let runtime = Runtime::new(
        dir.path().to_path_buf(),
        db_path(dir.path()),
        config,
        vec!["python".to_string()],
    );
    runtime.build(false, None)?;
    Ok((dir, runtime))
}

#[test]
fn enclosing_symbol_treats_end_position_as_exclusive() -> Result<()> {
    // Tree-sitter end positions are exclusive: a node's end_position
    // is the byte/point AFTER the last character it covers. The SQL
    // predicate must use strict `>` on end_col so that a position
    // exactly at (end_row, end_col) — one past the last character —
    // is NOT reported as enclosed. Otherwise stack-trace lookups that
    // point at the closing brace/quote of a symbol get the wrong hit.
    let (_dir, runtime) = build_nested_python_fixture()?;

    // Pick the byte just past `method_a`'s end. We don't know the
    // exact (end_row, end_col) without inspecting the parsed range,
    // so look it up from get_symbol first.
    let method_match = runtime.get_symbol(GetSymbolArgs {
        name: Some("method_a".to_string()),
        names: None,
        repo: None,
        kind: None,
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(method_match.matches.len(), 1);
    let end_row = method_match.matches[0].range.end.0;
    let end_col = method_match.matches[0].range.end.1;

    // Position exactly at (end_row, end_col): one past the last
    // character of method_a's range. Should NOT be reported as
    // enclosed by method_a.
    let response = runtime.enclosing_symbol(EnclosingSymbolArgs {
        file: "src/nested.py".to_string(),
        row: Some(end_row + 1),
        rows: None,
        col: Some(end_col),
        repo: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        depth: 5,
        limit: None,
        offset: None,
    })?;
    assert!(
        response.matches.iter().all(|m| m.name != "method_a"),
        "position at (end_row, end_col) sits one past method_a's last \
         character; method_a should NOT be reported as enclosing it. \
         Got matches: {:?}",
        response.matches.iter().map(|m| &m.name).collect::<Vec<_>>()
    );

    // Sanity: the column immediately before the end is still inside.
    if end_col > 0 {
        let inside = runtime.enclosing_symbol(EnclosingSymbolArgs {
            file: "src/nested.py".to_string(),
            row: Some(end_row + 1),
            rows: None,
            col: Some(end_col - 1),
            repo: None,
            include_body: false,
            context_lines: 0,
            max_body_lines: None,
            depth: 5,
            limit: None,
            offset: None,
        })?;
        assert!(
            inside.matches.iter().any(|m| m.name == "method_a"),
            "position one column before end_col should still be enclosed by \
             method_a; got matches: {:?}",
            inside.matches.iter().map(|m| &m.name).collect::<Vec<_>>()
        );
    }

    Ok(())
}

#[test]
fn enclosing_symbol_returns_innermost_at_position() -> Result<()> {
    // Row 3 (1-based) is `    def method_a(self):` — the start of
    // method_a's body. There's no nested variable assignment on this row,
    // so the innermost symbol should be method_a itself. (Note: row 4 has
    // `x = 1`, which Python's symbol_query captures as a `variable`
    // symbol — so the innermost there is `x`, not `method_a`. See
    // the depth-trail test below.)
    let (_dir, runtime) = build_nested_python_fixture()?;

    let response = runtime.enclosing_symbol(EnclosingSymbolArgs {
        file: "src/nested.py".to_string(),
        row: Some(3),
        rows: None,
        col: Some(8),
        repo: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        depth: 1,
        limit: None,
        offset: None,
    })?;

    assert_eq!(response.matches.len(), 1);
    assert_eq!(response.matches[0].name, "method_a");
    assert_eq!(response.file, "src/nested.py");
    Ok(())
}

#[test]
fn enclosing_symbol_with_depth_returns_outermost_first_trail() -> Result<()> {
    // Row 4 (1-based) is `        x = 1`. Python's symbol_query captures
    // the assignment as a `variable` symbol named `x`, so this row sits
    // inside three nested symbols at once: Outer (class) → method_a
    // (function) → x (variable). depth=3 returns all three; the
    // response is ordered outermost → innermost so callers can read
    // it as a containment trail.
    let (_dir, runtime) = build_nested_python_fixture()?;

    let response = runtime.enclosing_symbol(EnclosingSymbolArgs {
        file: "src/nested.py".to_string(),
        row: Some(4),
        rows: None,
        col: Some(8),
        repo: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        depth: 3,
        limit: None,
        offset: None,
    })?;

    let names: Vec<&str> = response
        .matches
        .iter()
        .map(|symbol| symbol.name.as_str())
        .collect();
    assert_eq!(names, vec!["Outer", "method_a", "x"]);
    Ok(())
}

#[test]
fn enclosing_symbol_supports_row_only_query() -> Result<()> {
    // When col is None the lookup is row-only — useful for stack-trace
    // lines and commit hunks that carry a line number but no column.
    // Row 3 (1-based) again to keep the innermost = method_a (no variable
    // assignments on this line).
    let (_dir, runtime) = build_nested_python_fixture()?;

    let response = runtime.enclosing_symbol(EnclosingSymbolArgs {
        file: "src/nested.py".to_string(),
        row: Some(3),
        rows: None,
        col: None,
        repo: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        depth: 1,
        limit: None,
        offset: None,
    })?;

    assert_eq!(response.matches.len(), 1);
    assert_eq!(response.matches[0].name, "method_a");
    Ok(())
}

#[test]
fn enclosing_symbol_returns_empty_matches_when_position_is_outside_any_symbol() -> Result<()> {
    // Row 1 (1-based) is the `# top-level comment` line. No symbol's
    // range contains it (Outer starts at row 2), so matches must be empty
    // — but the call must succeed because the file IS in the index.
    let (_dir, runtime) = build_nested_python_fixture()?;

    let response = runtime.enclosing_symbol(EnclosingSymbolArgs {
        file: "src/nested.py".to_string(),
        row: Some(1),
        rows: None,
        col: Some(0),
        repo: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        depth: 1,
        limit: None,
        offset: None,
    })?;

    assert!(
        response.matches.is_empty(),
        "expected no enclosing symbol at the top-level comment line, got: {:?}",
        response
            .matches
            .iter()
            .map(|symbol| &symbol.name)
            .collect::<Vec<_>>()
    );
    // Empty matches still report the repo that owns the file — the
    // file existed in the index, we just didn't find a symbol enclosing
    // the position. Earlier behavior returned `""` here as a sentinel,
    // which forced consumers to special-case the empty string.
    assert!(
        !response.repo.is_empty(),
        "expected the resolved repo name to populate even on empty matches; \
         was empty"
    );
    assert_eq!(response.file, "src/nested.py");
    Ok(())
}

#[test]
fn enclosing_symbol_errors_when_file_is_not_indexed() -> Result<()> {
    // A typo'd or unindexed file path is almost always a caller bug,
    // so we surface it as an error rather than silently returning an
    // empty result. Distinguishing this from the "no enclosing match"
    // case above is the point of the file-existence check.
    let (_dir, runtime) = build_nested_python_fixture()?;

    let result = runtime.enclosing_symbol(EnclosingSymbolArgs {
        file: "src/does_not_exist.py".to_string(),
        row: Some(1),
        rows: None,
        col: None,
        repo: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        depth: 1,
        limit: None,
        offset: None,
    });

    let error = result.expect_err("expected an error for an unindexed file");
    let message = format!("{error}");
    assert!(
        message.contains("not found in index"),
        "expected 'not found in index' in error message, got: {message}"
    );
    Ok(())
}

#[test]
fn enclosing_symbol_includes_body_when_requested() -> Result<()> {
    // include_body=true must route through extract_body (the Phase A
    // helper) and produce a body that contains the method's source.
    // Use row 3 (1-based) so the innermost is method_a rather than an
    // inner variable.
    let (_dir, runtime) = build_nested_python_fixture()?;

    let response = runtime.enclosing_symbol(EnclosingSymbolArgs {
        file: "src/nested.py".to_string(),
        row: Some(3),
        rows: None,
        col: Some(8),
        repo: None,
        include_body: true,
        context_lines: 0,
        max_body_lines: None,
        depth: 1,
        limit: None,
        offset: None,
    })?;

    assert_eq!(response.matches.len(), 1);
    let body = response.matches[0]
        .body
        .as_deref()
        .expect("body should be present when include_body=true");
    assert!(
        body.contains("def method_a"),
        "expected method_a body, got: {body:?}"
    );
    Ok(())
}

#[test]
fn migrates_legacy_single_repo_database_to_repo_aware_schema() -> Result<()> {
    let dir = tempdir()?;
    fs::create_dir_all(dir.path().join("src"))?;
    fs::create_dir_all(dir.path().join(".tsindex"))?;
    fs::write(
        dir.path().join("src").join("login.py"),
        "def authenticate_user(email, password):\n    return email\n",
    )?;

    let db = db_path(dir.path());
    let conn = Connection::open(&db)?;
    conn.execute_batch(
        r#"
        PRAGMA user_version = 0;
        CREATE TABLE files (
          id INTEGER PRIMARY KEY,
          path TEXT UNIQUE NOT NULL,
          language TEXT NOT NULL,
          sha TEXT NOT NULL,
          mtime_ns INTEGER NOT NULL,
          byte_size INTEGER NOT NULL
        );
        CREATE TABLE symbols (
          id INTEGER PRIMARY KEY,
          file_id INTEGER NOT NULL,
          kind TEXT NOT NULL,
          name TEXT NOT NULL,
          qualified TEXT,
          start_row INTEGER NOT NULL,
          start_col INTEGER NOT NULL,
          end_row INTEGER NOT NULL,
          end_col INTEGER NOT NULL,
          signature TEXT,
          docstring TEXT
        );
        CREATE TABLE refs (
          id INTEGER PRIMARY KEY,
          file_id INTEGER NOT NULL,
          name TEXT NOT NULL,
          start_row INTEGER NOT NULL,
          start_col INTEGER NOT NULL,
          end_row INTEGER NOT NULL,
          end_col INTEGER NOT NULL,
          context TEXT
        );
        INSERT INTO files(id, path, language, sha, mtime_ns, byte_size)
        VALUES(1, 'src/login.py', 'python', 'legacysha', 1, 42);
        INSERT INTO symbols(id, file_id, kind, name, qualified, start_row, start_col, end_row, end_col, signature, docstring)
        VALUES(1, 1, 'function', 'authenticate_user', NULL, 0, 0, 1, 0, 'def authenticate_user(email, password):', NULL);
        INSERT INTO refs(id, file_id, name, start_row, start_col, end_row, end_col, context)
        VALUES(1, 1, 'authenticate_user', 0, 4, 0, 21, 'identifier');
        "#,
    )?;
    drop(conn);

    let runtime = Runtime::new(
        dir.path().to_path_buf(),
        db.clone(),
        TsIndexConfig::default(),
        vec!["python".to_string()],
    );
    runtime.initialize()?;

    let conn = Connection::open(&db)?;
    let version: i64 = conn.pragma_query_value(None, "user_version", |row| row.get(0))?;
    // The legacy layout is migrated in place but its rows were extracted by
    // a v0 binary, so the data is stale until rebuilt. It is parked at
    // version 1 (not the current SCHEMA_VERSION) so reads keep warning and a
    // later open cannot mistake it for a fresh database.
    assert_eq!(version, 1);

    let repo_count: i64 = conn.query_row("SELECT COUNT(*) FROM repos", [], |row| row.get(0))?;
    assert_eq!(repo_count, 1);

    let files_repo_id_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM files WHERE repo_id IS NOT NULL",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(files_repo_id_count, 1);

    let symbol = runtime.get_symbol(GetSymbolArgs {
        name: Some("authenticate_user".to_string()),
        names: None,
        repo: None,
        kind: None,
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(symbol.matches.len(), 1);
    assert_eq!(symbol.matches[0].file, "src/login.py");

    Ok(())
}

#[test]
fn indexes_kotlin_symbols_and_references() -> Result<()> {
    let catalog = tempdir()?;
    let repo = tempdir()?;

    fs::create_dir_all(repo.path().join("src").join("main").join("kotlin"))?;
    fs::write(
        repo.path().join("build.gradle.kts"),
        "plugins { kotlin(\"jvm\") version \"1.9.0\" }\n",
    )?;
    fs::write(
        repo.path()
            .join("src")
            .join("main")
            .join("kotlin")
            .join("OrderService.kt"),
        r#"package com.example.orders

import java.time.Instant

typealias OrderId = String

object OrderRegistry {
    val activeCount = 0
}

interface OrderRepository {
    fun findOrder(orderId: OrderId): OrderId
}

class OrderService {
    val cacheName = "orders"

    fun submitOrder(orderId: OrderId): String {
        val submittedAt = Instant.now()
        return "$orderId:$submittedAt"
    }
}
"#,
    )?;

    let config = TsIndexConfig {
        repos: vec![RepoConfig {
            name: "kotlin-service".to_string(),
            path: repo.path().to_string_lossy().to_string(),
            languages: Vec::new(),
            ignore: Vec::new(),
        }],
        ..TsIndexConfig::default()
    };
    config.write(catalog.path())?;

    let runtime = Runtime::new(
        catalog.path().to_path_buf(),
        db_path(catalog.path()),
        config,
        Vec::new(),
    );
    runtime.build(false, None)?;

    let languages = runtime.detected_languages()?;
    assert!(
        languages.iter().any(|repo| repo.repo == "kotlin-service"
            && repo
                .languages
                .iter()
                .any(|language| language.language == "kotlin")),
        "Kotlin should be detected from build.gradle.kts and .kt files"
    );

    let class = runtime.get_symbol(GetSymbolArgs {
        name: Some("OrderService".to_string()),
        names: None,
        repo: Some("kotlin-service".to_string()),
        kind: Some("class".to_string()),
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert!(!class.matches.is_empty(), "Kotlin class should be indexed");

    let interface = runtime.get_symbol(GetSymbolArgs {
        name: Some("OrderRepository".to_string()),
        names: None,
        repo: Some("kotlin-service".to_string()),
        kind: Some("interface".to_string()),
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert!(
        !interface.matches.is_empty(),
        "Kotlin interface should be indexed"
    );

    let interface_as_class = runtime.get_symbol(GetSymbolArgs {
        name: Some("OrderRepository".to_string()),
        names: None,
        repo: Some("kotlin-service".to_string()),
        kind: Some("class".to_string()),
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert!(
        interface_as_class.matches.is_empty(),
        "Kotlin interface should not also be indexed as a class"
    );

    let function = runtime.get_symbol(GetSymbolArgs {
        name: Some("submitOrder".to_string()),
        names: None,
        repo: Some("kotlin-service".to_string()),
        kind: Some("function".to_string()),
        file_glob: None,
        include_body: true,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert!(
        function.matches.iter().any(|item| item
            .body
            .as_deref()
            .unwrap_or_default()
            .contains("fun submitOrder")),
        "Kotlin function body should be available"
    );

    let property = runtime.get_symbol(GetSymbolArgs {
        name: Some("cacheName".to_string()),
        names: None,
        repo: Some("kotlin-service".to_string()),
        kind: Some("variable".to_string()),
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert!(
        !property.matches.is_empty(),
        "Kotlin property should be indexed as a variable"
    );

    let alias = runtime.get_symbol(GetSymbolArgs {
        name: Some("OrderId".to_string()),
        names: None,
        repo: Some("kotlin-service".to_string()),
        kind: Some("type".to_string()),
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert!(
        !alias.matches.is_empty(),
        "Kotlin typealias should be indexed"
    );

    let alias_target = runtime.get_symbol(GetSymbolArgs {
        name: Some("String".to_string()),
        names: None,
        repo: Some("kotlin-service".to_string()),
        kind: Some("type".to_string()),
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert!(
        alias_target.matches.is_empty(),
        "Kotlin typealias should index the alias name, not the target type"
    );

    let refs = runtime.find_references(FindReferencesArgs {
        name: Some("OrderId".to_string()),
        names: None,
        repo: Some("kotlin-service".to_string()),
        scope: None,
        exclude_tests: false,
        include_declarations: false,
        group_by_file: true,
        snippet_lines: 1,
        limit: None,
        offset: None,
        counts_only: false,
    })?;
    assert!(refs.total >= 2, "Kotlin references should be indexed");

    Ok(())
}

#[test]
fn indexes_csharp_symbols_and_references() -> Result<()> {
    let catalog = tempdir()?;
    let repo = tempdir()?;

    fs::create_dir_all(repo.path().join("src"))?;
    fs::write(
        repo.path().join("src").join("PaymentService.cs"),
        r#"using System;

namespace LZ.Payments;

public record PaymentRequest(decimal Amount, string Currency);

public enum PaymentStatus
{
    Pending,
    Complete
}

public delegate void PaymentCallback(PaymentRequest request);

public interface IPaymentProcessor
{
    bool ProcessPayment(PaymentRequest request);
}

public class PaymentProcessor : IPaymentProcessor
{
    private readonly string _name;
    public event EventHandler<PaymentRequest> OnPaymentReceived;
    public string Name { get; set; }

    public PaymentProcessor(string name)
    {
        _name = name;
    }

    ~PaymentProcessor()
    {
    }

    public bool ProcessPayment(PaymentRequest request)
    {
        OnPaymentReceived?.Invoke(this, request);
        return request.Amount > 0;
    }
}
"#,
    )?;

    let config = TsIndexConfig {
        repos: vec![RepoConfig {
            name: "payment-service".to_string(),
            path: repo.path().to_string_lossy().to_string(),
            languages: Vec::new(),
            ignore: Vec::new(),
        }],
        ..TsIndexConfig::default()
    };
    config.write(catalog.path())?;

    let runtime = Runtime::new(
        catalog.path().to_path_buf(),
        db_path(catalog.path()),
        config,
        Vec::new(),
    );
    runtime.build(false, None)?;

    let class = runtime.get_symbol(GetSymbolArgs {
        name: Some("PaymentProcessor".to_string()),
        names: None,
        repo: Some("payment-service".to_string()),
        kind: Some("class".to_string()),
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert!(!class.matches.is_empty(), "C# class should be indexed");

    let record = runtime.get_symbol(GetSymbolArgs {
        name: Some("PaymentRequest".to_string()),
        names: None,
        repo: Some("payment-service".to_string()),
        kind: Some("class".to_string()),
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert!(!record.matches.is_empty(), "C# record should be indexed");

    let interface = runtime.get_symbol(GetSymbolArgs {
        name: Some("IPaymentProcessor".to_string()),
        names: None,
        repo: Some("payment-service".to_string()),
        kind: Some("interface".to_string()),
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert!(
        !interface.matches.is_empty(),
        "C# interface should be indexed"
    );

    let method = runtime.get_symbol(GetSymbolArgs {
        name: Some("ProcessPayment".to_string()),
        names: None,
        repo: Some("payment-service".to_string()),
        kind: Some("method".to_string()),
        file_glob: None,
        include_body: true,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert!(
        method.matches.iter().any(|item| item
            .body
            .as_deref()
            .unwrap_or_default()
            .contains("ProcessPayment")),
        "C# method body should be available"
    );

    let property = runtime.get_symbol(GetSymbolArgs {
        name: Some("Name".to_string()),
        names: None,
        repo: Some("payment-service".to_string()),
        kind: Some("property".to_string()),
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert!(
        !property.matches.is_empty(),
        "C# property should be indexed"
    );

    let field = runtime.get_symbol(GetSymbolArgs {
        name: Some("_name".to_string()),
        names: None,
        repo: Some("payment-service".to_string()),
        kind: Some("variable".to_string()),
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert!(!field.matches.is_empty(), "C# field should be indexed");

    let event = runtime.get_symbol(GetSymbolArgs {
        name: Some("OnPaymentReceived".to_string()),
        names: None,
        repo: Some("payment-service".to_string()),
        kind: Some("event".to_string()),
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert!(!event.matches.is_empty(), "C# event should be indexed");

    let delegate = runtime.get_symbol(GetSymbolArgs {
        name: Some("PaymentCallback".to_string()),
        names: None,
        repo: Some("payment-service".to_string()),
        kind: Some("delegate".to_string()),
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert!(
        !delegate.matches.is_empty(),
        "C# delegate should be indexed"
    );

    let enum_member = runtime.get_symbol(GetSymbolArgs {
        name: Some("Pending".to_string()),
        names: None,
        repo: Some("payment-service".to_string()),
        kind: Some("variable".to_string()),
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert!(
        !enum_member.matches.is_empty(),
        "C# enum member should be indexed"
    );

    let outline = runtime.list_file_outline(OutlineArgs {
        file: "src/PaymentService.cs".to_string(),
        repo: Some("payment-service".to_string()),
        depth: 2,
        include_signatures: true,
        include_docstrings: false,
        include_imports: false,
        include_bodies_for: None,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert!(
        outline
            .symbols
            .iter()
            .any(|symbol| symbol.kind == "namespace" && symbol.name.contains("Payments")),
        "C# file-scoped namespace should be indexed"
    );

    let refs = runtime.find_references(FindReferencesArgs {
        name: Some("PaymentRequest".to_string()),
        names: None,
        repo: Some("payment-service".to_string()),
        scope: None,
        exclude_tests: false,
        include_declarations: false,
        group_by_file: true,
        snippet_lines: 1,
        limit: None,
        offset: None,
        counts_only: false,
    })?;
    assert!(refs.total >= 2, "C# references should be indexed");

    Ok(())
}

#[test]
fn indexes_php_symbols_and_references() -> Result<()> {
    let dir = tempdir()?;
    fs::create_dir_all(dir.path().join("src"))?;
    fs::write(
        dir.path().join("composer.json"),
        r#"{"autoload":{"psr-4":{"App\\":"src/"}}}"#,
    )?;
    fs::write(
        dir.path().join("src").join("PaymentService.php"),
        r#"<?php

namespace App\Payments;

use DateTimeImmutable;

interface PaymentGateway
{
    public function charge(PaymentRequest $request): PaymentStatus;
}

trait LogsPayments
{
    public function logPayment(PaymentRequest $request): void {}
}

enum PaymentStatus: string
{
    case Pending = 'pending';
    case Complete = 'complete';
}

class PaymentRequest
{
    public const DEFAULT_CURRENCY = 'USD';

    public function __construct(
        public int $amount,
        public string $currency = self::DEFAULT_CURRENCY,
    ) {}
}

class PaymentService implements PaymentGateway
{
    use LogsPayments;

    private string $currency = PaymentRequest::DEFAULT_CURRENCY;

    public function charge(PaymentRequest $request): PaymentStatus
    {
        $submittedAt = new DateTimeImmutable();
        $this->logPayment($request);
        return PaymentStatus::Complete;
    }
}

function create_payment(PaymentRequest $request): PaymentStatus
{
    return (new PaymentService())->charge($request);
}
"#,
    )?;

    let detected = detect_languages(dir.path())?;
    assert!(detected.iter().any(|item| item.language == "php"));

    let config = TsIndexConfig::default();
    config.write(dir.path())?;
    let runtime = Runtime::new(
        dir.path().to_path_buf(),
        db_path(dir.path()),
        config,
        vec!["php".to_string()],
    );
    runtime.build(false, None)?;

    let class = runtime.get_symbol(GetSymbolArgs {
        name: Some("PaymentService".to_string()),
        names: None,
        repo: None,
        kind: Some("class".to_string()),
        file_glob: None,
        include_body: true,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(class.matches.len(), 1);
    assert_eq!(class.matches[0].language, "php");
    assert!(
        class.matches[0]
            .body
            .as_deref()
            .unwrap_or_default()
            .contains("class PaymentService")
    );

    let interface = runtime.get_symbol(GetSymbolArgs {
        name: Some("PaymentGateway".to_string()),
        names: None,
        repo: None,
        kind: Some("interface".to_string()),
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(interface.matches.len(), 1);

    let trait_symbol = runtime.get_symbol(GetSymbolArgs {
        name: Some("LogsPayments".to_string()),
        names: None,
        repo: None,
        kind: Some("trait".to_string()),
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(trait_symbol.matches.len(), 1);

    let enum_symbol = runtime.get_symbol(GetSymbolArgs {
        name: Some("PaymentStatus".to_string()),
        names: None,
        repo: None,
        kind: Some("enum".to_string()),
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(enum_symbol.matches.len(), 1);

    let method = runtime.get_symbol(GetSymbolArgs {
        name: Some("charge".to_string()),
        names: None,
        repo: None,
        kind: Some("method".to_string()),
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert!(
        method.matches.len() >= 2,
        "interface and class methods are indexed"
    );

    let function = runtime.get_symbol(GetSymbolArgs {
        name: Some("create_payment".to_string()),
        names: None,
        repo: None,
        kind: Some("function".to_string()),
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(function.matches.len(), 1);

    let property = runtime.get_symbol(GetSymbolArgs {
        name: Some("currency".to_string()),
        names: None,
        repo: None,
        kind: Some("property".to_string()),
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(property.matches.len(), 2);

    let promoted_property = runtime.get_symbol(GetSymbolArgs {
        name: Some("amount".to_string()),
        names: None,
        repo: None,
        kind: Some("property".to_string()),
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(promoted_property.matches.len(), 1);

    let constant = runtime.get_symbol(GetSymbolArgs {
        name: Some("DEFAULT_CURRENCY".to_string()),
        names: None,
        repo: None,
        kind: Some("constant".to_string()),
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(constant.matches.len(), 1);

    let enum_case = runtime.get_symbol(GetSymbolArgs {
        name: Some("Complete".to_string()),
        names: None,
        repo: None,
        kind: Some("variable".to_string()),
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(enum_case.matches.len(), 1);

    let outline = runtime.list_file_outline(OutlineArgs {
        file: "src/PaymentService.php".to_string(),
        repo: None,
        depth: 2,
        include_signatures: true,
        include_docstrings: false,
        include_imports: false,
        include_bodies_for: None,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert!(outline.symbols.iter().any(|s| s.name == "PaymentService"));

    let refs = runtime.find_references(FindReferencesArgs {
        name: Some("PaymentRequest".to_string()),
        names: None,
        repo: None,
        scope: None,
        exclude_tests: false,
        include_declarations: false,
        group_by_file: true,
        snippet_lines: 1,
        limit: None,
        offset: None,
        counts_only: false,
    })?;
    assert!(refs.total >= 4, "PHP references should be indexed");

    Ok(())
}

#[test]
fn indexes_css_scss_and_yaml_files() -> Result<()> {
    let dir = tempdir()?;
    fs::create_dir_all(dir.path().join("styles"))?;
    fs::create_dir_all(dir.path().join("config"))?;
    fs::write(
        dir.path().join("styles").join("app.css"),
        r#"
@import "theme.css";

@keyframes fade-in {
  from { opacity: 0; }
  to { opacity: 1; }
}

.card {
  animation: fade-in 120ms ease-out;
  color: var(--brand-color);
}
"#,
    )?;
    fs::write(
        dir.path().join("styles").join("theme.scss"),
        r#"
$brand-color: #056ef0;

@mixin center($gap: 1rem) {
  display: flex;
  gap: $gap;
}

%control {
  padding: 1rem;
}

.button {
  @include center(2rem);
  color: $brand-color;
}
"#,
    )?;
    fs::write(
        dir.path().join("config").join("service.yaml"),
        r#"
apiVersion: v1
kind: Service
metadata:
  name: checkout
  labels: &labels
    app: checkout
spec:
  selector: *labels
"#,
    )?;

    let detected = detect_languages(dir.path())?;
    assert!(detected.iter().any(|item| item.language == "css"));
    assert!(detected.iter().any(|item| item.language == "scss"));
    assert!(detected.iter().any(|item| item.language == "yaml"));

    let config = TsIndexConfig::default();
    config.write(dir.path())?;
    let runtime = Runtime::new(
        dir.path().to_path_buf(),
        db_path(dir.path()),
        config,
        vec!["css".to_string(), "scss".to_string(), "yaml".to_string()],
    );
    runtime.build(false, None)?;

    let keyframes = runtime.get_symbol(GetSymbolArgs {
        name: Some("fade-in".to_string()),
        names: None,
        repo: None,
        kind: Some("keyframes".to_string()),
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(keyframes.matches.len(), 1);
    assert_eq!(keyframes.matches[0].language, "css");

    let selector = runtime.get_symbol(GetSymbolArgs {
        name: Some(".card".to_string()),
        names: None,
        repo: None,
        kind: Some("selector".to_string()),
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(selector.matches.len(), 1);
    assert_eq!(selector.matches[0].language, "css");

    let mixin = runtime.get_symbol(GetSymbolArgs {
        name: Some("center".to_string()),
        names: None,
        repo: None,
        kind: Some("mixin".to_string()),
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(mixin.matches.len(), 1);
    assert_eq!(mixin.matches[0].language, "scss");

    let variable = runtime.get_symbol(GetSymbolArgs {
        name: Some("$brand-color".to_string()),
        names: None,
        repo: None,
        kind: Some("variable".to_string()),
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(variable.matches.len(), 1);
    assert_eq!(variable.matches[0].language, "scss");

    let yaml_key = runtime.get_symbol(GetSymbolArgs {
        name: Some("metadata".to_string()),
        names: None,
        repo: None,
        kind: Some("key".to_string()),
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(yaml_key.matches.len(), 1);
    assert_eq!(yaml_key.matches[0].language, "yaml");

    let refs = runtime.find_references(FindReferencesArgs {
        name: Some("labels".to_string()),
        names: None,
        repo: None,
        scope: None,
        exclude_tests: false,
        include_declarations: false,
        group_by_file: true,
        snippet_lines: 1,
        limit: None,
        offset: None,
        counts_only: false,
    })?;
    assert!(refs.total >= 2);

    Ok(())
}

#[test]
fn indexes_json_files() -> Result<()> {
    let dir = tempdir()?;
    fs::create_dir_all(dir.path().join("config"))?;
    fs::write(
        dir.path().join("config").join("settings.json"),
        r#"{
  "database": {
    "host": "localhost",
    "port": 5432,
    "credentials": {
      "username": "admin"
    }
  },
  "features": ["search", "billing"]
}"#,
    )?;

    let detected = detect_languages(dir.path())?;
    assert!(detected.iter().any(|item| item.language == "json"));

    let config = TsIndexConfig::default();
    config.write(dir.path())?;
    let runtime = Runtime::new(
        dir.path().to_path_buf(),
        db_path(dir.path()),
        config,
        vec!["json".to_string()],
    );
    runtime.build(false, None)?;

    let key = runtime.get_symbol(GetSymbolArgs {
        name: Some("database".to_string()),
        names: None,
        repo: None,
        kind: Some("key".to_string()),
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(key.matches.len(), 1);
    assert_eq!(key.matches[0].language, "json");

    let nested_key = runtime.get_symbol(GetSymbolArgs {
        name: Some("credentials".to_string()),
        names: None,
        repo: None,
        kind: Some("key".to_string()),
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(nested_key.matches.len(), 1);

    let outline = runtime.list_file_outline(OutlineArgs {
        file: "config/settings.json".to_string(),
        repo: None,
        depth: 2,
        include_signatures: true,
        include_docstrings: false,
        include_imports: false,
        include_bodies_for: None,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert!(!outline.symbols.is_empty());
    assert!(
        outline.symbols.iter().any(|s| s.name == "database"),
        "JSON outline should include top-level keys"
    );

    let key_refs = runtime.find_references(FindReferencesArgs {
        name: Some("host".to_string()),
        names: None,
        repo: None,
        scope: None,
        exclude_tests: false,
        include_declarations: false,
        group_by_file: true,
        snippet_lines: 1,
        limit: None,
        offset: None,
        counts_only: false,
    })?;
    assert!(key_refs.total >= 1, "JSON keys should be indexed as refs");

    let value_refs = runtime.find_references(FindReferencesArgs {
        name: Some("localhost".to_string()),
        names: None,
        repo: None,
        scope: None,
        exclude_tests: false,
        include_declarations: false,
        group_by_file: true,
        snippet_lines: 1,
        limit: None,
        offset: None,
        counts_only: false,
    })?;
    assert_eq!(
        value_refs.total, 0,
        "JSON string values should not be indexed as refs"
    );

    Ok(())
}

#[test]
fn indexes_makefiles() -> Result<()> {
    let dir = tempdir()?;
    fs::write(
        dir.path().join("Makefile"),
        "CC := cc\nCFLAGS += -Wall\n\n.PHONY: all clean\nall: build\n\nbuild: main.o\n\t$(CC) $(CFLAGS) -o app main.o\n\nclean:\n\trm -f app *.o\n",
    )?;
    fs::write(
        dir.path().join("rules.mk"),
        "install: build\n\tcp app /tmp/app\n",
    )?;

    let detected = detect_languages(dir.path())?;
    assert!(detected.iter().any(|item| item.language == "make"));

    let config = TsIndexConfig::default();
    config.write(dir.path())?;
    let runtime = Runtime::new(
        dir.path().to_path_buf(),
        db_path(dir.path()),
        config,
        vec!["make".to_string()],
    );
    runtime.build(false, None)?;

    let target = runtime.get_symbol(GetSymbolArgs {
        name: Some("build".to_string()),
        names: None,
        repo: None,
        kind: Some("target".to_string()),
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(target.matches.len(), 1);
    assert_eq!(target.matches[0].language, "make");

    let variable = runtime.get_symbol(GetSymbolArgs {
        name: Some("CFLAGS".to_string()),
        names: None,
        repo: None,
        kind: Some("variable".to_string()),
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(variable.matches.len(), 1);

    let outline = runtime.list_file_outline(OutlineArgs {
        file: "Makefile".to_string(),
        repo: None,
        depth: 2,
        include_signatures: true,
        include_docstrings: false,
        include_imports: false,
        include_bodies_for: None,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert!(
        outline.symbols.iter().any(|s| s.name == "all"),
        "Makefile outline should include targets"
    );

    let refs = runtime.find_references(FindReferencesArgs {
        name: Some("CC".to_string()),
        names: None,
        repo: None,
        scope: None,
        exclude_tests: false,
        include_declarations: false,
        group_by_file: true,
        snippet_lines: 1,
        limit: None,
        offset: None,
        counts_only: false,
    })?;
    assert!(refs.total >= 1, "Make variables should be indexed as refs");

    Ok(())
}

/// Helper: build a single-repo Python runtime rooted at `dir`.
fn python_runtime_at(dir: &std::path::Path) -> Result<Runtime> {
    let config = TsIndexConfig::default();
    config.write(dir)?;
    Ok(Runtime::new(
        dir.to_path_buf(),
        db_path(dir),
        config,
        vec!["python".to_string()],
    ))
}

#[test]
fn update_paths_reindexes_only_a_modified_file() -> Result<()> {
    let dir = tempdir()?;
    fs::create_dir_all(dir.path().join("src"))?;
    fs::write(dir.path().join("pyproject.toml"), "[project]\nname='f'\n")?;
    fs::write(dir.path().join("src/a.py"), "def alpha():\n    return 1\n")?;
    fs::write(dir.path().join("src/b.py"), "def beta():\n    return 2\n")?;

    let runtime = python_runtime_at(dir.path())?;
    runtime.build(false, None)?;

    // Modify only a.py: add a new symbol.
    fs::write(
        dir.path().join("src/a.py"),
        "def alpha():\n    return 1\n\ndef alpha_two():\n    return 3\n",
    )?;

    let stats = runtime.update_paths(&[dir.path().join("src/a.py")])?;
    assert_eq!(stats.indexed, 1, "exactly one file should be re-indexed");

    // New symbol is now present...
    let found = runtime.get_symbol(GetSymbolArgs {
        name: Some("alpha_two".to_string()),
        names: None,
        repo: None,
        kind: None,
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(found.matches.len(), 1);

    // ...and the untouched file's symbol is still indexed (not purged).
    let beta = runtime.get_symbol(GetSymbolArgs {
        name: Some("beta".to_string()),
        names: None,
        repo: None,
        kind: None,
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(beta.matches.len(), 1, "untouched file must remain indexed");
    Ok(())
}

#[test]
fn update_paths_indexes_a_newly_created_file() -> Result<()> {
    let dir = tempdir()?;
    fs::create_dir_all(dir.path().join("src"))?;
    fs::write(dir.path().join("pyproject.toml"), "[project]\nname='f'\n")?;
    fs::write(dir.path().join("src/a.py"), "def alpha():\n    return 1\n")?;

    let runtime = python_runtime_at(dir.path())?;
    runtime.build(false, None)?;

    // Create a brand-new file and update just it.
    fs::write(dir.path().join("src/c.py"), "def gamma():\n    return 9\n")?;
    let stats = runtime.update_paths(&[dir.path().join("src/c.py")])?;
    assert_eq!(stats.indexed, 1);

    let gamma = runtime.get_symbol(GetSymbolArgs {
        name: Some("gamma".to_string()),
        names: None,
        repo: None,
        kind: None,
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(gamma.matches.len(), 1);
    Ok(())
}

#[test]
fn update_paths_removes_deleted_file_symbols() -> Result<()> {
    let dir = tempdir()?;
    fs::create_dir_all(dir.path().join("src"))?;
    fs::write(dir.path().join("pyproject.toml"), "[project]\nname='f'\n")?;
    fs::write(dir.path().join("src/a.py"), "def alpha():\n    return 1\n")?;
    fs::write(dir.path().join("src/b.py"), "def beta():\n    return 2\n")?;

    let runtime = python_runtime_at(dir.path())?;
    runtime.build(false, None)?;

    // Delete b.py from disk, then report it as changed.
    fs::remove_file(dir.path().join("src/b.py"))?;
    runtime.update_paths(&[dir.path().join("src/b.py")])?;

    let beta = runtime.get_symbol(GetSymbolArgs {
        name: Some("beta".to_string()),
        names: None,
        repo: None,
        kind: None,
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert!(
        beta.matches.is_empty(),
        "deleted file's symbols must be purged"
    );

    // The surviving file is untouched.
    let alpha = runtime.get_symbol(GetSymbolArgs {
        name: Some("alpha".to_string()),
        names: None,
        repo: None,
        kind: None,
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(alpha.matches.len(), 1);
    Ok(())
}

#[test]
fn update_paths_respects_tsindexignore_for_new_files() -> Result<()> {
    let dir = tempdir()?;
    fs::create_dir_all(dir.path().join("src"))?;
    fs::create_dir_all(dir.path().join("generated"))?;
    fs::write(dir.path().join("pyproject.toml"), "[project]\nname='f'\n")?;
    fs::write(dir.path().join(".tsindexignore"), "generated/\n")?;
    fs::write(dir.path().join("src/a.py"), "def alpha():\n    return 1\n")?;

    let runtime = python_runtime_at(dir.path())?;
    runtime.build(false, None)?;

    // Create a file inside an ignored directory and report it as changed.
    fs::write(
        dir.path().join("generated/g.py"),
        "def generated_symbol():\n    return 0\n",
    )?;
    let stats = runtime.update_paths(&[dir.path().join("generated/g.py")])?;
    assert_eq!(stats.indexed, 0, "ignored file must not be indexed");

    let g = runtime.get_symbol(GetSymbolArgs {
        name: Some("generated_symbol".to_string()),
        names: None,
        repo: None,
        kind: None,
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert!(
        g.matches.is_empty(),
        "ignored file's symbols must be absent"
    );
    Ok(())
}

#[test]
fn update_paths_expands_created_directories() -> Result<()> {
    let dir = tempdir()?;
    fs::create_dir_all(dir.path().join("src"))?;
    fs::write(dir.path().join("pyproject.toml"), "[project]\nname='f'\n")?;
    fs::write(dir.path().join("src/a.py"), "def alpha():\n    return 1\n")?;

    let runtime = python_runtime_at(dir.path())?;
    runtime.build(false, None)?;

    let created_dir = dir.path().join("src/newpkg");
    fs::create_dir_all(&created_dir)?;
    fs::write(
        created_dir.join("nested.py"),
        "def nested_symbol():\n    return 4\n",
    )?;

    let stats = runtime.update_paths(&[created_dir])?;
    assert_eq!(stats.indexed, 1, "directory event should index children");

    let nested = runtime.get_symbol(GetSymbolArgs {
        name: Some("nested_symbol".to_string()),
        names: None,
        repo: None,
        kind: None,
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(nested.matches.len(), 1);
    Ok(())
}

#[test]
fn update_paths_purges_deleted_directories() -> Result<()> {
    let dir = tempdir()?;
    fs::create_dir_all(dir.path().join("src/oldpkg"))?;
    fs::write(dir.path().join("pyproject.toml"), "[project]\nname='f'\n")?;
    fs::write(dir.path().join("src/a.py"), "def alpha():\n    return 1\n")?;
    fs::write(
        dir.path().join("src/oldpkg/obsolete.py"),
        "def obsolete_symbol():\n    return 4\n",
    )?;

    let runtime = python_runtime_at(dir.path())?;
    runtime.build(false, None)?;

    fs::remove_dir_all(dir.path().join("src/oldpkg"))?;
    runtime.update_paths(&[dir.path().join("src/oldpkg")])?;

    let obsolete = runtime.get_symbol(GetSymbolArgs {
        name: Some("obsolete_symbol".to_string()),
        names: None,
        repo: None,
        kind: None,
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert!(
        obsolete.matches.is_empty(),
        "deleted directory children should be purged"
    );

    let alpha = runtime.get_symbol(GetSymbolArgs {
        name: Some("alpha".to_string()),
        names: None,
        repo: None,
        kind: None,
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(alpha.matches.len(), 1, "unrelated files remain indexed");
    Ok(())
}

/// Regression: a directory event for a directory that STILL EXISTS (e.g. an
/// editor touching a folder's mtime, or a child write reported as a dir event)
/// must not purge the indexed rows under it. `resolved` only holds file paths,
/// so without the `is_dir()` guard the directory lands in `to_delete` and wipes
/// every unchanged child from the index.
#[test]
fn update_paths_keeps_existing_directory_with_unchanged_children() -> Result<()> {
    let dir = tempdir()?;
    fs::create_dir_all(dir.path().join("src/pkg"))?;
    fs::write(dir.path().join("pyproject.toml"), "[project]\nname='f'\n")?;
    fs::write(
        dir.path().join("src/pkg/one.py"),
        "def child_one():\n    return 1\n",
    )?;
    fs::write(
        dir.path().join("src/pkg/two.py"),
        "def child_two():\n    return 2\n",
    )?;

    let runtime = python_runtime_at(dir.path())?;
    runtime.build(false, None)?;

    // Report the still-present directory as changed without deleting it. The
    // children are untouched on disk, so the batch must purge nothing.
    let stats = runtime.update_paths(&[dir.path().join("src/pkg")])?;
    assert_eq!(stats.indexed, 0, "no file was modified");

    for sym in ["child_one", "child_two"] {
        let found = runtime.get_symbol(GetSymbolArgs {
            name: Some(sym.to_string()),
            names: None,
            repo: None,
            kind: None,
            file_glob: None,
            include_body: false,
            context_lines: 0,
            max_body_lines: None,
            limit: None,
            offset: None,
        })?;
        assert_eq!(
            found.matches.len(),
            1,
            "{sym} must survive a directory event when the directory still exists"
        );
    }
    Ok(())
}

/// Re-reporting an unmodified file as changed must hit the incremental-skip
/// path: the file's sha+mtime are unchanged, so it is counted as `skipped`
/// (not re-indexed) and the write transaction is never opened.
#[test]
fn update_paths_skips_unchanged_files() -> Result<()> {
    let dir = tempdir()?;
    fs::create_dir_all(dir.path().join("src"))?;
    fs::write(dir.path().join("pyproject.toml"), "[project]\nname='f'\n")?;
    fs::write(dir.path().join("src/a.py"), "def alpha():\n    return 1\n")?;

    let runtime = python_runtime_at(dir.path())?;
    runtime.build(false, None)?;

    // Report the file as changed without modifying it on disk.
    let stats = runtime.update_paths(&[dir.path().join("src/a.py")])?;
    assert_eq!(stats.indexed, 0, "unchanged file must not be re-indexed");
    assert_eq!(
        stats.skipped, 1,
        "unchanged file must be counted as skipped"
    );
    assert_eq!(stats.failed, 0);

    // The symbol is still present afterward (the no-op left the index intact).
    let alpha = runtime.get_symbol(GetSymbolArgs {
        name: Some("alpha".to_string()),
        names: None,
        repo: None,
        kind: None,
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(alpha.matches.len(), 1);
    Ok(())
}

/// An event batch that resolves to no upserts and no deletes (here: a path
/// that is ignored and was never indexed) is a true no-op — it must not error,
/// not re-index anything, and leave every indexed row untouched. The row
/// counts across files/symbols/refs must be byte-identical before and after:
/// a regression that lets the ignored path into `to_delete` would purge rows
/// under the (non-existent) prefix and drop these counts.
#[test]
fn update_paths_noop_for_ignored_untracked_path() -> Result<()> {
    let dir = tempdir()?;
    fs::create_dir_all(dir.path().join("src"))?;
    fs::write(dir.path().join("pyproject.toml"), "[project]\nname='f'\n")?;
    fs::write(dir.path().join(".tsindexignore"), "build/\n")?;
    fs::write(dir.path().join("src/a.py"), "def alpha():\n    return 1\n")?;

    let runtime = python_runtime_at(dir.path())?;
    runtime.build(false, None)?;

    let row_counts = |conn: &Connection| -> Result<(i64, i64, i64)> {
        Ok((
            conn.query_row("SELECT COUNT(*) FROM files", [], |r| r.get(0))?,
            conn.query_row("SELECT COUNT(*) FROM symbols", [], |r| r.get(0))?,
            conn.query_row("SELECT COUNT(*) FROM refs", [], |r| r.get(0))?,
        ))
    };
    let conn = Connection::open(db_path(dir.path()))?;
    let before = row_counts(&conn)?;

    // An ignored path that was never indexed: not an upsert (ignored) and not a
    // delete (no rows exist for it), so the batch must touch nothing.
    fs::create_dir_all(dir.path().join("build"))?;
    fs::write(
        dir.path().join("build/out.py"),
        "def built_symbol():\n    return 0\n",
    )?;
    let stats = runtime.update_paths(&[dir.path().join("build/out.py")])?;
    assert_eq!(stats.indexed, 0);
    assert_eq!(stats.skipped, 0);
    assert_eq!(stats.failed, 0);

    let after = row_counts(&conn)?;
    assert_eq!(
        before, after,
        "an ignored-but-never-indexed path must not delete any indexed rows"
    );

    // Pre-existing index is untouched.
    let alpha = runtime.get_symbol(GetSymbolArgs {
        name: Some("alpha".to_string()),
        names: None,
        repo: None,
        kind: None,
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(alpha.matches.len(), 1);
    Ok(())
}

/// Deleting a directory with several indexed children and reporting the
/// directory once must purge every child below the prefix in a single update,
/// exercising the prefix `DELETE ... WHERE path = ? OR path LIKE ?` path.
#[test]
fn update_paths_purges_all_children_under_deleted_prefix() -> Result<()> {
    let dir = tempdir()?;
    fs::create_dir_all(dir.path().join("src/pkg/sub"))?;
    fs::write(dir.path().join("pyproject.toml"), "[project]\nname='f'\n")?;
    fs::write(dir.path().join("src/a.py"), "def alpha():\n    return 1\n")?;
    fs::write(
        dir.path().join("src/pkg/one.py"),
        "def child_one():\n    return 1\n",
    )?;
    fs::write(
        dir.path().join("src/pkg/two.py"),
        "def child_two():\n    return 2\n",
    )?;
    fs::write(
        dir.path().join("src/pkg/sub/three.py"),
        "def child_three():\n    return 3\n",
    )?;

    let runtime = python_runtime_at(dir.path())?;
    runtime.build(false, None)?;

    // Sanity: all three nested files are indexed before the delete.
    let conn = Connection::open(db_path(dir.path()))?;
    let before: i64 = conn.query_row(
        "SELECT COUNT(*) FROM files WHERE path LIKE 'src/pkg/%'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(before, 3, "all three children indexed before delete");

    fs::remove_dir_all(dir.path().join("src/pkg"))?;
    runtime.update_paths(&[dir.path().join("src/pkg")])?;

    // Every child under the prefix is gone in one update.
    let after: i64 = conn.query_row(
        "SELECT COUNT(*) FROM files WHERE path LIKE 'src/pkg/%'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(after, 0, "all children under the prefix must be purged");

    for sym in ["child_one", "child_two", "child_three"] {
        let found = runtime.get_symbol(GetSymbolArgs {
            name: Some(sym.to_string()),
            names: None,
            repo: None,
            kind: None,
            file_glob: None,
            include_body: false,
            context_lines: 0,
            max_body_lines: None,
            limit: None,
            offset: None,
        })?;
        assert!(found.matches.is_empty(), "{sym} should be purged");
    }

    // The unrelated top-level file survives.
    let alpha = runtime.get_symbol(GetSymbolArgs {
        name: Some("alpha".to_string()),
        names: None,
        repo: None,
        kind: None,
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(alpha.matches.len(), 1, "unrelated files remain indexed");
    Ok(())
}

#[test]
fn indexes_markdown_headings() -> Result<()> {
    let dir = tempdir()?;
    fs::create_dir_all(dir.path().join("docs"))?;
    fs::write(
        dir.path().join("docs").join("guide.md"),
        "# Getting Started\n\nIntro text.\n\n## Installation\n\nSteps here.\n\n### Prerequisites\n\nDetails.\n\nLegacy Title\n============\n",
    )?;

    let detected = detect_languages(dir.path())?;
    assert!(detected.iter().any(|item| item.language == "markdown"));

    let config = TsIndexConfig::default();
    config.write(dir.path())?;
    let runtime = Runtime::new(
        dir.path().to_path_buf(),
        db_path(dir.path()),
        config,
        vec!["markdown".to_string()],
    );
    runtime.build(false, None)?;

    let atx = runtime.get_symbol(GetSymbolArgs {
        name: Some("Installation".to_string()),
        names: None,
        repo: None,
        kind: Some("heading".to_string()),
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(atx.matches.len(), 1);
    assert_eq!(atx.matches[0].language, "markdown");

    // Setext headings (underlined with `===`) are indexed too.
    let setext = runtime.get_symbol(GetSymbolArgs {
        name: Some("Legacy Title".to_string()),
        names: None,
        repo: None,
        kind: Some("heading".to_string()),
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(setext.matches.len(), 1);

    let outline = runtime.list_file_outline(OutlineArgs {
        file: "docs/guide.md".to_string(),
        repo: None,
        depth: 2,
        include_signatures: true,
        include_docstrings: false,
        include_imports: false,
        include_bodies_for: None,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert!(
        outline.symbols.iter().any(|s| s.name == "Getting Started"),
        "markdown outline should include the top-level heading"
    );
    let getting_started = outline
        .symbols
        .iter()
        .find(|s| s.name == "Getting Started")
        .expect("top-level heading");
    assert!(
        getting_started
            .children
            .iter()
            .any(|s| s.name == "Installation"),
        "markdown outline should nest the level-two heading"
    );

    Ok(())
}

/// Recursively assert no JSON `null` survives serialization. The payload
/// reduction relies on `skip_serializing_if` to omit empty/None fields
/// rather than emit `"field":null`; a regression that drops those
/// attributes would reintroduce nulls and re-bloat every response.
fn assert_no_nulls(value: &serde_json::Value, path: &str) {
    match value {
        serde_json::Value::Null => panic!("unexpected null at {path}"),
        serde_json::Value::Object(map) => {
            for (k, v) in map {
                assert_no_nulls(v, &format!("{path}.{k}"));
            }
        }
        serde_json::Value::Array(items) => {
            for (i, v) in items.iter().enumerate() {
                assert_no_nulls(v, &format!("{path}[{i}]"));
            }
        }
        _ => {}
    }
}

#[test]
fn mcp_tool_response_payload_stays_lean() -> Result<()> {
    // Regression guard for the response-payload reduction. Pins the three
    // structural properties that make tsindex tool results ~half their
    // former size — if any regresses, the payload silently re-bloats and
    // every cached re-read of the result costs more:
    //   1. the MCP envelope carries no `structuredContent` duplicate of `text`;
    //   2. uniform `repo`/`language` are hoisted once to the top level, not
    //      repeated on every match;
    //   3. empty/None fields are omitted rather than serialized as null.
    let dir = tempdir()?;
    fs::create_dir_all(dir.path().join("src"))?;
    fs::write(
        dir.path().join("src").join("handlers.rs"),
        "pub fn alpha_handler(x: i32) -> i32 {\n    x + 1\n}\n\n\
         pub fn beta_handler(y: i32) -> i32 {\n    y + 2\n}\n",
    )?;

    let config = TsIndexConfig::default();
    config.write(dir.path())?;
    let runtime = Runtime::new(
        dir.path().to_path_buf(),
        db_path(dir.path()),
        config,
        vec!["rust".to_string()],
    );
    runtime.build(false, None)?;

    let request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": "get_symbol",
            "arguments": {
                "names": ["alpha_handler", "beta_handler"],
                "include_body": true
            }
        }
    });
    let response = tsindex::mcp::handle_request(&runtime, request)?
        .expect("tools/call must produce a response");
    let result = &response["result"];

    // (1) No structuredContent duplicate. This single key, if reintroduced,
    // roughly doubles the wire/context cost of every tool response.
    assert!(
        result.get("structuredContent").is_none(),
        "MCP result must not carry a structuredContent duplicate of the text payload"
    );

    // The payload lives in content[0].text as compact JSON.
    let text = result["content"][0]["text"]
        .as_str()
        .expect("tool result must expose its payload as content[0].text");
    let payload: serde_json::Value = serde_json::from_str(text)?;

    // (2) repo/language hoisted to the top level, dropped from each match.
    assert!(
        payload.get("repo").and_then(|v| v.as_str()).is_some(),
        "uniform repo should be hoisted to the top level"
    );
    assert!(
        payload.get("language").and_then(|v| v.as_str()).is_some(),
        "uniform language should be hoisted to the top level"
    );
    let matches = payload["matches"].as_array().expect("matches array");
    assert_eq!(matches.len(), 2, "fixture defines two handlers");
    for m in matches {
        assert!(
            m.get("repo").is_none(),
            "per-match repo is pure duplication once hoisted; got {m}"
        );
        assert!(
            m.get("language").is_none(),
            "per-match language is pure duplication once hoisted; got {m}"
        );
    }

    // (3) No null fields anywhere in the payload.
    assert_no_nulls(&payload, "payload");

    // The opt-in max_body_lines skim must shrink the body it caps.
    let full_len = text.len();
    let capped_request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": {
            "name": "get_symbol",
            "arguments": {
                "names": ["alpha_handler", "beta_handler"],
                "include_body": true,
                "max_body_lines": 1
            }
        }
    });
    let capped = tsindex::mcp::handle_request(&runtime, capped_request)?
        .expect("capped tools/call must produce a response");
    let capped_len = capped["result"]["content"][0]["text"]
        .as_str()
        .expect("capped payload text")
        .len();
    assert!(
        capped_len < full_len,
        "max_body_lines=1 must yield a smaller payload than the full body \
         (capped {capped_len} vs full {full_len})"
    );

    Ok(())
}

/// Fixture exercising `find_references`' `total_files` field. `shared_symbol`
/// is declared in `src/decl.py` (a declaration-only file) and called from
/// `src/use_a.py`, `src/use_b.py`, and `tests/test_use.py`, so the distinct
/// file count shifts as filters drop each file.
fn build_shared_symbol_fixture() -> Result<(tempfile::TempDir, Runtime)> {
    let dir = tempdir()?;
    fs::create_dir_all(dir.path().join("src"))?;
    fs::create_dir_all(dir.path().join("tests"))?;
    fs::write(
        dir.path().join("pyproject.toml"),
        "[project]\nname = 'fixture'\n",
    )?;
    // Declaration-only: with the default include_declarations=false this file
    // contributes zero refs, so it only counts once declarations are included.
    fs::write(
        dir.path().join("src").join("decl.py"),
        "def shared_symbol():\n    pass\n",
    )?;
    fs::write(
        dir.path().join("src").join("use_a.py"),
        "def caller_a():\n    shared_symbol()\n",
    )?;
    fs::write(
        dir.path().join("src").join("use_b.py"),
        "def caller_b():\n    shared_symbol()\n",
    )?;
    fs::write(
        dir.path().join("tests").join("test_use.py"),
        "def test_thing():\n    shared_symbol()\n",
    )?;

    let config = TsIndexConfig::default();
    config.write(dir.path())?;
    let runtime = Runtime::new(
        dir.path().to_path_buf(),
        db_path(dir.path()),
        config,
        vec!["python".to_string()],
    );
    runtime.build(false, None)?;
    Ok((dir, runtime))
}

#[test]
fn find_references_total_files_is_true_distinct_file_count() -> Result<()> {
    let (_dir, runtime) = build_shared_symbol_fixture()?;

    // Default filters (exclude_tests=false, include_declarations=false): the
    // three call sites in use_a.py, use_b.py, and tests/test_use.py. decl.py
    // is declaration-only and excluded, so it does not count.
    let refs = runtime.find_references(FindReferencesArgs {
        name: Some("shared_symbol".to_string()),
        names: None,
        repo: None,
        scope: None,
        exclude_tests: false,
        include_declarations: false,
        group_by_file: true,
        snippet_lines: 1,
        limit: None,
        offset: None,
        counts_only: false,
    })?;
    // The unpaginated grouped view is the ground truth for distinct files here.
    let distinct_files: std::collections::HashSet<(String, String)> = refs
        .groups
        .iter()
        .map(|g| (g.repo.clone(), g.file.clone()))
        .collect();
    assert_eq!(
        refs.total_files,
        distinct_files.len(),
        "total_files must equal the distinct (repo, file) count of the full result"
    );
    assert_eq!(refs.total_files, 3);
    Ok(())
}

#[test]
fn find_references_bash_function_declaration_is_not_a_call() -> Result<()> {
    // The declaration heuristic keys on tree-sitter node kind names containing
    // "declaration"/"definition" (or the language-specific `_item`/`_specifier`
    // suffixes). Bash is the case where that could misfire — its function name
    // node is a `word` inside `function_definition`. Confirm the definition is
    // still classified "declaration" (excluded by default) while the call site
    // is a real reference.
    let dir = tempdir()?;
    fs::write(
        dir.path().join("tool.sh"),
        "greet() {\n    echo hi\n}\n\ngreet\n",
    )?;
    let config = TsIndexConfig::default();
    config.write(dir.path())?;
    let runtime = Runtime::new(
        dir.path().to_path_buf(),
        db_path(dir.path()),
        config,
        vec!["bash".to_string()],
    );
    runtime.build(false, None)?;

    let args = |include_declarations: bool| FindReferencesArgs {
        name: Some("greet".to_string()),
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
    };

    // Default: the `greet() {` definition is classified "declaration" and
    // excluded, leaving only the `greet` call.
    let calls_only = runtime.find_references(args(false))?;
    assert!(
        calls_only.refs.iter().all(|r| r.context != "declaration"),
        "no declaration should appear by default: {:?}",
        calls_only.refs
    );
    assert_eq!(calls_only.refs.len(), 1, "only the call site remains");

    // With declarations included, the definition is reported and explicitly
    // tagged "declaration".
    let with_decls = runtime.find_references(args(true))?;
    assert!(
        with_decls.refs.iter().any(|r| r.context == "declaration"),
        "the function definition must classify as declaration: {:?}",
        with_decls.refs
    );
    assert_eq!(with_decls.refs.len(), 2);
    Ok(())
}

#[test]
fn find_references_total_files_is_invariant_under_limit_and_offset() -> Result<()> {
    let (_dir, runtime) = build_shared_symbol_fixture()?;

    let limited = runtime.find_references(FindReferencesArgs {
        name: Some("shared_symbol".to_string()),
        names: None,
        repo: None,
        scope: None,
        exclude_tests: false,
        include_declarations: false,
        group_by_file: true,
        snippet_lines: 1,
        limit: Some(1),
        offset: None,
        counts_only: false,
    })?;
    let full = runtime.find_references(FindReferencesArgs {
        name: Some("shared_symbol".to_string()),
        names: None,
        repo: None,
        scope: None,
        exclude_tests: false,
        include_declarations: false,
        group_by_file: true,
        snippet_lines: 1,
        limit: Some(1000),
        offset: None,
        counts_only: false,
    })?;

    assert_eq!(
        limited.total_files, full.total_files,
        "total_files must be identical regardless of limit"
    );
    assert_eq!(limited.total_files, 3);
    // groups covers only the returned slice, so its length is NOT invariant —
    // that is precisely the gap total_files exists to close.
    assert_ne!(limited.groups.len(), full.groups.len());
    assert_eq!(limited.groups.len(), 1);
    assert_eq!(full.groups.len(), 3);

    // Offset must not perturb total_files either.
    let offset = runtime.find_references(FindReferencesArgs {
        name: Some("shared_symbol".to_string()),
        names: None,
        repo: None,
        scope: None,
        exclude_tests: false,
        include_declarations: false,
        group_by_file: true,
        snippet_lines: 1,
        limit: Some(1),
        offset: Some(1),
        counts_only: false,
    })?;
    assert_eq!(offset.total_files, full.total_files);
    Ok(())
}

#[test]
fn find_references_warns_loudly_when_truncated() -> Result<()> {
    let (_dir, runtime) = build_shared_symbol_fixture()?;

    let args = |limit: Option<usize>| FindReferencesArgs {
        name: Some("shared_symbol".to_string()),
        names: None,
        repo: None,
        scope: None,
        exclude_tests: false,
        include_declarations: false,
        group_by_file: false,
        snippet_lines: 1,
        limit,
        offset: None,
        counts_only: false,
    };

    // Truncated: limit below the 3-ref total must surface a loud, actionable
    // warning naming the partial extent, the resume offset, and the counts_only
    // escape hatch. (warning is computed at the serialization boundary, so
    // assert on the JSON the agent actually sees.)
    let page = runtime.find_references(args(Some(1)))?;
    assert!(page.truncated);
    let v = serde_json::to_value(&page)?;
    let warning = v["warning"]
        .as_str()
        .expect("a truncated response must carry a warning");
    assert!(warning.contains("PARTIAL RESULT"), "warning: {warning}");
    assert!(warning.contains("1 of 3"), "warning: {warning}");
    assert!(warning.contains("offset: 1"), "warning: {warning}");
    assert!(warning.contains("counts_only"), "warning: {warning}");

    // Not truncated: no warning key in the JSON.
    let full = runtime.find_references(args(Some(1000)))?;
    assert!(!full.truncated);
    let v = serde_json::to_value(&full)?;
    assert!(v.get("warning").is_none());
    Ok(())
}

#[test]
fn find_reference_counts_matches_full_row_walk() -> Result<()> {
    let (_dir, runtime) = build_shared_symbol_fixture()?;

    // The aggregate must equal the row-level response's totals exactly — same
    // filters, same underlying query — with no pagination needed to get there.
    let counts = runtime.find_reference_counts(FindReferencesArgs {
        name: Some("shared_symbol".to_string()),
        names: None,
        repo: None,
        scope: None,
        exclude_tests: false,
        include_declarations: false,
        group_by_file: false,
        snippet_lines: 1,
        limit: None,
        offset: None,
        counts_only: true,
    })?;

    let rows = runtime.find_references(FindReferencesArgs {
        name: Some("shared_symbol".to_string()),
        names: None,
        repo: None,
        scope: None,
        exclude_tests: false,
        include_declarations: false,
        group_by_file: false,
        snippet_lines: 1,
        limit: None,
        offset: None,
        counts_only: false,
    })?;

    assert_eq!(
        counts.total, rows.total,
        "aggregate total must match row total"
    );
    assert_eq!(
        counts.total_files, rows.total_files,
        "aggregate total_files must match row total_files"
    );
    assert_eq!(counts.total, 3, "three call sites of shared_symbol");
    assert_eq!(counts.total_files, 3);

    // Per-file counts sum to the total, and every file reported carries >= 1.
    let sum: usize = counts.files.iter().map(|f| f.count).sum();
    assert_eq!(sum, counts.total, "per-file counts must sum to total");
    assert!(counts.files.iter().all(|f| f.count >= 1));
    // Sorted by count descending.
    for w in counts.files.windows(2) {
        assert!(w[0].count >= w[1].count, "files must be count-descending");
    }
    // Single-name: no per-name breakdown emitted.
    assert!(counts.names.is_empty());
    Ok(())
}

#[test]
fn find_reference_counts_multi_name_breakdown() -> Result<()> {
    let dir = tempdir()?;
    fs::create_dir_all(dir.path().join("src"))?;
    fs::write(
        dir.path().join("src").join("a.py"),
        "def alpha():\n    return alpha()\n",
    )?;
    fs::write(
        dir.path().join("src").join("b.py"),
        "def beta():\n    return alpha() + beta()\n",
    )?;
    let config = TsIndexConfig::default();
    config.write(dir.path())?;
    let runtime = Runtime::new(
        dir.path().to_path_buf(),
        db_path(dir.path()),
        config,
        vec!["python".to_string()],
    );
    runtime.build(false, None)?;

    let counts = runtime.find_reference_counts(FindReferencesArgs {
        name: None,
        names: Some(vec!["alpha".to_string(), "beta".to_string()]),
        repo: None,
        scope: None,
        exclude_tests: false,
        include_declarations: false,
        group_by_file: false,
        snippet_lines: 1,
        limit: None,
        offset: None,
        counts_only: true,
    })?;

    // alpha: a.py (1) + b.py (1) = 2 ; beta: b.py (1) = 1. Total 3.
    assert_eq!(counts.total, 3);
    let by_name: std::collections::HashMap<_, _> = counts
        .names
        .iter()
        .map(|n| (n.name.as_str(), n.count))
        .collect();
    assert_eq!(by_name.get("alpha"), Some(&2));
    assert_eq!(by_name.get("beta"), Some(&1));
    // name breakdown sums to total.
    let name_sum: usize = counts.names.iter().map(|n| n.count).sum();
    assert_eq!(name_sum, counts.total);
    Ok(())
}

#[test]
fn find_reference_counts_surfaces_parse_recovery_partial() -> Result<()> {
    // counts_only must NOT silently drop the staleness/parse-recovery signal
    // the row path carries. Index a repo where one file has a syntax error but
    // still yields a reference; the indexed tree is parse-recovered, so
    // `partial` must be true on the aggregate just as it is on the row path.
    let dir = tempdir()?;
    // clean file: alpha reference is clean.
    fs::write(
        dir.path().join("good.py"),
        "def use():\n    return alpha()\n",
    )?;
    // parse-recovered file: a syntax error, but an `alpha` reference survives.
    fs::write(dir.path().join("bad.py"), "value = (\nalpha()\n")?;
    let config = TsIndexConfig::default();
    config.write(dir.path())?;
    let runtime = Runtime::new(
        dir.path().to_path_buf(),
        db_path(dir.path()),
        config,
        vec!["python".to_string()],
    );
    runtime.build(false, None)?;

    let counts = runtime.find_reference_counts(FindReferencesArgs {
        name: Some("alpha".to_string()),
        names: None,
        repo: None,
        scope: None,
        exclude_tests: false,
        include_declarations: false,
        group_by_file: false,
        snippet_lines: 1,
        limit: None,
        offset: None,
        counts_only: true,
    })?;
    assert!(
        counts.partial,
        "counts_only must surface parse-recovery staleness via `partial`"
    );

    // And on a clean index, partial must be false.
    let (_dir2, clean) = build_shared_symbol_fixture()?;
    let clean_counts = clean.find_reference_counts(FindReferencesArgs {
        name: Some("shared_symbol".to_string()),
        names: None,
        repo: None,
        scope: None,
        exclude_tests: false,
        include_declarations: false,
        group_by_file: false,
        snippet_lines: 1,
        limit: None,
        offset: None,
        counts_only: true,
    })?;
    assert!(!clean_counts.partial, "clean index must not report partial");
    Ok(())
}

#[test]
fn find_reference_counts_bounds_files_under_byte_budget() -> Result<()> {
    // counts_only must enforce the same serialized-byte budget the row path
    // does — otherwise a many-file repo produces an oversized response (the
    // exact failure this PR exists to fix). Use a tiny budget and many files,
    // each referencing `budgeted`, so the uncapped files array would overflow.
    let dir = tempdir()?;
    for index in 0..40 {
        fs::write(
            dir.path().join(format!("file_{index:02}.py")),
            "def use():\n    return budgeted()\n",
        )?;
    }
    let config = TsIndexConfig {
        server: ServerConfig {
            max_response_chars: 800,
            ..Default::default()
        },
        ..Default::default()
    };
    config.write(dir.path())?;
    let runtime = Runtime::new(
        dir.path().to_path_buf(),
        db_path(dir.path()),
        config,
        vec!["python".to_string()],
    );
    runtime.build(false, None)?;

    let counts = runtime.find_reference_counts(FindReferencesArgs {
        name: Some("budgeted".to_string()),
        names: None,
        repo: None,
        scope: None,
        exclude_tests: false,
        include_declarations: false,
        group_by_file: false,
        snippet_lines: 1,
        limit: None,
        offset: None,
        counts_only: true,
    })?;

    // The serialized response must fit the budget...
    let serialized = serde_json::to_string(&counts)?;
    assert!(
        serialized.len() <= 800,
        "counts_only response {} bytes exceeds budget 800",
        serialized.len()
    );
    // ...the files array is capped and flagged...
    assert!(counts.files_truncated, "files must be marked truncated");
    assert!(counts.files_returned < counts.total_files);
    assert_eq!(counts.files_returned, counts.files.len());
    // ...but total/total_files stay exact regardless of the cap.
    assert_eq!(counts.total, 40);
    assert_eq!(counts.total_files, 40);
    // The kept files are the count-sorted top of the list (all count 1 here,
    // so the cap keeps a deterministic prefix).
    assert!(counts.files.iter().all(|f| f.count >= 1));
    Ok(())
}

#[test]
fn find_references_total_files_respects_filters() -> Result<()> {
    let (_dir, runtime) = build_shared_symbol_fixture()?;

    let baseline = runtime.find_references(FindReferencesArgs {
        name: Some("shared_symbol".to_string()),
        names: None,
        repo: None,
        scope: None,
        exclude_tests: false,
        include_declarations: false,
        group_by_file: true,
        snippet_lines: 1,
        limit: None,
        offset: None,
        counts_only: false,
    })?;
    assert_eq!(baseline.total_files, 3);

    // exclude_tests drops tests/test_use.py → 2 files.
    let no_tests = runtime.find_references(FindReferencesArgs {
        name: Some("shared_symbol".to_string()),
        names: None,
        repo: None,
        scope: None,
        exclude_tests: true,
        include_declarations: false,
        group_by_file: true,
        snippet_lines: 1,
        limit: None,
        offset: None,
        counts_only: false,
    })?;
    assert_eq!(
        no_tests.total_files, 2,
        "exclude_tests must lower total_files"
    );

    // include_declarations adds src/decl.py → 4 files.
    let with_decls = runtime.find_references(FindReferencesArgs {
        name: Some("shared_symbol".to_string()),
        names: None,
        repo: None,
        scope: None,
        exclude_tests: false,
        include_declarations: true,
        group_by_file: true,
        snippet_lines: 1,
        limit: None,
        offset: None,
        counts_only: false,
    })?;
    assert_eq!(
        with_decls.total_files, 4,
        "include_declarations must raise total_files"
    );

    // scope restricts to src/**/*.py: use_a.py + use_b.py (decl.py is filtered
    // out by include_declarations=false) → 2 files.
    let scoped = runtime.find_references(FindReferencesArgs {
        name: Some("shared_symbol".to_string()),
        names: None,
        repo: None,
        scope: Some("src/**/*.py".to_string()),
        exclude_tests: false,
        include_declarations: false,
        group_by_file: true,
        snippet_lines: 1,
        limit: None,
        offset: None,
        counts_only: false,
    })?;
    assert_eq!(scoped.total_files, 2, "scope must lower total_files");
    Ok(())
}

/// Fixture exercising `find_references`' serialized-byte budget. `budgeted`
/// is called from many generated files so a small `max_response_chars` forces
/// the page to shrink even for a modest `limit`. Each caller file holds one
/// call site, so distinct files == reference count.
fn build_budget_fixture(max_response_chars: usize) -> Result<(tempfile::TempDir, Runtime)> {
    let dir = tempdir()?;
    fs::create_dir_all(dir.path().join("src"))?;
    fs::write(
        dir.path().join("pyproject.toml"),
        "[project]\nname = 'fixture'\n",
    )?;
    // One declaration + 120 call sites across as many files. 120 refs at
    // snippet_lines=3 serialize far past a small budget, guaranteeing the
    // page shrinks; the count is large enough to exercise multi-page walks.
    const REF_COUNT: usize = 120;
    fs::write(
        dir.path().join("src").join("decl.py"),
        "def budgeted():\n    pass\n",
    )?;
    for i in 0..REF_COUNT {
        fs::write(
            dir.path().join("src").join(format!("use_{i:03}.py")),
            format!("def caller_{i}():\n    budgeted()\n    return {i}\n"),
        )?;
    }

    let config = TsIndexConfig {
        server: ServerConfig {
            max_response_chars,
            ..Default::default()
        },
        ..Default::default()
    };
    config.write(dir.path())?;
    let runtime = Runtime::new(
        dir.path().to_path_buf(),
        db_path(dir.path()),
        config,
        vec!["python".to_string()],
    );
    runtime.build(false, None)?;
    Ok((dir, runtime))
}

#[test]
fn find_references_budget_shrinks_page_to_fit() -> Result<()> {
    // Tiny budget: even a few refs at snippet_lines=3 must overflow it, so
    // the page shrinks below the requested limit.
    let (_dir, runtime) = build_budget_fixture(1_500)?;

    let refs = runtime.find_references(FindReferencesArgs {
        name: Some("budgeted".to_string()),
        names: None,
        repo: None,
        scope: None,
        exclude_tests: false,
        include_declarations: false,
        group_by_file: false,
        snippet_lines: 3,
        limit: Some(120),
        offset: None,
        counts_only: false,
    })?;

    let serialized = serde_json::to_string(&refs)?;
    assert!(
        serialized.len() <= 1_500,
        "serialized response {} must be within the 1_500 budget, got {} chars",
        refs.returned,
        serialized.len()
    );
    assert!(
        refs.returned < 120,
        "budget must shrink the page below the requested limit, got returned={}",
        refs.returned
    );
    assert!(refs.returned >= 1, "must keep at least one reference");
    assert!(
        refs.truncated,
        "page must be truncated when the budget cut it below the full set"
    );
    assert_eq!(
        refs.next_offset,
        Some(refs.returned),
        "next_offset must equal offset + returned when truncated"
    );
    Ok(())
}

#[test]
fn find_references_budget_walks_full_set_without_gaps_or_dupes() -> Result<()> {
    let (_dir, runtime) = build_budget_fixture(1_500)?;

    // Ground truth: an unbudgeted single-page fetch collects every reference.
    // Use a separate runtime with the budget disabled (0) so the page is
    // governed by limit alone and we fetch the whole set in one call.
    let (_dir2, unbounded) = build_budget_fixture(0)?;
    let full = unbounded.find_references(FindReferencesArgs {
        name: Some("budgeted".to_string()),
        names: None,
        repo: None,
        scope: None,
        exclude_tests: false,
        include_declarations: false,
        group_by_file: false,
        snippet_lines: 3,
        limit: Some(120),
        offset: None,
        counts_only: false,
    })?;
    assert_eq!(full.returned, full.total);

    // Walk the budgeted result set page by page via next_offset.
    let mut seen: Vec<(String, usize, usize)> = Vec::new();
    let mut offset: Option<usize> = None;
    let mut baseline: Option<(usize, usize)> = None;
    loop {
        let page = runtime.find_references(FindReferencesArgs {
            name: Some("budgeted".to_string()),
            names: None,
            repo: None,
            scope: None,
            exclude_tests: false,
            include_declarations: false,
            group_by_file: false,
            snippet_lines: 3,
            limit: Some(120),
            offset,
            counts_only: false,
        })?;
        // total/total_files describe the whole set and must not change.
        match baseline {
            None => baseline = Some((page.total, page.total_files)),
            Some((t, tf)) => {
                assert_eq!(page.total, t, "total must be invariant across pages");
                assert_eq!(
                    page.total_files, tf,
                    "total_files must be invariant across pages"
                );
            }
        }
        for row in &page.refs {
            // file + range start uniquely identifies a reference in this
            // fixture (one call site per file).
            seen.push((row.file.clone(), row.range.start.0, row.range.start.1));
        }
        match page.next_offset {
            Some(next) => offset = Some(next),
            None => break,
        }
    }

    // No duplicates and no gaps: the walk yields exactly `total` references.
    let mut deduped = seen.clone();
    deduped.sort();
    deduped.dedup();
    assert_eq!(
        deduped.len(),
        seen.len(),
        "walk must not visit any reference twice"
    );
    assert_eq!(
        seen.len(),
        full.total,
        "walk must visit every reference exactly once: got {}, expected {}",
        seen.len(),
        full.total
    );
    Ok(())
}

#[test]
fn find_references_budget_zero_disables_it() -> Result<()> {
    // With the budget disabled, the page size is governed by limit alone.
    let (_dir, runtime) = build_budget_fixture(0)?;
    let refs = runtime.find_references(FindReferencesArgs {
        name: Some("budgeted".to_string()),
        names: None,
        repo: None,
        scope: None,
        exclude_tests: false,
        include_declarations: false,
        group_by_file: false,
        snippet_lines: 3,
        limit: Some(120),
        offset: None,
        counts_only: false,
    })?;
    assert_eq!(
        refs.returned, 120,
        "with budget disabled, returned must equal the requested limit"
    );
    assert_eq!(refs.returned, refs.total);
    assert!(!refs.truncated);
    assert!(refs.next_offset.is_none());
    Ok(())
}

#[test]
fn read_tools_add_missing_partial_column_on_upgrade() -> Result<()> {
    let dir = tempdir()?;
    fs::write(
        dir.path().join("sample.py"),
        "def alpha():\n    return alpha()\n",
    )?;
    let config = TsIndexConfig::default();
    config.write(dir.path())?;
    let runtime = Runtime::new(
        dir.path().to_path_buf(),
        db_path(dir.path()),
        config,
        vec!["python".to_string()],
    );
    runtime.build(false, None)?;

    let db = db_path(dir.path());
    let conn = Connection::open(&db)?;
    conn.execute("ALTER TABLE files DROP COLUMN partial", [])?;
    conn.pragma_update(None, "user_version", 3_i64)?;
    drop(conn);

    let symbol = runtime.get_symbol(GetSymbolArgs {
        name: Some("alpha".into()),
        names: None,
        repo: None,
        kind: None,
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(symbol.matches.len(), 1);

    let outline = runtime.list_file_outline(OutlineArgs {
        file: "sample.py".into(),
        repo: None,
        depth: 2,
        include_signatures: true,
        include_docstrings: true,
        include_imports: false,
        include_bodies_for: None,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(outline.symbols.len(), 1);

    let refs = runtime.find_references(FindReferencesArgs {
        name: Some("alpha".into()),
        names: None,
        repo: None,
        scope: None,
        exclude_tests: false,
        include_declarations: false,
        group_by_file: false,
        snippet_lines: 1,
        limit: None,
        offset: None,
        counts_only: false,
    })?;
    assert_eq!(refs.total, 1);

    let enclosing = runtime.enclosing_symbol(EnclosingSymbolArgs {
        file: "sample.py".into(),
        row: Some(2),
        rows: None,
        col: None,
        repo: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        depth: 1,
        limit: None,
        offset: None,
    })?;
    assert_eq!(enclosing.matches.len(), 1);

    let conn = Connection::open(&db)?;
    let has_partial: i64 = conn.query_row(
        "SELECT COUNT(*) FROM pragma_table_info('files') WHERE name = 'partial'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(has_partial, 1);
    Ok(())
}

#[test]
fn read_tools_degrade_on_read_only_v3_index() -> Result<()> {
    let dir = tempdir()?;
    fs::write(
        dir.path().join("sample.py"),
        "def alpha():\n    return alpha()\n",
    )?;
    let config = TsIndexConfig::default();
    config.write(dir.path())?;
    let runtime = Runtime::new(
        dir.path().to_path_buf(),
        db_path(dir.path()),
        config,
        vec!["python".to_string()],
    );
    runtime.build(false, None)?;

    // Simulate a pre-`partial` (v3) index, then make the DB read-only so the
    // additive migration cannot run. Reads must degrade to a tolerant
    // projection rather than failing.
    let db = db_path(dir.path());
    let conn = Connection::open(&db)?;
    conn.execute("ALTER TABLE files DROP COLUMN partial", [])?;
    conn.pragma_update(None, "user_version", 3_i64)?;
    drop(conn);
    let mut perms = fs::metadata(&db)?.permissions();
    perms.set_readonly(true);
    fs::set_permissions(&db, perms)?;

    let symbol = runtime.get_symbol(GetSymbolArgs {
        name: Some("alpha".into()),
        names: None,
        repo: None,
        kind: None,
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    });
    // Restore writability regardless of the assertion outcome so tempdir
    // cleanup on Windows isn't blocked by a read-only file.
    let mut perms = fs::metadata(&db)?.permissions();
    #[allow(clippy::permissions_set_readonly_false)]
    perms.set_readonly(false);
    fs::set_permissions(&db, perms)?;

    let symbol = symbol?;
    assert_eq!(symbol.matches.len(), 1);
    assert!(
        !symbol.matches[0].partial,
        "degraded v3 read must report partial=false, not fail"
    );
    Ok(())
}

#[test]
fn get_symbol_relocates_stale_body_without_losing_clean_batch_matches() -> Result<()> {
    let dir = tempdir()?;
    fs::write(dir.path().join("a.py"), "def alpha():\n    return 1\n")?;
    fs::write(dir.path().join("b.py"), "def beta():\n    return 2\n")?;
    let config = TsIndexConfig::default();
    config.write(dir.path())?;
    let runtime = Runtime::new(
        dir.path().to_path_buf(),
        db_path(dir.path()),
        config,
        vec!["python".to_string()],
    );
    runtime.build(false, None)?;
    fs::write(
        dir.path().join("a.py"),
        "# inserted after indexing\ndef alpha():\n    return 1\n",
    )?;

    let response = runtime.get_symbol(GetSymbolArgs {
        name: None,
        names: Some(vec!["alpha".into(), "beta".into()]),
        repo: None,
        kind: None,
        file_glob: None,
        include_body: true,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert_eq!(response.matches.len(), 2);
    let alpha = response
        .matches
        .iter()
        .find(|item| item.name == "alpha")
        .unwrap();
    let beta = response
        .matches
        .iter()
        .find(|item| item.name == "beta")
        .unwrap();
    assert!(alpha.stale);
    assert!(
        alpha
            .body
            .as_deref()
            .unwrap_or_default()
            .contains("def alpha")
    );
    assert!(alpha.body_unavailable.is_none());
    assert!(!beta.stale);
    assert!(
        beta.body
            .as_deref()
            .unwrap_or_default()
            .contains("def beta")
    );
    Ok(())
}

#[test]
fn stale_reference_omits_snippet_and_explains_why() -> Result<()> {
    let dir = tempdir()?;
    fs::write(dir.path().join("decl.py"), "def target():\n    pass\n")?;
    fs::write(dir.path().join("use.py"), "def caller():\n    target()\n")?;
    let config = TsIndexConfig::default();
    config.write(dir.path())?;
    let runtime = Runtime::new(
        dir.path().to_path_buf(),
        db_path(dir.path()),
        config,
        vec!["python".to_string()],
    );
    runtime.build(false, None)?;
    fs::write(
        dir.path().join("use.py"),
        "# changed\ndef caller():\n    target()\n",
    )?;

    let response = runtime.find_references(FindReferencesArgs {
        name: Some("target".into()),
        names: None,
        repo: None,
        scope: None,
        exclude_tests: false,
        include_declarations: false,
        group_by_file: false,
        snippet_lines: 1,
        limit: None,
        offset: None,
        counts_only: false,
    })?;
    assert!(response.partial);
    assert_eq!(response.refs.len(), 1);
    assert!(response.refs[0].snippet_unavailable.is_some());
    let json = serde_json::to_value(&response)?;
    assert!(json["refs"][0].get("snippet").is_none());
    assert!(json["refs"][0].get("snippet_unavailable").is_some());
    Ok(())
}

#[test]
fn query_ignores_parse_errors_from_files_without_returned_captures() -> Result<()> {
    let dir = tempdir()?;
    fs::write(dir.path().join("good.py"), "def ok():\n    pass\n")?;
    fs::write(dir.path().join("bad.py"), "value = (\n")?;
    let config = TsIndexConfig::default();
    config.write(dir.path())?;
    let runtime = Runtime::new(
        dir.path().to_path_buf(),
        db_path(dir.path()),
        config,
        vec!["python".to_string()],
    );

    let response = runtime.query(QueryArgs {
        language: "python".into(),
        repo: None,
        query: "(function_definition name: (identifier) @fn)".into(),
        file_glob: None,
        capture: Some("fn".into()),
        limit: None,
        offset: None,
    })?;
    assert_eq!(response.captures.len(), 1);
    assert_eq!(response.captures[0].text, "ok");
    assert!(!response.partial);
    assert!(
        !db_path(dir.path()).exists(),
        "live query must not create an index DB"
    );
    Ok(())
}

#[test]
fn stale_outline_body_is_partial_not_truncated() -> Result<()> {
    let dir = tempdir()?;
    fs::write(
        dir.path().join("sample.py"),
        "def alpha():\n    pass\n\ndef beta():\n    pass\n",
    )?;
    let config = TsIndexConfig::default();
    config.write(dir.path())?;
    let runtime = Runtime::new(
        dir.path().to_path_buf(),
        db_path(dir.path()),
        config,
        vec!["python".to_string()],
    );
    runtime.build(false, None)?;
    fs::write(
        dir.path().join("sample.py"),
        "# changed\ndef alpha():\n    pass\n\ndef beta():\n    pass\n",
    )?;

    let response = runtime.list_file_outline(OutlineArgs {
        file: "sample.py".into(),
        repo: None,
        depth: 2,
        include_signatures: true,
        include_docstrings: true,
        include_imports: false,
        include_bodies_for: Some(vec!["alpha".into()]),
        max_body_lines: None,
        limit: None,
        offset: None,
    })?;
    assert!(response.partial);
    assert!(!response.truncated);
    assert!(response.next_offset.is_none());
    Ok(())
}

#[test]
fn extensionless_shebang_detection_is_bounded_and_binary_safe() -> Result<()> {
    let dir = tempdir()?;
    fs::write(
        dir.path().join("shell-tool"),
        b"#!/usr/bin/env sh\necho ok\n",
    )?;
    // Case-insensitive: the unified shebang matcher lowercases before matching,
    // so `env SH` resolves to bash exactly like `env sh`. This is the case the
    // old duplicated detection disagreed on (lang.rs matched, detect.rs didn't).
    fs::write(
        dir.path().join("shell-tool-upper"),
        b"#!/usr/bin/env SH\necho ok\n",
    )?;
    fs::write(dir.path().join("binary"), [0xff, 0xfe, 0xfd, 0x00])?;
    let detected = detect_languages(dir.path())?;
    assert!(detected.iter().any(|item| item.language == "bash"));
    Ok(())
}

#[test]
fn get_symbol_budget_pages_without_gaps_or_duplicates() -> Result<()> {
    let dir = tempdir()?;
    for index in 0..24 {
        fs::write(
            dir.path().join(format!("shared_{index:02}.py")),
            "def shared():\n    return 1\n",
        )?;
    }
    let config = TsIndexConfig {
        server: ServerConfig {
            max_response_chars: 700,
            ..Default::default()
        },
        ..Default::default()
    };
    config.write(dir.path())?;
    let runtime = Runtime::new(
        dir.path().to_path_buf(),
        db_path(dir.path()),
        config,
        vec!["python".to_string()],
    );
    runtime.build(false, None)?;

    let mut offset = None;
    let mut seen = Vec::new();
    loop {
        let page = runtime.get_symbol(GetSymbolArgs {
            name: Some("shared".into()),
            names: None,
            repo: None,
            kind: None,
            file_glob: None,
            include_body: false,
            context_lines: 0,
            max_body_lines: None,
            limit: Some(24),
            offset,
        })?;
        assert!(serde_json::to_string(&page)?.len() <= 700);
        seen.extend(page.matches.iter().map(|item| item.file.clone()));
        match page.next_offset {
            Some(next) => offset = Some(next),
            None => break,
        }
    }
    let mut unique = seen.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(seen.len(), 24);
    assert_eq!(unique.len(), seen.len());
    Ok(())
}

#[test]
fn enclosing_symbol_paginates_rows() -> Result<()> {
    let dir = tempdir()?;
    let src = dir.path().join("src");
    fs::create_dir_all(&src)?;
    // Ten functions on consecutive lines; rows 1..=10 each resolve to one.
    let body: String = (1..=10)
        .map(|i| format!("def fn{i}():\n    return {i}\n"))
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(src.join("m.py"), body)?;
    let runtime = Runtime::new(
        dir.path().to_path_buf(),
        db_path(dir.path()),
        TsIndexConfig::default(),
        vec!["python".to_string()],
    );
    runtime.build(false, None)?;
    let rows: Vec<usize> = (1..=10).collect();
    let args = |offset: Option<usize>, limit: Option<usize>| EnclosingSymbolArgs {
        file: "src/m.py".to_string(),
        row: None,
        rows: Some(rows.clone()),
        col: None,
        repo: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        depth: 1,
        limit,
        offset,
    };

    // limit splits the batch and next_offset resumes it.
    let first = runtime.enclosing_symbol(args(None, Some(4)))?;
    assert_eq!(first.total, 10);
    assert_eq!(first.results.len(), 4);
    assert!(first.truncated);
    assert_eq!(first.next_offset, Some(4));
    assert_eq!(first.results[0].row, 1);

    let second = runtime.enclosing_symbol(args(first.next_offset, Some(4)))?;
    assert_eq!(second.results.len(), 4);
    assert_eq!(second.results[0].row, 5);
    assert_eq!(second.next_offset, Some(8));

    let third = runtime.enclosing_symbol(args(second.next_offset, Some(4)))?;
    assert_eq!(third.results.len(), 2);
    assert_eq!(third.results[0].row, 9);
    assert!(!third.truncated);
    assert!(third.next_offset.is_none());
    Ok(())
}

#[test]
fn enclosing_symbol_budget_pages_rows_without_gaps_or_duplicates() -> Result<()> {
    let dir = tempdir()?;
    let src = dir.path().join("src");
    fs::create_dir_all(&src)?;
    // 30 rows across 15 small functions; with bodies + a tight budget the
    // page must shrink below the requested limit and page cleanly.
    let body: String = (1..=15)
        .map(|i| format!("def fn{i}():\n    value = {i}\n    return value\n"))
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(src.join("m.py"), body)?;
    let config = TsIndexConfig {
        server: ServerConfig {
            max_response_chars: 600,
            ..Default::default()
        },
        ..Default::default()
    };
    config.write(dir.path())?;
    let runtime = Runtime::new(
        dir.path().to_path_buf(),
        db_path(dir.path()),
        config,
        vec!["python".to_string()],
    );
    runtime.build(false, None)?;

    let rows: Vec<usize> = (1..=15).map(|i| (i - 1) * 3 + 1).collect();
    let mut offset = None;
    let mut seen = Vec::new();
    loop {
        let page = runtime.enclosing_symbol(EnclosingSymbolArgs {
            file: "src/m.py".to_string(),
            row: None,
            rows: Some(rows.clone()),
            col: None,
            repo: None,
            include_body: true,
            context_lines: 0,
            max_body_lines: None,
            depth: 1,
            limit: Some(30),
            offset,
        })?;
        assert!(serde_json::to_string(&page)?.len() <= 600);
        seen.extend(page.results.iter().map(|hit| hit.row));
        match page.next_offset {
            Some(next) => offset = Some(next),
            None => break,
        }
    }
    let mut unique = seen.clone();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(seen.len(), rows.len());
    assert_eq!(unique.len(), seen.len(), "no row resolved twice");
    Ok(())
}
