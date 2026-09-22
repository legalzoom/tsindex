//! Regression tests for language extraction: C++ nested qualifiers, Java
//! type references, `.C`/`.H` detection, Ruby symbol literals, and the
//! declaration-filter exemption for properties/constants.

use std::fs;
use std::path::Path;

use anyhow::Result;
use tempfile::{TempDir, tempdir};

use tsindex::config::{TsIndexConfig, db_path};
use tsindex::index::{FindReferencesArgs, GetSymbolArgs, Runtime};

fn fixture(languages: &[&str], files: &[(&str, &str)]) -> Result<(TempDir, Runtime)> {
    let dir = tempdir()?;
    for (name, body) in files {
        let path = dir.path().join(name);
        fs::create_dir_all(path.parent().unwrap())?;
        fs::write(path, body)?;
    }
    let root: &Path = dir.path();
    let config = TsIndexConfig::default();
    config.write(root)?;
    let runtime = Runtime::new(
        root.to_path_buf(),
        db_path(root),
        config,
        languages.iter().map(|l| l.to_string()).collect(),
    );
    runtime.build(false, None)?;
    Ok((dir, runtime))
}

fn symbol_args(name: &str) -> GetSymbolArgs {
    GetSymbolArgs {
        name: Some(name.to_string()),
        names: None,
        repo: None,
        kind: None,
        file_glob: None,
        include_body: false,
        context_lines: 0,
        max_body_lines: None,
        limit: None,
        offset: None,
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

#[test]
fn cpp_out_of_class_method_on_namespaced_class_is_named_by_innermost_identifier() -> Result<()> {
    let (_dir, runtime) = fixture(
        &["cpp"],
        &[(
            "a.cpp",
            "namespace A { class B { public: void m(); ~B(); }; }\nvoid A::B::m() {}\nA::B::~B() {}\nvoid Shape::draw() {}\n",
        )],
    )?;
    let m = runtime.get_symbol(symbol_args("m"))?;
    let kinds: Vec<&str> = m.matches.iter().map(|s| s.kind.as_str()).collect();
    assert!(
        kinds.contains(&"method") && m.matches.iter().any(|s| s.range.start.0 == 1),
        "expected the out-of-class definition of m on 0-based row 1: {:?}",
        m.matches
            .iter()
            .map(|s| (s.kind.as_str(), s.range.start.0, s.qualified.clone()))
            .collect::<Vec<_>>()
    );
    let none = runtime.get_symbol(symbol_args("B::m"))?;
    assert_eq!(none.total, 0, "qualifier must not leak into the name");
    let dtor = runtime.get_symbol(symbol_args("~B"))?;
    assert!(dtor.matches.iter().any(|s| s.range.start.0 == 2));
    let draw = runtime.get_symbol(symbol_args("draw"))?;
    assert_eq!(draw.total, 1, "single-level qualifier still works");
    Ok(())
}

#[test]
fn java_type_positions_are_references() -> Result<()> {
    let (_dir, runtime) = fixture(
        &["java"],
        &[(
            "Rec.java",
            "class Rec { Rec() {} void m() { Rec r = new Rec(); } }\n",
        )],
    )?;
    let uses = runtime.find_references(refs_args("Rec", false))?;
    assert_eq!(uses.total, 2, "type position and `new Rec()` are uses");
    let all = runtime.find_references(refs_args("Rec", true))?;
    assert!(
        all.total > uses.total,
        "declaration rows come back when asked"
    );
    Ok(())
}

#[test]
fn uppercase_c_and_h_extensions_index_as_cpp() -> Result<()> {
    let (_dir, runtime) = fixture(
        &["c", "cpp"],
        &[
            (
                "legacy.C",
                "template <typename T> class Box { public: T v; };\nclass Legacy {};\n",
            ),
            ("legacy.H", "class Header {};\n"),
            ("plain.c", "int add(int a, int b) { return a + b; }\n"),
        ],
    )?;
    for (name, file) in [
        ("Box", "legacy.C"),
        ("Legacy", "legacy.C"),
        ("Header", "legacy.H"),
    ] {
        let found = runtime.get_symbol(symbol_args(name))?;
        assert_eq!(found.total, 1, "{name} in {file}");
        assert_eq!(
            found.matches[0].language, "cpp",
            "{file} must use the C++ grammar"
        );
    }
    let add = runtime.get_symbol(symbol_args("add"))?;
    assert_eq!(add.matches[0].language, "c");
    Ok(())
}

#[test]
fn ruby_symbol_literals_do_not_produce_colon_prefixed_refs() -> Result<()> {
    let (_dir, runtime) = fixture(
        &["ruby"],
        &[(
            "a.rb",
            "class Cart\n  attr_reader :items\n  def total\n    items.sum\n  end\nend\n",
        )],
    )?;
    let colon = runtime.find_references(refs_args(":items", true))?;
    assert_eq!(colon.total, 0, "no inert `:items` rows");
    let plain = runtime.find_references(refs_args("items", true))?;
    assert!(plain.total >= 1, "the `items.sum` use is still a reference");
    Ok(())
}

#[test]
fn property_and_constant_declarations_are_reported_like_variables() -> Result<()> {
    let (_dir, runtime) = fixture(
        &["php"],
        &[(
            "money.php",
            "<?php\nclass Money {\n  const DEFAULT_CURRENCY = 'USD';\n  public string $currency;\n}\n",
        )],
    )?;
    for name in ["currency", "DEFAULT_CURRENCY"] {
        let refs = runtime.find_references(refs_args(name, false))?;
        assert!(
            refs.total >= 1,
            "{name}: value bindings keep their defining occurrence with include_declarations=false"
        );
    }
    Ok(())
}

#[test]
fn sql_detects_extensions_and_indexes_declarations() -> Result<()> {
    let source = r#"CREATE DATABASE shop WITH OWNER = db_owner;
CREATE SCHEMA app AUTHORIZATION db_owner;
-- Registered users.
CREATE TABLE IF NOT EXISTS app.users (
    id INTEGER PRIMARY KEY,
    account_id INTEGER REFERENCES app.accounts(id)
);
CREATE VIEW app.user_ids AS SELECT id FROM app.users;
CREATE MATERIALIZED VIEW app.cached_ids AS SELECT id FROM app.users;
CREATE UNIQUE INDEX user_id_idx ON app.users(id);
CREATE FUNCTION app.first_user() RETURNS app.users LANGUAGE SQL AS 'SELECT * FROM app.users LIMIT 1';
CREATE SEQUENCE IF NOT EXISTS app.user_seq OWNED BY app.users.id;
CREATE TYPE app.status AS ENUM ('active', 'inactive');
CREATE TRIGGER user_changed AFTER INSERT ON app.users FOR EACH ROW EXECUTE FUNCTION app.notify_user();
"#;
    let (dir, runtime) = fixture(&[], &[("schema.SQL", source)])?;
    let detected = tsindex::detect::detect_languages(dir.path())?;
    assert!(detected.iter().any(|language| language.language == "sql"));
    for file in ["schema.sql", "schema.SQL", "schema.Sql"] {
        assert_eq!(
            tsindex::lang::detect_language_from_file(dir.path(), &dir.path().join(file))?,
            Some("sql")
        );
    }
    for (name, kind) in [
        ("shop", "database"),
        ("app", "schema"),
        ("users", "table"),
        ("id", "column"),
        ("account_id", "column"),
        ("user_ids", "view"),
        ("cached_ids", "view"),
        ("user_id_idx", "index"),
        ("first_user", "function"),
        ("user_seq", "sequence"),
        ("status", "type"),
        ("user_changed", "trigger"),
    ] {
        let found = runtime.get_symbol(symbol_args(name))?;
        assert_eq!(found.total, 1, "{name}: {:?}", found.matches);
        assert_eq!(found.matches[0].kind, kind);
        assert_eq!(found.matches[0].language, "sql");
        assert!(!found.matches[0].partial, "{name}: parse recovery");
    }
    for name in ["db_owner", "accounts", "notify_user", "active"] {
        assert_eq!(runtime.get_symbol(symbol_args(name))?.total, 0, "{name}");
    }
    let table = runtime.get_symbol(symbol_args("users"))?;
    assert_eq!(table.matches[0].range.start.0, 3);
    assert_eq!(table.matches[0].range.end.0, 6);
    assert_eq!(
        table.matches[0].docstring.as_deref(),
        Some("Registered users.")
    );
    let column = runtime.get_symbol(symbol_args("account_id"))?;
    assert_eq!(
        column.matches[0].qualified.as_deref(),
        Some("users.account_id")
    );
    let outline = runtime.list_file_outline(serde_json::from_value(serde_json::json!({
        "file": "schema.SQL", "depth": 2, "include_bodies_for": ["users"]
    }))?)?;
    assert!(!outline.partial);
    let users = outline
        .symbols
        .iter()
        .find(|symbol| symbol.name == "users")
        .unwrap();
    assert_eq!(users.children.len(), 2);
    assert!(
        users
            .body
            .as_deref()
            .unwrap()
            .contains("REFERENCES app.accounts(id)")
    );
    let enclosing = runtime.enclosing_symbol(serde_json::from_value(serde_json::json!({
        "file": "schema.SQL", "row": 6, "depth": 2
    }))?)?;
    let names: Vec<&str> = enclosing
        .matches
        .iter()
        .map(|symbol| symbol.name.as_str())
        .collect();
    assert_eq!(names, ["users", "account_id"]);
    Ok(())
}

#[test]
fn sql_references_exclude_declarations_comments_and_literals() -> Result<()> {
    let (_dir, runtime) = fixture(
        &["sql"],
        &[(
            "queries.sql",
            r#"CREATE TABLE app.users (id INTEGER);
-- users comment_only
SELECT id, 'users literal_only' FROM app.users;
INSERT INTO app.users (id) VALUES (1);
UPDATE app.users SET id = 2;
DELETE FROM app.users WHERE id = 2;
WITH recent(user_id) AS (SELECT id FROM app.users) SELECT user_id FROM recent;
"#,
        )],
    )?;
    let uses = runtime.find_references(refs_args("users", false))?;
    assert_eq!(uses.total, 5);
    assert!(!uses.partial);
    let all = runtime.find_references(refs_args("users", true))?;
    assert_eq!(all.total, 6);
    assert_eq!(
        all.refs
            .iter()
            .filter(|reference| reference.context == "declaration")
            .count(),
        1
    );
    for name in ["comment_only", "literal_only", "SELECT"] {
        assert_eq!(runtime.find_references(refs_args(name, true))?.total, 0);
    }
    let cte = runtime.get_symbol(symbol_args("recent"))?;
    assert_eq!(cte.total, 1);
    assert_eq!(cte.matches[0].kind, "cte");
    assert_eq!(runtime.get_symbol(symbol_args("user_id"))?.total, 0);
    assert_eq!(
        runtime.find_references(refs_args("recent", false))?.total,
        1
    );
    let query = runtime.query(serde_json::from_value(serde_json::json!({
        "language": "sql", "query": "(cte . (identifier) @name)", "capture": "name"
    }))?)?;
    assert!(!query.partial);
    assert_eq!(query.captures.len(), 1);
    assert_eq!(query.captures[0].text, "recent");
    Ok(())
}

#[test]
fn sql_preserves_quoted_identifiers_and_keyword_case() -> Result<()> {
    let (_dir, runtime) = fixture(
        &["sql"],
        &[(
            "quoted.sql",
            r#"create table "Order Items" ("Item ID" integer);
select items."Item ID", "double_string" from "Order Items" items;
insert into "Order Items" ("Item ID") values (1);
create index "items_idx" on "Order Items" ("Item ID");
create table `orders` (`order_id` integer);
select `order_id` from `orders`;
"#,
        )],
    )?;
    for (name, kind, references) in [
        ("\"Order Items\"", "table", 3),
        ("\"Item ID\"", "column", 3),
        ("\"items_idx\"", "index", 0),
        ("`orders`", "table", 1),
        ("`order_id`", "column", 1),
    ] {
        let found = runtime.get_symbol(symbol_args(name))?;
        assert_eq!(found.total, 1, "{name}");
        assert_eq!(found.matches[0].kind, kind);
        assert!(!found.matches[0].partial);
        let uses = runtime.find_references(refs_args(name, false))?;
        assert_eq!(uses.total, references, "{name}");
        assert!(!uses.partial);
    }
    assert_eq!(
        runtime
            .find_references(refs_args("\"double_string\"", true))?
            .total,
        0
    );
    Ok(())
}
