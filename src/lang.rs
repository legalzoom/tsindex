use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};

use anyhow::{Result, anyhow};
use tree_sitter::{Language, Parser, Query};

#[derive(Clone)]
pub struct LanguageSpec {
    pub id: &'static str,
    pub manifests: &'static [&'static str],
    pub extensions: &'static [&'static str],
    pub comment_prefixes: &'static [&'static str],
    pub language: fn() -> Language,
    pub symbol_query: &'static str,
    pub ref_query: &'static str,
    /// AST node kinds that wrap a definition with leading trivia (e.g.
    /// Python's `decorated_definition` wraps `function_definition` and
    /// `class_definition` when decorators are present). When a captured
    /// `*.def` node sits directly inside one of these wrappers,
    /// `extract_symbols` expands its range upward so the captured symbol
    /// includes its decorators.
    ///
    /// Most languages don't need this because their grammar inlines
    /// decorators/annotations into the definition node itself (e.g.
    /// tree-sitter-java's `method_declaration` already includes
    /// `(modifiers ...)`). Leave empty for those languages.
    pub decorator_wrappers: &'static [&'static str],
}

/// Tree-sitter `Query` compilation is expensive and was previously redone for
/// every file (and thus on every rayon worker), which dominated the per-worker
/// memory churn during a full build. Compiled queries are immutable and
/// `Sync`, so we compile each language's queries exactly once and share them
/// across threads via `Arc`; only the per-call `QueryCursor` stays per-thread.
struct CompiledQueries {
    symbol: Arc<Query>,
    references: Arc<Query>,
}

fn query_cache() -> &'static Mutex<HashMap<&'static str, CompiledQueries>> {
    static CACHE: OnceLock<Mutex<HashMap<&'static str, CompiledQueries>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

impl LanguageSpec {
    pub fn parser(&self) -> Result<Parser> {
        let mut parser = Parser::new();
        parser.set_language(&(self.language)())?;
        Ok(parser)
    }

    /// Compile (or fetch the cached) symbol and reference queries for this
    /// language. The language id is the cache key; the grammar is instantiated
    /// at most once per language to build its queries.
    ///
    /// `Query::new` is expensive, so we never hold the cache lock across it —
    /// otherwise the first compile for one language would block every other
    /// thread (even ones needing a different language) during a parallel build.
    /// Instead we check the cache under the lock, compile unlocked on a miss,
    /// then re-lock to insert. A race can compile the same language twice, but
    /// `entry().or_insert` keeps the first result so all callers still share a
    /// single `Arc` — a rare, cheap duplicate beats serializing every worker.
    fn compiled_queries(&self) -> Result<(Arc<Query>, Arc<Query>)> {
        {
            let cache = query_cache()
                .lock()
                .map_err(|_| anyhow!("query cache mutex poisoned"))?;
            if let Some(compiled) = cache.get(self.id) {
                return Ok((compiled.symbol.clone(), compiled.references.clone()));
            }
        }
        let language = (self.language)();
        let symbol = Arc::new(Query::new(&language, self.symbol_query)?);
        let references = Arc::new(Query::new(&language, self.ref_query)?);
        let mut cache = query_cache()
            .lock()
            .map_err(|_| anyhow!("query cache mutex poisoned"))?;
        let compiled = cache
            .entry(self.id)
            .or_insert(CompiledQueries { symbol, references });
        Ok((compiled.symbol.clone(), compiled.references.clone()))
    }

    pub fn symbol_query(&self) -> Result<Arc<Query>> {
        Ok(self.compiled_queries()?.0)
    }

    pub fn ref_query(&self) -> Result<Arc<Query>> {
        Ok(self.compiled_queries()?.1)
    }
}

pub fn known_language_ids() -> Vec<&'static str> {
    all_languages().iter().map(|spec| spec.id).collect()
}

pub fn lookup_language(id: &str) -> Option<LanguageSpec> {
    all_languages().into_iter().find(|spec| spec.id == id)
}

pub fn languages_for_manifest(file_name: &str) -> Vec<&'static str> {
    all_languages()
        .into_iter()
        .filter(|spec| {
            spec.manifests
                .iter()
                .any(|manifest| manifest_matches(manifest, file_name))
        })
        .map(|spec| spec.id)
        .collect()
}

fn manifest_matches(pattern: &str, file_name: &str) -> bool {
    pattern == file_name
        || pattern
            .strip_prefix('*')
            .is_some_and(|suffix| file_name.ends_with(suffix))
}

pub fn detect_language_from_file(root: &Path, path: &Path) -> Result<Option<&'static str>> {
    let relative = path.strip_prefix(root).unwrap_or(path);
    for component in relative.components() {
        let part = component.as_os_str().to_string_lossy();
        if matches!(
            part.as_ref(),
            "node_modules"
                | "vendor"
                | "target"
                | ".venv"
                | ".git"
                | "dist"
                | "build"
                | ".next"
                | ".turbo"
                | ".parcel-cache"
        ) {
            return Ok(None);
        }
    }

    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default();
    // GNU convention: uppercase `.C`/`.H` are C++, so decide before the
    // case-fold below would send them to the C grammar.
    if matches!(extension, "C" | "H") {
        return Ok(Some("cpp"));
    }
    let extension = extension.to_ascii_lowercase();
    let extension = extension.as_str();
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or_default();
    if matches!(file_name, "build.gradle.kts" | "settings.gradle.kts") {
        return Ok(None);
    }
    if extension.is_empty()
        && let Some(language) = detect_shebang_language(path)?
    {
        return Ok(Some(language));
    }
    for spec in all_languages() {
        if spec.id == "make"
            && spec
                .manifests
                .iter()
                .any(|pattern| manifest_matches(pattern, file_name))
        {
            return Ok(Some(spec.id));
        }
        if spec.extensions.contains(&extension) {
            return Ok(Some(spec.id));
        }
    }
    Ok(None)
}

/// Bound on how many leading bytes of a file we inspect for a shebang. A
/// shebang line is short in practice; capping the read keeps detection from
/// pulling a whole (potentially huge) file into memory just to look at line 1.
const SHEBANG_PROBE_BYTES: usize = 256;

/// Extract the shebang line from `path` as lowercased ASCII bytes, or `None`
/// when the file can't be read, doesn't start with `#!`, or is otherwise not a
/// shebang script. Reading is bounded to [`SHEBANG_PROBE_BYTES`] and done on
/// raw bytes so invalid UTF-8 never trips detection — a shebang is pure ASCII,
/// so a byte-substring match is exact and encoding-agnostic. Lowercasing once
/// here keeps the match case-insensitive (`#!/usr/bin/env SH` counts as sh).
fn read_shebang_line(path: &Path) -> Option<Vec<u8>> {
    use std::io::Read;

    let mut file = std::fs::File::open(path).ok()?;
    let mut buf = vec![0_u8; SHEBANG_PROBE_BYTES];
    let read = file.read(&mut buf).ok()?;
    let mut line = buf[..read]
        .split(|byte| *byte == b'\n')
        .next()
        .unwrap_or_default()
        .to_vec();
    if !line.starts_with(b"#!") {
        return None;
    }
    line.make_ascii_lowercase();
    Some(line)
}

/// The language a shebang line points at, matched case-insensitively on
/// lowercased bytes. Shared by `detect_shebang_language` (extensionless files)
/// and `has_matching_shebang` (detection confirmation in `detect.rs`) so the
/// two paths can't drift. `env sh` (POSIX shell) counts as bash, matching how
/// shells are used in the wild.
pub(crate) fn language_for_shebang_line(line: &[u8]) -> Option<&'static str> {
    let contains = |needle: &[u8]| line.windows(needle.len()).any(|window| window == needle);
    if contains(b"python") {
        Some("python")
    } else if contains(b"ruby") {
        Some("ruby")
    } else if contains(b"node") {
        Some("javascript")
    } else if contains(b"bash")
        || contains(b"zsh")
        || contains(b"dash")
        || contains(b"ksh")
        || contains(b"/sh")
        || contains(b" sh")
    {
        Some("bash")
    } else {
        None
    }
}

fn detect_shebang_language(path: &Path) -> Result<Option<&'static str>> {
    Ok(read_shebang_line(path)
        .as_deref()
        .and_then(language_for_shebang_line))
}

/// Confirm whether `path`'s shebang resolves to `language`. Used by
/// `detect.rs` to add a confirmation vote for a file whose language was
/// already detected another way. Any read failure or a missing/mismatched
/// shebang is `false` rather than an error — one unreadable file should not
/// abort the language-detection walk.
pub(crate) fn shebang_matches(path: &Path, language: &str) -> bool {
    read_shebang_line(path)
        .as_deref()
        .and_then(language_for_shebang_line)
        .is_some_and(|detected| detected == language)
}

pub fn all_languages() -> Vec<LanguageSpec> {
    vec![
        LanguageSpec {
            id: "python",
            manifests: &["pyproject.toml", "requirements.txt"],
            extensions: &["py", "pyi"],
            comment_prefixes: &["#"],
            language: || tree_sitter_python::LANGUAGE.into(),
            symbol_query: r#"
                (import_statement) @import
                (import_from_statement) @import
                (class_definition name: (identifier) @class.name) @class.def
                (function_definition name: (identifier) @function.name) @function.def
                (assignment left: (identifier) @variable.name) @variable.def
            "#,
            ref_query: r#"(identifier) @ref"#,
            // Python wraps decorated functions/classes in `decorated_definition`
            // — the bare class_definition/function_definition node range
            // excludes preceding decorators. extract_symbols expands the
            // captured range upward when the parent is this wrapper kind.
            decorator_wrappers: &["decorated_definition"],
        },
        LanguageSpec {
            id: "javascript",
            manifests: &["package.json"],
            extensions: &["js", "jsx", "mjs", "cjs"],
            comment_prefixes: &["//", "/*", "*"],
            language: || tree_sitter_javascript::LANGUAGE.into(),
            symbol_query: r#"
                (import_statement) @import
                (class_declaration name: (identifier) @class.name) @class.def
                (function_declaration name: (identifier) @function.name) @function.def
                (generator_function_declaration name: (identifier) @function.name) @function.def
                (method_definition name: (property_identifier) @method.name) @method.def
                (lexical_declaration (variable_declarator name: (identifier) @variable.name)) @variable.def
                (variable_declaration (variable_declarator name: (identifier) @variable.name)) @variable.def
            "#,
            ref_query: r#"
                (identifier) @ref
                (property_identifier) @ref
            "#,
            decorator_wrappers: &[],
        },
        LanguageSpec {
            id: "html",
            manifests: &[],
            extensions: &["html", "htm"],
            comment_prefixes: &["<!--"],
            language: || tree_sitter_html::LANGUAGE.into(),
            // HTML has no functions/classes; the navigationally useful "symbols"
            // are element id attributes (anchor/CSS/JS targets) plus the
            // <script>/<style> sections. Capture id values via an #eq? predicate
            // on the attribute name.
            symbol_query: r#"
                (element (start_tag
                    (attribute (attribute_name) @_attr (quoted_attribute_value (attribute_value) @id.name))
                    (#eq? @_attr "id"))) @id.def
                (script_element (start_tag (tag_name) @tag.name)) @tag.def
                (style_element (start_tag (tag_name) @tag.name)) @tag.def
            "#,
            // References are the attribute/tag names that point at ids: class
            // and id values. Keep it minimal — plain values cover both.
            ref_query: r#"
                (attribute_value) @ref
                (tag_name) @ref
            "#,
            decorator_wrappers: &[],
        },
        LanguageSpec {
            id: "css",
            manifests: &[],
            extensions: &["css"],
            comment_prefixes: &["/*", "*"],
            language: || tree_sitter_css::LANGUAGE.into(),
            symbol_query: r#"
                (import_statement) @import
                (keyframes_statement (keyframes_name) @keyframes.name) @keyframes.def
                (rule_set (selectors) @selector.name) @selector.def
            "#,
            ref_query: r#"
                (class_name) @ref
                (id_name) @ref
                (tag_name) @ref
                (property_name) @ref
                (keyframes_name) @ref
                (function_name) @ref
                (plain_value) @ref
            "#,
            decorator_wrappers: &[],
        },
        LanguageSpec {
            id: "scss",
            manifests: &[],
            extensions: &["scss"],
            comment_prefixes: &["//", "/*", "*"],
            language: tree_sitter_scss::language,
            symbol_query: r#"
                (import_statement) @import
                (use_statement) @import
                (forward_statement) @import
                (keyframes_statement (keyframes_name) @keyframes.name) @keyframes.def
                (mixin_statement name: (identifier) @mixin.name) @mixin.def
                (function_statement name: (identifier) @function.name) @function.def
                (placeholder (identifier) @placeholder.name) @placeholder.def
                (declaration (variable) @variable.name) @variable.def
                (rule_set (selectors) @selector.name) @selector.def
            "#,
            ref_query: r#"
                (identifier) @ref
                (variable) @ref
                (class_name) @ref
                (id_name) @ref
                (tag_name) @ref
                (property_name) @ref
                (keyframes_name) @ref
                (function_name) @ref
                (plain_value) @ref
            "#,
            decorator_wrappers: &[],
        },
        LanguageSpec {
            id: "typescript",
            manifests: &["package.json", "tsconfig.json"],
            extensions: &["ts"],
            comment_prefixes: &["//", "/*", "*"],
            language: || tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into(),
            symbol_query: r#"
                (import_statement) @import
                (class_declaration name: (type_identifier) @class.name) @class.def
                (abstract_class_declaration name: (type_identifier) @class.name) @class.def
                (internal_module name: (identifier) @namespace.name) @namespace.def
                (function_declaration name: (identifier) @function.name) @function.def
                (generator_function_declaration name: (identifier) @function.name) @function.def
                (method_definition name: (property_identifier) @method.name) @method.def
                (interface_declaration name: (type_identifier) @interface.name) @interface.def
                (type_alias_declaration name: (type_identifier) @type.name) @type.def
                (enum_declaration name: (identifier) @enum.name) @enum.def
                (lexical_declaration (variable_declarator name: (identifier) @variable.name)) @variable.def
            "#,
            ref_query: r#"
                (identifier) @ref
                (type_identifier) @ref
                (property_identifier) @ref
            "#,
            decorator_wrappers: &[],
        },
        LanguageSpec {
            id: "tsx",
            manifests: &["package.json", "tsconfig.json"],
            extensions: &["tsx"],
            comment_prefixes: &["//", "/*", "*"],
            language: || tree_sitter_typescript::LANGUAGE_TSX.into(),
            symbol_query: r#"
                (import_statement) @import
                (class_declaration name: (type_identifier) @class.name) @class.def
                (abstract_class_declaration name: (type_identifier) @class.name) @class.def
                (internal_module name: (identifier) @namespace.name) @namespace.def
                (function_declaration name: (identifier) @function.name) @function.def
                (generator_function_declaration name: (identifier) @function.name) @function.def
                (method_definition name: (property_identifier) @method.name) @method.def
                (interface_declaration name: (type_identifier) @interface.name) @interface.def
                (type_alias_declaration name: (type_identifier) @type.name) @type.def
                (enum_declaration name: (identifier) @enum.name) @enum.def
                (lexical_declaration (variable_declarator name: (identifier) @variable.name)) @variable.def
            "#,
            ref_query: r#"
                (identifier) @ref
                (type_identifier) @ref
                (property_identifier) @ref
            "#,
            decorator_wrappers: &[],
        },
        LanguageSpec {
            id: "rust",
            manifests: &["Cargo.toml"],
            extensions: &["rs"],
            comment_prefixes: &["//", "/*", "*"],
            language: || tree_sitter_rust::LANGUAGE.into(),
            symbol_query: r#"
                (use_declaration) @import
                (function_item name: (identifier) @function.name) @function.def
                (struct_item name: (type_identifier) @struct.name) @struct.def
                (enum_item name: (type_identifier) @enum.name) @enum.def
                (trait_item name: (type_identifier) @trait.name) @trait.def
                (type_item name: (type_identifier) @type.name) @type.def
                (const_item name: (identifier) @variable.name) @variable.def
                (static_item name: (identifier) @variable.name) @variable.def
            "#,
            ref_query: r#"
                (identifier) @ref
                (type_identifier) @ref
                (field_identifier) @ref
            "#,
            decorator_wrappers: &[],
        },
        LanguageSpec {
            id: "go",
            manifests: &["go.mod"],
            extensions: &["go"],
            comment_prefixes: &["//", "/*", "*"],
            language: || tree_sitter_go::LANGUAGE.into(),
            symbol_query: r#"
                (import_declaration) @import
                (function_declaration name: (identifier) @function.name) @function.def
                (method_declaration name: (field_identifier) @method.name) @method.def
                (type_declaration (type_spec name: (type_identifier) @type.name)) @type.def
                (var_declaration (var_spec name: (identifier) @variable.name)) @variable.def
                (const_declaration (const_spec name: (identifier) @variable.name)) @variable.def
            "#,
            ref_query: r#"
                (identifier) @ref
                (type_identifier) @ref
                (field_identifier) @ref
            "#,
            decorator_wrappers: &[],
        },
        LanguageSpec {
            id: "java",
            manifests: &["pom.xml", "build.gradle", "build.gradle.kts"],
            extensions: &["java"],
            comment_prefixes: &["//", "/*", "*"],
            language: || tree_sitter_java::LANGUAGE.into(),
            symbol_query: r#"
                (import_declaration) @import
                (class_declaration name: (identifier) @class.name) @class.def
                (interface_declaration name: (identifier) @interface.name) @interface.def
                (enum_declaration name: (identifier) @enum.name) @enum.def
                (record_declaration name: (identifier) @class.name) @class.def
                (method_declaration name: (identifier) @method.name) @method.def
                (constructor_declaration name: (identifier) @method.name) @method.def
                (field_declaration (variable_declarator name: (identifier) @variable.name)) @variable.def
            "#,
            ref_query: r#"
                (identifier) @ref
                (type_identifier) @ref
            "#,
            decorator_wrappers: &[],
        },
        LanguageSpec {
            id: "kotlin",
            manifests: &["build.gradle.kts", "settings.gradle.kts"],
            extensions: &["kt", "kts"],
            comment_prefixes: &["//", "/*", "*"],
            language: || tree_sitter_kotlin_ng::LANGUAGE.into(),
            // tree-sitter-kotlin-ng names the typealias identifier field "type".
            symbol_query: r#"
                (import) @import
                (package_header (qualified_identifier) @namespace.name) @namespace.def
                (class_declaration "class" name: (identifier) @class.name) @class.def
                (class_declaration "interface" name: (identifier) @interface.name) @interface.def
                (function_declaration name: (identifier) @function.name) @function.def
                (object_declaration name: (identifier) @object.name) @object.def
                (companion_object name: (identifier) @object.name) @object.def
                (enum_entry (identifier) @variable.name) @variable.def
                (type_alias type: (identifier) @type.name) @type.def
                (property_declaration (variable_declaration (identifier) @variable.name)) @variable.def
            "#,
            ref_query: r#"
                (identifier) @ref
            "#,
            decorator_wrappers: &[],
        },
        LanguageSpec {
            id: "swift",
            manifests: &["Package.swift"],
            extensions: &["swift"],
            comment_prefixes: &["//", "/*", "*"],
            language: || tree_sitter_swift::LANGUAGE.into(),
            symbol_query: r#"
                (import_declaration) @import
                (class_declaration declaration_kind: "actor" name: (type_identifier) @actor.name) @actor.def
                (class_declaration declaration_kind: "class" name: (type_identifier) @class.name) @class.def
                (class_declaration declaration_kind: "enum" name: (type_identifier) @enum.name) @enum.def
                (class_declaration declaration_kind: "struct" name: (type_identifier) @struct.name) @struct.def
                (protocol_declaration name: (type_identifier) @protocol.name) @protocol.def
                (function_declaration name: (simple_identifier) @function.name) @function.def
                (protocol_function_declaration name: (simple_identifier) @function.name) @function.def
                (property_declaration name: (pattern (simple_identifier) @variable.name)) @variable.def
                (typealias_declaration name: (type_identifier) @type.name) @type.def
            "#,
            ref_query: r#"
                (simple_identifier) @ref
                (type_identifier) @ref
            "#,
            decorator_wrappers: &[],
        },
        LanguageSpec {
            id: "c",
            manifests: &["CMakeLists.txt"],
            extensions: &["c", "h"],
            comment_prefixes: &["//", "/*", "*"],
            language: || tree_sitter_c::LANGUAGE.into(),
            symbol_query: r#"
                (function_definition declarator: (function_declarator declarator: (identifier) @function.name)) @function.def
                (declaration (function_declarator declarator: (identifier) @prototype.name)) @prototype.def
                (declaration (init_declarator declarator: (identifier) @variable.name)) @variable.def
            "#,
            ref_query: r#"(identifier) @ref"#,
            decorator_wrappers: &[],
        },
        LanguageSpec {
            id: "csharp",
            manifests: &[
                "*.csproj",
                "*.sln",
                "*.slnx",
                "Directory.Build.props",
                "Directory.Build.targets",
                "packages.config",
            ],
            extensions: &["cs"],
            comment_prefixes: &["//", "/*", "*"],
            language: || tree_sitter_c_sharp::LANGUAGE.into(),
            symbol_query: r#"
                (using_directive) @import
                (namespace_declaration name: (_) @namespace.name) @namespace.def
                (file_scoped_namespace_declaration name: (_) @namespace.name) @namespace.def
                (class_declaration name: (identifier) @class.name) @class.def
                (interface_declaration name: (identifier) @interface.name) @interface.def
                (struct_declaration name: (identifier) @struct.name) @struct.def
                (enum_declaration name: (identifier) @enum.name) @enum.def
                (enum_member_declaration name: (identifier) @variable.name) @variable.def
                (record_declaration name: (identifier) @class.name) @class.def
                (delegate_declaration name: (identifier) @delegate.name) @delegate.def
                (event_declaration name: (identifier) @event.name) @event.def
                (event_field_declaration (variable_declaration (variable_declarator (identifier) @event.name))) @event.def
                (method_declaration name: (identifier) @method.name) @method.def
                (constructor_declaration name: (identifier) @method.name) @method.def
                (destructor_declaration name: (identifier) @method.name) @method.def
                (property_declaration name: (identifier) @property.name) @property.def
                (field_declaration (variable_declaration (variable_declarator (identifier) @variable.name))) @variable.def
            "#,
            ref_query: r#"(identifier) @ref"#,
            decorator_wrappers: &[],
        },
        LanguageSpec {
            id: "cpp",
            manifests: &["CMakeLists.txt"],
            extensions: &["cc", "cpp", "cxx", "hpp", "hh", "hxx"],
            comment_prefixes: &["//", "/*", "*"],
            language: || tree_sitter_cpp::LANGUAGE.into(),
            symbol_query: r#"
                (namespace_definition name: (namespace_identifier) @namespace.name) @namespace.def
                (class_specifier name: (type_identifier) @class.name) @class.def
                (struct_specifier name: (type_identifier) @struct.name) @struct.def
                (enum_specifier name: (type_identifier) @enum.name) @enum.def
                (union_specifier name: (type_identifier) @union.name) @union.def
                (preproc_include) @import
                (function_definition declarator: (function_declarator declarator: (identifier) @function.name)) @function.def
                (function_definition declarator: (function_declarator declarator: (field_identifier) @method.name)) @method.def
                (function_definition declarator: (function_declarator declarator: (destructor_name) @method.name)) @method.def
                ((function_definition declarator: (function_declarator declarator: (qualified_identifier name: (_) @method.name))) @method.def
                    (#not-match? @method.name "::"))
                ((function_definition declarator: (function_declarator declarator: (qualified_identifier name: (qualified_identifier name: (_) @method.name)))) @method.def
                    (#not-match? @method.name "::"))
                ((function_definition declarator: (function_declarator declarator: (qualified_identifier name: (qualified_identifier name: (qualified_identifier name: (_) @method.name))))) @method.def
                    (#not-match? @method.name "::"))
                (field_declaration declarator: (function_declarator declarator: (field_identifier) @method.name)) @method.def
                (declaration declarator: (function_declarator declarator: (destructor_name) @method.name)) @method.def
                (declaration (function_declarator declarator: (identifier) @prototype.name)) @prototype.def
                (declaration (init_declarator declarator: (identifier) @variable.name)) @variable.def
            "#,
            ref_query: r#"
                (identifier) @ref
                (type_identifier) @ref
                (field_identifier) @ref
                (namespace_identifier) @ref
            "#,
            decorator_wrappers: &[],
        },
        LanguageSpec {
            id: "ruby",
            manifests: &["Gemfile"],
            extensions: &["rb"],
            comment_prefixes: &["#"],
            language: || tree_sitter_ruby::LANGUAGE.into(),
            symbol_query: r#"
                ((call method: (identifier) @import.name arguments: (argument_list (_) @import.arg)) @import
                    (#match? @import.name "^(require|require_relative|load|include|extend|prepend|autoload)$"))
                (class name: (_) @class.name) @class.def
                (module name: (_) @module.name) @module.def
                (method name: (identifier) @method.name) @method.def
                (singleton_method name: (identifier) @method.name) @method.def
                (assignment left: (identifier) @variable.name) @variable.def
            "#,
            ref_query: r#"
                (identifier) @ref
                (constant) @ref
            "#,
            decorator_wrappers: &[],
        },
        LanguageSpec {
            id: "php",
            manifests: &["composer.json", "composer.lock"],
            extensions: &["php", "phtml", "php3", "php4", "php5", "phps"],
            comment_prefixes: &["//", "#", "/*", "*"],
            language: || tree_sitter_php::LANGUAGE_PHP.into(),
            symbol_query: r#"
                (namespace_use_declaration) @import
                (namespace_definition name: (namespace_name) @namespace.name) @namespace.def
                (class_declaration name: (name) @class.name) @class.def
                (interface_declaration name: (name) @interface.name) @interface.def
                (trait_declaration name: (name) @trait.name) @trait.def
                (enum_declaration name: (name) @enum.name) @enum.def
                (function_definition name: (name) @function.name) @function.def
                (method_declaration name: (name) @method.name) @method.def
                (property_declaration (property_element name: (variable_name (name) @property.name))) @property.def
                (property_promotion_parameter name: (variable_name (name) @property.name)) @property.def
                (const_declaration (const_element (name) @constant.name)) @constant.def
                (enum_case name: (name) @variable.name) @variable.def
            "#,
            ref_query: r#"
                (name) @ref
                (variable_name) @ref
            "#,
            decorator_wrappers: &[],
        },
        LanguageSpec {
            id: "bash",
            manifests: &[".bashrc"],
            extensions: &["sh", "bash"],
            comment_prefixes: &["#"],
            language: || tree_sitter_bash::LANGUAGE.into(),
            symbol_query: r#"
                (function_definition name: (word) @function.name) @function.def
                (variable_assignment name: (variable_name) @variable.name) @variable.def
            "#,
            ref_query: r#"
                (variable_name) @ref
                (command_name) @ref
                (word) @ref
            "#,
            decorator_wrappers: &[],
        },
        LanguageSpec {
            id: "sql",
            manifests: &[],
            extensions: &["sql"],
            comment_prefixes: &["--", "/*", "*"],
            language: || tree_sitter_sequel::LANGUAGE.into(),
            symbol_query: r#"
                (create_database (keyword_database) . (keyword_if)? . (keyword_not)? . (keyword_exists)? . (identifier) @database.name) @database.def
                (create_schema (keyword_schema) . (keyword_if)? . (keyword_not)? . (keyword_exists)? . (keyword_authorization)? . (identifier) @schema.name) @schema.def
                (create_table (keyword_table) . (keyword_if)? . (keyword_not)? . (keyword_exists)? . (object_reference name: (identifier) @table.name)) @table.def
                (column_definition name: [(identifier) (literal)] @column.name) @column.def
                (create_view (object_reference name: (identifier) @view.name)) @view.def
                (create_materialized_view (object_reference name: (identifier) @view.name)) @view.def
                (create_index column: [(identifier) (literal)] @index.name) @index.def
                (create_function (keyword_function) . (object_reference name: (identifier) @function.name)) @function.def
                (create_sequence (keyword_sequence) . (keyword_if)? . (keyword_not)? . (keyword_exists)? . (object_reference name: (identifier) @sequence.name)) @sequence.def
                (create_type (keyword_type) . (object_reference name: (identifier) @type.name)) @type.def
                (create_trigger (object_reference name: (identifier) @trigger.name) . [(keyword_before) (keyword_after) (keyword_instead)]) @trigger.def
                (cte . (identifier) @cte.name) @cte.def
            "#,
            ref_query: r#"
                (identifier) @ref
                (column_definition name: (literal) @ref)
                (create_index column: (literal) @ref)
                (field column: (literal) @ref)
                (column (literal) @ref)
            "#,
            decorator_wrappers: &[],
        },
        LanguageSpec {
            id: "make",
            manifests: &["Makefile", "makefile", "GNUmakefile"],
            extensions: &["mk", "mak"],
            comment_prefixes: &["#"],
            language: || tree_sitter_make::LANGUAGE.into(),
            symbol_query: r#"
                (include_directive) @import
                (rule (targets (word) @target.name)) @target.def
                (variable_assignment name: (word) @variable.name) @variable.def
            "#,
            ref_query: r#"
                (word) @ref
            "#,
            decorator_wrappers: &[],
        },
        LanguageSpec {
            id: "json",
            manifests: &[],
            extensions: &["json"],
            comment_prefixes: &[],
            language: || tree_sitter_json::LANGUAGE.into(),
            symbol_query: r#"
                (pair key: (string (string_content) @key.name)) @key.def
            "#,
            ref_query: r#"
                (pair key: (string (string_content) @ref))
            "#,
            decorator_wrappers: &[],
        },
        LanguageSpec {
            id: "yaml",
            manifests: &[],
            extensions: &["yaml", "yml"],
            comment_prefixes: &["#"],
            language: || tree_sitter_yaml::LANGUAGE.into(),
            symbol_query: r#"
                (block_mapping_pair key: (flow_node (plain_scalar (string_scalar) @key.name))) @key.def
                (block_mapping_pair key: (flow_node (single_quote_scalar) @key.name)) @key.def
                (block_mapping_pair key: (flow_node (double_quote_scalar) @key.name)) @key.def
                (flow_pair key: (flow_node (plain_scalar (string_scalar) @key.name))) @key.def
                (flow_pair key: (flow_node (single_quote_scalar) @key.name)) @key.def
                (flow_pair key: (flow_node (double_quote_scalar) @key.name)) @key.def
                (anchor (anchor_name) @anchor.name) @anchor.def
            "#,
            ref_query: r#"
                (string_scalar) @ref
                (anchor_name) @ref
                (alias_name) @ref
                (tag) @ref
            "#,
            decorator_wrappers: &[],
        },
        LanguageSpec {
            id: "markdown",
            manifests: &[],
            extensions: &["md", "markdown"],
            comment_prefixes: &[],
            // tree-sitter-md's block grammar; headings live in the block tree.
            language: || tree_sitter_md_025::LANGUAGE.into(),
            // Index headings as symbols. ATX headings (`# Title`) carry their
            // text in a child `inline` node; setext headings (text underlined
            // with `===`/`---`) carry it in a nested `paragraph (inline)`.
            symbol_query: r#"
                (atx_heading (inline) @heading.name) @heading.def
                (setext_heading (paragraph (inline) @heading.name)) @heading.def
            "#,
            // Markdown has no symbol references to resolve.
            ref_query: "",
            decorator_wrappers: &[],
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compiled_queries_are_cached_and_shared_across_spec_instances() {
        // Two independent spec instances for the same language (the build path
        // clones a spec per file) must resolve to the SAME compiled query, so a
        // query is compiled at most once per language rather than once per file.
        let first = lookup_language("rust").unwrap();
        let second = lookup_language("rust").unwrap();

        let q1 = first.symbol_query().unwrap();
        let q2 = first.symbol_query().unwrap();
        let q3 = second.symbol_query().unwrap();
        assert!(
            Arc::ptr_eq(&q1, &q2),
            "repeat calls must return the cached Arc"
        );
        assert!(
            Arc::ptr_eq(&q1, &q3),
            "separate spec instances must share one cached query"
        );

        let r1 = first.ref_query().unwrap();
        let r2 = second.ref_query().unwrap();
        assert!(Arc::ptr_eq(&r1, &r2), "ref queries must be cached too");
    }

    #[test]
    fn empty_ref_query_compiles_and_caches() {
        // Markdown declares an empty ref_query (""); it must still compile to a
        // valid (zero-pattern) cached query rather than erroring.
        let spec = lookup_language("markdown").unwrap();
        let q1 = spec.ref_query().unwrap();
        let q2 = spec.ref_query().unwrap();
        assert!(Arc::ptr_eq(&q1, &q2));
    }
}

#[cfg(test)]
mod swift_smoke {
    use super::*;
    use std::path::Path;
    use streaming_iterator::StreamingIterator;
    use tree_sitter::QueryCursor;

    fn captures(query_for: fn(&LanguageSpec) -> Result<Arc<Query>>, src: &[u8]) -> Vec<String> {
        let spec = lookup_language("swift").unwrap();
        let query = query_for(&spec).unwrap();
        let mut parser = spec.parser().unwrap();
        let tree = parser.parse(src, None).unwrap();
        let mut cursor = QueryCursor::new();
        let mut values = Vec::new();
        let mut matches = cursor.matches(&query, tree.root_node(), src);
        while let Some(query_match) = matches.next() {
            for capture in query_match.captures {
                values.push(capture.node.utf8_text(src).unwrap().to_string());
            }
        }
        values
    }

    #[test]
    fn registered_for_swift_packages_and_source_files() {
        let spec = lookup_language("swift").unwrap();
        assert_eq!(spec.manifests, &["Package.swift"]);
        assert_eq!(spec.extensions, &["swift"]);
        assert_eq!(languages_for_manifest("Package.swift"), vec!["swift"]);
        assert_eq!(
            detect_language_from_file(Path::new("/repo"), Path::new("/repo/Sources/App.swift"))
                .unwrap(),
            Some("swift")
        );
    }

    #[test]
    fn captures_swift_symbols_and_references() {
        let src = br#"import Foundation

typealias OrderId = String

protocol OrderRepository {
    func find(id: OrderId) -> OrderId
}

struct Order {
    let id: OrderId
}

enum OrderStatus {
    case pending
}

final class OrderService {
    let repository: OrderRepository

    func submit(id: OrderId) -> Order {
        repository.find(id: id)
        return Order(id: id)
    }
}
"#;

        let symbols = captures(LanguageSpec::symbol_query, src);
        for expected in [
            "OrderId",
            "OrderRepository",
            "Order",
            "OrderStatus",
            "OrderService",
            "find",
            "submit",
            "repository",
        ] {
            assert!(
                symbols.iter().any(|value| value == expected),
                "missing {expected}: {symbols:?}"
            );
        }

        let references = captures(LanguageSpec::ref_query, src);
        assert!(
            references
                .iter()
                .filter(|value| value.as_str() == "OrderId")
                .count()
                >= 3,
            "missing OrderId references: {references:?}"
        );
    }
}

#[cfg(test)]
mod html_smoke {
    use super::*;
    use std::path::Path;
    use streaming_iterator::StreamingIterator;
    use tree_sitter::QueryCursor;

    /// Parse `src` with the html grammar and return every `(capture, text)`
    /// pair whose capture name ends in `suffix` (".name" for symbol names,
    /// "ref" for references).
    fn captures(
        query_for: fn(&LanguageSpec) -> Result<Arc<Query>>,
        src: &[u8],
    ) -> Vec<(String, String)> {
        let spec = lookup_language("html").unwrap();
        let q = query_for(&spec).unwrap();
        let mut parser = spec.parser().unwrap();
        let tree = parser.parse(src, None).unwrap();
        let names = q.capture_names();
        let mut cur = QueryCursor::new();
        let mut got = Vec::new();
        let mut m = cur.matches(&q, tree.root_node(), src);
        while let Some(mm) = m.next() {
            for c in mm.captures {
                let n = names[c.index as usize];
                got.push((n.to_string(), c.node.utf8_text(src).unwrap().to_string()));
            }
        }
        got
    }

    fn symbol_names(src: &[u8]) -> Vec<(String, String)> {
        captures(LanguageSpec::symbol_query, src)
            .into_iter()
            .filter(|(n, _)| n.ends_with(".name"))
            .collect()
    }

    #[test]
    fn registered_with_html_extensions() {
        let spec = lookup_language("html").unwrap();
        assert_eq!(spec.extensions, &["html", "htm"]);
        let root = Path::new("/repo");
        assert_eq!(
            detect_language_from_file(root, Path::new("/repo/index.html")).unwrap(),
            Some("html")
        );
        assert_eq!(
            detect_language_from_file(root, Path::new("/repo/page.htm")).unwrap(),
            Some("html")
        );
    }

    #[test]
    fn id_attribute_captured_classes_ignored() {
        let got = symbol_names(br#"<div id="main" class="x"><span class="y">hi</span></div>"#);
        assert!(got.iter().any(|(k, v)| k == "id.name" && v == "main"));
        assert!(
            !got.iter().any(|(_, v)| v == "x" || v == "y"),
            "class leaked: {got:?}"
        );
    }

    #[test]
    fn script_and_style_tags_captured() {
        let got = symbol_names(br#"<script>1</script><style>a{}</style>"#);
        let tags: Vec<_> = got
            .iter()
            .filter(|(k, _)| k == "tag.name")
            .map(|(_, v)| v.as_str())
            .collect();
        assert!(tags.contains(&"script"), "missing script: {got:?}");
        assert!(tags.contains(&"style"), "missing style: {got:?}");
    }

    #[test]
    fn nested_and_multiple_ids_all_captured() {
        let got = symbol_names(br#"<div id="outer"><p id="inner">x</p></div><a id="link">y</a>"#);
        let ids: Vec<_> = got
            .iter()
            .filter(|(k, _)| k == "id.name")
            .map(|(_, v)| v.as_str())
            .collect();
        assert!(
            ids.contains(&"outer") && ids.contains(&"inner") && ids.contains(&"link"),
            "{got:?}"
        );
    }

    #[test]
    fn ref_query_captures_attribute_and_tag_values() {
        let refs: Vec<_> = captures(LanguageSpec::ref_query, br#"<div id="main">hi</div>"#)
            .into_iter()
            .map(|(_, v)| v)
            .collect();
        // tag name "div" and the id value "main" both resolve as references.
        assert!(refs.contains(&"div".to_string()), "{refs:?}");
        assert!(refs.contains(&"main".to_string()), "{refs:?}");
    }
}

#[cfg(test)]
mod query_fixtures {
    use super::*;
    use std::path::Path;
    use streaming_iterator::StreamingIterator;
    use tree_sitter::QueryCursor;

    /// `(capture_name, text)` pairs for `src` parsed with `lang`.
    fn captures(
        lang: &str,
        query_for: fn(&LanguageSpec) -> Result<Arc<Query>>,
        src: &[u8],
    ) -> Vec<(String, String)> {
        let spec = lookup_language(lang).unwrap();
        let query = query_for(&spec).unwrap();
        let mut parser = spec.parser().unwrap();
        let tree = parser.parse(src, None).unwrap();
        let names = query.capture_names();
        let mut cursor = QueryCursor::new();
        let mut got = Vec::new();
        let mut matches = cursor.matches(&query, tree.root_node(), src);
        while let Some(m) = matches.next() {
            for c in m.captures {
                got.push((
                    names[c.index as usize].to_string(),
                    c.node.utf8_text(src).unwrap().to_string(),
                ));
            }
        }
        got
    }

    fn names(lang: &str, src: &[u8]) -> Vec<(String, String)> {
        captures(lang, LanguageSpec::symbol_query, src)
            .into_iter()
            .filter(|(n, _)| n.ends_with(".name") || n == "import")
            .collect()
    }

    fn has(got: &[(String, String)], capture: &str, text: &str) -> bool {
        got.iter().any(|(n, t)| n == capture && t == text)
    }

    #[test]
    fn captures_cpp_methods_ctors_and_out_of_class_definitions() {
        let src = br#"#include <string>
enum Color { Red };
union Bits { int i; float f; };
class Shape {
public:
    Shape();
    ~Shape();
    virtual double area() const;
    int inlined() { return 1; }
};
Shape::Shape() {}
Shape::~Shape() {}
double Shape::area() const { return 0.0; }
int free_fn(int x);
int free_fn(int x) { return x; }
"#;
        let got = names("cpp", src);
        assert!(has(&got, "import", "#include <string>\n"), "{got:?}");
        assert!(has(&got, "enum.name", "Color"), "{got:?}");
        assert!(has(&got, "union.name", "Bits"), "{got:?}");
        assert!(has(&got, "class.name", "Shape"), "{got:?}");
        assert!(has(&got, "method.name", "area"), "{got:?}");
        assert!(has(&got, "method.name", "inlined"), "{got:?}");
        assert!(has(&got, "method.name", "~Shape"), "{got:?}");
        assert!(has(&got, "method.name", "Shape"), "{got:?}");
        assert!(has(&got, "prototype.name", "free_fn"), "{got:?}");
        assert!(has(&got, "function.name", "free_fn"), "{got:?}");
        // In-class declaration + out-of-class definition: two method rows.
        assert_eq!(
            got.iter()
                .filter(|(n, t)| n == "method.name" && t == "area")
                .count(),
            2,
            "{got:?}"
        );
    }

    #[test]
    fn c_prototype_is_not_a_function_symbol() {
        let src = b"int add(int a, int b);\nint add(int a, int b) { return a + b; }\n";
        let got = names("c", src);
        assert!(has(&got, "prototype.name", "add"), "{got:?}");
        assert_eq!(
            got.iter()
                .filter(|(n, t)| n == "function.name" && t == "add")
                .count(),
            1,
            "{got:?}"
        );
    }

    #[test]
    fn ruby_captures_constant_refs_and_only_real_imports() {
        let src = br#"require "json"
class OrderService
  attr_reader :items
  include Billing
  def submit
    OrderService.new(:fast)
  end
end
"#;
        let symbols = names("ruby", src);
        let imports: Vec<&str> = symbols
            .iter()
            .filter(|(n, _)| n == "import")
            .map(|(_, t)| t.as_str())
            .collect();
        assert_eq!(
            imports,
            vec!["require \"json\"", "include Billing"],
            "{symbols:?}"
        );
        let refs = captures("ruby", LanguageSpec::ref_query, src);
        assert!(has(&refs, "ref", "OrderService"), "{refs:?}");
        assert!(has(&refs, "ref", "Billing"), "{refs:?}");
        // Symbol literals are not captured: `simple_symbol` is a leaf token so
        // the ref would be stored as `:items`, unreachable by identifier.
        assert!(!has(&refs, "ref", ":items"), "{refs:?}");
        assert!(!has(&refs, "ref", ":fast"), "{refs:?}");
    }

    #[test]
    fn typescript_captures_abstract_class_namespace_generator() {
        let src =
            b"abstract class Base {}\nnamespace N { export const x = 1; }\nfunction* gen() {}\n";
        for lang in ["typescript", "tsx"] {
            let got = names(lang, src);
            assert!(has(&got, "class.name", "Base"), "{lang}: {got:?}");
            assert!(has(&got, "namespace.name", "N"), "{lang}: {got:?}");
            assert!(has(&got, "function.name", "gen"), "{lang}: {got:?}");
        }
        let js = names("javascript", b"function* gen() {}\n");
        assert!(has(&js, "function.name", "gen"), "{js:?}");
    }

    #[test]
    fn java_captures_record_and_constructor() {
        let src = b"record Point(int x, int y) {}\nclass Rec { Rec() {} void m() {} }\n";
        let got = names("java", src);
        assert!(has(&got, "class.name", "Point"), "{got:?}");
        assert!(has(&got, "class.name", "Rec"), "{got:?}");
        assert!(has(&got, "method.name", "Rec"), "{got:?}");
        assert!(has(&got, "method.name", "m"), "{got:?}");
    }

    #[test]
    fn extension_match_is_case_insensitive() {
        let root = Path::new("/repo");
        assert_eq!(
            detect_language_from_file(root, Path::new("/repo/Upper.PY")).unwrap(),
            Some("python")
        );
        assert_eq!(
            detect_language_from_file(root, Path::new("/repo/Main.Java")).unwrap(),
            Some("java")
        );
    }

    #[test]
    fn shebang_detects_node_dash_ksh() {
        assert_eq!(
            language_for_shebang_line(b"#!/usr/bin/env node"),
            Some("javascript")
        );
        assert_eq!(language_for_shebang_line(b"#!/bin/dash"), Some("bash"));
        assert_eq!(language_for_shebang_line(b"#!/bin/ksh"), Some("bash"));
        assert_eq!(language_for_shebang_line(b"#!/usr/bin/perl"), None);
    }
}
