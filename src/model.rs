use serde::Serialize;
use serde::ser::{SerializeSeq, Serializer};

fn is_false(value: &bool) -> bool {
    !*value
}

fn is_zero(value: &usize) -> bool {
    *value == 0
}

/// Build the truncation warning for a paginated response that has more rows
/// after this page. Agents reliably ignore a bare `truncated: true` flag and
/// treat a partial page as the complete answer; this makes the partial nature
/// explicit and tells the agent exactly how to get the rest. `counts_hint`
/// names the counts_only-capable tool when one exists (so the agent knows a
/// cheaper exact path); omit it for tools without counts_only. `total` may be
/// unknown (e.g. a streamed query) — then the warning omits the "N of M"
/// framing and just says more results exist.
///
/// Fires only when `next_offset` is set, i.e. when there really is a next
/// page. A `returned < total` test is wrong for continuation pages: the final
/// page of a paginated walk legitimately returns fewer than `total` rows while
/// `truncated` is already false, and warning there sent agents looping.
pub(crate) fn truncation_warning(
    returned: usize,
    total: Option<usize>,
    next_offset: Option<usize>,
    counts_hint: Option<&str>,
) -> Option<String> {
    let next_offset = next_offset?;
    let extent = match total {
        Some(total) => {
            format!("this page holds {returned} of {total} total — a sample, NOT the full set")
        }
        None => "there are MORE results beyond this page".to_string(),
    };
    let counts = counts_hint
        .map(|tool| {
            format!("; for an exact count without rows, use {tool} with `counts_only: true`")
        })
        .unwrap_or_default();
    Some(format!(
        "PARTIAL RESULT: {extent}. \
         Any count, ranking, or exhaustive list derived from it is incomplete. \
         To get the full set, resume at `offset: {next_offset}` and repeat until \
         `truncated` is false{counts}."
    ))
}

#[derive(Debug, Clone, Serialize)]
pub struct RangePoint(pub usize, pub usize);

#[derive(Debug, Clone)]
pub struct SourceRange {
    pub start: RangePoint,
    pub end: RangePoint,
}

// Serialize as a flat `[start_row, start_col, end_row, end_col]` array rather
// than `{"start":[r,c],"end":[r,c]}`. Ranges appear on every symbol/ref/capture,
// so the flat form meaningfully shrinks tool output (see issue #38) while
// carrying the same four numbers. The struct keeps its named fields for
// in-process use; only the wire form changes.
impl Serialize for SourceRange {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // Rows are 1-based on the wire so `[row, col, ...]` reads as an
        // editor/`file:line` location (matching rg, grep, and GitHub). They
        // are stored 0-based internally (tree-sitter convention); the +1 is
        // applied only here, at the serialization boundary. Columns stay
        // 0-based. `enclosing_symbol` undoes the +1 on its row input so
        // round-trips stay consistent.
        let mut seq = serializer.serialize_seq(Some(4))?;
        seq.serialize_element(&(self.start.0 + 1))?;
        seq.serialize_element(&self.start.1)?;
        seq.serialize_element(&(self.end.0 + 1))?;
        seq.serialize_element(&self.end.1)?;
        seq.end()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct LanguageBreakdown {
    pub language: String,
    pub files: usize,
    pub confidence: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SymbolMatch {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub repo: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub file: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub language: String,
    pub kind: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub qualified: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    pub range: SourceRange,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub docstring: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
    #[serde(skip_serializing_if = "is_false", default)]
    pub stale: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body_unavailable: Option<String>,
    #[serde(skip_serializing_if = "is_false", default)]
    pub partial: bool,
}

#[derive(Debug, Clone)]
pub struct GetSymbolResponse {
    pub matches: Vec<SymbolMatch>,
    pub truncated: bool,
    pub total: usize,
    pub next_offset: Option<usize>,
    pub truncated_names: Vec<String>,
}

/// Return the value shared by every element of `pick`, or `None` if the slice
/// is empty or the values differ. Used to hoist a repo/language that's uniform
/// across matches up to the response level so it's serialized once, not N times.
fn common_value(matches: &[SymbolMatch], pick: impl Fn(&SymbolMatch) -> &str) -> Option<String> {
    let first = pick(matches.first()?);
    matches
        .iter()
        .all(|m| pick(m) == first)
        .then(|| first.to_string())
}

#[derive(Serialize)]
struct WireSymbolMatch<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    repo: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    file: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    language: Option<&'a str>,
    kind: &'a str,
    name: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    qualified: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    signature: Option<&'a str>,
    range: &'a SourceRange,
    #[serde(skip_serializing_if = "Option::is_none")]
    docstring: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    body: Option<&'a str>,
    #[serde(skip_serializing_if = "is_false")]
    stale: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    body_unavailable: Option<&'a str>,
    #[serde(skip_serializing_if = "is_false")]
    partial: bool,
}

fn non_empty_str(value: &str) -> Option<&str> {
    (!value.is_empty()).then_some(value)
}

/// Borrow a `SymbolMatch` for serialization and omit fields hoisted to the
/// response level without cloning potentially large `body`/`docstring` strings.
fn wire_symbol_match(
    m: &SymbolMatch,
    drop_repo: bool,
    drop_language: bool,
    drop_file: bool,
) -> WireSymbolMatch<'_> {
    WireSymbolMatch {
        repo: (!drop_repo).then(|| non_empty_str(&m.repo)).flatten(),
        file: (!drop_file).then(|| non_empty_str(&m.file)).flatten(),
        language: (!drop_language)
            .then(|| non_empty_str(&m.language))
            .flatten(),
        kind: &m.kind,
        name: &m.name,
        qualified: m.qualified.as_deref(),
        signature: m.signature.as_deref(),
        range: &m.range,
        docstring: m.docstring.as_deref(),
        body: m.body.as_deref(),
        stale: m.stale,
        body_unavailable: m.body_unavailable.as_deref(),
        partial: m.partial,
    }
}

impl Serialize for GetSymbolResponse {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let repo = common_value(&self.matches, |m| &m.repo);
        let language = common_value(&self.matches, |m| &m.language);

        #[derive(Serialize)]
        struct Wire<'a> {
            #[serde(skip_serializing_if = "Option::is_none")]
            repo: Option<&'a str>,
            #[serde(skip_serializing_if = "Option::is_none")]
            language: Option<&'a str>,
            matches: Vec<WireSymbolMatch<'a>>,
            truncated: bool,
            total: usize,
            #[serde(skip_serializing_if = "Option::is_none")]
            next_offset: Option<usize>,
            #[serde(skip_serializing_if = "Option::is_none")]
            warning: Option<String>,
            #[serde(skip_serializing_if = "Vec::is_empty")]
            truncated_names: &'a Vec<String>,
        }

        Wire {
            repo: repo.as_deref(),
            language: language.as_deref(),
            matches: self
                .matches
                .iter()
                .map(|m| wire_symbol_match(m, repo.is_some(), language.is_some(), false))
                .collect(),
            truncated: self.truncated,
            total: self.total,
            next_offset: self.next_offset,
            warning: truncation_warning(
                self.matches.len(),
                Some(self.total),
                self.next_offset,
                None,
            ),
            truncated_names: &self.truncated_names,
        }
        .serialize(serializer)
    }
}

#[derive(Debug, Clone)]
pub struct EnclosingSymbolResponse {
    /// Repo containing the file. Empty when no enclosing symbol was
    /// found (the file may still exist in the index — the position
    /// just sits outside any captured symbol's range).
    pub repo: String,
    /// The repository-relative file path that was queried.
    pub file: String,
    /// Symbols enclosing the requested position, ordered outermost →
    /// innermost. Empty when no symbol's range contains the point.
    /// For a batch (`rows`) lookup this is empty; see `results` instead.
    pub matches: Vec<SymbolMatch>,
    /// One entry per requested row when several `rows` were looked up in one
    /// call. Empty (and omitted) for a single-position lookup.
    pub results: Vec<EnclosingHit>,
    /// Total number of `rows` requested in the batch, before pagination. 0
    /// for a single-position lookup. Lets a caller tell how many rows remain
    /// past the returned window.
    pub total: usize,
    /// True when pagination cut the batch short: some requested `rows` were
    /// not resolved in this page. Follow `next_offset` to resume. Distinct
    /// from `partial` (which signals stale/parse-recovery content, not
    /// pagination).
    pub truncated: bool,
    /// Offset into the requested `rows` list to resume from when `truncated`
    /// is true. Absent when the batch was fully resolved.
    pub next_offset: Option<usize>,
}

impl Serialize for EnclosingSymbolResponse {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // repo and file are always uniform for one lookup (one file, one repo),
        // so they live at the top level here. That makes the per-match repo/file
        // copies pure duplication — drop them — and the language is uniform too,
        // so hoist it alongside.
        let language = self
            .matches
            .iter()
            .chain(self.results.iter().flat_map(|hit| hit.matches.iter()))
            .map(|m| m.language.as_str())
            .find(|l| !l.is_empty())
            .map(str::to_string);

        #[derive(Serialize)]
        struct Wire<'a> {
            repo: &'a str,
            file: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            language: Option<&'a str>,
            #[serde(skip_serializing_if = "Vec::is_empty")]
            matches: Vec<WireSymbolMatch<'a>>,
            #[serde(skip_serializing_if = "Vec::is_empty")]
            results: Vec<WireEnclosingHit<'a>>,
            #[serde(skip_serializing_if = "is_zero")]
            total: usize,
            #[serde(skip_serializing_if = "is_false")]
            truncated: bool,
            #[serde(skip_serializing_if = "Option::is_none")]
            next_offset: Option<usize>,
            #[serde(skip_serializing_if = "Option::is_none")]
            warning: Option<String>,
        }

        #[derive(Serialize)]
        struct WireEnclosingHit<'a> {
            row: usize,
            matches: Vec<WireSymbolMatch<'a>>,
        }

        Wire {
            repo: &self.repo,
            file: &self.file,
            language: language.as_deref(),
            matches: self
                .matches
                .iter()
                .map(|m| wire_symbol_match(m, true, true, true))
                .collect(),
            results: self
                .results
                .iter()
                .map(|hit| WireEnclosingHit {
                    row: hit.row,
                    matches: hit
                        .matches
                        .iter()
                        .map(|m| wire_symbol_match(m, true, true, true))
                        .collect(),
                })
                .collect(),
            total: self.total,
            truncated: self.truncated,
            next_offset: self.next_offset,
            warning: truncation_warning(
                self.results.len(),
                Some(self.total),
                self.next_offset,
                None,
            ),
        }
        .serialize(serializer)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct EnclosingHit {
    /// The 1-based row this entry answers (echoed from the request).
    pub row: usize,
    /// Symbols enclosing that row, ordered outermost → innermost.
    pub matches: Vec<SymbolMatch>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReplaceSymbolResponse {
    pub repo: String,
    pub file: String,
    pub name: String,
    pub kind: String,
    /// Range the symbol occupied before the edit.
    pub old_range: SourceRange,
    /// Range the symbol occupies after the edit, re-derived by parsing
    /// the written file. Omitted when the written file no longer contains a
    /// symbol matching the request (e.g. an empty `new_body` deleted it, or
    /// the replacement renamed it).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_range: Option<SourceRange>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OutlineSymbol {
    pub kind: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub docstring: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub range: Option<SourceRange>,
    /// Full source body, populated only for symbols whose name was passed in
    /// `include_bodies_for` — lets one outline call also fetch selected bodies.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
    /// Present only when this symbol's body was requested via
    /// `include_bodies_for` but could not be read (stale index, missing or
    /// unreadable source). The outline itself is still complete.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body_unavailable: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub children: Vec<OutlineSymbol>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FileOutlineResponse {
    pub repo: String,
    pub file: String,
    pub language: String,
    pub symbols: Vec<OutlineSymbol>,
    pub total: usize,
    /// Pagination signal: the top-level symbol list was cut short. Follow
    /// `next_offset` to resume. Orthogonal to `partial` — both can be true.
    pub truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_offset: Option<usize>,
    /// Content-omission signal: some content (e.g. a stale symbol body, or a
    /// parse-recovered file) was omitted or degraded. NOT pagination — never
    /// means "page me". Orthogonal to `truncated`; both can be true.
    #[serde(skip_serializing_if = "is_false", default)]
    pub partial: bool,
    /// Loud truncation warning, set when `truncated` is true. Agents ignore a
    /// bare flag and treat a partial page as complete; this states the partial
    /// nature explicitly and how to resume. Omitted when nothing was truncated.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub warning: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReferenceMatch {
    /// Which queried name this reference is to. Set only when several names
    /// were requested in one call, so single-name responses are unchanged.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub range: SourceRange,
    pub context: String,
    pub snippet: String,
    /// Row the FIRST line of `snippet` sits on. Stored 0-based like every other
    /// row here; the +1 to 1-based is applied at the serialization boundary, so
    /// on the wire it shares `range`'s convention and
    /// `range[0] - snippet_start_row` indexes the reference's line within
    /// `snippet`. Present only for multi-line snippets (`snippet_lines > 1`),
    /// where `range` alone does not say which snippet line is the reference.
    /// Omitted when `snippet` is a single line — then `range` is that line.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snippet_start_row: Option<usize>,
    /// Present only when `snippet` could not be produced (the live source no
    /// longer matches the indexed SHA, so the recorded range may point at the
    /// wrong text). Carries a short actionable reason; the caller should
    /// `find_references` again after `tsindex build` rather than trust an empty
    /// snippet. Omitted when a snippet was produced.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snippet_unavailable: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReferenceGroup {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub repo: String,
    pub file: String,
    pub refs: Vec<ReferenceMatch>,
}

/// One file's reference count in a `counts_only` aggregate response. Carries
/// no per-reference rows — just how many references land in this file.
#[derive(Debug, Clone, Serialize)]
pub struct FileRefCount {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub repo: String,
    pub file: String,
    pub count: usize,
}

/// Aggregate answer to a `counts_only` `find_references` call: exact totals and
/// per-file counts over the FULL filtered set, computed in one pass with no
/// pagination, no snippets, and no row-level detail. This is the cheap, exact
/// way to answer "how many references / which files use X" — a fraction of the
/// row-level payload.
#[derive(Debug, Clone, Serialize)]
pub struct RefCountsResponse {
    /// Total references across all requested names, post-filter. Always exact
    /// (computed over the full set, independent of the `files` array cap).
    pub total: usize,
    /// Distinct (repo, file) pairs that contain at least one reference. Always
    /// exact, like `total`.
    pub total_files: usize,
    /// Per-file counts, sorted by count descending then file ascending so the
    /// top users surface first. Bounded by the server's serialized-byte budget:
    /// when the full list won't fit, the top `files_returned` are kept (already
    /// the most-referenced files) and `files_truncated` is set.
    pub files: Vec<FileRefCount>,
    /// Number of entries in `files` after the byte-budget cap. Equals
    /// `files.len()`; present so a caller can tell the kept window from
    /// `total_files` without counting.
    pub files_returned: usize,
    /// True when the `files` array was capped by the byte budget and does NOT
    /// cover all `total_files` files. `total`/`total_files`/`names` remain
    /// exact regardless. NOT row-level pagination — there is no `next_offset`;
    /// narrow the query (scope, fewer names) to see more files.
    #[serde(skip_serializing_if = "is_false", default)]
    pub files_truncated: bool,
    /// Staleness signal: true when any contributing file was parse-recovered
    /// during indexing (the indexed tree had a syntax error), so the count may
    /// undercount that file. `counts_only` does NOT re-read files to verify
    /// source SHAs (that's what makes it cheap), so this reflects parse
    /// recovery only, not post-index source drift — run `tsindex build` if the
    /// source changed since indexing.
    #[serde(skip_serializing_if = "is_false", default)]
    pub partial: bool,
    /// Per-name totals, present only when several names were requested — lets
    /// the caller attribute the aggregate to each identifier. Sorted by count
    /// descending. Always exact (independent of the `files` cap).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub names: Vec<NameRefCount>,
}

/// One identifier's total in a multi-name `counts_only` response.
#[derive(Debug, Clone, Serialize)]
pub struct NameRefCount {
    pub name: String,
    pub count: usize,
}

#[derive(Debug, Clone)]
pub struct FindReferencesResponse {
    pub total: usize,
    /// Distinct `(repo, file)` pairs across the full result set for ALL
    /// requested names combined, computed pre-pagination. For a multi-name
    /// (`names: [A, B]`) lookup this is the count of files referencing ANY of
    /// the names — not a per-name count. Computed independently of `groups`,
    /// which only covers the returned page when `truncated`.
    pub total_files: usize,
    pub returned: usize,
    /// Pagination signal: the reference list was cut short. Follow
    /// `next_offset` to resume. Orthogonal to `partial` — both can be true.
    pub truncated: bool,
    pub next_offset: Option<usize>,
    /// Content-omission signal: some snippets were omitted (stale index) or a
    /// file was parse-recovered. NOT pagination. Orthogonal to `truncated`;
    /// both can be true.
    pub partial: bool,
    pub groups: Vec<ReferenceGroup>,
    pub refs: Vec<ReferenceRow>,
}

impl Serialize for FindReferencesResponse {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // repo is almost always uniform (single-repo workspace). Hoist it to the
        // top level when every group/row agrees so it's written once instead of
        // once per group and once per row. file legitimately varies, so it stays.
        let repos = self
            .groups
            .iter()
            .map(|g| g.repo.as_str())
            .chain(self.refs.iter().map(|r| r.repo.as_str()));
        let first = repos.clone().next();
        let repo = first
            .filter(|first| repos.clone().all(|r| r == *first))
            .map(str::to_string);
        let drop_repo = repo.is_some();

        #[derive(Serialize)]
        struct Wire<'a> {
            total: usize,
            total_files: usize,
            returned: usize,
            truncated: bool,
            #[serde(skip_serializing_if = "Option::is_none")]
            next_offset: Option<usize>,
            #[serde(skip_serializing_if = "is_false")]
            partial: bool,
            #[serde(skip_serializing_if = "Option::is_none")]
            warning: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            repo: Option<&'a str>,
            #[serde(skip_serializing_if = "Vec::is_empty")]
            groups: Vec<WireReferenceGroup<'a>>,
            #[serde(skip_serializing_if = "Vec::is_empty")]
            refs: Vec<WireReferenceRow<'a>>,
        }

        #[derive(Serialize)]
        struct WireReferenceMatch<'a> {
            #[serde(skip_serializing_if = "Option::is_none")]
            name: Option<&'a str>,
            range: &'a SourceRange,
            context: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            snippet: Option<&'a str>,
            #[serde(skip_serializing_if = "Option::is_none")]
            snippet_start_row: Option<usize>,
            #[serde(skip_serializing_if = "Option::is_none")]
            snippet_unavailable: Option<&'a str>,
        }

        #[derive(Serialize)]
        struct WireReferenceGroup<'a> {
            #[serde(skip_serializing_if = "Option::is_none")]
            repo: Option<&'a str>,
            file: &'a str,
            refs: Vec<WireReferenceMatch<'a>>,
        }

        #[derive(Serialize)]
        struct WireReferenceRow<'a> {
            #[serde(skip_serializing_if = "Option::is_none")]
            name: Option<&'a str>,
            #[serde(skip_serializing_if = "Option::is_none")]
            repo: Option<&'a str>,
            file: &'a str,
            range: &'a SourceRange,
            context: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            snippet: Option<&'a str>,
            #[serde(skip_serializing_if = "Option::is_none")]
            snippet_start_row: Option<usize>,
            #[serde(skip_serializing_if = "Option::is_none")]
            snippet_unavailable: Option<&'a str>,
        }

        Wire {
            total: self.total,
            total_files: self.total_files,
            returned: self.returned,
            truncated: self.truncated,
            next_offset: self.next_offset,
            partial: self.partial,
            warning: truncation_warning(
                self.returned,
                Some(self.total),
                self.next_offset,
                Some("find_references"),
            ),
            repo: repo.as_deref(),
            groups: self
                .groups
                .iter()
                .map(|g| WireReferenceGroup {
                    repo: (!drop_repo).then(|| non_empty_str(&g.repo)).flatten(),
                    file: &g.file,
                    refs: g
                        .refs
                        .iter()
                        .map(|r| WireReferenceMatch {
                            name: r.name.as_deref(),
                            range: &r.range,
                            context: &r.context,
                            snippet: r
                                .snippet_unavailable
                                .is_none()
                                .then_some(r.snippet.as_str()),
                            snippet_start_row: r.snippet_start_row.map(|row| row + 1),
                            snippet_unavailable: r.snippet_unavailable.as_deref(),
                        })
                        .collect(),
                })
                .collect(),
            refs: self
                .refs
                .iter()
                .map(|r| WireReferenceRow {
                    name: r.name.as_deref(),
                    repo: (!drop_repo).then(|| non_empty_str(&r.repo)).flatten(),
                    file: &r.file,
                    range: &r.range,
                    context: &r.context,
                    snippet: r
                        .snippet_unavailable
                        .is_none()
                        .then_some(r.snippet.as_str()),
                    snippet_start_row: r.snippet_start_row.map(|row| row + 1),
                    snippet_unavailable: r.snippet_unavailable.as_deref(),
                })
                .collect(),
        }
        .serialize(serializer)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ReferenceRow {
    /// Which queried name this reference is to. Set only when several names
    /// were requested in one call, so single-name responses are unchanged.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub repo: String,
    pub file: String,
    pub range: SourceRange,
    pub context: String,
    pub snippet: String,
    /// Row the FIRST line of `snippet` sits on. Stored 0-based like every other
    /// row here; the +1 to 1-based is applied at the serialization boundary, so
    /// on the wire it shares `range`'s convention and
    /// `range[0] - snippet_start_row` indexes the reference's line within
    /// `snippet`. Present only for multi-line snippets (`snippet_lines > 1`),
    /// where `range` alone does not say which snippet line is the reference.
    /// Omitted when `snippet` is a single line — then `range` is that line.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snippet_start_row: Option<usize>,
    /// Present only when `snippet` could not be produced (stale/missing source).
    /// See [`ReferenceMatch::snippet_unavailable`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snippet_unavailable: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueryCapture {
    pub repo: String,
    pub name: String,
    pub text: String,
    pub file: String,
    pub range: SourceRange,
    #[serde(skip_serializing_if = "is_false", default)]
    pub text_truncated: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueryResponse {
    pub language: String,
    pub captures: Vec<QueryCapture>,
    pub truncated: bool,
    #[serde(skip_serializing_if = "is_false", default)]
    pub timed_out: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_offset: Option<usize>,
    #[serde(skip_serializing_if = "is_false", default)]
    pub partial: bool,
    /// Loud truncation warning, set when `truncated`/`timed_out` means more
    /// results exist. Agents ignore a bare flag; this states it explicitly.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub warning: Option<String>,
}

#[derive(Debug, Clone)]
pub struct IndexedSymbol {
    pub id: i64,
    pub file_id: i64,
    pub repo: String,
    pub file: String,
    pub language: String,
    pub kind: String,
    pub name: String,
    pub qualified: Option<String>,
    pub start_row: usize,
    pub start_col: usize,
    pub end_row: usize,
    pub end_col: usize,
    pub signature: Option<String>,
    pub docstring: Option<String>,
    pub source_sha: String,
    pub partial: bool,
}

#[derive(Debug, Clone)]
pub struct IndexedRef {
    pub repo: String,
    pub file: String,
    pub start_row: usize,
    pub start_col: usize,
    pub end_row: usize,
    pub end_col: usize,
    pub context: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct RepoLanguageReport {
    pub repo: String,
    pub root: String,
    pub languages: Vec<LanguageBreakdown>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RepoInfo {
    pub name: String,
    pub path: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bare_match() -> SymbolMatch {
        SymbolMatch {
            repo: "r".into(),
            file: "f.rs".into(),
            language: "rust".into(),
            kind: "function".into(),
            name: "f".into(),
            qualified: None,
            signature: None,
            range: SourceRange {
                start: RangePoint(0, 0),
                end: RangePoint(1, 0),
            },
            docstring: None,
            body: None,
            stale: false,
            body_unavailable: None,
            partial: false,
        }
    }

    #[test]
    fn symbol_match_omits_null_optional_fields() {
        // A match with no qualified/signature/docstring/body must not emit
        // those keys at all — emitting `"qualified":null` etc. is pure token
        // bloat that hit every match in get_symbol/enclosing_symbol output.
        let json = serde_json::to_string(&bare_match()).unwrap();
        for key in ["qualified", "signature", "docstring", "body"] {
            assert!(
                !json.contains(&format!("\"{key}\"")),
                "expected `{key}` to be omitted when None, got: {json}"
            );
        }
        // Non-optional fields are still present.
        assert!(json.contains("\"name\":\"f\""));
        assert!(json.contains("\"range\":[1,0,2,0]"));
    }

    #[test]
    fn symbol_match_emits_present_optional_fields() {
        let mut m = bare_match();
        m.signature = Some("fn f()".into());
        let json = serde_json::to_string(&m).unwrap();
        assert!(json.contains("\"signature\":\"fn f()\""));
        // Still omits the ones left None.
        assert!(!json.contains("\"qualified\""));
        assert!(!json.contains("\"docstring\""));
    }

    fn match_in(repo: &str, language: &str, file: &str, name: &str) -> SymbolMatch {
        SymbolMatch {
            repo: repo.into(),
            file: file.into(),
            language: language.into(),
            kind: "function".into(),
            name: name.into(),
            qualified: None,
            signature: None,
            range: SourceRange {
                start: RangePoint(0, 0),
                end: RangePoint(1, 0),
            },
            docstring: None,
            body: None,
            stale: false,
            body_unavailable: None,
            partial: false,
        }
    }

    #[test]
    fn get_symbol_hoists_shared_repo_and_language() {
        // Two matches in the same repo+language, different files: repo/language
        // appear once at the top, not on either match; file stays per-match.
        let response = GetSymbolResponse {
            matches: vec![
                match_in("r", "rust", "a.rs", "f"),
                match_in("r", "rust", "b.rs", "g"),
            ],
            truncated: false,
            total: 2,
            next_offset: None,
            truncated_names: Vec::new(),
        };
        let v: serde_json::Value = serde_json::to_value(&response).unwrap();
        assert_eq!(v["repo"], "r");
        assert_eq!(v["language"], "rust");
        assert_eq!(v["matches"][0]["file"], "a.rs");
        assert_eq!(v["matches"][1]["file"], "b.rs");
        // Hoisted fields are gone from the individual matches.
        assert!(v["matches"][0].get("repo").is_none());
        assert!(v["matches"][0].get("language").is_none());
        // Matches stay attributable by name.
        assert_eq!(v["matches"][0]["name"], "f");
        assert_eq!(v["matches"][1]["name"], "g");
    }

    #[test]
    fn get_symbol_keeps_repo_per_match_when_repos_differ() {
        // Cross-repo results can't hoist: each match keeps its own repo.
        let response = GetSymbolResponse {
            matches: vec![
                match_in("r1", "rust", "a.rs", "f"),
                match_in("r2", "rust", "b.rs", "f"),
            ],
            truncated: false,
            total: 2,
            next_offset: None,
            truncated_names: Vec::new(),
        };
        let v: serde_json::Value = serde_json::to_value(&response).unwrap();
        assert!(v.get("repo").is_none(), "mixed repos must not hoist: {v}");
        assert_eq!(v["matches"][0]["repo"], "r1");
        assert_eq!(v["matches"][1]["repo"], "r2");
        // language is still uniform, so it hoists independently.
        assert_eq!(v["language"], "rust");
        assert!(v["matches"][0].get("language").is_none());
    }

    #[test]
    fn enclosing_drops_redundant_per_match_repo_file_language() {
        let response = EnclosingSymbolResponse {
            repo: "r".into(),
            file: "a.rs".into(),
            matches: vec![match_in("r", "rust", "a.rs", "outer")],
            results: Vec::new(),
            total: 0,
            truncated: false,
            next_offset: None,
        };
        let v: serde_json::Value = serde_json::to_value(&response).unwrap();
        assert_eq!(v["repo"], "r");
        assert_eq!(v["file"], "a.rs");
        assert_eq!(v["language"], "rust");
        let m = &v["matches"][0];
        assert_eq!(m["name"], "outer");
        for redundant in ["repo", "file", "language"] {
            assert!(
                m.get(redundant).is_none(),
                "match should drop {redundant}: {v}"
            );
        }
    }

    #[test]
    fn find_references_hoists_shared_repo_across_groups() {
        let response = FindReferencesResponse {
            total: 2,
            total_files: 2,
            returned: 0,
            truncated: false,
            next_offset: None,
            partial: false,
            groups: vec![
                ReferenceGroup {
                    repo: "r".into(),
                    file: "a.rs".into(),
                    refs: vec![],
                },
                ReferenceGroup {
                    repo: "r".into(),
                    file: "b.rs".into(),
                    refs: vec![],
                },
            ],
            refs: Vec::new(),
        };
        let v: serde_json::Value = serde_json::to_value(&response).unwrap();
        assert_eq!(v["repo"], "r");
        assert!(v["groups"][0].get("repo").is_none());
        assert_eq!(v["groups"][0]["file"], "a.rs");
        assert_eq!(v["groups"][1]["file"], "b.rs");
    }

    #[test]
    fn find_references_response_carries_no_caveat_field() {
        // The static caveat now lives in the find_references tool description,
        // not in every response body.
        let response = FindReferencesResponse {
            total: 0,
            total_files: 0,
            returned: 0,
            truncated: false,
            next_offset: None,
            partial: false,
            groups: Vec::new(),
            refs: Vec::new(),
        };
        let json = serde_json::to_string(&response).unwrap();
        assert!(!json.contains("caveat"), "got: {json}");
    }
}
