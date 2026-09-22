## Code Reading Rules (tsindex)

You have access to tsindex MCP tools for code navigation. Use them as the
primary method for reading and understanding code structure.

ALWAYS pass `repo="<name>"` on every tsindex call. The index spans many repos,
so an unscoped lookup fans out across all of them and returns a huge,
mostly-irrelevant blob that wastes tokens. Scope to the repo you are working in
(the basename of the workspace root). Only omit `repo` if you were explicitly
told to search across repos.

Required workflow (replace `<repo>` with the active repo name):
1. To orient to a file:
   `mcp__tsindex__list_file_outline(file="path", repo="<repo>")`
2. To find a symbol: `mcp__tsindex__get_symbol(name="SymbolName", repo="<repo>")`
   - Use `include_body=false` first to scan matches cheaply.
   - Then use `include_body=true` for the specific match you need.
3. To find callers/dependents:
   `mcp__tsindex__find_references(names=["symbol"], repo="<repo>")`
   - To COUNT occurrences or list which files use a symbol, add
     `counts_only=true`: exact totals and per-file counts, no paging.
   - Pass several `names` to batch a multi-symbol lookup in one call.
4. To find the symbol around a line/column:
   `mcp__tsindex__enclosing_symbol(file="path", row=1, col=0, repo="<repo>")`
   - Pass `rows=[...]` to resolve a whole stack trace in one call.
5. To query code structure:
   `mcp__tsindex__query(language="typescript", query="...", repo="<repo>")`
6. To orient and read selected bodies in one call:
   `mcp__tsindex__list_file_outline(file="path", include_bodies_for=["name"], repo="<repo>")`
7. To rewrite a whole named function/class/method without reading the file
   first: `mcp__tsindex__replace_symbol(file="path", name="fn", new_body="...", repo="<repo>")`
   (it refuses edits that would break parsing).
8. Use `rg` for non-structural searches such as literals, config keys, env
   vars, log messages, and prose; `rg --count-matches` for raw substring
   counts (`rg -c` counts lines, not occurrences).
9. Use full-file reads only after outline or symbol lookup shows they are
   necessary.

If tsindex is unavailable or stale, fall back to `rg` and targeted reads, and
note the degradation in your output.
