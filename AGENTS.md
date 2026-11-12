# Agent instructions

## Versioning pull requests

Before creating a pull request, the coding harness must review the complete diff
against the PR base branch, decide the appropriate Semantic Versioning bump, and
update `[package].version` in `Cargo.toml`:

- **Major**: backward-incompatible changes to public Rust APIs, CLI commands,
  MCP/HTTP contracts, configuration, or persisted index compatibility.
- **Minor**: backward-compatible new features or capabilities.
- **Patch**: backward-compatible bug or security fixes, performance improvements,
  refactors, dependency updates, and documentation, test, or build-only changes.

Use the highest-impact change in the PR to choose the bump. Base the target
version on the base branch's package version and reset lower version components
according to SemVer. Bump once per PR, not once per commit or review response;
reassess the target if the PR's scope changes.

Keep the `tsindex` package version in `Cargo.lock` synchronized without upgrading
unrelated dependencies. Include the old and new versions and a brief explanation
of the harness's major/minor/patch decision in the PR description.
