# Contributing

Open an issue for a bug report or a proposed change, and send focused pull
requests with a description of the behavior being changed. Use synthetic
examples; do not include private source, customer information, credentials,
machine-specific configuration, or raw agent transcripts.

The Rust toolchain is pinned in `rust-toolchain.toml`. Before submitting a change,
run:

```sh
cargo fmt --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked
bash skills/tsindex/scripts/test-start-tsindex.sh
```

CI also scans for credentials and dependency advisories. Update
`THIRD_PARTY_NOTICES.md` when changing locked dependencies, preserving their
license texts and copyright notices. Check the relevant dependency's published
source if its package omits the license text.

Contributions are made under the project's MIT license. Only contribute code
you have the right to distribute under that license.

Keep discussions respectful, specific, and focused on the work. Harassment and
disclosure of another person's private information are not acceptable. Maintainers
may remove inappropriate content or restrict participation. Follow the
[Code of Conduct](CODE_OF_CONDUCT.md) when participating in this project.

For vulnerabilities, follow [SECURITY.md](SECURITY.md) instead of opening a
public issue.
