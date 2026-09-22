.PHONY: audit fmt clippy test check

audit:
	cargo audit

fmt:
	cargo fmt --check

clippy:
	cargo clippy --locked --all-targets --all-features -- -D warnings

test:
	cargo test --locked

check: fmt clippy test
