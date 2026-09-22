# Security

## Reporting a vulnerability

Use [GitHub private vulnerability reporting](https://github.com/legalzoom/tsindex/security/advisories/new).
Include the affected version, a minimal reproduction, and the expected impact.
Use synthetic data and omit credentials, private source code, and customer data.
Do not report an unpatched vulnerability in a public issue. If private reporting
is unavailable, contact a repository maintainer privately to arrange a secure
reporting channel.

Security fixes target the latest release. Upgrade to the latest version before
checking whether an issue has already been fixed.

## Trust boundaries

tsindex reads source code from repositories you configure. Only expose its MCP
connection to trusted clients. The MCP `replace_symbol` tool can change source
files; HTTP does not expose that tool.

The HTTP server binds to `127.0.0.1` by default. Host and Origin validation reduce
browser-based attacks, but do not authenticate clients. The HTTP API includes
repository cloning, removal, and rebuild operations. Keep the default loopback
binding, or place the service behind authenticated access controls before making
it reachable on a network. Repository cloning accepts validated GitHub HTTPS
URLs from any owner; this is URL validation, not an authorization policy.

Index databases and local usage records can identify source files and repositories.
Keep `.tsindex/`, agent configuration, credentials, and benchmark transcripts out
of commits and shared artifacts. Usage records stay on the local filesystem;
tsindex does not send them to an analytics service.

## Release verification

Releases include checksums, the project license, and third-party notices. Verify
the checksums before using a binary. When a release includes a GitHub build
attestation, verify it with `gh attestation verify <binary> --repo legalzoom/tsindex`.
