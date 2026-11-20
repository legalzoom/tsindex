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

The default `serve --mcp` executes in its launcher's environment. To enforce a
harness's filesystem sandbox, launch the MCP executable inside that sandbox.
Some hosts launch MCP servers outside their shell sandbox, so configure their
MCP command through the platform's sandbox launcher. The Linux example in
[README.md](README.md#mcp-servers-and-the-shared-daemon) exposes one workspace
and runtime libraries. Include writable index and cache locations in the
sandbox's grants. An index database can contain cached source snippets, so
apply the same read policy to a database as to its indexed sources.

`serve --mcp --via-daemon` explicitly authorizes a trusted session to use the
shared daemon's user authority. Accepted clients must authenticate as the
daemon's effective user. Server connections require a Unix socket leaf owned
by the current user in a private user directory. Neither the leaf nor its
immediate parent directory may be a symlink. Linux server credentials must
match that user. On macOS, clients also accept the trusted UID 0 listener
creator because launchd creates the listener as root and XNU retains those
credentials after handoff to the user daemon. Keep that endpoint outside the
filesystem view of untrusted sandboxed processes. A reachable same-user broker
can read and modify files using its own permissions. A path supplied by a
client carries no proof of that client's kernel sandbox.

Managed session workers receive an inherited private socket for their checkout's
index owner. It has no filesystem address for another client to open. To share
inside a sandbox, run its own foreground daemon within the same restrictions
and give only those harnesses its private socket and index storage.

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

Releases include checksums, the project license texts, and third-party notices. Verify
the checksums before using a binary. When a release includes a GitHub build
attestation, verify it with `gh attestation verify <binary> --repo legalzoom/tsindex`.
