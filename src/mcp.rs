use std::ffi::OsString;
use std::fs;
use std::io::{self, BufRead, Read, Write};
use std::net::{IpAddr, Shutdown, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use crate::config::{RepoConfig, config_path};
use crate::index::{
    EnclosingSymbolArgs, FindReferencesArgs, GetSymbolArgs, OutlineArgs, QueryArgs,
    ReplaceSymbolArgs, Runtime,
};

static PARENT_WATCHDOG_STARTED: AtomicBool = AtomicBool::new(false);

pub fn serve_mcp(runtime: Runtime) -> Result<()> {
    spawn_parent_watchdog();
    let stdin = io::stdin();
    let stdout = io::stdout();
    serve_mcp_io(&runtime, stdin.lock(), stdout.lock())
}

/// The stdio JSON-RPC loop, split from `serve_mcp` so tests can drive it with
/// an in-memory reader/writer.
fn serve_mcp_io(runtime: &Runtime, input: impl BufRead, mut output: impl Write) -> Result<()> {
    // Read raw bytes: one non-UTF-8 byte from the client must not end the
    // session, but it must not be decoded lossily either (U+FFFD substitution
    // could turn a corrupt request into a different, valid one). Answer with a
    // -32700 parse error and keep going; only a real I/O error ends the loop.
    for line in input.split(b'\n') {
        let response = match String::from_utf8(line?) {
            Ok(line) if line.trim().is_empty() => continue,
            Ok(line) => handle_json_rpc_line(runtime, &line)?,
            Err(_) => Some(json_rpc_error(
                None,
                -32700,
                "invalid JSON-RPC message: request is not valid UTF-8",
            )),
        };
        if let Some(response) = response {
            writeln!(output, "{}", serde_json::to_string(&response)?)?;
            output.flush()?;
        }
    }
    Ok(())
}

pub fn spawn_parent_watchdog() {
    if !claim_parent_watchdog_start() {
        return;
    }

    let initial = unsafe { libc::getppid() };
    // A parent of PID 1 at startup means init/launchd spawned us directly, or
    // the MCP client itself is PID 1 (containers). There is no parent lifetime
    // to track, so run without the watchdog instead of exiting immediately.
    // Trade-off: a client that spawns us and dies before this first
    // `getppid()` has already reparented us to 1, so that orphan lives until
    // its transport closes. The window is microseconds and the container case
    // is the common one, so it is accepted rather than guessed at.
    if initial <= 1 {
        return;
    }
    thread::spawn(move || {
        loop {
            thread::sleep(Duration::from_secs(10));
            if check_parent_watchdog(
                initial,
                || unsafe { libc::getppid() },
                |initial, current| shutdown_after_parent_exit(initial, current),
            ) {
                break;
            }
        }
    });
}

fn claim_parent_watchdog_start() -> bool {
    !PARENT_WATCHDOG_STARTED.swap(true, Ordering::SeqCst)
}

fn check_parent_watchdog(
    initial: libc::pid_t,
    get_parent: impl FnOnce() -> libc::pid_t,
    on_exit: impl FnOnce(libc::pid_t, libc::pid_t),
) -> bool {
    if initial <= 1 {
        return false;
    }
    let current = get_parent();
    if current <= 1 || current != initial {
        on_exit(initial, current);
        return true;
    }
    false
}

fn shutdown_after_parent_exit(initial: libc::pid_t, current: libc::pid_t) -> ! {
    // The parent MCP client may already have gone away, leaving stderr as a
    // broken pipe/socket. `eprintln!` can panic on write failure; if that
    // happens inside this watchdog thread before `exit`, only the watchdog dies
    // and the orphaned MCP server keeps running. Keep logging best-effort so the
    // process termination is unconditional.
    let _ = writeln!(
        io::stderr(),
        "{}",
        parent_watchdog_shutdown_message(initial, current)
    );
    std::process::exit(0);
}

fn parent_watchdog_shutdown_message(initial: libc::pid_t, current: libc::pid_t) -> String {
    format!("tsindex: parent {initial} exited (now reparented to {current}); shutting down")
}

fn handle_json_rpc_line(runtime: &Runtime, line: &str) -> Result<Option<Value>> {
    Ok(match serde_json::from_str::<Value>(line) {
        Ok(request) => handle_request(runtime, request)?,
        Err(error) => Some(json_rpc_error(
            None,
            -32700,
            format!("invalid JSON-RPC message: {error:#}"),
        )),
    })
}

/// Maximum HTTP request body size accepted by the JSON server, in bytes.
///
/// Anything larger gets rejected with 413 before any allocation. The
/// previous code unconditionally ran `vec![0u8; content_length]` based
/// on whatever the client put in the Content-Length header, so a
/// well-formed `Content-Length: 4294967296` would allocate 4 GiB and
/// OOM the process. 1 MiB is comfortably above the largest reasonable
/// JSON payload any tools/call request would send.
const MAX_HTTP_BODY_BYTES: usize = 1 << 20; // 1 MiB

/// Cap on the request line plus headers. Lines are read through `Take`, so a
/// header without a newline cannot grow memory past this before we answer 431.
const MAX_HTTP_HEADER_BYTES: u64 = 64 * 1024;

/// Per-`read()`/`write()` socket timeout: a fully idle half-open client is
/// dropped after this long.
const HTTP_IO_TIMEOUT: Duration = Duration::from_secs(10);

/// Total budget for receiving one request (line, headers and body). The
/// per-read timeout alone lets a client dripping one byte every few seconds
/// hold a connection for minutes; past this deadline it gets 408. Each
/// connection runs on its own thread, so a slow client only delays itself.
const HTTP_REQUEST_DEADLINE: Duration = Duration::from_secs(30);

pub fn serve_http(runtime: Runtime, bind: &str, port: u16, allowed_hosts: &[String]) -> Result<()> {
    let bind_addr: IpAddr = bind
        .parse()
        .with_context(|| format!("invalid --bind address: {bind:?}"))?;
    let listener = TcpListener::bind((bind_addr, port))
        .with_context(|| format!("failed to bind http server on {bind}:{port}"))?;
    let bound_port = listener.local_addr().map(|a| a.port()).unwrap_or(port);
    eprintln!("tsindex http server listening on http://{bind}:{bound_port}");
    let runtime = Arc::new(Mutex::new(runtime));
    let allowed_hosts = Arc::new(allowed_hosts.to_vec());
    for stream in listener.incoming() {
        let stream = stream?;
        let runtime = Arc::clone(&runtime);
        let allowed_hosts = Arc::clone(&allowed_hosts);
        thread::spawn(move || {
            if let Err(error) = handle_http_connection(&runtime, stream, bound_port, &allowed_hosts)
            {
                eprintln!("http request failed: {error:#}");
            }
        });
    }
    Ok(())
}

/// Serve one connection: parse and dispatch the request, then close the
/// socket politely. The request is read without holding the runtime lock, so
/// a slow client does not block other connections' tool calls.
fn handle_http_connection(
    runtime: &Mutex<Runtime>,
    mut stream: TcpStream,
    bound_port: u16,
    allowed_hosts: &[String],
) -> Result<()> {
    stream.set_read_timeout(Some(HTTP_IO_TIMEOUT))?;
    stream.set_write_timeout(Some(HTTP_IO_TIMEOUT))?;
    let result = serve_http_request(runtime, &mut stream, bound_port, allowed_hosts);
    // Early rejections (431/413/408) leave request bytes unread in the
    // socket. Closing then makes the kernel send RST, and a client still
    // reading the response can see ECONNRESET instead of our body. Send FIN
    // first, then drain (bounded) what the client already sent so the close
    // is clean.
    let _ = stream.shutdown(Shutdown::Write);
    let _ = stream.set_read_timeout(Some(Duration::from_secs(1)));
    let _ = io::copy(
        &mut (&stream).take(MAX_HTTP_BODY_BYTES as u64),
        &mut io::sink(),
    );
    result
}

fn serve_http_request(
    runtime: &Mutex<Runtime>,
    stream: &mut TcpStream,
    bound_port: u16,
    allowed_hosts: &[String],
) -> Result<()> {
    let deadline = Instant::now() + HTTP_REQUEST_DEADLINE;
    let mut reader = io::BufReader::new(stream.try_clone()?);
    let mut header_budget = MAX_HTTP_HEADER_BYTES;
    let mut first_line = String::new();
    (&mut reader)
        .take(header_budget)
        .read_line(&mut first_line)?;
    header_budget -= first_line.len() as u64;
    if first_line.is_empty() {
        return Ok(());
    }
    // The cap covers the request line too: a line that filled the budget
    // without reaching its newline was truncated, not read.
    if header_budget == 0 && !first_line.ends_with('\n') {
        return write_http_headers_too_large(stream);
    }
    let mut content_length = 0usize;
    let mut host_header: Option<String> = None;
    let mut origin_header: Option<String> = None;
    let mut fetch_site: Option<String> = None;
    loop {
        if Instant::now() > deadline {
            return write_http_request_timeout(stream);
        }
        let mut header = String::new();
        (&mut reader).take(header_budget).read_line(&mut header)?;
        header_budget -= header.len() as u64;
        // Budget exhausted mid-line, or exactly on the previous line's
        // newline (`take(0)` then reads nothing, which must not pass as the
        // blank end-of-headers line): the headers did not fit.
        if header_budget == 0 && !header.ends_with('\n') {
            return write_http_headers_too_large(stream);
        }
        if header == "\r\n" || header.is_empty() {
            break;
        }
        // HTTP/1.1 header names are case-insensitive (curl --http2 and some
        // libraries send lower case), so match on the name, not a prefix.
        let Some((name, value)) = header.split_once(':') else {
            continue;
        };
        let value = value.trim();
        if name.eq_ignore_ascii_case("content-length") {
            let Ok(length) = value.parse::<usize>() else {
                return write_http_response(
                    stream,
                    400,
                    json!({ "error": format!("invalid Content-Length: {value:?}") }),
                );
            };
            content_length = length;
        } else if name.eq_ignore_ascii_case("host") {
            host_header = Some(value.to_string());
        } else if name.eq_ignore_ascii_case("origin") {
            origin_header = Some(value.to_string());
        } else if name.eq_ignore_ascii_case("sec-fetch-site") {
            fetch_site = Some(value.to_ascii_lowercase());
        }
    }

    // DNS-rebinding mitigation: a malicious webpage could resolve an
    // attacker-controlled DNS name to 127.0.0.1 and then POST to our
    // server from a browser. Browsers send the original DNS name in
    // the Host header rather than the resolved address, so a strict
    // allowlist on Host neutralizes that attack class.
    if !is_allowed_host(host_header.as_deref(), bound_port, allowed_hosts) {
        return write_http_response(
            stream,
            403,
            json!({
                "error": "forbidden: Host header must be 127.0.0.1, localhost, or an --allowed-host name on the server's port",
            }),
        );
    }

    let mut parts = first_line.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let path = parts.next().unwrap_or_default();

    // CSRF mitigation: the Host check does not stop a page on ANOTHER local
    // origin (a dev server, a notebook) from POSTing here — its Host header
    // is still `localhost:<port>`. Browsers always attach `Origin` to a
    // cross-origin POST, so a request that carries one must come from an
    // allowed host; scripts and curl send neither header and stay allowed.
    if method == "POST"
        && !is_same_origin_request(
            origin_header.as_deref(),
            fetch_site.as_deref(),
            bound_port,
            allowed_hosts,
        )
    {
        return write_http_response(
            stream,
            403,
            json!({ "error": "forbidden: cross-origin POST (Origin/Sec-Fetch-Site does not match this server)" }),
        );
    }

    if content_length > MAX_HTTP_BODY_BYTES {
        return write_http_response(
            stream,
            413,
            json!({
                "error": format!(
                    "request body too large: {content_length} bytes exceeds the {MAX_HTTP_BODY_BYTES}-byte cap",
                ),
            }),
        );
    }
    let mut body = Vec::with_capacity(content_length);
    let mut chunk = [0u8; 8192];
    while body.len() < content_length {
        if Instant::now() > deadline {
            return write_http_request_timeout(stream);
        }
        let want = chunk.len().min(content_length - body.len());
        let n = reader.read(&mut chunk[..want])?;
        if n == 0 {
            return Err(anyhow!(
                "connection closed before the request body was complete"
            ));
        }
        body.extend_from_slice(&chunk[..n]);
    }
    let mut runtime = runtime
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let runtime = &mut *runtime;

    let payload = match (method, path) {
        ("GET", "/health") => json!({ "ok": true }),
        ("GET", "/dashboard") => {
            return write_http_text_response(
                stream,
                200,
                "text/html; charset=utf-8",
                dashboard_html(),
            );
        }
        ("GET", "/dashboard/data") => match handle_dashboard_data(runtime) {
            Ok(v) => v,
            Err(e) => return write_http_internal_error(stream, &e),
        },
        ("GET", "/repos") => match handle_repos_list(runtime) {
            Ok(v) => v,
            Err(e) => return write_http_internal_error(stream, &e),
        },
        ("POST", "/repos/rebuild") => {
            return handle_repos_rebuild(runtime, &body, stream);
        }
        ("POST", "/repos/add") => {
            return handle_repos_add(runtime, &body, stream);
        }
        ("POST", "/repos/remove") => {
            return handle_repos_remove(runtime, &body, stream);
        }
        ("POST", "/tools/list") => tool_list(false),
        ("POST", "/tools/call") => {
            let request: HttpCallRequest = match serde_json::from_slice(&body) {
                Ok(request) => request,
                Err(error) => {
                    return write_http_response(
                        stream,
                        400,
                        json!({ "error": format!("invalid JSON request: {error:#}") }),
                    );
                }
            };
            match call_tool(
                runtime,
                &request.name,
                request.arguments.unwrap_or_default(),
                false,
            ) {
                Ok(response) => response,
                Err(error) => {
                    if !is_invalid_params(&error) {
                        return write_http_internal_error(stream, &error);
                    }
                    return write_http_response(
                        stream,
                        400,
                        json!({ "error": format!("{error:#}") }),
                    );
                }
            }
        }
        _ => return write_http_response(stream, 404, json!({ "error": "not found" })),
    };
    write_http_response(stream, 200, payload)
}

/// Validate the request's `Host` header against this server's bound
/// port + the localhost-only host names we accept.
///
/// We bind to `127.0.0.1` so remote attackers can't reach us, but a
/// malicious webpage in the user's browser could still POST via DNS
/// rebinding (resolve attacker.example to 127.0.0.1 in the user's
/// resolver, then issue a same-origin request from JS). Browsers send
/// the original hostname in the Host header, so checking it against a
/// short allowlist neutralizes that attack class.
///
/// `allowed_hosts` extends the allowlist with operator-declared names
/// (e.g. a Kubernetes Service DNS name when serving with a non-loopback
/// --bind). The localhost forms are always accepted; the port must
/// match the bound port for every entry.
fn is_allowed_host(host: Option<&str>, bound_port: u16, allowed_hosts: &[String]) -> bool {
    let Some(raw) = host else {
        return false;
    };
    // Strip any whitespace; some clients pad. Take only up to the
    // first comma in case of header folding.
    let value = raw.trim();
    if value.is_empty() {
        return false;
    }
    // Split host:port. Host headers may legally omit the port if it's
    // the default HTTP port (80), but we never bind to 80, so any
    // request without a port is rejected.
    let (host_part, port_part) = match value.rsplit_once(':') {
        Some((h, p)) => (h, p),
        None => return false,
    };
    let Ok(port) = port_part.parse::<u16>() else {
        return false;
    };
    if port != bound_port {
        return false;
    }
    matches!(host_part, "127.0.0.1" | "localhost" | "[::1]")
        || allowed_hosts.iter().any(|allowed| allowed == host_part)
}

/// Whether a POST may proceed given the browser-only `Origin` and
/// `Sec-Fetch-Site` headers. Browsers attach `Origin` to every cross-origin
/// POST, so one that is present must resolve to an allowed host on our port;
/// `Sec-Fetch-Site` must be `same-origin` or `none` when present. A request
/// with neither header is a non-browser client and is allowed through.
fn is_same_origin_request(
    origin: Option<&str>,
    fetch_site: Option<&str>,
    bound_port: u16,
    allowed_hosts: &[String],
) -> bool {
    if let Some(site) = fetch_site
        && !matches!(site, "same-origin" | "none")
    {
        return false;
    }
    let Some(origin) = origin else {
        return true;
    };
    let origin = origin.trim();
    origin
        .strip_prefix("http://")
        .or_else(|| origin.strip_prefix("https://"))
        .is_some_and(|host| is_allowed_host(Some(host), bound_port, allowed_hosts))
}

fn write_http_headers_too_large(stream: &mut TcpStream) -> Result<()> {
    write_http_response(
        stream,
        431,
        json!({ "error": format!("request line and headers exceed the {MAX_HTTP_HEADER_BYTES}-byte cap") }),
    )
}

fn write_http_request_timeout(stream: &mut TcpStream) -> Result<()> {
    write_http_response(
        stream,
        408,
        json!({ "error": format!("request not received within {}s", HTTP_REQUEST_DEADLINE.as_secs()) }),
    )
}

fn write_http_response(stream: &mut TcpStream, status: u16, body: Value) -> Result<()> {
    let payload = serde_json::to_vec(&body)?;
    write_http_bytes_response(stream, status, "application/json", &payload)
}

/// 5xx bodies stay generic: the full error chain names database paths,
/// workspace roots, and git output, which belong in the server log only.
fn write_http_internal_error(stream: &mut TcpStream, error: &anyhow::Error) -> Result<()> {
    eprintln!("http request failed: {error:#}");
    write_http_response(stream, 500, json!({ "error": "internal error" }))
}

fn write_http_text_response(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &str,
) -> Result<()> {
    write_http_bytes_response(stream, status, content_type, body.as_bytes())
}

fn write_http_bytes_response(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    payload: &[u8],
) -> Result<()> {
    let status_text = match status {
        200 => "OK",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        408 => "Request Timeout",
        409 => "Conflict",
        413 => "Payload Too Large",
        431 => "Request Header Fields Too Large",
        _ => "Internal Server Error",
    };
    write!(
        stream,
        "HTTP/1.1 {status} {status_text}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        payload.len()
    )?;
    stream.write_all(payload)?;
    stream.flush()?;
    Ok(())
}

/// Dispatch a single JSON-RPC request and return its response envelope.
/// Public so integration tests can assert on the exact wire payload the
/// MCP client receives (e.g. the dropped `structuredContent` duplicate).
pub fn handle_request(runtime: &Runtime, request: Value) -> Result<Option<Value>> {
    let id = request.get("id").cloned();
    let Some(method) = request.get("method").and_then(Value::as_str) else {
        return Ok(Some(json_rpc_error(id, -32600, "missing method")));
    };
    let params = request.get("params").cloned().unwrap_or_else(|| json!({}));

    let result = match method {
        "initialize" => Some(json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": { "tools": {} },
                "serverInfo": {
                    "name": "tsindex-mcp",
                    "version": env!("CARGO_PKG_VERSION"),
                },
                "instructions": "Use tsindex for compact structural code navigation. Scope every call with `repo` when the workspace is multi-repo (it is the index key; an unscoped call fans out across all repos and wastes output). Batch with `names`/`rows`/`include_bodies_for` — one call per file or task, not one per item. `find_references` always requires `names`, including one identifier (`names: [\"symbol\"]`). To understand a symbol, first `get_symbol` with `include_body: false` to scan the matches, then re-fetch with `include_body: true` only for the keeper. Start orientation with `list_file_outline`, fetch only needed bodies, and verify syntactic occurrences before consequential edits. Follow `next_offset` when `truncated` to resume. Use text search (`rg`) for literals/config/prose and semantic tooling when binding accuracy matters."
            }
        })),
        "notifications/initialized" => None,
        "ping" => Some(json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {}
        })),
        "tools/list" => Some(json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": tool_list(true)
        })),
        "tools/call" => {
            let Some(name) = params.get("name").and_then(Value::as_str) else {
                return Ok(Some(json_rpc_error(id, -32602, "missing tool name")));
            };
            let arguments = params
                .get("arguments")
                .cloned()
                .unwrap_or_else(|| json!({}));
            match call_tool(runtime, name, arguments, true) {
                // Return the payload once, as the MCP-mandatory `content[].text`
                // string. We deliberately omit `structuredContent`: it would be a
                // byte-for-byte duplicate of `text` (the payload serialized twice),
                // and since these tools declare no `outputSchema`, the spec makes it
                // purely optional. Clients that want structured data parse `text`,
                // which is already compact JSON. Dropping it ~halves the wire/context
                // cost of every tool response (see bench/).
                Ok(structured) => Some(json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": serde_json::to_string(&structured)?
                            }
                        ]
                    }
                })),
                Err(error) => {
                    let code = if is_invalid_params(&error) {
                        -32602
                    } else {
                        -32603
                    };
                    Some(json_rpc_error(id, code, format!("{error:#}")))
                }
            }
        }
        _ => Some(json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {
                "code": -32601,
                "message": format!("unknown method {method}")
            }
        })),
    };
    Ok(result)
}

fn json_rpc_error(id: Option<Value>, code: i64, message: impl Into<String>) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id.unwrap_or(Value::Null),
        "error": {
            "code": code,
            "message": message.into(),
        }
    })
}

/// Build the advertised tool list. `writable` gates the file-mutating
/// tools (currently `replace_symbol`) so only the local MCP/stdio
/// transport offers them; the HTTP server stays read-only.
fn tool_schema(name: &str, raw: &str) -> Value {
    serde_json::from_str(raw)
        .unwrap_or_else(|error| panic!("invalid bundled schema for {name}: {error}"))
}

fn tool_list(writable: bool) -> Value {
    let mut tools = vec![
        json!({
            "name": "get_symbol",
            "description": "Look up symbols by name. Batch with `names`; scan without bodies first, then fetch only needed bodies. Results are bounded and paginated.",
            "inputSchema": tool_schema("get_symbol", include_str!("../schemas/get_symbol.json"))
        }),
        json!({
            "name": "list_file_outline",
            "description": "Return a paginated file outline. Pagination and byte-budget shrinking operate on whole top-level roots, never by silently dropping children. Use `include_bodies_for` to inline selected bodies in the same call.",
            "inputSchema": tool_schema("list_file_outline", include_str!("../schemas/list_file_outline.json"))
        }),
        json!({
            "name": "enclosing_symbol",
            "description": "Map one or more 1-based file rows to enclosing symbols, ordered outermost to innermost.",
            "inputSchema": tool_schema("enclosing_symbol", include_str!("../schemas/enclosing_symbol.json"))
        }),
        json!({
            "name": "find_references",
            "description": "Find bounded, paginated syntactic occurrences of identifiers — use it for WHERE and in what context a symbol is used. To COUNT occurrences or rank which files use a symbol, set `counts_only: true` — it returns exact `total`/`total_files`/per-name totals plus a byte-budget-bounded top-N list of per-file counts, in one small aggregate response (no rows, no snippets, no pagination), far cheaper than paging the full set. For raw substring/line counts use `rg --count-matches` (note `rg -c` counts LINES, not occurrences, and matches substrings — pass `-w`). This is not binding-aware dependency analysis; verify results before refactors.",
            "inputSchema": tool_schema("find_references", include_str!("../schemas/find_references.json"))
        }),
        json!({
            "name": "query",
            "description": "Run a bounded raw tree-sitter query when the higher-level tools cannot express the structure needed.",
            "inputSchema": tool_schema("query", include_str!("../schemas/query.json"))
        }),
    ];
    if writable {
        tools.push(json!({
            "name": "replace_symbol",
            "description": "Rewrite one whole symbol from live source without a prior read; rejects ambiguity and newly introduced syntax errors.",
            "inputSchema": tool_schema("replace_symbol", include_str!("../schemas/replace_symbol.json"))
        }));
    }
    json!({ "tools": tools })
}

/// A caller-side argument failure: the `arguments` object was wrong, not the
/// server. Tagged so the transports can report it as JSON-RPC `-32602`
/// (invalid params) / HTTP 400 instead of an internal-error code, letting
/// clients tell "I passed the wrong thing" from "the server broke".
#[derive(Debug)]
struct InvalidParams(String);

impl std::fmt::Display for InvalidParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for InvalidParams {}

/// Deserialize tool arguments, tagging failures as [`InvalidParams`]. Only the
/// parse is tagged — a later `to_value` serialization fault is a genuine
/// server error and must keep its internal-error code.
fn tool_args<T: DeserializeOwned>(arguments: Value) -> Result<T> {
    serde_json::from_value(arguments).map_err(|error| InvalidParams(error.to_string()).into())
}

/// True when the failure was the caller's fault (bad arguments).
fn is_invalid_params(error: &anyhow::Error) -> bool {
    error.downcast_ref::<InvalidParams>().is_some()
}

fn call_tool(runtime: &Runtime, name: &str, arguments: Value, writable: bool) -> Result<Value> {
    let result = match name {
        "get_symbol" => {
            let args: GetSymbolArgs = tool_args(arguments)?;
            if args.name.is_none() && args.names.as_ref().is_none_or(Vec::is_empty) {
                return Err(InvalidParams(
                    "get_symbol requires `name` or a non-empty `names` array".into(),
                )
                .into());
            }
            serde_json::to_value(runtime.get_symbol(args)?)?
        }
        "list_file_outline" => {
            let args: OutlineArgs = tool_args(arguments)?;
            serde_json::to_value(runtime.list_file_outline(args)?)?
        }
        "enclosing_symbol" => {
            let args: EnclosingSymbolArgs = tool_args(arguments)?;
            if args.row.is_none() && args.rows.as_ref().is_none_or(Vec::is_empty) {
                return Err(InvalidParams(
                    "enclosing_symbol requires `row` or a non-empty `rows` array".into(),
                )
                .into());
            }
            if args.row == Some(0) || args.rows.as_ref().is_some_and(|rows| rows.contains(&0)) {
                return Err(
                    InvalidParams("enclosing_symbol rows are 1-based; received 0".into()).into(),
                );
            }
            serde_json::to_value(runtime.enclosing_symbol(args)?)?
        }
        "find_references" => {
            let args: FindReferencesArgs = tool_args(arguments)?;
            if args.name.is_some() {
                return Err(InvalidParams(
                    "find_references accepts `names` only; use `names: [\"symbol\"]` for one identifier"
                        .into(),
                )
                .into());
            }
            if args.names.as_ref().is_none_or(Vec::is_empty) {
                return Err(InvalidParams(
                    "find_references requires a non-empty `names` array".into(),
                )
                .into());
            }
            if args.counts_only {
                serde_json::to_value(runtime.find_reference_counts(args)?)?
            } else {
                serde_json::to_value(runtime.find_references(args)?)?
            }
        }
        "query" => {
            let args: QueryArgs = tool_args(arguments)?;
            serde_json::to_value(runtime.query(args)?)?
        }
        "replace_symbol" => {
            if !writable {
                return Err(InvalidParams(
                    "replace_symbol is only available over the MCP/stdio transport".into(),
                )
                .into());
            }
            let args: ReplaceSymbolArgs = tool_args(arguments)?;
            serde_json::to_value(runtime.replace_symbol(args)?)?
        }
        _ => return Err(InvalidParams(format!("unknown tool {name}")).into()),
    };
    record_dashboard_tool_usage(runtime, name, &result);
    Ok(result)
}

fn handle_repos_list(runtime: &Runtime) -> Result<Value> {
    let workspaces = runtime.workspaces()?;
    let conn = rusqlite::Connection::open(&runtime.db_path).ok();
    let repos: Vec<Value> = workspaces
        .into_iter()
        .map(|w| {
            let indexed = conn.as_ref().is_some_and(|c| {
                c.query_row(
                    "SELECT COUNT(*) FROM files f JOIN repos r ON r.id = f.repo_id WHERE r.name = ?1",
                    [&w.name],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap_or(0)
                    > 0
            });
            json!({
                "name": w.name,
                "path": w.root.to_string_lossy(),
                "indexed": indexed,
            })
        })
        .collect();
    Ok(json!({ "repos": repos }))
}

fn handle_dashboard_data(runtime: &Runtime) -> Result<Value> {
    let repos = handle_repos_list(runtime)?["repos"].clone();
    let tools = tool_list(false)["tools"].as_array().map_or(0, Vec::len);
    let adoption = dashboard_adoption(runtime);
    let configuration = dashboard_configuration(runtime);
    let features = dashboard_features(runtime, None)?;
    let conn = match rusqlite::Connection::open(&runtime.db_path) {
        Ok(conn) => conn,
        Err(_) => {
            let economics = with_realized_savings(dashboard_economics(0, 0), &runtime.root);
            return Ok(json!({
                "ok": true,
                "version": env!("CARGO_PKG_VERSION"),
                "db_path": runtime.db_path.to_string_lossy(),
                "totals": {
                    "repos": repos.as_array().map_or(0, Vec::len),
                    "files": 0,
                    "symbols": 0,
                    "refs": 0,
                    "bytes": 0,
                    "tools": tools,
                },
                "repos": repos,
                "languages": [],
                "symbol_kinds": [],
                "largest_files": [],
                "adoption": adoption,
                "configuration": configuration,
                "features": features,
                "economics": economics,
            }));
        }
    };

    let files = scalar_count(&conn, "SELECT COUNT(*) FROM files");
    let symbols = scalar_count(&conn, "SELECT COUNT(*) FROM symbols");
    let refs = scalar_count(&conn, "SELECT COUNT(*) FROM refs");
    let bytes = scalar_count(&conn, "SELECT COALESCE(SUM(byte_size), 0) FROM files");
    let repo_details = dashboard_repo_details(&conn)?;
    let languages = dashboard_languages(&conn)?;
    let symbol_kinds = dashboard_symbol_kinds(&conn)?;
    let largest_files = dashboard_largest_files(&conn)?;
    let features = dashboard_features(runtime, Some(&conn))?;
    let economics = with_realized_savings(dashboard_economics(refs, bytes), &runtime.root);

    Ok(json!({
        "ok": true,
        "version": env!("CARGO_PKG_VERSION"),
        "db_path": runtime.db_path.to_string_lossy(),
        "totals": {
            "repos": repos.as_array().map_or(0, Vec::len),
            "files": files,
            "symbols": symbols,
            "refs": refs,
            "bytes": bytes,
            "tools": tools,
        },
        "repos": repo_details,
        "languages": languages,
        "symbol_kinds": symbol_kinds,
        "largest_files": largest_files,
        "adoption": adoption,
        "configuration": configuration,
        "features": features,
        "economics": economics,
    }))
}

fn dashboard_adoption(runtime: &Runtime) -> Value {
    let config_file = config_path(&runtime.root);
    let agent_hooks = dashboard_agent_hooks(&runtime.root, &config_file);
    let rtk = dashboard_rtk_integration(&runtime.root);
    let coverage_percent = if agent_hooks.is_empty() {
        0
    } else {
        agent_hooks
            .iter()
            .filter(|hook| {
                hook.get("detected")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
            })
            .count()
            * 100
            / agent_hooks.len()
    };
    json!({
        "agent_hooks": agent_hooks,
        "rtk": rtk,
        "custom_toml_filters": dashboard_custom_toml_filter_count(runtime, &config_file),
        "coverage_percent": coverage_percent,
    })
}

fn dashboard_configuration(runtime: &Runtime) -> Value {
    let config_file = config_path(&runtime.root);
    let config_exists = config_file.exists();
    let project_count = runtime.config.repos.len().max(1);
    json!({
        "config_exists": config_exists,
        "config_path": config_file.to_string_lossy(),
        "excluded_commands": dashboard_excluded_command_count(&runtime.root),
        "project_count": project_count,
        "repo_ignore_patterns": runtime.config.ignore.extra.len()
            + runtime.config.repos.iter().map(|repo| repo.ignore.len()).sum::<usize>(),
        "language_filters": runtime.config.languages.include.len()
            + runtime.config.repos.iter().map(|repo| repo.languages.len()).sum::<usize>(),
    })
}

/// Dashboard command name → MCP tool name.
const COMMAND_TOOL_PAIRS: [(&str, &str); 5] = [
    ("gain", "get_symbol"),
    ("discover", "list_file_outline"),
    ("locate", "enclosing_symbol"),
    ("proxy", "find_references"),
    ("verify", "query"),
];

fn dashboard_features(
    runtime: &Runtime,
    conn: Option<&rusqlite::Connection>,
) -> Result<Vec<Value>> {
    let mut rows = Vec::with_capacity(COMMAND_TOOL_PAIRS.len());
    for (command, tool) in COMMAND_TOOL_PAIRS {
        let indexed_signal = match (command, conn) {
            ("gain", Some(conn)) => scalar_count(conn, "SELECT COUNT(*) FROM symbols"),
            ("discover", Some(conn)) => scalar_count(conn, "SELECT COUNT(*) FROM files"),
            ("proxy", Some(conn)) => scalar_count(conn, "SELECT COUNT(*) FROM refs"),
            ("verify", Some(conn)) => {
                scalar_count(conn, "SELECT COUNT(DISTINCT language) FROM files")
            }
            _ => 0,
        };
        rows.push(json!({
            "command": command,
            "tool": tool,
            "usage_count": dashboard_command_usage_count(&runtime.root, command)?,
            "indexed_signal": indexed_signal,
        }));
    }
    Ok(rows)
}

fn record_dashboard_tool_usage(runtime: &Runtime, tool: &str, result: &Value) {
    if let Some((command, _)) = COMMAND_TOOL_PAIRS
        .iter()
        .find(|(_, candidate)| *candidate == tool)
    {
        let dir = runtime.root.join(".tsindex");
        if fs::create_dir_all(&dir).is_ok() {
            let path = dir.join("dashboard-usage.jsonl");
            let mut line = json!({
                "tool": tool,
                "command": command,
                "ts": unix_timestamp_secs(),
            })
            .to_string();
            line.push('\n');
            if let Ok(mut file) = fs::OpenOptions::new().create(true).append(true).open(path) {
                // O_APPEND + a single write_all of one small record lands
                // atomically: concurrent sessions append without clobbering
                // each other (no read-modify-write aggregate to race on). The
                // record is small (one JSON object + newline), so write_all
                // completes in one underlying write() under O_APPEND.
                let _ = file.write_all(line.as_bytes());
            }
        }
        record_repo_savings(runtime, result);
    }
}

/// Per-repo directory under `~/.tsindex/<slug>` where `<slug>` is the repo's
/// absolute path with separators replaced by underscores (matching the watch
/// wrapper convention). Used to persist real-time savings globally per repo so
/// the totals survive across sessions and a regenerated repo-local `.tsindex`.
fn global_repo_dir(root: &Path) -> PathBuf {
    let slug = root.to_string_lossy().replace(['/', '\\'], "_");
    tsindex_home().join(slug)
}

/// `~/.tsindex`, or `$TSINDEX_HOME` when set so tests and CI can point the
/// telemetry sink at a scratch directory instead of the developer's home.
fn tsindex_home() -> PathBuf {
    tsindex_home_from(std::env::var_os("TSINDEX_HOME"))
}

/// Pure half of `tsindex_home`, so tests need not mutate the process
/// environment. An empty value (`TSINDEX_HOME=` in a `.env` template) counts
/// as unset: `PathBuf::from("")` is relative and would put telemetry in the CWD.
fn tsindex_home_from(var: Option<OsString>) -> PathBuf {
    var.filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| home_path(".tsindex"))
}

/// Cache-read input-token price used to convert saved tokens to USD. tsindex
/// keeps file contents out of the prompt, so the bytes it avoided would at most
/// have been cheap cached reads on later turns — the conservative basis.
const SAVINGS_USD_PER_MILLION_TOKENS: f64 = 0.30;
const SAVINGS_BYTES_PER_TOKEN: u64 = 4;

/// Stable per-process session id. Each Claude instance runs its own MCP server
/// process, so the pid distinguishes concurrent sessions on the same repo.
fn savings_session_id() -> u32 {
    std::process::id()
}

/// Append one measured-savings event for an indexed tool call to the repo's
/// global append-only log. `avoided` is the byte size of the source regions the
/// response actually delivered — each match's own line range, which unions to
/// roughly the whole file for an outline and to the matched lines for
/// find_references. That is the conservative counterfactual: the span you would
/// otherwise have read to learn what the call returned, not the whole file.
/// `saved` is `avoided` minus `returned` (response bytes), in tokens.
///
/// Appends are atomic, so any number of concurrent sessions on the same repo
/// accumulate without clobbering each other (unlike a read-modify-write
/// aggregate). Totals are summed at read time by `read_repo_savings`.
fn record_repo_savings(runtime: &Runtime, result: &Value) {
    let returned_bytes = serde_json::to_vec(result)
        .map(|v| v.len() as u64)
        .unwrap_or(0);
    let avoided_bytes = sum_result_region_bytes(runtime, result);
    let saved_tokens = avoided_bytes.saturating_sub(returned_bytes) / SAVINGS_BYTES_PER_TOKEN;

    let dir = global_repo_dir(&runtime.root);
    if fs::create_dir_all(&dir).is_err() {
        return;
    }
    let repo = runtime
        .root
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    let mut line = json!({
        "ts": unix_timestamp_secs(),
        "session": savings_session_id(),
        "repo": repo,
        "avoided_bytes": avoided_bytes,
        "returned_bytes": returned_bytes,
        "saved_tokens": saved_tokens,
    })
    .to_string();
    line.push('\n');
    if let Ok(mut file) = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join("savings.jsonl"))
    {
        // O_APPEND + a single write_all of one small record lands atomically:
        // concurrent sessions append without clobbering each other (no
        // read-modify-write aggregate to race on). The record is small, so
        // write_all completes in one underlying write() under O_APPEND.
        let _ = file.write_all(line.as_bytes());
    }
}

/// Sum the on-disk byte size of the source line ranges a tool response
/// delivered. Each line is counted once even when ranges overlap (an outline's
/// symbol ranges nest), so the total never exceeds the file and approximates the
/// whole file only when the response actually spans it.
fn sum_result_region_bytes(runtime: &Runtime, result: &Value) -> u64 {
    let mut regions: std::collections::BTreeMap<(String, String), Vec<(usize, usize)>> =
        std::collections::BTreeMap::new();
    collect_result_regions(result, None, None, &mut regions);
    regions
        .into_iter()
        .map(|((repo, file), ranges)| {
            // Resolve each repo-relative `file` against its own workspace root so
            // multi-repo catalogs measure the right file (and not a coincidental
            // same-relative-path file under the serve root). Fall back to the
            // serve root when the repo is unknown — the single-repo default.
            let path = if repo.is_empty() {
                runtime.root.join(&file)
            } else {
                runtime
                    .source_path(&repo, &file)
                    .unwrap_or_else(|_| runtime.root.join(&file))
            };
            region_file_bytes(&path, &ranges)
        })
        .sum()
}

/// Collect `((repo, file), [start_row, end_row])` line ranges from a response.
/// The wire `range` is 1-based on its rows (see `SourceRange` in `model.rs`), so
/// shift to the 0-based line indices used here. `repo`/`file` are siblings of
/// `range` on a match (get_symbol, outline) but live on the enclosing group for
/// find_references, so the nearest enclosing pair is threaded down the recursion.
fn collect_result_regions<'a>(
    value: &'a Value,
    current_repo: Option<&'a str>,
    current_file: Option<&'a str>,
    out: &mut std::collections::BTreeMap<(String, String), Vec<(usize, usize)>>,
) {
    match value {
        Value::Object(map) => {
            let repo = map.get("repo").and_then(|v| v.as_str()).or(current_repo);
            let file = map.get("file").and_then(|v| v.as_str()).or(current_file);
            if let (Some(file), Some(range)) = (file, map.get("range").and_then(|v| v.as_array()))
                && let (Some(start), Some(end)) = (
                    range.first().and_then(Value::as_u64),
                    range.get(2).and_then(Value::as_u64),
                )
            {
                // Wire rows are 1-based; convert to 0-based line indices.
                let start = start.saturating_sub(1) as usize;
                let end = end.saturating_sub(1) as usize;
                out.entry((repo.unwrap_or_default().to_string(), file.to_string()))
                    .or_default()
                    .push((start, end));
            }
            for child in map.values() {
                collect_result_regions(child, repo, file, out);
            }
        }
        Value::Array(items) => items
            .iter()
            .for_each(|item| collect_result_regions(item, current_repo, current_file, out)),
        _ => {}
    }
}

/// Bytes of the given 0-based inclusive line ranges in a file, deduplicating
/// overlapping lines. Returns 0 if the file can't be read.
fn region_file_bytes(path: &Path, ranges: &[(usize, usize)]) -> u64 {
    let Ok(content) = fs::read_to_string(path) else {
        return 0;
    };
    let lines: Vec<&str> = content.split_inclusive('\n').collect();
    if lines.is_empty() {
        return 0;
    }
    let last = lines.len() - 1;
    let mut wanted = vec![false; lines.len()];
    for &(start, end) in ranges {
        for wanted_line in wanted.iter_mut().take(end.min(last) + 1).skip(start) {
            *wanted_line = true;
        }
    }
    lines
        .iter()
        .zip(&wanted)
        .filter(|(_, wanted)| **wanted)
        .map(|(line, _)| line.len() as u64)
        .sum()
}

/// Roll up every event in a repo's append-only savings log into one summary.
/// Each line counts as one tool call (a `calls` field is honored if present so
/// a future compaction checkpoint can fold many events into one line). A legacy
/// `savings.json` aggregate written by older builds is folded in so totals do
/// not reset when upgrading.
fn read_repo_savings(dir: &Path) -> Value {
    let mut calls = 0u64;
    let mut avoided = 0u64;
    let mut returned = 0u64;
    let mut saved = 0u64;
    let mut updated = 0u64;
    let mut repo = String::new();
    let mut sessions = std::collections::BTreeSet::new();

    // Stream the append-only log line by line; it grows without bound, so never
    // pull the whole thing into memory.
    if let Ok(file) = fs::File::open(dir.join("savings.jsonl")) {
        for line in std::io::BufReader::new(file).lines().map_while(Result::ok) {
            let Ok(event) = serde_json::from_str::<Value>(&line) else {
                continue;
            };
            let field = |key: &str| event.get(key).and_then(Value::as_u64).unwrap_or(0);
            calls += event.get("calls").and_then(Value::as_u64).unwrap_or(1);
            avoided += field("avoided_bytes");
            returned += field("returned_bytes");
            saved += field("saved_tokens");
            updated = updated.max(field("ts"));
            if let Some(session) = event.get("session").and_then(Value::as_u64) {
                sessions.insert(session);
            }
            if repo.is_empty()
                && let Some(name) = event.get("repo").and_then(Value::as_str)
            {
                repo = name.to_string();
            }
        }
    }

    // Fold in a pre-append-log aggregate so upgrades don't drop prior savings.
    if let Some(legacy) = fs::read_to_string(dir.join("savings.json"))
        .ok()
        .and_then(|s| serde_json::from_str::<Value>(&s).ok())
    {
        let field = |key: &str| legacy.get(key).and_then(Value::as_u64).unwrap_or(0);
        calls += field("calls");
        avoided += field("avoided_bytes");
        returned += field("returned_bytes");
        saved += field("saved_tokens");
        updated = updated.max(field("updated_ts"));
    }

    if repo.is_empty() {
        repo = dir
            .file_name()
            .and_then(|name| name.to_str())
            .map(|slug| slug.rsplit('_').next().unwrap_or(slug).to_string())
            .unwrap_or_default();
    }

    let usd_saved = saved as f64 / 1_000_000.0 * SAVINGS_USD_PER_MILLION_TOKENS;
    json!({
        "repo": repo,
        "calls": calls,
        "avoided_bytes": avoided,
        "returned_bytes": returned,
        "saved_tokens": saved,
        "usd_saved": usd_saved,
        "sessions": sessions.len(),
        "updated_ts": updated,
    })
}

/// Aggregate realized savings across every repo under `~/.tsindex/*`, so any
/// running dashboard can show a global, cross-repo, all-sessions total.
fn dashboard_global_savings() -> Value {
    let mut repos: Vec<Value> = Vec::new();
    let mut total_calls = 0u64;
    let mut total_saved = 0u64;
    let mut total_sessions = 0u64;

    if let Ok(entries) = fs::read_dir(tsindex_home()) {
        for entry in entries.flatten() {
            let dir = entry.path();
            if !dir.is_dir() {
                continue;
            }
            let summary = read_repo_savings(&dir);
            let field = |key: &str| summary.get(key).and_then(Value::as_u64).unwrap_or(0);
            if field("calls") == 0 {
                continue;
            }
            total_calls += field("calls");
            total_saved += field("saved_tokens");
            total_sessions += field("sessions");
            repos.push(summary);
        }
    }

    repos.sort_by(|a, b| {
        let saved = |value: &Value| {
            value
                .get("saved_tokens")
                .and_then(Value::as_u64)
                .unwrap_or(0)
        };
        saved(b).cmp(&saved(a))
    });
    let repo_count = repos.len();
    let total_usd = total_saved as f64 / 1_000_000.0 * SAVINGS_USD_PER_MILLION_TOKENS;
    json!({
        "repos": repos,
        "repo_count": repo_count,
        "total_calls": total_calls,
        "total_saved_tokens": total_saved,
        "total_usd_saved": total_usd,
        "total_sessions": total_sessions,
    })
}

/// Attach this repo's realized (real-time, usage-driven) savings and the global
/// cross-repo rollup to the static byte-based economics estimate, so the
/// dashboard can show realized vs potential and a global all-repos total.
fn with_realized_savings(mut economics: Value, root: &Path) -> Value {
    let dir = global_repo_dir(root);
    let mut realized = read_repo_savings(&dir);
    if let Value::Object(map) = &mut realized {
        map.insert(
            "store_path".to_string(),
            json!(dir.join("savings.jsonl").to_string_lossy()),
        );
    }
    if let Value::Object(map) = &mut economics {
        map.insert("realized".to_string(), realized);
        map.insert("global".to_string(), dashboard_global_savings());
    }
    economics
}

fn dashboard_economics(refs: i64, bytes: i64) -> Value {
    const AVG_BYTES_PER_TOKEN: f64 = 4.0;
    const CACHE_READ_SAVINGS_RATE: f64 = 0.308;
    const INPUT_USD_PER_MILLION_TOKENS: f64 = 3.0;

    let indexed_tokens = (bytes as f64 / AVG_BYTES_PER_TOKEN).round() as i64;
    let saved_tokens = (indexed_tokens as f64 * CACHE_READ_SAVINGS_RATE).round() as i64;
    let estimated_usd_saved = saved_tokens as f64 / 1_000_000.0 * INPUT_USD_PER_MILLION_TOKENS;
    json!({
        "estimated_indexed_tokens": indexed_tokens,
        "estimated_saved_tokens": saved_tokens,
        "estimated_usd_saved": estimated_usd_saved,
        "input_usd_per_million_tokens": INPUT_USD_PER_MILLION_TOKENS,
        "savings_rate": CACHE_READ_SAVINGS_RATE,
        "evidence": "Illustrative estimate from indexed bytes and a fixed heuristic savings rate; not measured billing.",
        "reference_count": refs,
    })
}

fn dashboard_agent_hooks(root: &Path, config_file: &Path) -> Vec<Value> {
    let checks = [
        (
            "claude",
            vec![
                root.join(".claude/settings.json"),
                home_path(".claude.json"),
                home_path(".claude/settings.json"),
            ],
        ),
        (
            "gemini",
            vec![
                root.join(".gemini/settings.json"),
                root.join(".gemini/settings.toml"),
                home_path(".gemini/settings.json"),
                home_path(".gemini/settings.toml"),
            ],
        ),
        (
            "codex",
            vec![
                root.join(".codex/config.toml"),
                home_path(".codex/config.toml"),
            ],
        ),
    ];
    checks
        .into_iter()
        .map(|(agent, paths)| {
            let detected = paths
                .into_iter()
                .chain(std::iter::once(config_file.to_path_buf()))
                .any(|path| file_mentions_tsindex(&path));
            json!({ "agent": agent, "detected": detected })
        })
        .collect()
}

fn dashboard_rtk_integration(root: &Path) -> Value {
    let paths = [
        root.join(".rtk"),
        root.join(".rtk/config.toml"),
        root.join("rtk.toml"),
        root.join("rtk.json"),
        home_path(".rtk"),
        home_path(".config/rtk"),
    ];
    let detected = paths.iter().any(|path| path.exists());
    json!({
        "detected": detected,
        "repo_url": "https://github.com/rtk-ai/rtk",
        "mcp_command": format!("tsindex --root {} serve --mcp", root.to_string_lossy()),
        "notes": "Run RTK alongside tsindex: RTK compresses shell command output, while tsindex supplies MCP code-navigation tools. Configure your agent with both RTK hooks and the shown MCP command.",
    })
}

fn dashboard_custom_toml_filter_count(runtime: &Runtime, config_file: &Path) -> usize {
    let default_config = !config_file.exists();
    let language_filters = runtime.config.languages.include.len()
        + runtime
            .config
            .repos
            .iter()
            .map(|repo| repo.languages.len())
            .sum::<usize>();
    let ignore_filters = runtime.config.ignore.extra.len()
        + runtime
            .config
            .repos
            .iter()
            .map(|repo| repo.ignore.len())
            .sum::<usize>();
    if default_config {
        0
    } else {
        language_filters + ignore_filters + runtime.config.grammars.len()
    }
}

fn dashboard_excluded_command_count(root: &Path) -> usize {
    [
        root.join(".claude/settings.json"),
        root.join(".codex/config.toml"),
        root.join(".gemini/settings.json"),
        root.join(".gemini/settings.toml"),
        home_path(".claude/settings.json"),
        home_path(".codex/config.toml"),
        home_path(".gemini/settings.json"),
        home_path(".gemini/settings.toml"),
    ]
    .into_iter()
    .map(|path| {
        count_file_occurrences(&path, &["excluded_commands", "deny", "block", "exclude"])
            .unwrap_or(0)
    })
    .sum()
}

fn dashboard_command_usage_count(root: &Path, command: &str) -> Result<usize> {
    let roots = [
        root.join(".claude"),
        root.join(".codex"),
        root.join(".gemini"),
        root.join(".tsindex"),
    ];
    let needle = format!("tsindex {command}");
    let slash_needle = format!("/{command}");
    let dollar_needle = format!("${command}");
    let dashboard_needle = format!("\"command\":\"{command}\"");
    let mut count = 0;
    for path in roots {
        count += count_path_occurrences(
            &path,
            &[&needle, &slash_needle, &dollar_needle, &dashboard_needle],
        )?;
    }
    for path in [
        home_path(".claude/settings.json"),
        home_path(".claude.json"),
        home_path(".codex/config.toml"),
        home_path(".gemini/settings.json"),
        home_path(".gemini/settings.toml"),
    ] {
        count += count_file_occurrences(
            &path,
            &[&needle, &slash_needle, &dollar_needle, &dashboard_needle],
        )?;
    }
    Ok(count)
}

fn count_path_occurrences(path: &Path, needles: &[&str]) -> Result<usize> {
    // Use symlink_metadata so symlinks are neither followed nor counted; that
    // keeps a symlink cycle from looping forever during the recursive walk.
    let Ok(meta) = fs::symlink_metadata(path) else {
        return Ok(0);
    };
    if meta.file_type().is_symlink() {
        return Ok(0);
    }
    if meta.is_file() {
        return count_file_occurrences(path, needles);
    }
    if !meta.is_dir() {
        return Ok(0);
    }

    // Treat an unreadable directory as empty rather than failing the whole
    // dashboard render.
    let Ok(entries) = fs::read_dir(path) else {
        return Ok(0);
    };
    let mut count = 0;
    for entry in entries.flatten() {
        let child = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with('.') && name != ".tsindex" {
            continue;
        }
        if name == "target" || name == "node_modules" || name == ".git" {
            continue;
        }
        count += count_path_occurrences(&child, needles)?;
    }
    Ok(count)
}

fn count_file_occurrences(path: &Path, needles: &[&str]) -> Result<usize> {
    let Ok(raw) = fs::read_to_string(path) else {
        return Ok(0);
    };
    Ok(needles
        .iter()
        .map(|needle| raw.matches(needle).count())
        .sum())
}

fn file_mentions_tsindex(path: &Path) -> bool {
    fs::read_to_string(path)
        .map(|raw| raw.contains("tsindex"))
        .unwrap_or(false)
}

fn home_path(relative: &str) -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/"))
        .join(relative)
}

fn unix_timestamp_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn scalar_count(conn: &rusqlite::Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |row| row.get::<_, i64>(0))
        .unwrap_or(0)
}

fn dashboard_repo_details(conn: &rusqlite::Connection) -> Result<Vec<Value>> {
    let mut stmt = conn.prepare(
        r#"
        SELECT
          r.name,
          r.root_path,
          (SELECT COUNT(*) FROM files f WHERE f.repo_id = r.id) AS files,
          (SELECT COUNT(*) FROM symbols s JOIN files f ON f.id = s.file_id WHERE f.repo_id = r.id) AS symbols,
          (SELECT COUNT(*) FROM refs rf JOIN files f ON f.id = rf.file_id WHERE f.repo_id = r.id) AS refs,
          COALESCE((SELECT SUM(byte_size) FROM files f WHERE f.repo_id = r.id), 0) AS bytes
        FROM repos r
        ORDER BY r.name
        "#,
    )?;
    let rows = stmt
        .query_map([], |row| {
            Ok(json!({
                "name": row.get::<_, String>(0)?,
                "path": row.get::<_, String>(1)?,
                "files": row.get::<_, i64>(2)?,
                "symbols": row.get::<_, i64>(3)?,
                "refs": row.get::<_, i64>(4)?,
                "bytes": row.get::<_, i64>(5)?,
            }))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn dashboard_languages(conn: &rusqlite::Connection) -> Result<Vec<Value>> {
    let mut stmt = conn.prepare(
        r#"
        SELECT r.name, f.language, COUNT(DISTINCT f.id), COUNT(s.id)
        FROM files f
        JOIN repos r ON r.id = f.repo_id
        LEFT JOIN symbols s ON s.file_id = f.id
        GROUP BY r.name, f.language
        ORDER BY COUNT(DISTINCT f.id) DESC, f.language
        LIMIT 20
        "#,
    )?;
    let rows = stmt
        .query_map([], |row| {
            Ok(json!({
                "repo": row.get::<_, String>(0)?,
                "language": row.get::<_, String>(1)?,
                "files": row.get::<_, i64>(2)?,
                "symbols": row.get::<_, i64>(3)?,
            }))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn dashboard_symbol_kinds(conn: &rusqlite::Connection) -> Result<Vec<Value>> {
    let mut stmt = conn.prepare(
        r#"
        SELECT kind, COUNT(*)
        FROM symbols
        GROUP BY kind
        ORDER BY COUNT(*) DESC, kind
        LIMIT 12
        "#,
    )?;
    let rows = stmt
        .query_map([], |row| {
            Ok(json!({
                "kind": row.get::<_, String>(0)?,
                "count": row.get::<_, i64>(1)?,
            }))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn dashboard_largest_files(conn: &rusqlite::Connection) -> Result<Vec<Value>> {
    let mut stmt = conn.prepare(
        r#"
        SELECT
          r.name,
          f.path,
          f.language,
          f.byte_size,
          (SELECT COUNT(*) FROM symbols s WHERE s.file_id = f.id) AS symbols
        FROM files f
        JOIN repos r ON r.id = f.repo_id
        ORDER BY f.byte_size DESC
        LIMIT 12
        "#,
    )?;
    let rows = stmt
        .query_map([], |row| {
            Ok(json!({
                "repo": row.get::<_, String>(0)?,
                "path": row.get::<_, String>(1)?,
                "language": row.get::<_, String>(2)?,
                "bytes": row.get::<_, i64>(3)?,
                "symbols": row.get::<_, i64>(4)?,
            }))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn dashboard_html() -> &'static str {
    r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>tsindex Dashboard</title>
  <style>
    :root { color-scheme: dark; --bg:#070b14; --panel:#0f1728; --panel2:#121d31; --line:#26344d; --text:#e7eefc; --muted:#91a0ba; --accent:#7dd3fc; --green:#7cf6b2; --amber:#ffd166; --pink:#ff7ab6; }
    * { box-sizing: border-box; }
    body { margin: 0; min-height: 100vh; font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; color: var(--text); background: radial-gradient(circle at top left, rgba(125,211,252,.16), transparent 36rem), radial-gradient(circle at 80% 10%, rgba(255,122,182,.12), transparent 30rem), var(--bg); }
    header { position: sticky; top: 0; z-index: 2; display:flex; align-items:center; justify-content:space-between; gap:1rem; padding: 1rem 1.25rem; border-bottom:1px solid rgba(255,255,255,.08); background: rgba(7,11,20,.78); backdrop-filter: blur(18px); }
    h1 { margin:0; letter-spacing:.18em; font-size:1.05rem; }
    .version { color:var(--muted); font-size:.82rem; margin-top:.2rem; }
    .header-right { display:flex; align-items:center; gap:.85rem; flex-wrap:wrap; justify-content:flex-end; }
    .pill { border:1px solid var(--line); background:rgba(255,255,255,.04); padding:.45rem .7rem; border-radius:999px; color:var(--muted); font-size:.82rem; }
    .pill strong { color:var(--green); font-weight:700; }
    button { border:1px solid rgba(125,211,252,.45); background:rgba(125,211,252,.12); color:var(--text); border-radius:999px; padding:.5rem .8rem; cursor:pointer; font-weight:700; }
    main { width:min(1320px, 100%); margin:0 auto; padding:1.25rem; }
    .grid { display:grid; gap:1rem; }
    .cards { grid-template-columns: repeat(6, minmax(0, 1fr)); }
    .two { grid-template-columns: 1.1fr .9fr; margin-top:1rem; }
    .three { grid-template-columns: repeat(3, minmax(0, 1fr)); margin-top:1rem; }
    .card, .panel { border:1px solid rgba(255,255,255,.09); border-radius:22px; background:linear-gradient(180deg, rgba(255,255,255,.06), rgba(255,255,255,.025)); box-shadow: 0 18px 55px rgba(0,0,0,.25); }
    .card { padding:1rem; min-height:116px; }
    .label { color:var(--muted); text-transform:uppercase; letter-spacing:.12em; font-size:.7rem; }
    .value { font-size:2rem; font-weight:800; margin-top:.55rem; }
    .sub { color:var(--muted); font-size:.82rem; margin-top:.35rem; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
    .panel { padding:1rem; overflow:hidden; }
    .panel h2 { font-size:.95rem; margin:.1rem 0 1rem; letter-spacing:.08em; text-transform:uppercase; color:#c9d7ee; }
    .bars { display:grid; gap:.65rem; }
    .bar-row { display:grid; grid-template-columns:minmax(7rem, 1fr) 3fr auto; gap:.7rem; align-items:center; color:var(--muted); font-size:.82rem; }
    .bar { height:.7rem; border-radius:999px; overflow:hidden; background:rgba(255,255,255,.08); }
    .bar > span { display:block; height:100%; border-radius:inherit; background:linear-gradient(90deg, var(--accent), var(--green)); }
    .metric-list { display:grid; gap:.75rem; }
    .metric-row { display:flex; align-items:flex-start; justify-content:space-between; gap:1rem; color:var(--muted); font-size:.84rem; }
    .metric-row strong { color:var(--text); font-size:.98rem; }
    .dot { display:inline-block; width:.55rem; height:.55rem; border-radius:999px; margin-right:.45rem; background:var(--line); }
    .dot.on { background:var(--green); box-shadow:0 0 18px rgba(124,246,178,.35); }
    .tiny { color:var(--muted); font-size:.74rem; line-height:1.35; }
    table { width:100%; border-collapse:collapse; font-size:.84rem; }
    th, td { padding:.72rem .55rem; border-bottom:1px solid rgba(255,255,255,.07); text-align:left; vertical-align:top; }
    th { color:var(--muted); font-size:.68rem; letter-spacing:.1em; text-transform:uppercase; font-weight:800; }
    th.num, td.num { text-align:right; }
    td.num { font-variant-numeric: tabular-nums; }
    #global-summary { margin-bottom:1rem; padding-bottom:1rem; border-bottom:1px solid rgba(255,255,255,.07); }
    code { color:#bfdbfe; word-break:break-all; }
    .empty { color:var(--muted); padding:1rem; border:1px dashed var(--line); border-radius:16px; }
    footer { color:var(--muted); padding:1rem 1.25rem 2rem; display:flex; justify-content:center; gap:1rem; }
    @media (max-width: 1050px) { .cards { grid-template-columns: repeat(3, minmax(0, 1fr)); } .two, .three { grid-template-columns:1fr; } }
    @media (max-width: 620px) { header { align-items:flex-start; flex-direction:column; } .cards { grid-template-columns: repeat(2, minmax(0, 1fr)); } .value { font-size:1.55rem; } }
  </style>
</head>
<body>
  <header>
    <div><h1>TSINDEX</h1><div class="version" id="version">tree-sitter code intelligence</div></div>
    <div class="header-right"><button id="refresh">Refresh</button><div class="pill">Status <strong id="status">Loading</strong></div><div class="pill" id="updated">Updated —</div></div>
  </header>
  <main>
    <section class="grid cards" id="cards"></section>
    <section class="grid two">
      <div class="panel"><h2>Language Breakdown</h2><div class="bars" id="languages"></div></div>
      <div class="panel"><h2>Symbol Kinds</h2><div class="bars" id="kinds"></div></div>
    </section>
    <section class="grid three">
      <div class="panel"><h2>Repositories</h2><div id="repos"></div></div>
      <div class="panel"><h2>Largest Indexed Files</h2><div id="files"></div></div>
      <div class="panel"><h2>API Surface</h2><div class="bars" id="api"></div></div>
    </section>
    <section class="grid three">
      <div class="panel"><h2>Adoption</h2><div id="adoption"></div></div>
      <div class="panel"><h2>Configuration</h2><div id="configuration"></div></div>
      <div class="panel"><h2>Economics</h2><div id="economics"></div></div>
    </section>
    <section class="grid">
      <div class="panel"><h2>Global Savings · All Repos &amp; Sessions</h2><div class="metric-list" id="global-summary"></div><div id="global-savings"></div></div>
    </section>
    <section class="grid two">
      <div class="panel"><h2>Meta-command Usage</h2><div class="bars" id="features"></div></div>
      <div class="panel"><h2>Tracking Notes</h2><div class="metric-list" id="notes"></div></div>
    </section>
  </main>
  <footer><span>Press R to refresh</span><span>•</span><span id="db-path"></span></footer>
  <script>
    const fmt = new Intl.NumberFormat();
    const bytes = n => { n = Number(n || 0); const u = ['B','KB','MB','GB']; let i = 0; while (n >= 1024 && i < u.length - 1) { n /= 1024; i++; } return `${n.toFixed(i ? 1 : 0)} ${u[i]}`; };
    const cell = (label, value, sub = '') => `<article class="card"><div class="label">${label}</div><div class="value">${value}</div><div class="sub">${sub}</div></article>`;
    const esc = value => String(value ?? '').replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
    function bars(id, rows, label, value, color) {
      const el = document.getElementById(id);
      const max = Math.max(1, ...rows.map(value));
      el.innerHTML = rows.length ? rows.map((row, i) => `<div class="bar-row"><div title="${esc(label(row))}">${esc(label(row))}</div><div class="bar"><span style="width:${Math.max(3, value(row) / max * 100)}%; ${color ? `background:${color(i)}` : ''}"></span></div><div>${fmt.format(value(row))}</div></div>`).join('') : '<div class="empty">No indexed data yet. Run a build or rebuild a repo.</div>';
    }
    function table(id, cols, rows) {
      document.getElementById(id).innerHTML = rows.length ? `<table><thead><tr>${cols.map(c => `<th class="${c.num ? 'num' : ''}">${c.h}</th>`).join('')}</tr></thead><tbody>${rows.map(r => `<tr>${cols.map(c => `<td class="${c.num ? 'num' : ''}">${c.f(r)}</td>`).join('')}</tr>`).join('')}</tbody></table>` : '<div class="empty">No rows to show.</div>';
    }
    const metric = (label, value, sub = '') => `<div class="metric-row"><div>${label}${sub ? `<div class="tiny">${sub}</div>` : ''}</div><strong>${value}</strong></div>`;
    const money = n => `$${Number(n || 0).toFixed(2)}`;
    async function load() {
      const res = await fetch('/dashboard/data', { cache: 'no-store' });
      if (!res.ok) throw new Error(await res.text());
      const data = await res.json();
      const t = data.totals || {};
      const adoption = data.adoption || {};
      const config = data.configuration || {};
      const economics = data.economics || {};
      document.getElementById('status').textContent = data.ok ? 'Healthy' : 'Degraded';
      document.getElementById('version').textContent = `v${data.version} · tree-sitter code intelligence`;
      document.getElementById('updated').textContent = `Updated ${new Date().toLocaleTimeString()}`;
      document.getElementById('db-path').textContent = data.db_path || '';
      document.getElementById('cards').innerHTML = [
        cell('Repos', fmt.format(t.repos), 'configured workspaces'),
        cell('Files', fmt.format(t.files), 'indexed source files'),
        cell('Symbols', fmt.format(t.symbols), 'navigable definitions'),
        cell('References', fmt.format(t.refs), 'syntactic references'),
        cell('Bytes', bytes(t.bytes), 'indexed source size'),
        cell('Projected value', money(economics.estimated_usd_saved), 'heuristic, not observed billing'),
      ].join('');
      bars('languages', data.languages || [], r => `${r.language} · ${r.repo}`, r => Number(r.files || 0));
      bars('kinds', data.symbol_kinds || [], r => r.kind, r => Number(r.count || 0), i => `linear-gradient(90deg, ${i % 2 ? 'var(--pink)' : 'var(--amber)'}, var(--accent))`);
      table('repos', [
        {h:'Repo', f:r=>`<strong>${esc(r.name)}</strong><div class="sub">${esc(r.path)}</div>`},
        {h:'Files', num:true, f:r=>fmt.format(r.files || 0)},
        {h:'Symbols', num:true, f:r=>fmt.format(r.symbols || 0)},
      ], data.repos || []);
      table('files', [
        {h:'Path', f:r=>`<code>${esc(r.path)}</code><div class="sub">${esc(r.repo)} · ${esc(r.language)}</div>`},
        {h:'Size', num:true, f:r=>bytes(r.bytes)},
        {h:'Symbols', num:true, f:r=>fmt.format(r.symbols || 0)},
      ], data.largest_files || []);
      bars('api', [
        {name:'get_symbol', count:1}, {name:'list_file_outline', count:1}, {name:'find_references', count:1}, {name:'enclosing_symbol', count:1}, {name:'query', count:1},
      ], r => r.name, r => r.count);
      const rtk = adoption.rtk || {};
      document.getElementById('adoption').innerHTML = `<div class="metric-list">${(adoption.agent_hooks || []).map(h => `<div class="metric-row"><div><span class="dot ${h.detected ? 'on' : ''}"></span>${esc(h.agent)}</div><strong>${h.detected ? 'hooked' : 'missing'}</strong></div>`).join('')}${metric('RTK', rtk.detected ? 'detected' : 'available', `<code>${esc(rtk.mcp_command || '')}</code>`)}${metric('Custom TOML filters', fmt.format(adoption.custom_toml_filters || 0), 'language, ignore, and grammar filters')}${metric('Integration coverage', `${fmt.format(adoption.coverage_percent || 0)}%`, 'claude/gemini/codex hook detection')}</div>`;
      document.getElementById('configuration').innerHTML = `<div class="metric-list">${metric('config.toml', config.config_exists ? 'present' : 'missing', esc(config.config_path || ''))}${metric('Excluded commands', fmt.format(config.excluded_commands || 0), 'agent permission/config rules')}${metric('Projects', fmt.format(config.project_count || 0), 'configured catalog repos')}${metric('Repo ignore patterns', fmt.format(config.repo_ignore_patterns || 0))}${metric('Language filters', fmt.format(config.language_filters || 0))}</div>`;
      const realized = economics.realized || {};
      document.getElementById('economics').innerHTML = `<div class="metric-list">${metric('Usage-tally estimate', money(realized.usd_saved), `${fmt.format(realized.saved_tokens || 0)} token-equivalent estimate over ${fmt.format(realized.calls || 0)} calls · heuristic source-range counterfactual, not billing`)}${metric('Codebase projection', money(economics.estimated_usd_saved), `${fmt.format(economics.estimated_saved_tokens || 0)} token-equivalent estimate from indexed bytes`)}${metric('Indexed token basis', fmt.format(economics.estimated_indexed_tokens || 0), 'bytes ÷ 4 chars/token')}${metric('Heuristic savings rate', `${((economics.savings_rate || 0) * 100).toFixed(1)}%`, esc(economics.evidence || ''))}${metric('Notional price basis', `${money(economics.input_usd_per_million_tokens)}/1M`, 'for projection only; no observed bill')}</div>`;
      const global = economics.global || {};
      document.getElementById('global-summary').innerHTML = `${metric('Global tally estimate', money(global.total_usd_saved), `heuristic across ${fmt.format(global.repo_count || 0)} repos`)}${metric('Tool calls', fmt.format(global.total_calls || 0), `${fmt.format(global.total_sessions || 0)} sessions total`)}`;
      table('global-savings', [
        {h:'Repo', f:r=>`<code>${esc(r.repo)}</code>`},
        {h:'USD saved', num:true, f:r=>money(r.usd_saved)},
        {h:'Tokens', num:true, f:r=>fmt.format(r.saved_tokens || 0)},
        {h:'Calls', num:true, f:r=>fmt.format(r.calls || 0)},
        {h:'Sessions', num:true, f:r=>fmt.format(r.sessions || 0)},
      ], global.repos || []);
      bars('features', data.features || [], r => `/${r.command} → ${r.tool}`, r => Number(r.usage_count || 0));
      document.getElementById('notes').innerHTML = `<div class="metric-list">${metric('Adoption', 'hooks + TOML', 'Tracks AI agent coverage and custom DSL/filter uptake.')}${metric('Configuration', 'maturity', 'Shows config presence, excluded commands, and project count.')}${metric('Features', 'usage counts', 'Counts locally recorded meta-command/tool invocations where available.')}${metric('Economics', 'estimated', 'Uses fixed heuristic assumptions and notional input-token pricing.')}</div>`;
    }
    document.getElementById('refresh').addEventListener('click', () => load().catch(showError));
    addEventListener('keydown', e => { if (e.key.toLowerCase() === 'r') load().catch(showError); });
    function showError(error) { document.getElementById('status').textContent = 'Error'; document.getElementById('cards').innerHTML = `<article class="card" style="grid-column:1/-1"><div class="label">Error</div><div class="sub">${esc(error.message)}</div></article>`; }
    load().catch(showError);
  </script>
</body>
</html>"#
}

fn handle_repos_rebuild(runtime: &mut Runtime, body: &[u8], stream: &mut TcpStream) -> Result<()> {
    let request: RebuildRequest = match serde_json::from_slice(body) {
        Ok(r) => r,
        Err(e) => {
            return write_http_response(
                stream,
                400,
                json!({ "error": format!("invalid request: {e:#}") }),
            );
        }
    };

    if !is_safe_repo_name(&request.repo) {
        return write_http_response(
            stream,
            400,
            json!({ "error": "invalid repo name: must not be empty, '.', '..', or contain path separators" }),
        );
    }

    let workspaces = runtime.workspaces()?;
    let Some(workspace) = workspaces.into_iter().find(|w| w.name == request.repo) else {
        return write_http_response(
            stream,
            404,
            json!({ "error": format!("repo {} not found", request.repo) }),
        );
    };

    if request.fetch {
        let output = Command::new("git")
            .args(["fetch", "--depth", "1", "origin"])
            .current_dir(&workspace.root)
            .output()
            .with_context(|| format!("failed to run git fetch in {}", workspace.root.display()))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return write_http_internal_error(stream, &anyhow!("git fetch failed: {stderr}"));
        }

        let output = Command::new("git")
            .args(["checkout", "FETCH_HEAD"])
            .current_dir(&workspace.root)
            .output()
            .with_context(|| {
                format!("failed to run git checkout in {}", workspace.root.display())
            })?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return write_http_internal_error(stream, &anyhow!("git checkout failed: {stderr}"));
        }
    }

    let start = Instant::now();
    let stats = match runtime.build(true, Some(&request.repo)) {
        Ok(s) => s,
        Err(e) => {
            return write_http_internal_error(stream, &e);
        }
    };
    let duration_ms = start.elapsed().as_millis() as u64;

    write_http_response(
        stream,
        200,
        json!({
            "ok": true,
            "stats": {
                "indexed": stats.indexed,
                "skipped": stats.skipped,
                "failed": stats.failed,
            },
            "duration_ms": duration_ms,
        }),
    )
}

fn handle_repos_add(runtime: &mut Runtime, body: &[u8], stream: &mut TcpStream) -> Result<()> {
    let request: AddRepoRequest = match serde_json::from_slice(body) {
        Ok(r) => r,
        Err(e) => {
            return write_http_response(
                stream,
                400,
                json!({ "error": format!("invalid request: {e:#}") }),
            );
        }
    };

    if !is_safe_repo_name(&request.repo) {
        return write_http_response(
            stream,
            400,
            json!({ "error": "invalid repo name: must not be empty, '.', '..', or contain path separators" }),
        );
    }

    if !is_safe_clone_url(&request.clone_url) {
        return write_http_response(
            stream,
            400,
            json!({ "error": "invalid clone_url: must be https://github.com/<owner>/<repo-name>.git" }),
        );
    }

    if runtime.workspaces()?.iter().any(|w| w.name == request.repo) && !request.force {
        return write_http_response(
            stream,
            400,
            json!({ "error": format!("repo {} already exists", request.repo) }),
        );
    }

    let clone_dir = runtime.root.join(&request.repo);
    if !clone_dir.starts_with(&runtime.root) || clone_dir == runtime.root {
        return write_http_response(
            stream,
            400,
            json!({ "error": "invalid repo name: resolved path escapes catalog root" }),
        );
    }
    if clone_dir.exists() {
        if !request.force {
            return write_http_response(
                stream,
                409,
                json!({ "error": format!("clone directory already exists for {}. Use force=true to replace.", request.repo) }),
            );
        }
        if let Err(e) = std::fs::remove_dir_all(&clone_dir) {
            return write_http_internal_error(
                stream,
                &anyhow!("failed to remove stale clone: {e}"),
            );
        }
    }

    let output = Command::new("git")
        .args([
            "clone",
            "--depth",
            "1",
            "--branch",
            &request.branch,
            "--",
            &request.clone_url,
            &clone_dir.to_string_lossy(),
        ])
        .output()
        .context("failed to run git clone")?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return write_http_internal_error(stream, &anyhow!("git clone failed: {stderr}"));
    }

    let mut config = runtime.config.clone();
    config.repos.retain(|r| r.name != request.repo);
    config.repos.push(RepoConfig {
        name: request.repo.clone(),
        path: clone_dir.to_string_lossy().to_string(),
        languages: Vec::new(),
        ignore: Vec::new(),
    });

    let updated_runtime = Runtime::new(
        runtime.root.clone(),
        runtime.db_path.clone(),
        config,
        runtime.languages.clone(),
    );

    let start = Instant::now();
    let stats = match updated_runtime.build(false, Some(&request.repo)) {
        Ok(s) => s,
        Err(e) => {
            return write_http_internal_error(stream, &e);
        }
    };
    let duration_ms = start.elapsed().as_millis() as u64;

    if let Err(e) = updated_runtime.config.write(&runtime.root) {
        return write_http_internal_error(stream, &anyhow!("failed to write config: {e:#}"));
    }
    runtime.config = updated_runtime.config;

    write_http_response(
        stream,
        200,
        json!({
            "ok": true,
            "repo": request.repo,
            "stats": {
                "indexed": stats.indexed,
                "skipped": stats.skipped,
                "failed": stats.failed,
            },
            "duration_ms": duration_ms,
        }),
    )
}

fn handle_repos_remove(runtime: &mut Runtime, body: &[u8], stream: &mut TcpStream) -> Result<()> {
    let request: RemoveRepoRequest = match serde_json::from_slice(body) {
        Ok(r) => r,
        Err(e) => {
            return write_http_response(
                stream,
                400,
                json!({ "error": format!("invalid request: {e:#}") }),
            );
        }
    };

    if !is_safe_repo_name(&request.repo) {
        return write_http_response(
            stream,
            400,
            json!({ "error": "invalid repo name: must not be empty, '.', '..', or contain path separators" }),
        );
    }

    let Some(repo_config) = runtime.config.repos.iter().find(|r| r.name == request.repo) else {
        return write_http_response(
            stream,
            404,
            json!({ "error": format!("repo {} not found in catalog", request.repo) }),
        );
    };

    let clone_dir = {
        let p = std::path::PathBuf::from(&repo_config.path);
        if p.is_absolute() {
            p
        } else {
            runtime.root.join(&p)
        }
    };
    if !clone_dir.starts_with(&runtime.root) || clone_dir == runtime.root {
        return write_http_response(
            stream,
            400,
            json!({ "error": "invalid repo config: resolved path escapes catalog root" }),
        );
    }

    // Delete the directory BEFORE writing config so that if deletion fails,
    // config is unchanged and the operation is retryable.
    if clone_dir.exists()
        && let Err(e) = std::fs::remove_dir_all(&clone_dir)
    {
        return write_http_internal_error(stream, &anyhow!("failed to remove repo directory: {e}"));
    }

    runtime.config.repos.retain(|r| r.name != request.repo);
    if let Err(e) = runtime.config.write(&runtime.root) {
        return write_http_internal_error(stream, &anyhow!("failed to write config: {e:#}"));
    }

    write_http_response(stream, 200, json!({ "ok": true, "removed": request.repo }))
}

fn is_safe_repo_name(name: &str) -> bool {
    !name.is_empty() && name != "." && name != ".." && !name.contains('/') && !name.contains('\\')
}

fn is_safe_clone_url(url: &str) -> bool {
    let Some(path) = url.strip_prefix("https://github.com/") else {
        return false;
    };
    let Some((owner, repository)) = path.split_once('/') else {
        return false;
    };
    let name = repository.strip_suffix(".git").unwrap_or(repository);
    !owner.is_empty()
        && !owner.starts_with('-')
        && !owner.ends_with('-')
        && owner
            .bytes()
            .all(|character| character.is_ascii_alphanumeric() || character == b'-')
        && !name.is_empty()
        && !name.starts_with('.')
        && name.bytes().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, b'-' | b'_' | b'.')
        })
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RebuildRequest {
    repo: String,
    #[serde(default)]
    fetch: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AddRepoRequest {
    repo: String,
    clone_url: String,
    #[serde(default = "default_branch")]
    branch: String,
    #[serde(default)]
    force: bool,
}

#[derive(Debug, Deserialize)]
struct RemoveRepoRequest {
    repo: String,
}

fn default_branch() -> String {
    "main".to_string()
}

#[derive(Debug, Deserialize)]
struct HttpCallRequest {
    name: String,
    #[serde(default)]
    arguments: Option<Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::tempdir;

    use crate::config::{TsIndexConfig, db_path};

    fn tool_names(list: &Value) -> Vec<String> {
        list["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|tool| tool["name"].as_str().unwrap().to_string())
            .collect()
    }

    #[test]
    fn replace_symbol_is_exposed_only_on_the_writable_transport() {
        assert!(
            tool_names(&tool_list(true)).contains(&"replace_symbol".to_string()),
            "MCP/stdio transport should advertise replace_symbol"
        );
        assert!(
            !tool_names(&tool_list(false)).contains(&"replace_symbol".to_string()),
            "read-only HTTP transport must not advertise replace_symbol"
        );
    }

    #[test]
    fn find_references_mcp_schema_requires_names_array() {
        let tools = tool_list(true);
        let find_references = tools["tools"]
            .as_array()
            .unwrap()
            .iter()
            .find(|tool| tool["name"] == "find_references")
            .expect("find_references should be advertised");
        let schema = &find_references["inputSchema"];

        assert_eq!(schema["required"], json!(["names"]));
        assert!(schema.get("anyOf").is_none());
        assert!(schema["properties"].get("name").is_none());
        assert_eq!(schema["properties"]["names"]["type"], "array");
        assert_eq!(schema["properties"]["names"]["minItems"], 1);
    }

    #[test]
    fn advertised_tool_schemas_avoid_top_level_unions() {
        for tool in tool_list(true)["tools"].as_array().unwrap() {
            let schema = &tool["inputSchema"];
            let name = tool["name"].as_str().unwrap_or("<unknown>");

            assert_eq!(
                schema["type"], "object",
                "{name} must advertise an object schema"
            );
            assert!(
                schema["properties"].is_object(),
                "{name} must advertise object properties"
            );

            for keyword in ["oneOf", "allOf", "anyOf"] {
                assert!(
                    schema.get(keyword).is_none(),
                    "{name} uses unsupported top-level {keyword}"
                );
            }
        }
    }

    #[test]
    fn selector_omissions_are_reported_as_invalid_params() -> Result<()> {
        let dir = tempdir()?;
        let runtime = Runtime::new(
            dir.path().to_path_buf(),
            db_path(dir.path()),
            TsIndexConfig::default(),
            Vec::new(),
        );
        runtime.build(false, None)?;

        for (name, arguments, expected) in [
            (
                "get_symbol",
                json!({}),
                "requires `name` or a non-empty `names` array",
            ),
            (
                "get_symbol",
                json!({ "names": [] }),
                "requires `name` or a non-empty `names` array",
            ),
            (
                "enclosing_symbol",
                json!({ "file": "src/lib.rs" }),
                "requires `row` or a non-empty `rows` array",
            ),
            (
                "enclosing_symbol",
                json!({ "file": "src/lib.rs", "rows": [] }),
                "requires `row` or a non-empty `rows` array",
            ),
            (
                "enclosing_symbol",
                json!({ "file": "src/lib.rs", "row": 0 }),
                "rows are 1-based; received 0",
            ),
            (
                "enclosing_symbol",
                json!({ "file": "src/lib.rs", "rows": [1, 0] }),
                "rows are 1-based; received 0",
            ),
        ] {
            let response = handle_request(
                &runtime,
                json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "tools/call",
                    "params": { "name": name, "arguments": arguments }
                }),
            )?
            .expect("tool call should return a JSON-RPC response");

            assert_eq!(response["error"]["code"], -32602, "tool: {name}");
            assert!(
                response["error"]["message"]
                    .as_str()
                    .unwrap_or_default()
                    .contains(expected),
                "unexpected response for {name}: {response}"
            );
        }
        Ok(())
    }

    /// The advertised schemas all set `additionalProperties: false`, but serde
    /// is the only thing enforcing it at runtime. Without
    /// `deny_unknown_fields` a misnamed filter (`file` instead of `file_glob`)
    /// was silently dropped, turning a one-file query into a whole-repo scan —
    /// the exact token blowup these tools exist to avoid. Fail loudly instead.
    #[test]
    fn tool_args_reject_unknown_fields_instead_of_widening_the_search() {
        let err = serde_json::from_value::<QueryArgs>(json!({
            "language": "rust",
            "query": "(struct_item) @s",
            "file": "src/mcp.rs",
        }))
        .expect_err("a misnamed filter must not be silently ignored");
        assert!(
            err.to_string().contains("unknown field `file`"),
            "unexpected error: {err}"
        );

        let err = serde_json::from_value::<OutlineArgs>(json!({
            "file": "src/mcp.rs",
            "paths": ["src/mcp.rs"],
        }))
        .expect_err("a misnamed batch arg must not be silently ignored");
        assert!(
            err.to_string().contains("unknown field `paths`"),
            "unexpected error: {err}"
        );
    }

    /// A bad argument is the caller's mistake, so it must come back as
    /// `-32602` (invalid params), not `-32603` (internal error) — otherwise a
    /// client can't tell "I passed the wrong thing" from "the server broke"
    /// and retries a call that will never succeed. Runtime faults keep
    /// `-32603`; see `tool_failures_return_json_rpc_errors`.
    #[test]
    fn unknown_argument_is_reported_as_invalid_params() -> Result<()> {
        let dir = tempdir()?;
        let runtime = Runtime::new(
            dir.path().to_path_buf(),
            db_path(dir.path()),
            TsIndexConfig::default(),
            Vec::new(),
        );
        runtime.build(false, None)?;

        let response = handle_request(
            &runtime,
            json!({
                "jsonrpc": "2.0",
                "id": 9,
                "method": "tools/call",
                "params": {
                    "name": "query",
                    "arguments": {
                        "language": "rust",
                        "query": "(struct_item) @s",
                        "file": "src/mcp.rs",
                    }
                }
            }),
        )?
        .expect("tool call should return a JSON-RPC response");

        assert_eq!(response["error"]["code"], -32602);
        let message = response["error"]["message"].as_str().unwrap_or_default();
        assert!(
            message.contains("unknown field `file`"),
            "unexpected message: {message}"
        );
        // The valid names ride along, so a client that guessed can self-correct
        // on the next call instead of guessing again.
        assert!(
            message.contains("file_glob"),
            "error should list the valid field names: {message}"
        );
        Ok(())
    }

    #[test]
    fn find_references_mcp_rejects_legacy_name_and_accepts_names() -> Result<()> {
        let dir = tempdir()?;
        let source = dir.path().join("src").join("lib.rs");
        fs::create_dir_all(source.parent().unwrap())?;
        fs::write(&source, "fn needle() {}\nfn caller() { needle(); }\n")?;

        let runtime = Runtime::new(
            dir.path().to_path_buf(),
            db_path(dir.path()),
            TsIndexConfig::default(),
            vec!["rust".to_string()],
        );
        runtime.build(false, None)?;

        let legacy_name = handle_request(
            &runtime,
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": "find_references",
                    "arguments": { "name": "needle" }
                }
            }),
        )?
        .expect("tool call should return a JSON-RPC response");
        assert_eq!(legacy_name["error"]["code"], -32602);
        assert!(
            legacy_name["error"]["message"]
                .as_str()
                .unwrap_or_default()
                .contains("`names` only")
        );

        let names = handle_request(
            &runtime,
            json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "find_references",
                    "arguments": { "names": ["needle"] }
                }
            }),
        )?
        .expect("tool call should return a JSON-RPC response");
        assert!(names.get("error").is_none());
        Ok(())
    }

    #[test]
    fn dashboard_reports_rtk_mcp_command() -> Result<()> {
        let dir = tempdir()?;
        let runtime = Runtime::new(
            dir.path().to_path_buf(),
            db_path(dir.path()),
            TsIndexConfig::default(),
            Vec::new(),
        );

        let adoption = dashboard_adoption(&runtime);
        // detection depends on the runner's $HOME (~/.rtk etc.), so just assert the shape
        assert!(adoption["rtk"]["detected"].is_boolean());
        assert!(
            adoption["rtk"]["mcp_command"]
                .as_str()
                .unwrap()
                .contains("serve --mcp")
        );
        Ok(())
    }

    #[test]
    fn dashboard_detects_repo_local_rtk_config() -> Result<()> {
        let dir = tempdir()?;
        fs::write(dir.path().join("rtk.toml"), "# rtk-ai\n")?;

        let rtk = dashboard_rtk_integration(dir.path());
        assert_eq!(rtk["detected"], true);
        Ok(())
    }

    #[test]
    fn replace_symbol_is_rejected_on_the_read_only_transport() -> Result<()> {
        let dir = tempdir()?;
        let runtime = Runtime::new(
            dir.path().to_path_buf(),
            db_path(dir.path()),
            TsIndexConfig::default(),
            Vec::new(),
        );

        let error = call_tool(
            &runtime,
            "replace_symbol",
            json!({ "file": "x.py", "name": "f", "new_body": "" }),
            false,
        )
        .expect_err("read-only transport must refuse replace_symbol");
        assert!(format!("{error:#}").contains("MCP/stdio"));
        Ok(())
    }

    #[test]
    fn tool_failures_return_json_rpc_errors() -> Result<()> {
        let dir = tempdir()?;
        let runtime = Runtime::new(
            dir.path().to_path_buf(),
            db_path(dir.path()),
            TsIndexConfig::default(),
            Vec::new(),
        );
        runtime.initialize()?;

        let response = handle_request(
            &runtime,
            json!({
                "jsonrpc": "2.0",
                "id": 7,
                "method": "tools/call",
                "params": {
                    "name": "list_file_outline",
                    "arguments": { "file": "src/lib.rs" }
                }
            }),
        )?
        .expect("tool call should return a JSON-RPC response");

        assert_eq!(response["id"], 7);
        assert_eq!(response["error"]["code"], -32603);
        assert!(
            response["error"]["message"]
                .as_str()
                .unwrap_or_default()
                .contains("index is not ready")
        );
        Ok(())
    }

    #[test]
    fn missing_indexed_file_still_returns_specific_error() -> Result<()> {
        let dir = tempdir()?;
        let runtime = Runtime::new(
            dir.path().to_path_buf(),
            db_path(dir.path()),
            TsIndexConfig::default(),
            Vec::new(),
        );
        runtime.build(false, None)?;

        let response = handle_request(
            &runtime,
            json!({
                "jsonrpc": "2.0",
                "id": 8,
                "method": "tools/call",
                "params": {
                    "name": "list_file_outline",
                    "arguments": { "file": "src/lib.rs" }
                }
            }),
        )?
        .expect("tool call should return a JSON-RPC response");

        assert_eq!(response["error"]["code"], -32603);
        assert!(
            response["error"]["message"]
                .as_str()
                .unwrap_or_default()
                .contains("file src/lib.rs not found in index")
        );
        Ok(())
    }

    #[test]
    fn malformed_json_returns_parse_error() -> Result<()> {
        let dir = tempdir()?;
        let runtime = Runtime::new(
            dir.path().to_path_buf(),
            db_path(dir.path()),
            TsIndexConfig::default(),
            Vec::new(),
        );

        let response = handle_json_rpc_line(&runtime, "{not json")?
            .expect("malformed JSON should return a JSON-RPC response");

        assert_eq!(response["id"], Value::Null);
        assert_eq!(response["error"]["code"], -32700);
        assert!(
            response["error"]["message"]
                .as_str()
                .unwrap_or_default()
                .contains("invalid JSON-RPC message")
        );
        Ok(())
    }

    #[test]
    fn repos_list_returns_repos_on_fresh_catalog() -> Result<()> {
        let dir = tempdir()?;
        let runtime = Runtime::new(
            dir.path().to_path_buf(),
            db_path(dir.path()),
            TsIndexConfig::default(),
            Vec::new(),
        );
        runtime.initialize()?;
        let result = handle_repos_list(&runtime)?;
        let repos = result["repos"].as_array().unwrap();
        // A fresh default-config catalog has one implicit workspace (the root dir)
        assert_eq!(repos.len(), 1);
        assert!(!repos[0]["indexed"].as_bool().unwrap());
        Ok(())
    }

    #[test]
    fn missing_method_returns_invalid_request() -> Result<()> {
        let dir = tempdir()?;
        let runtime = Runtime::new(
            dir.path().to_path_buf(),
            db_path(dir.path()),
            TsIndexConfig::default(),
            Vec::new(),
        );

        let response = handle_request(&runtime, json!({ "jsonrpc": "2.0", "id": 9 }))?
            .expect("missing method should return a JSON-RPC response");

        assert_eq!(response["id"], 9);
        assert_eq!(response["error"]["code"], -32600);
        assert_eq!(response["error"]["message"], "missing method");
        Ok(())
    }

    #[test]
    fn safe_clone_url_accepts_github_https() {
        assert!(is_safe_clone_url(
            "https://github.com/example-org/example-repo.git"
        ));
    }

    #[test]
    fn safe_clone_url_accepts_without_git_suffix() {
        assert!(is_safe_clone_url(
            "https://github.com/example-org/example-repo"
        ));
    }

    #[test]
    fn safe_clone_url_accepts_multiple_owners() {
        assert!(is_safe_clone_url("https://github.com/example-org/repo.git"));
        assert!(is_safe_clone_url(
            "https://github.com/example-user/repo.git"
        ));
    }

    #[test]
    fn safe_clone_url_rejects_invalid_owners_and_authorities() {
        for url in [
            "https://github.com//repo.git",
            "https://github.com/../repo.git",
            "https://github.com/%2e%2e/repo.git",
            "https://github.com/-owner/repo.git",
            "https://github.com/owner-/repo.git",
            "https://github.com/owner_name/repo.git",
            "https://github.com/example-org/repo.git?token=example",
            "https://github.com/example-org/repo.git#fragment",
            "https://github.com@example.com/owner/repo.git",
            "https://user:password@github.com/owner/repo.git",
            "https://github.com.example.com/owner/repo.git",
            "https://github.com:443/owner/repo.git",
            "http://github.com/owner/repo.git",
        ] {
            assert!(!is_safe_clone_url(url), "accepted invalid URL: {url}");
        }
    }

    #[test]
    fn safe_clone_url_rejects_path_traversal() {
        assert!(!is_safe_clone_url(
            "https://github.com/example-org/../../another-org/repo.git"
        ));
    }

    #[test]
    fn safe_clone_url_rejects_extra_path_segments() {
        assert!(!is_safe_clone_url(
            "https://github.com/example-org/repo/extra.git"
        ));
    }

    #[test]
    fn safe_clone_url_rejects_encoded_traversal() {
        assert!(!is_safe_clone_url(
            "https://github.com/example-org/%2e%2e.git"
        ));
    }

    #[test]
    fn safe_clone_url_rejects_bare_prefix() {
        assert!(!is_safe_clone_url("https://github.com/"));
        assert!(!is_safe_clone_url("https://github.com/example-org"));
        assert!(!is_safe_clone_url("https://github.com/example-org/"));
        assert!(!is_safe_clone_url("https://github.com/example-org/.git"));
    }

    #[test]
    fn safe_clone_url_rejects_dot_prefixed_name() {
        assert!(!is_safe_clone_url(
            "https://github.com/example-org/.hidden-repo.git"
        ));
        assert!(!is_safe_clone_url("https://github.com/example-org/..git"));
    }

    #[test]
    fn safe_clone_url_rejects_ssh() {
        assert!(!is_safe_clone_url("git@github.com:example-org/repo.git"));
    }

    #[test]
    fn safe_clone_url_rejects_ext_transport() {
        assert!(!is_safe_clone_url("ext::sh -c evil%"));
    }

    #[test]
    fn safe_clone_url_rejects_file_protocol() {
        assert!(!is_safe_clone_url("file:///etc/passwd"));
    }

    #[test]
    fn safe_clone_url_rejects_empty() {
        assert!(!is_safe_clone_url(""));
    }

    #[test]
    fn rebuild_request_rejects_invalid_repo_name() {
        let body = serde_json::to_vec(&json!({ "repo": "../escape" })).unwrap();
        let request: RebuildRequest = serde_json::from_slice(&body).unwrap();
        assert!(request.repo.contains(".."));
    }

    #[test]
    fn add_request_deserializes_with_defaults() {
        let body = serde_json::to_vec(&json!({
            "repo": "myrepo",
            "clone_url": "https://github.com/org/repo.git"
        }))
        .unwrap();
        let request: AddRepoRequest = serde_json::from_slice(&body).unwrap();
        assert_eq!(request.branch, "main");
    }

    #[test]
    fn allowed_host_accepts_localhost_aliases_on_bound_port() {
        // Anything that round-trips back to the loopback interface on
        // the server's bound port is fair game. Anything else is a
        // DNS-rebinding attempt or a misconfigured client.
        let port = 7337;
        assert!(is_allowed_host(Some("127.0.0.1:7337"), port, &[]));
        assert!(is_allowed_host(Some("localhost:7337"), port, &[]));
        assert!(is_allowed_host(Some("[::1]:7337"), port, &[]));
        assert!(is_allowed_host(Some("  127.0.0.1:7337  "), port, &[]));
    }

    #[test]
    fn allowed_host_rejects_dns_rebinding_attempts() {
        // The classic DNS-rebinding payload: a hostname that resolves
        // to 127.0.0.1 in the user's DNS, but the browser still sends
        // the original name in Host. Without this check, the server
        // happily accepted the request because TCP saw it on the loop-
        // back interface.
        let port = 7337;
        assert!(!is_allowed_host(
            Some("attacker.example.com:7337"),
            port,
            &[]
        ));
        // Right name, wrong port — defends against requests sent to a
        // server colocated with us under a misconfigured proxy.
        assert!(!is_allowed_host(Some("127.0.0.1:9999"), port, &[]));
        // Missing port — Host header without a port means "default
        // port for the scheme," which we never bind on.
        assert!(!is_allowed_host(Some("127.0.0.1"), port, &[]));
        // Missing header entirely.
        assert!(!is_allowed_host(None, port, &[]));
        assert!(!is_allowed_host(Some(""), port, &[]));
        // Non-numeric port.
        assert!(!is_allowed_host(Some("127.0.0.1:abc"), port, &[]));
    }

    #[test]
    fn allowed_host_accepts_operator_declared_names() {
        // A Kubernetes Service client sends the service DNS name in
        // Host. Operator-declared names pass on the bound port only.
        let port = 7337;
        let extra = vec!["opr-tsindex-dev".to_string()];
        assert!(is_allowed_host(Some("opr-tsindex-dev:7337"), port, &extra));
        // Wrong port fails even for a declared name.
        assert!(!is_allowed_host(Some("opr-tsindex-dev:9999"), port, &extra));
        // Missing port fails even for a declared name.
        assert!(!is_allowed_host(Some("opr-tsindex-dev"), port, &extra));
        // Undeclared names still fail: the extra list is additive, not
        // a wildcard.
        assert!(!is_allowed_host(
            Some("attacker.example.com:7337"),
            port,
            &extra
        ));
        // Localhost forms keep working alongside declared names.
        assert!(is_allowed_host(Some("localhost:7337"), port, &extra));
    }

    #[test]
    fn http_request_caps_content_length_before_allocation() -> Result<()> {
        use std::io::Write;
        use std::net::TcpStream;
        use std::time::Duration;

        // End-to-end test: a malicious Content-Length must be rejected
        // with 413 BEFORE the server allocates the buffer. The previous
        // code ran `vec![0u8; content_length]` unconditionally, so a
        // request claiming 4 GiB would OOM the process. We can't test
        // a 4 GiB allocation directly (it'd OOM the test runner if the
        // bug came back), so we use a slightly-over-cap value
        // (1 MiB + 1 byte) and trust that the same code path handles
        // pathological values.
        let temp = tempfile::tempdir()?;
        let runtime = Runtime::new(
            temp.path().to_path_buf(),
            temp.path().join("index.db"),
            crate::config::TsIndexConfig::default(),
            Vec::new(),
        );
        runtime.initialize()?;

        let listener = TcpListener::bind(("127.0.0.1", 0))?;
        let bound_port = listener.local_addr()?.port();

        let server = std::thread::spawn(move || -> Result<()> {
            let (stream, _) = listener.accept()?;
            handle_http_connection(&Mutex::new(runtime), stream, bound_port, &[])?;
            Ok(())
        });

        let mut client = TcpStream::connect(("127.0.0.1", bound_port))?;
        client.set_read_timeout(Some(Duration::from_secs(2)))?;
        let oversize = MAX_HTTP_BODY_BYTES + 1;
        write!(
            client,
            "POST /tools/call HTTP/1.1\r\nHost: 127.0.0.1:{bound_port}\r\nContent-Length: {oversize}\r\n\r\n"
        )?;
        client.flush()?;

        let mut response = Vec::new();
        let _ = std::io::Read::read_to_end(&mut client, &mut response);
        let text = String::from_utf8_lossy(&response);
        assert!(
            text.starts_with("HTTP/1.1 413 "),
            "expected 413 Payload Too Large response; got: {}",
            text.lines().next().unwrap_or_default()
        );

        drop(client);
        let _ = server.join();
        Ok(())
    }

    #[test]
    fn parent_watchdog_stays_alive_while_parent_is_unchanged() {
        let exits = Arc::new(Mutex::new(Vec::new()));
        let triggered = check_parent_watchdog(4242, || 4242, {
            let exits = Arc::clone(&exits);
            move |initial, current| exits.lock().unwrap().push((initial, current))
        });

        assert!(!triggered);
        assert!(exits.lock().unwrap().is_empty());
    }

    #[test]
    fn parent_watchdog_requests_shutdown_when_reparented() {
        let exits = Arc::new(Mutex::new(Vec::new()));
        let triggered = check_parent_watchdog(4242, || 1, {
            let exits = Arc::clone(&exits);
            move |initial, current| exits.lock().unwrap().push((initial, current))
        });

        assert!(triggered);
        assert_eq!(exits.lock().unwrap().as_slice(), &[(4242, 1)]);
    }

    #[test]
    fn parent_watchdog_ignores_pid_1_parent_at_startup() {
        // Launched directly by init/launchd, or the MCP client is PID 1 in a
        // container: there is no parent lifetime to track. The old code
        // treated this as "orphaned" and exited before `initialize`.
        let exits = Arc::new(Mutex::new(Vec::new()));
        let triggered = check_parent_watchdog(1, || 1, {
            let exits = Arc::clone(&exits);
            move |initial, current| exits.lock().unwrap().push((initial, current))
        });

        assert!(!triggered);
        assert!(exits.lock().unwrap().is_empty());
    }

    #[test]
    fn parent_watchdog_shutdown_message_names_both_pids() {
        assert_eq!(
            parent_watchdog_shutdown_message(4242, 1),
            "tsindex: parent 4242 exited (now reparented to 1); shutting down"
        );
    }

    #[test]
    fn mcp_stdio_survives_invalid_utf8_line() -> Result<()> {
        let dir = tempdir()?;
        let runtime = Runtime::new(
            dir.path().to_path_buf(),
            db_path(dir.path()),
            TsIndexConfig::default(),
            Vec::new(),
        );
        runtime.initialize()?;

        let mut input = Vec::new();
        input.extend_from_slice(br#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#);
        input.extend_from_slice(b"\n\xff\xfe not utf-8\n");
        input.extend_from_slice(br#"{"jsonrpc":"2.0","id":2,"method":"ping"}"#);
        input.push(b'\n');

        let mut output = Vec::new();
        serve_mcp_io(&runtime, io::Cursor::new(input), &mut output)?;

        let responses: Vec<Value> = String::from_utf8(output)?
            .lines()
            .map(serde_json::from_str)
            .collect::<std::result::Result<_, _>>()?;
        assert_eq!(responses.len(), 3, "bad line must answer, not end the loop");
        assert_eq!(responses[0]["id"], 1);
        assert_eq!(responses[1]["error"]["code"], -32700);
        assert_eq!(responses[2]["id"], 2);
        Ok(())
    }

    #[test]
    fn tsindex_home_from_env_value() {
        // Pure helper: no `set_var`/`remove_var`, which are unsound while
        // other test threads read the environment and would also discard the
        // sink `.cargo/config.toml` injects for the whole test binary.
        let home = home_path(".tsindex");
        assert_eq!(tsindex_home_from(None), home);
        assert_eq!(
            tsindex_home_from(Some(OsString::new())),
            home,
            "TSINDEX_HOME= (empty) must not resolve to a relative path in the CWD"
        );
        assert_eq!(
            tsindex_home_from(Some(OsString::from("/tmp/sink"))),
            PathBuf::from("/tmp/sink")
        );
    }

    #[test]
    fn savings_log_lands_under_tsindex_home() -> Result<()> {
        // `.cargo/config.toml` points TSINDEX_HOME at target/tsindex-home for
        // every cargo-spawned test; read it, never mutate it.
        let repo = tempdir()?;
        fs::write(repo.path().join("lib.rs"), "pub fn hello() {}\n")?;
        let runtime = Runtime::new(
            repo.path().to_path_buf(),
            db_path(repo.path()),
            TsIndexConfig::default(),
            Vec::new(),
        );
        runtime.build(false, None)?;

        let call = json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": { "name": "get_symbol", "arguments": { "name": "hello" } }
        });
        handle_request(&runtime, call)?;

        let slug = repo.path().to_string_lossy().replace(['/', '\\'], "_");
        assert!(
            tsindex_home().join(&slug).join("savings.jsonl").is_file(),
            "savings must be written under tsindex_home()"
        );
        Ok(())
    }

    /// Send one raw HTTP request to a fresh server and return the response
    /// text. Runs the real `handle_http_connection` over a loopback socket.
    fn http_roundtrip(request: &str) -> Result<String> {
        let temp = tempdir()?;
        let runtime = Runtime::new(
            temp.path().to_path_buf(),
            temp.path().join("index.db"),
            TsIndexConfig::default(),
            Vec::new(),
        );
        runtime.initialize()?;
        let listener = TcpListener::bind(("127.0.0.1", 0))?;
        let bound_port = listener.local_addr()?.port();
        let server = std::thread::spawn(move || -> Result<()> {
            let (stream, _) = listener.accept()?;
            handle_http_connection(&Mutex::new(runtime), stream, bound_port, &[])?;
            Ok(())
        });
        let mut client = TcpStream::connect(("127.0.0.1", bound_port))?;
        client.set_read_timeout(Some(Duration::from_secs(5)))?;
        client.write_all(
            request
                .replace("{port}", &bound_port.to_string())
                .as_bytes(),
        )?;
        client.flush()?;
        let mut response = Vec::new();
        let _ = client.read_to_end(&mut response);
        // Close our side first: the server drains the socket until EOF (or
        // 1 s) before returning, so joining first would wait that second.
        drop(client);
        let _ = server.join();
        Ok(String::from_utf8_lossy(&response).into_owned())
    }

    fn status_line(response: &str) -> &str {
        response.lines().next().unwrap_or_default()
    }

    #[test]
    fn http_post_rejects_cross_origin_requests() -> Result<()> {
        // A page on another localhost origin POSTs with a valid Host header;
        // only Origin/Sec-Fetch-Site reveal it as cross-site.
        let foreign = http_roundtrip(
            "POST /tools/list HTTP/1.1\r\nHost: localhost:{port}\r\nOrigin: http://localhost:9999\r\nContent-Length: 0\r\n\r\n",
        )?;
        assert!(
            status_line(&foreign).starts_with("HTTP/1.1 403 "),
            "{foreign}"
        );

        let cross_site = http_roundtrip(
            "POST /tools/list HTTP/1.1\r\nHost: localhost:{port}\r\nSec-Fetch-Site: cross-site\r\nContent-Length: 0\r\n\r\n",
        )?;
        assert!(
            status_line(&cross_site).starts_with("HTTP/1.1 403 "),
            "{cross_site}"
        );

        let same_origin = http_roundtrip(
            "POST /tools/list HTTP/1.1\r\nHost: localhost:{port}\r\nOrigin: http://localhost:{port}\r\nSec-Fetch-Site: same-origin\r\nContent-Length: 0\r\n\r\n",
        )?;
        assert!(
            status_line(&same_origin).starts_with("HTTP/1.1 200 "),
            "{same_origin}"
        );

        // curl and scripts send neither header and must keep working.
        let script = http_roundtrip(
            "POST /tools/list HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nContent-Length: 0\r\n\r\n",
        )?;
        assert!(
            status_line(&script).starts_with("HTTP/1.1 200 "),
            "{script}"
        );

        assert!(!is_same_origin_request(Some("null"), None, 7337, &[]));
        assert!(!is_same_origin_request(
            Some("http://localhost:7338"),
            None,
            7337,
            &[]
        ));
        assert!(is_same_origin_request(
            Some("http://127.0.0.1:7337"),
            None,
            7337,
            &[]
        ));
        Ok(())
    }

    #[test]
    fn http_headers_are_case_insensitive_and_content_length_is_validated() -> Result<()> {
        let body = r#"{"name":"get_symbol","arguments":{"name":"x"}}"#;
        let lower = http_roundtrip(&format!(
            "POST /tools/list HTTP/1.1\r\nhost: localhost:{{port}}\r\ncontent-length: {}\r\n\r\n{body}",
            body.len()
        ))?;
        assert!(status_line(&lower).starts_with("HTTP/1.1 200 "), "{lower}");

        let negative = http_roundtrip(
            "POST /tools/list HTTP/1.1\r\nHost: localhost:{port}\r\nContent-Length: -5\r\n\r\n",
        )?;
        assert!(
            status_line(&negative).starts_with("HTTP/1.1 400 "),
            "{negative}"
        );
        Ok(())
    }

    #[test]
    fn http_oversized_headers_return_431() -> Result<()> {
        // One header line longer than the cap, never terminated: the old
        // `read_line` would buffer it without bound.
        let huge = "X".repeat(MAX_HTTP_HEADER_BYTES as usize + 16);
        let response = http_roundtrip(&format!(
            "GET /health HTTP/1.1\r\nHost: localhost:{{port}}\r\nX-Pad: {huge}"
        ))?;
        assert_eq!(
            status_line(&response),
            "HTTP/1.1 431 Request Header Fields Too Large",
            "{response}"
        );
        Ok(())
    }

    #[test]
    fn http_oversized_request_line_returns_431() -> Result<()> {
        // The cap covers the request line: a path longer than the budget
        // with no newline used to be truncated and routed as-is.
        let huge = "X".repeat(MAX_HTTP_HEADER_BYTES as usize + 16);
        let response = http_roundtrip(&format!("GET /{huge}"))?;
        assert!(
            status_line(&response).starts_with("HTTP/1.1 431 "),
            "{}",
            status_line(&response)
        );
        Ok(())
    }

    #[test]
    fn http_header_budget_exhausted_on_line_boundary_returns_431() -> Result<()> {
        // Headers that consume the budget exactly at a newline, followed by
        // more headers: `take(0)` reads nothing, which must not be mistaken
        // for the blank end-of-headers line (the rest would become the body).
        let prefix = "GET /health HTTP/1.1\r\nHost: localhost:{port}\r\n";
        let port_len = 5; // {port} expands to at most 5 digits; pad afterwards
        let mut request = prefix.to_string();
        let used = prefix.len() - "{port}".len() + port_len;
        let pad_line_overhead = "X-Pad: \r\n".len();
        let pad = MAX_HTTP_HEADER_BYTES as usize - used - pad_line_overhead;
        request.push_str(&format!("X-Pad: {}\r\n", "Y".repeat(pad)));
        request.push_str("X-More: 1\r\n\r\n");
        let response = http_roundtrip(&request)?;
        // With a 4-digit port the budget is exhausted one byte early instead
        // of on the boundary; both must reject, never route the request.
        assert!(
            status_line(&response).starts_with("HTTP/1.1 431 "),
            "{}",
            status_line(&response)
        );
        Ok(())
    }

    #[test]
    fn http_unknown_tool_and_stdio_only_tool_are_400_not_500() -> Result<()> {
        let body = r#"{"name":"no_such_tool","arguments":{}}"#;
        let response = http_roundtrip(&format!(
            "POST /tools/call HTTP/1.1\r\nHost: localhost:{{port}}\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        ))?;
        assert!(
            status_line(&response).starts_with("HTTP/1.1 400 "),
            "{response}"
        );
        assert!(response.contains("unknown tool no_such_tool"), "{response}");

        let body = r#"{"name":"replace_symbol","arguments":{"name":"x","new_body":"y"}}"#;
        let response = http_roundtrip(&format!(
            "POST /tools/call HTTP/1.1\r\nHost: localhost:{{port}}\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        ))?;
        assert!(
            status_line(&response).starts_with("HTTP/1.1 400 "),
            "{response}"
        );
        assert!(
            response.contains("only available over the MCP/stdio transport"),
            "{response}"
        );
        Ok(())
    }

    #[test]
    fn http_internal_errors_do_not_echo_details() -> Result<()> {
        // `get_symbol` fails to open a database whose directory is gone; the
        // error chain names the absolute DB path and must stay server-side.
        let temp = tempdir()?;
        let missing = temp.path().join("gone");
        let runtime = Runtime::new(
            missing.clone(),
            missing.join("nope").join("index.db"),
            TsIndexConfig::default(),
            Vec::new(),
        );
        let listener = TcpListener::bind(("127.0.0.1", 0))?;
        let bound_port = listener.local_addr()?.port();
        let server = std::thread::spawn(move || -> Result<()> {
            let (stream, _) = listener.accept()?;
            handle_http_connection(&Mutex::new(runtime), stream, bound_port, &[])?;
            Ok(())
        });
        let mut client = TcpStream::connect(("127.0.0.1", bound_port))?;
        client.set_read_timeout(Some(Duration::from_secs(5)))?;
        let body = r#"{"name":"get_symbol","arguments":{"name":"x"}}"#;
        write!(
            client,
            "POST /tools/call HTTP/1.1\r\nHost: localhost:{bound_port}\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )?;
        let mut response = String::new();
        let _ = client.read_to_string(&mut response);
        drop(client);
        let _ = server.join();
        assert!(
            status_line(&response).starts_with("HTTP/1.1 500 "),
            "{response}"
        );
        assert!(
            response.ends_with(r#"{"error":"internal error"}"#),
            "{response}"
        );
        assert!(!response.contains("index.db"), "path leaked: {response}");
        Ok(())
    }

    #[test]
    fn parent_watchdog_start_claim_is_idempotent() {
        static WATCHDOG_TEST_LOCK: Mutex<()> = Mutex::new(());
        let _guard = WATCHDOG_TEST_LOCK.lock().unwrap();

        PARENT_WATCHDOG_STARTED.store(false, Ordering::SeqCst);

        assert!(claim_parent_watchdog_start());
        assert!(!claim_parent_watchdog_start());

        PARENT_WATCHDOG_STARTED.store(false, Ordering::SeqCst);
    }
}
