//! Login service installation (`launchd` on macOS, `systemd --user` on Linux)
//! and `start` / `stop` / `restart` control helpers for the orchestrator daemon.

use std::env;
use std::fs::{self, OpenOptions};
use std::io::{self, Read};
use std::os::fd::OwnedFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::daemon::{DaemonClient, DaemonPaths, DaemonStatus, ensure_private_dir};
use crate::lifecycle::ProcessOwner;
use crate::mcp::home_path;

pub const MACOS_SERVICE_LABEL: &str = "com.legalzoom.tsindex.daemon";
pub const LINUX_SERVICE_NAME: &str = "tsindex.service";
pub const LINUX_SOCKET_NAME: &str = "tsindex.socket";
#[cfg(target_os = "macos")]
const DEFAULT_MACOS_PATH: &str = "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin";
const START_TIMEOUT: Duration = Duration::from_secs(30);
const STOP_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Deserialize, Serialize)]
struct InstalledEndpoint {
    socket: PathBuf,
    // Older service records predate this field. New installs distinguish an
    // explicitly isolated runtime directory from the user's ordinary login.
    #[serde(default)]
    default_socket: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ServiceInfo {
    pub manager: &'static str,
    pub installed: bool,
    pub service_file: PathBuf,
}

/// The manager has one fixed service name per account. Installation uses the
/// account's manager configuration directory, which must be in its search path.
pub fn service_file_path() -> PathBuf {
    #[cfg(target_os = "macos")]
    {
        home_path(&format!("Library/LaunchAgents/{MACOS_SERVICE_LABEL}.plist"))
    }
    #[cfg(target_os = "linux")]
    {
        let config_dir = env::var_os("XDG_CONFIG_HOME")
            .filter(|v| !v.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| home_path(".config"));
        config_dir.join("systemd/user").join(LINUX_SERVICE_NAME)
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        home_path(".tsindex/daemon.service")
    }
}

pub fn inspect_service() -> ServiceInfo {
    let service_file = service_file_path();
    let manager = if cfg!(target_os = "macos") {
        "launchd"
    } else if cfg!(target_os = "linux") {
        "systemd-user"
    } else {
        "detached"
    };
    ServiceInfo {
        manager,
        installed: service_file.is_file(),
        service_file,
    }
}

/// Render a macOS `launchd` LaunchAgent plist for `tsindex daemon run`.
pub fn render_launchd_plist(
    binary: &Path,
    socket: Option<&Path>,
    log_path: &Path,
    path_env: &str,
) -> String {
    let home = env::var_os("TSINDEX_HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from);
    render_launchd_plist_with_home(binary, socket, log_path, path_env, home.as_deref())
}

fn render_launchd_plist_with_home(
    binary: &Path,
    socket: Option<&Path>,
    log_path: &Path,
    path_env: &str,
    home: Option<&Path>,
) -> String {
    let mut args = vec![
        binary.to_string_lossy().into_owned(),
        "daemon".to_string(),
        "run".to_string(),
        "--socket-activated".to_string(),
    ];
    if let Some(socket) = socket {
        args.push("--socket".to_string());
        args.push(socket.to_string_lossy().into_owned());
    }
    let rendered_args: String = args
        .iter()
        .map(|arg| format!("        <string>{}</string>\n", xml_escape(arg)))
        .collect();
    let log = xml_escape(&log_path.to_string_lossy());
    let path_env = xml_escape(path_env);
    let default_paths = DaemonPaths::resolve(None).ok();
    let socket = socket.or_else(|| {
        default_paths
            .as_ref()
            .map(|paths| paths.socket_path.as_path())
    });
    let socket = xml_escape(
        &socket
            .unwrap_or(Path::new("/invalid/tsindex.sock"))
            .to_string_lossy(),
    );
    let home_env = home
        .map(|value| {
            format!(
                "        <key>TSINDEX_HOME</key>\n        <string>{}</string>\n",
                xml_escape(&value.to_string_lossy())
            )
        })
        .unwrap_or_default();

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{MACOS_SERVICE_LABEL}</string>
    <key>ProgramArguments</key>
    <array>
{rendered_args}    </array>
    <key>Sockets</key>
    <dict>
        <key>Listeners</key>
        <dict>
            <key>SockType</key>
            <string>stream</string>
            <key>SockPathName</key>
            <string>{socket}</string>
            <key>SockPathMode</key>
            <integer>384</integer>
        </dict>
    </dict>
    <key>ThrottleInterval</key>
    <integer>2</integer>
    <key>ExitTimeOut</key>
    <integer>15</integer>
    <key>Umask</key>
    <integer>63</integer>
    <key>AbandonProcessGroup</key>
    <false/>
    <key>StandardOutPath</key>
    <string>{log}</string>
    <key>StandardErrorPath</key>
    <string>{log}</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>{path_env}</string>
{home_env}    </dict>
</dict>
</plist>
"#
    )
}

/// Render a Linux `systemd --user` service unit for `tsindex daemon run`.
pub fn render_systemd_unit(binary: &Path, socket: Option<&Path>) -> String {
    let home = env::var_os("TSINDEX_HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from);
    render_systemd_unit_with_home(binary, socket, home.as_deref())
}

fn render_systemd_unit_with_home(
    binary: &Path,
    socket: Option<&Path>,
    home: Option<&Path>,
) -> String {
    let mut exec_start = format!(
        "{} daemon run --socket-activated",
        systemd_quote(&binary.to_string_lossy(), false)
    );
    if let Some(socket) = socket {
        exec_start.push_str(" --socket ");
        exec_start.push_str(&systemd_quote(&socket.to_string_lossy(), true));
    }
    let home_env = home
        .map(|value| {
            format!(
                "Environment={}\n",
                systemd_environment_quote(&format!("TSINDEX_HOME={}", value.to_string_lossy()))
            )
        })
        .unwrap_or_default();
    format!(
        r#"[Unit]
Description=tsindex code-navigation orchestrator daemon
Requires=tsindex.socket
After=tsindex.socket

[Service]
Type=simple
ExecStart={exec_start}
Restart=on-failure
RestartSec=2
KillMode=mixed
TimeoutStopSec=15
UMask=0077
{home_env}
"#
    )
}

/// The manager owns this endpoint across service restarts. Activating the socket
/// starts the foreground supervisor only when a client first connects.
pub fn render_systemd_socket(binary: &Path, socket: &Path) -> String {
    // A socket unit can be started at login without going through the CLI.
    // Validate the endpoint before systemd is allowed to replace its pathname.
    // https://www.freedesktop.org/software/systemd/man/latest/systemd.socket.html
    format!(
        "[Unit]\nDescription=tsindex activation socket\n\n[Socket]\nExecStartPre={} --socket {} daemon check-socket\nListenStream={}\nSocketMode=0600\nDirectoryMode=0700\nAccept=no\n\n[Install]\nWantedBy=sockets.target\n",
        systemd_quote(&binary.to_string_lossy(), false),
        systemd_quote(&socket.to_string_lossy(), true),
        socket.to_string_lossy().replace('%', "%%")
    )
}

fn systemd_environment_quote(value: &str) -> String {
    systemd_quote(value, false)
}

fn xml_escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(ch),
        }
    }
    out
}

fn systemd_quote(value: &str, expand_arguments: bool) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '%' => out.push_str("%%"),
            // systemd expands dollars in argv but not in the executable token.
            '$' if expand_arguments => out.push_str("$$"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            _ => out.push(ch),
        }
    }
    out.push('"');
    out
}

pub(super) fn invocation_binary() -> Result<PathBuf> {
    let running = env::current_exe().context(
        "cannot locate the tsindex executable; a private Linux daemon needs /proc/self/exe in its PID namespace",
    )?;
    let current = fs::metadata(&running)?;
    // current_exe can name a versioned package target. Preserve the invocation
    // link across upgrades only when it identifies the file we actually ran.
    if let Some(arg) = env::args_os().next() {
        let arg = PathBuf::from(arg);
        let candidates = if arg.components().count() > 1 || arg.is_absolute() {
            vec![arg]
        } else {
            env::split_paths(&env::var_os("PATH").unwrap_or_default())
                .map(|dir| dir.join(&arg))
                .collect()
        };
        for candidate in candidates {
            if fs::metadata(&candidate)
                .is_ok_and(|m| m.dev() == current.dev() && m.ino() == current.ino())
            {
                return Ok(std::path::absolute(candidate)?);
            }
        }
    }
    Ok(running)
}

fn require_service_text(path: &Path) -> Result<()> {
    if path.to_str().is_none() || path.as_os_str().as_bytes().iter().any(u8::is_ascii_control) {
        bail!("service path cannot be represented safely: {path:?}");
    }
    Ok(())
}

fn resolve_binary(binary_override: Option<&Path>) -> Result<PathBuf> {
    let binary = match binary_override {
        Some(path) => std::path::absolute(path)?,
        None => invocation_binary()?,
    };
    require_service_text(&binary)?;
    let meta =
        fs::metadata(&binary).with_context(|| format!("cannot inspect executable {binary:?}"))?;
    if !meta.is_file() {
        bail!("service executable must be a regular file: {binary:?}");
    }
    let encoded = std::ffi::CString::new(binary.as_os_str().as_bytes())?;
    if unsafe { libc::access(encoded.as_ptr(), libc::X_OK) } != 0 {
        return Err(io::Error::last_os_error())
            .with_context(|| format!("cannot execute {binary:?}"));
    }
    Ok(binary)
}

fn probe_service_binary(
    binary: &Path,
    args: &[&str],
    capture_stdout: bool,
) -> Result<(std::process::ExitStatus, Vec<u8>)> {
    let mut command = Command::new(binary);
    command
        .args(args)
        .stdin(Stdio::null())
        .stderr(Stdio::null());
    let output_reader = if capture_stdout {
        let (output, stdout) = UnixStream::pair()?;
        output.set_nonblocking(true)?;
        command.stdout(Stdio::from(OwnedFd::from(stdout)));
        Some(output)
    } else {
        command.stdout(Stdio::null());
        None
    };
    let mut child = command
        .spawn()
        .with_context(|| format!("cannot execute {binary:?}"))?;
    drop(command);
    let deadline = Instant::now() + Duration::from_secs(3);
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(10)),
            result => {
                let _ = child.kill();
                let _ = child.wait();
                result?;
                bail!(
                    "service executable {binary:?} timed out responding to `{}`",
                    args.join(" ")
                );
            }
        }
    };
    let mut captured = Vec::new();
    if let Some(mut output) = output_reader {
        let mut buf = [0; 256];
        if let Ok(len) = output.read(&mut buf) {
            captured.extend_from_slice(&buf[..len]);
        }
    }
    Ok((status, captured))
}

fn verify_service_binary(binary: &Path) -> Result<()> {
    let (status, version) = probe_service_binary(binary, &["--version"], true)?;
    if !(status.success() && version.starts_with(b"tsindex ") && version.ends_with(b"\n")) {
        bail!(
            "service executable {binary:?} did not identify itself as tsindex (--version: {status})"
        );
    }
    // Verify the commands required by the generated activation units before
    // stopping the currently running service or modifying unit definitions.
    let (status, _) = probe_service_binary(
        binary,
        &["daemon", "run", "--socket-activated", "--help"],
        false,
    )?;
    if !status.success() {
        bail!(
            "service executable {binary:?} does not support socket-activated daemon units (`daemon run --socket-activated`: {status})"
        );
    }
    #[cfg(target_os = "linux")]
    {
        let (status, _) =
            probe_service_binary(binary, &["daemon", "check-socket", "--help"], false)?;
        if !status.success() {
            bail!(
                "service executable {binary:?} does not support systemd socket validation (`daemon check-socket`: {status})"
            );
        }
    }
    Ok(())
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn write_file_atomically(path: &Path, contents: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create directory {}", parent.display()))?;
    }
    let tmp = path.with_extension(format!("tmp.{}", std::process::id()));
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&tmp)
        .with_context(|| format!("failed to create {}", tmp.display()))?;
    let result = (|| {
        file.write_all(contents.as_bytes())?;
        fs::rename(&tmp, path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result.with_context(|| format!("failed to install {}", path.display()))?;
    Ok(())
}

fn wait_for_daemon_ready(paths: &DaemonPaths, timeout: Duration) -> Result<DaemonStatus> {
    let client = DaemonClient::new(paths.clone());
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(status) = client.ping() {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            let log_hint = read_log_tail(&paths.log_path);
            if log_hint.is_empty() {
                bail!(
                    "tsindex daemon did not become ready at {} within {}s",
                    paths.socket_path.display(),
                    timeout.as_secs()
                );
            }
            bail!(
                "tsindex daemon did not become ready at {} within {}s\nrecent log:\n{}",
                paths.socket_path.display(),
                timeout.as_secs(),
                log_hint
            );
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn wait_for_daemon_exit(owner: &ProcessOwner, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    while !owner.has_ended() {
        if Instant::now() >= deadline {
            bail!(
                "timed out waiting for tsindex daemon PID {} to exit",
                owner.pid()
            );
        }
        thread::sleep(Duration::from_millis(25));
    }
    Ok(())
}

/// Absence is a normal status. An unreadable, overloaded, or incompatible
/// daemon must surface its error so an operator knows a process may remain.
pub fn probe_daemon(paths: &DaemonPaths) -> Result<Option<DaemonStatus>> {
    if !inspect_socket_node(paths)? {
        return Ok(None);
    }
    match DaemonClient::new(paths.clone()).ping() {
        Ok(status) => Ok(Some(status)),
        Err(error)
            if error.downcast_ref::<io::Error>().is_some_and(|cause| {
                matches!(
                    cause.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
                )
            }) =>
        {
            Ok(None)
        }
        Err(error) => Err(error).context("could not determine whether the daemon is running"),
    }
}

fn capture_running_daemon(
    paths: &DaemonPaths,
    allow_unresponsive: bool,
) -> Result<Option<ProcessOwner>> {
    let status = match probe_daemon(paths) {
        Ok(Some(status)) => status,
        Ok(None) => return Ok(None),
        // The manager can stop an installed service even when it cannot answer
        // Ping. Fresh installation still requires an independent socket check.
        Err(_) if allow_unresponsive => return Ok(None),
        Err(error) => return Err(error),
    };
    ProcessOwner::capture_if_running(status.daemon_pid, &[std::process::id()])
}

fn stop_captured_daemon(paths: &DaemonPaths, owner: Option<ProcessOwner>) -> Result<bool> {
    let Some(owner) = owner else { return Ok(false) };
    if !owner.has_ended() {
        let shutdown = DaemonClient::new(paths.clone()).shutdown();
        // The initial PR #18 build used a flat lease path and removed its socket
        // late in shutdown. Wait for the captured process, without creating a
        // new lease or connecting again after its listener has closed.
        if let Err(timeout) = wait_for_daemon_exit(&owner, STOP_TIMEOUT) {
            return Err(shutdown.err().unwrap_or(timeout));
        }
    }
    Ok(true)
}

fn read_log_tail(log_path: &Path) -> String {
    use std::io::{Read, Seek, SeekFrom};
    let Ok(mut file) = fs::File::open(log_path) else {
        return String::new();
    };
    let _ = file.seek(SeekFrom::End(-8192));
    let mut bytes = Vec::new();
    if file.take(8192).read_to_end(&mut bytes).is_err() {
        return String::new();
    }
    let contents = String::from_utf8_lossy(&bytes);
    let lines: Vec<&str> = contents.lines().collect();
    let start = lines.len().saturating_sub(15);
    lines[start..].join("\n")
}

fn prepare_service_dirs(paths: &DaemonPaths) -> Result<()> {
    ensure_private_dir(paths.socket_dir()?)?;
    crate::daemon::orchestrator::prepare_metadata_dir(paths)?;
    if let Some(log_dir) = paths.log_path.parent() {
        ensure_private_dir(log_dir)?;
    }
    Ok(())
}

/// Install and activate the user service at the resolved endpoint.
pub fn install_service(
    paths: &DaemonPaths,
    _socket_override: Option<&Path>,
    binary_override: Option<&Path>,
) -> Result<(ServiceInfo, DaemonStatus)> {
    validate_service_socket_path(&paths.socket_path)?;
    let endpoint_exists = inspect_socket_node(paths)?;
    let binary = resolve_binary(binary_override)?;
    verify_service_binary(&binary)?;
    // Rendered service paths must survive the manager's text format. Check
    // every installer-controlled path before disrupting an existing service.
    if let Some(home) = resolved_service_home()? {
        require_service_text(&home)?;
    }
    require_service_text(&paths.log_path)?;
    #[cfg(target_os = "macos")]
    if let Some(path) = env::var_os("PATH") {
        require_service_text(Path::new(&path))?;
    }
    let _install_lock = installation_lock()?;
    prepare_service_dirs(paths)?;
    let previous = PreviousInstall::capture()?;
    let owner = capture_running_daemon(paths, true)?;
    // Selecting an unrelated live endpoint must fail before stopping a current
    // installation at another endpoint. A known installed listener may be
    // unresponsive while its service manager remains able to stop it.
    if endpoint_exists && owner.is_none() && !installed_endpoint_matches(paths).unwrap_or(false) {
        check_socket(paths)?;
    }
    let mut installing_definitions = false;
    let result = (|| -> Result<DaemonStatus> {
        if previous.service.is_some() {
            stop_os_service()?;
        }
        stop_captured_daemon(paths, owner)?;
        check_socket(paths)?;
        installing_definitions = true;
        install_platform_service(paths, &binary)?;
        write_file_atomically(
            &endpoint_file(),
            &serde_json::to_string(&InstalledEndpoint {
                socket: paths.socket_path.clone(),
                default_socket: Some(std::path::absolute(legacy_default_socket())?),
            })?,
        )?;
        start_os_service()?;
        wait_for_daemon_ready(paths, START_TIMEOUT)
    })();
    match result {
        Ok(status) => Ok((inspect_service(), status)),
        Err(error) => {
            if let Err(rollback) = previous.restore(installing_definitions) {
                return Err(error).context(format!(
                    "service installation failed and rollback was incomplete: {rollback:#}"
                ));
            }
            Err(error)
                .context("service installation failed; restored the previous service configuration")
        }
    }
}

/// Validate immediately before a new listener is bound, including automatic
/// systemd socket-unit activation at login. Stop existing listeners first.
pub fn check_socket(paths: &DaemonPaths) -> Result<()> {
    if !inspect_socket_node(paths)? {
        return Ok(());
    }
    match crate::daemon::connect_with_timeout(&paths.socket_path, Duration::from_millis(300)) {
        Ok(_) => bail!(
            "refusing to replace a live listening socket {}",
            paths.socket_path.display()
        ),
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
            ) =>
        {
            Ok(())
        }
        Err(error) => Err(error).context("cannot verify existing socket; refusing replacement"),
    }
}

fn validate_service_socket_path(path: &Path) -> Result<()> {
    require_service_text(path)?;
    // ListenStream parses a raw value, trims trailing whitespace, and treats a
    // terminal backslash as a line continuation. Reject ambiguous paths before
    // creating service files or asking a manager to bind the endpoint.
    let bytes = path.as_os_str().as_bytes();
    if cfg!(target_os = "linux")
        && (bytes.last().is_some_and(u8::is_ascii_whitespace) || bytes.last() == Some(&b'\\'))
    {
        bail!("service socket path cannot be represented safely in an activation unit");
    }
    Ok(())
}

fn inspect_socket_node(paths: &DaemonPaths) -> Result<bool> {
    match fs::symlink_metadata(paths.socket_dir()?) {
        Ok(_) => crate::daemon::validate_private_dir(paths.socket_dir()?)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
        Err(error) => return Err(error).context("failed to inspect socket directory"),
    }
    let metadata = match fs::symlink_metadata(&paths.socket_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error).context("failed to inspect service socket endpoint"),
    };
    if !metadata.file_type().is_socket() || metadata.uid() != crate::daemon::current_uid() {
        bail!(
            "refusing to replace non-socket or foreign endpoint {}",
            paths.socket_path.display()
        );
    }
    Ok(true)
}

fn resolved_service_home() -> Result<Option<PathBuf>> {
    env::var_os("TSINDEX_HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .map(|home| {
            // A login manager starts in its own working directory. Preserve the
            // installer's interpretation of a relative telemetry/cache path.
            if home.is_absolute() {
                Ok(home)
            } else {
                Ok(env::current_dir()?.join(home))
            }
        })
        .transpose()
}

// Installation and removal share three files and one manager registration.
// Keep competing installers from restoring each other's partially written state.
fn installation_lock() -> Result<crate::maintenance::FileLease> {
    crate::maintenance::FileLease::try_acquire(service_file_path().with_extension("install.lock"))?
        .context("another service installation or removal is already in progress")
}

struct PreviousInstall {
    service: Option<String>,
    #[cfg(target_os = "linux")]
    socket: Option<String>,
    endpoint: Option<String>,
    active: bool,
    #[cfg(target_os = "linux")]
    enablement: Option<SystemdEnablement>,
    #[cfg(target_os = "linux")]
    service_was_active: bool,
}

#[cfg(target_os = "linux")]
enum SystemdEnablement {
    Persistent,
    Runtime,
}

fn read_definition(path: &Path) -> Result<Option<String>> {
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.is_file() => Ok(Some(fs::read_to_string(path)?)),
        Ok(_) => bail!("refusing to replace a non-regular service definition: {path:?}"),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn restore_definition(path: &Path, contents: Option<&str>) -> Result<()> {
    match contents {
        Some(contents) => write_file_atomically(path, contents),
        None => match fs::remove_file(path) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            result => Ok(result?),
        },
    }
}

#[cfg(target_os = "linux")]
fn systemd_unit_state(unit: &str, action: &str) -> Result<Option<String>> {
    let output = Command::new("systemctl")
        .args(["--user", action, unit])
        .output()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let state = stdout.trim();
    if output.status.success() {
        return Ok(Some(state.to_owned()));
    }
    // is-active and is-enabled use a nonzero exit for valid inactive states.
    // Other errors must abort before the working installation is changed.
    if matches!(
        state,
        "inactive" | "deactivating" | "failed" | "unknown" | "disabled" | "not-found"
    ) {
        return Ok(None);
    }
    bail!(
        "cannot inspect {unit} ({action}): {}",
        String::from_utf8_lossy(&output.stderr).trim()
    );
}

impl PreviousInstall {
    fn capture() -> Result<Self> {
        let path = service_file_path();
        let service = read_definition(&path)?;
        let endpoint = read_definition(&endpoint_file())?;
        #[cfg(target_os = "linux")]
        {
            let socket = read_definition(&path.with_extension("socket"))?;
            let installed = service.is_some() || socket.is_some();
            let old_unit = if socket.is_some() {
                LINUX_SOCKET_NAME
            } else {
                LINUX_SERVICE_NAME
            };
            let enablement = if installed {
                systemd_unit_state(old_unit, "is-enabled")?.map(|state| {
                    if state == "enabled-runtime" {
                        SystemdEnablement::Runtime
                    } else {
                        SystemdEnablement::Persistent
                    }
                })
            } else {
                None
            };
            let active = installed && systemd_unit_state(old_unit, "is-active")?.is_some();
            let service_was_active = socket.is_some()
                && service.is_some()
                && systemd_unit_state(LINUX_SERVICE_NAME, "is-active")?.is_some();
            Ok(Self {
                service,
                socket,
                endpoint,
                enablement,
                active,
                service_was_active,
            })
        }
        #[cfg(target_os = "macos")]
        {
            let active = service.is_some() && launchd_job_loaded()?;
            Ok(Self {
                service,
                endpoint,
                active,
            })
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        bail!("unsupported user service manager")
    }

    fn restore(&self, definitions_changed: bool) -> Result<()> {
        let mut errors = Vec::new();
        let mut record = |result: Result<()>| {
            if let Err(error) = result {
                errors.push(format!("{error:#}"));
                return false;
            }
            true
        };
        let mut can_restart = true;
        if definitions_changed {
            // A manager outage can also prevent cleanup of the failed job.
            // Recover the durable definitions even then, and restart only when
            // its listener and configuration are known to have been restored.
            let path = service_file_path();
            if path.is_file() {
                can_restart &= record(stop_os_service());
            }
            #[cfg(target_os = "linux")]
            if path.with_extension("socket").is_file()
                && !(self.socket.is_some()
                    && matches!(self.enablement, Some(SystemdEnablement::Persistent)))
            {
                record(manager_output(
                    Command::new("systemctl").args(["--user", "disable", LINUX_SOCKET_NAME]),
                    "disable replacement activation socket",
                ));
            }
            can_restart &= record(restore_definition(&path, self.service.as_deref()));
            #[cfg(target_os = "linux")]
            {
                can_restart &= record(restore_definition(
                    &path.with_extension("socket"),
                    self.socket.as_deref(),
                ));
            }
            can_restart &= record(restore_definition(
                &endpoint_file(),
                self.endpoint.as_deref(),
            ));
            #[cfg(target_os = "linux")]
            {
                can_restart &= record(manager_output(
                    Command::new("systemctl").args(["--user", "daemon-reload"]),
                    "reload restored service definitions",
                ));
                if let Some(enablement) = &self.enablement {
                    let unit = if self.socket.is_some() {
                        LINUX_SOCKET_NAME
                    } else {
                        LINUX_SERVICE_NAME
                    };
                    let mut command = Command::new("systemctl");
                    command.arg("--user");
                    if matches!(enablement, SystemdEnablement::Runtime) {
                        command.arg("--runtime");
                    }
                    record(manager_output(
                        command.args(["enable", unit]),
                        "restore previous login enablement",
                    ));
                }
            }
        }
        if can_restart && self.active {
            record(start_os_service());
        }
        #[cfg(target_os = "linux")]
        if can_restart && self.service_was_active {
            record(manager_output(
                Command::new("systemctl").args(["--user", "start", LINUX_SERVICE_NAME]),
                "restart the previously active user service",
            ));
        }
        if !errors.is_empty() {
            bail!("{}", errors.join("; "));
        }
        Ok(())
    }
}

fn endpoint_file() -> PathBuf {
    service_file_path().with_extension("json")
}

fn installed_endpoint() -> Result<Option<InstalledEndpoint>> {
    if !inspect_service().installed {
        return Ok(None);
    }
    let installed = match fs::read(endpoint_file()) {
        Ok(bytes) => serde_json::from_slice::<InstalledEndpoint>(&bytes).context(
            "invalid service endpoint metadata; run `tsindex daemon install --socket PATH` again",
        )?,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            // Older builds stored the resolved endpoint only in the generated
            // command. Keep it discoverable before the first reinstall.
            InstalledEndpoint {
                socket: endpoint_from_service()?,
                default_socket: None,
            }
        }
        Err(error) => return Err(error).context("failed to read installed service endpoint"),
    };
    Ok(Some(installed))
}

/// An explicit CLI or environment choice selects an independent endpoint.
/// With ordinary login defaults, both control clients and managed MCP use the
/// installed service even when its socket was placed at a custom path.
pub fn client_paths(socket: Option<&Path>) -> Result<DaemonPaths> {
    let selected = DaemonPaths::resolve(socket)?;
    if socket.is_some()
        || ["TSINDEX_SOCKET", "TSINDEX_HOME"]
            .iter()
            .any(|name| env::var_os(name).is_some_and(|value| !value.is_empty()))
    {
        return Ok(selected);
    }
    if let Some(install) = installed_endpoint()? {
        let ordinary_login = match &install.default_socket {
            Some(default) => {
                *default == selected.socket_path
                    || ordinary_linux_defaults(default, &selected.socket_path)
            }
            // Legacy metadata lacks its install-time runtime directory. A
            // custom Linux XDG directory remains isolated until reinstallation.
            None => {
                !cfg!(target_os = "linux")
                    || env::var_os("XDG_RUNTIME_DIR")
                        .filter(|value| !value.is_empty())
                        .is_none_or(|dir| {
                            Path::new(&dir)
                                == Path::new(&format!("/run/user/{}", crate::daemon::current_uid()))
                        })
            }
        };
        if ordinary_login {
            return DaemonPaths::resolve(Some(&install.socket));
        }
    }
    Ok(selected)
}

fn ordinary_linux_defaults(installed: &Path, selected: &Path) -> bool {
    if !cfg!(target_os = "linux") {
        return false;
    }
    let uid = crate::daemon::current_uid();
    let home = home_path(".tsindex");
    let session =
        crate::daemon::default_socket_path(Some(format!("/run/user/{uid}").into()), &home, uid);
    let fallback = crate::daemon::default_socket_path(None, &home, uid);
    (installed == session && selected == fallback) || (installed == fallback && selected == session)
}

fn installed_endpoint_matches(paths: &DaemonPaths) -> Result<bool> {
    let Some(install) = installed_endpoint()? else {
        return Ok(false);
    };
    let installed = DaemonPaths::resolve(Some(&install.socket))?;
    if installed.socket_path == paths.socket_path {
        return Ok(true);
    }
    if installed.socket_path.file_name() != paths.socket_path.file_name() {
        return Ok(false);
    }
    // The leaf may not exist while a service is stopped. Compare its parent
    // directory first, then validate both spellings of a matching endpoint.
    // An unrelated private manual daemon remains independently controllable.
    let canonical_parent = |p: &DaemonPaths| -> Result<Option<PathBuf>> {
        match p.socket_dir()?.canonicalize() {
            Ok(parent) => Ok(Some(parent)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    };
    let Some(parent) = canonical_parent(paths)? else {
        return Ok(false);
    };
    if canonical_parent(&installed)?.as_ref() != Some(&parent) {
        return Ok(false);
    }
    inspect_socket_node(paths)?;
    inspect_socket_node(&installed)?;
    Ok(true)
}

fn legacy_default_socket() -> PathBuf {
    crate::daemon::default_socket_path(
        env::var_os("XDG_RUNTIME_DIR"),
        &home_path(".tsindex"),
        crate::daemon::current_uid(),
    )
}

#[cfg(any(target_os = "linux", test))]
fn parse_systemd_words(input: &str) -> Result<Vec<String>> {
    let mut args = Vec::new();
    let mut remaining = input.trim();
    // Our original renderer emitted JSON-compatible double-quoted paths and
    // plain command words. Decode that finite format without executing a shell.
    while !remaining.is_empty() {
        if remaining.starts_with('"') {
            let mut quoted = serde_json::Deserializer::from_str(remaining).into_iter::<String>();
            let argument = quoted
                .next()
                .transpose()?
                .context("missing service argument")?;
            remaining = &remaining[quoted.byte_offset()..];
            args.push(argument.replace("%%", "%"));
        } else {
            let end = remaining
                .find(char::is_whitespace)
                .unwrap_or(remaining.len());
            args.push(remaining[..end].to_string());
            remaining = &remaining[end..];
        }
        remaining = remaining.trim_start();
    }
    Ok(args)
}

#[cfg(any(target_os = "linux", test))]
fn parse_systemd_unit_env(source: &str) -> std::collections::HashMap<String, String> {
    let mut env = std::collections::HashMap::new();
    let mut in_service = false;
    for line in source.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') && trimmed.ends_with(']') {
            in_service = trimmed == "[Service]";
            continue;
        }
        if !in_service {
            continue;
        }
        let Some(value) = trimmed.strip_prefix("Environment=") else {
            continue;
        };
        let Ok(words) = parse_systemd_words(value) else {
            continue;
        };
        if words.is_empty() {
            env.clear();
            continue;
        }
        for word in words {
            if let Some((key, val)) = word.split_once('=') {
                env.insert(key.to_string(), val.to_string());
            }
        }
    }
    env
}

#[cfg(any(target_os = "linux", test))]
fn parse_manager_environment(output: &str) -> std::collections::HashMap<String, String> {
    fn unquote_shell_value(raw: &str) -> String {
        let mut out = Vec::with_capacity(raw.len());
        let mut chars = raw.chars().peekable();
        while let Some(ch) = chars.next() {
            match ch {
                '$' if chars.peek() == Some(&'\'') => {
                    chars.next();
                    let mut closed = false;
                    while let Some(c) = chars.next() {
                        if c == '\'' {
                            closed = true;
                            break;
                        }
                        if c != '\\' {
                            let mut buf = [0u8; 4];
                            out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
                            continue;
                        }
                        let Some(esc) = chars.next() else {
                            return raw.to_string();
                        };
                        match esc {
                            'a' => out.push(0x07),
                            'b' => out.push(0x08),
                            'e' | 'E' => out.push(0x1b),
                            'f' => out.push(0x0c),
                            'n' => out.push(b'\n'),
                            'r' => out.push(b'\r'),
                            't' => out.push(b'\t'),
                            'v' => out.push(0x0b),
                            '\\' => out.push(b'\\'),
                            '\'' => out.push(b'\''),
                            '"' => out.push(b'"'),
                            'x' => {
                                let mut val = 0u8;
                                let mut digits = 0;
                                while digits < 2 {
                                    let Some(&next) = chars.peek() else { break };
                                    let Some(d) = next.to_digit(16) else { break };
                                    chars.next();
                                    val = val * 16 + d as u8;
                                    digits += 1;
                                }
                                if digits == 0 {
                                    out.extend_from_slice(b"\\x");
                                } else {
                                    out.push(val);
                                }
                            }
                            'u' | 'U' => {
                                let max_digits = if esc == 'u' { 4 } else { 8 };
                                let mut val = 0u32;
                                let mut digits = 0;
                                while digits < max_digits {
                                    let Some(&next) = chars.peek() else { break };
                                    let Some(d) = next.to_digit(16) else { break };
                                    chars.next();
                                    val = val * 16 + d;
                                    digits += 1;
                                }
                                if digits > 0
                                    && let Some(decoded) = char::from_u32(val)
                                {
                                    let mut buf = [0u8; 4];
                                    out.extend_from_slice(decoded.encode_utf8(&mut buf).as_bytes());
                                } else {
                                    return raw.to_string();
                                }
                            }
                            '0'..='7' => {
                                let mut val = esc.to_digit(8).unwrap();
                                for _ in 0..2 {
                                    let Some(&next) = chars.peek() else { break };
                                    let Some(d) = next.to_digit(8) else { break };
                                    chars.next();
                                    val = val * 8 + d;
                                }
                                out.push((val & 0xff) as u8);
                            }
                            other => {
                                out.push(b'\\');
                                let mut buf = [0u8; 4];
                                out.extend_from_slice(other.encode_utf8(&mut buf).as_bytes());
                            }
                        }
                    }
                    if !closed {
                        return raw.to_string();
                    }
                }
                '\'' => {
                    let mut closed = false;
                    for c in chars.by_ref() {
                        if c == '\'' {
                            closed = true;
                            break;
                        }
                        let mut buf = [0u8; 4];
                        out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
                    }
                    if !closed {
                        return raw.to_string();
                    }
                }
                '"' => {
                    let mut closed = false;
                    while let Some(c) = chars.next() {
                        if c == '"' {
                            closed = true;
                            break;
                        }
                        if c == '\\' {
                            let Some(esc) = chars.next() else {
                                return raw.to_string();
                            };
                            match esc {
                                '$' | '`' | '"' | '\\' => out.push(esc as u8),
                                '\n' => {}
                                other => {
                                    out.push(b'\\');
                                    let mut buf = [0u8; 4];
                                    out.extend_from_slice(other.encode_utf8(&mut buf).as_bytes());
                                }
                            }
                        } else {
                            let mut buf = [0u8; 4];
                            out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
                        }
                    }
                    if !closed {
                        return raw.to_string();
                    }
                }
                '\\' => {
                    if let Some(esc) = chars.next() {
                        let mut buf = [0u8; 4];
                        out.extend_from_slice(esc.encode_utf8(&mut buf).as_bytes());
                    } else {
                        out.push(b'\\');
                    }
                }
                other => {
                    let mut buf = [0u8; 4];
                    out.extend_from_slice(other.encode_utf8(&mut buf).as_bytes());
                }
            }
        }
        String::from_utf8(out).unwrap_or_else(|_| raw.to_string())
    }

    let mut env = std::collections::HashMap::new();
    for line in output.lines() {
        let Some((key, raw)) = line.split_once('=') else {
            continue;
        };
        env.insert(key.to_string(), unquote_shell_value(raw));
    }
    env
}

#[cfg(target_os = "linux")]
fn systemd_user_environment() -> Option<String> {
    let output = Command::new("systemctl")
        .args(["--user", "show-environment"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8(output.stdout).ok()?;
    (!text.trim().is_empty()).then_some(text)
}

#[cfg(any(target_os = "linux", test))]
fn endpoint_from_systemd_unit(
    source: &str,
    manager_env: impl FnOnce() -> Option<String>,
    fallback_xdg_runtime_dir: Option<std::ffi::OsString>,
    default_home: &Path,
    uid: u32,
) -> Result<PathBuf> {
    let command = source
        .lines()
        .find_map(|line| line.strip_prefix("ExecStart="))
        .context("installed service has no ExecStart; run `tsindex daemon install` again")?;
    let args = parse_systemd_words(command)?;
    if let Some(socket) = args
        .windows(2)
        .find(|pair| pair[0] == "--socket")
        .map(|pair| PathBuf::from(&pair[1]))
    {
        return Ok(socket);
    }
    let socket_activated = args.iter().any(|arg| arg == "--socket-activated");
    // A socketless legacy unit receives XDG_RUNTIME_DIR (and any unit
    // Environment= overrides) from systemd --user rather than the invoking shell.
    let unit_env = parse_systemd_unit_env(source);
    let mgr_env = manager_env().map(|text| parse_manager_environment(&text));
    let lookup = |key: &str| -> Option<String> {
        if let Some(value) = unit_env.get(key) {
            return (!value.is_empty()).then(|| value.clone());
        }
        mgr_env
            .as_ref()
            .and_then(|map| map.get(key))
            .filter(|value| !value.is_empty())
            .cloned()
    };
    if let Some(socket) = lookup("TSINDEX_SOCKET") {
        return Ok(PathBuf::from(socket));
    }
    if let Some(home) = lookup("TSINDEX_HOME") {
        // Do not infer the layout from an existing socket: it might belong
        // to an independent daemon rather than this installed service.
        return Ok(PathBuf::from(home).join(if socket_activated {
            "run/daemon.sock"
        } else {
            "daemon.sock"
        }));
    }
    let xdg_runtime_dir = if unit_env.contains_key("XDG_RUNTIME_DIR") {
        unit_env
            .get("XDG_RUNTIME_DIR")
            .filter(|value| !value.is_empty())
            .map(std::ffi::OsString::from)
    } else if let Some(mgr) = &mgr_env {
        mgr.get("XDG_RUNTIME_DIR")
            .filter(|value| !value.is_empty())
            .map(std::ffi::OsString::from)
    } else {
        fallback_xdg_runtime_dir
    };
    #[cfg(target_os = "linux")]
    {
        Ok(crate::daemon::default_socket_path(
            xdg_runtime_dir,
            default_home,
            uid,
        ))
    }
    #[cfg(not(target_os = "linux"))]
    {
        if let Some(runtime) = xdg_runtime_dir.filter(|value| !value.is_empty()) {
            let candidate = PathBuf::from(runtime).join("tsindex").join("daemon.sock");
            if candidate.as_os_str().as_encoded_bytes().len() <= 100 {
                return Ok(candidate);
            }
        }
        Ok(crate::daemon::default_socket_path(None, default_home, uid))
    }
}

#[cfg(target_os = "linux")]
fn endpoint_from_service() -> Result<PathBuf> {
    let source = fs::read_to_string(service_file_path())?;
    let socket = endpoint_from_systemd_unit(
        &source,
        systemd_user_environment,
        env::var_os("XDG_RUNTIME_DIR"),
        &home_path(".tsindex"),
        crate::daemon::current_uid(),
    )?;
    if socket.is_absolute() {
        return Ok(socket);
    }
    // Relative arguments belong to the service's cwd, including any
    // WorkingDirectory override. A stock user unit defaults to the login home.
    Ok(systemd_service_working_dir()
        .unwrap_or_else(|| home_path(""))
        .join(socket))
}

#[cfg(target_os = "linux")]
fn systemd_service_working_dir() -> Option<PathBuf> {
    let output = Command::new("systemctl")
        .args([
            "--user",
            "show",
            "--property=WorkingDirectory",
            "--value",
            LINUX_SERVICE_NAME,
        ])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let value = output.stdout.strip_suffix(b"\n").unwrap_or(&output.stdout);
    // systemd prefixes the property with '!' when a missing directory is
    // allowed. A value of '~' falls back to the login home.
    let path = Path::new(std::ffi::OsStr::from_bytes(
        value.strip_prefix(b"!").unwrap_or(value),
    ));
    path.is_absolute().then(|| path.to_path_buf())
}

#[cfg(target_os = "macos")]
fn endpoint_from_service() -> Result<PathBuf> {
    endpoint_from_launchd_plist(
        &fs::read_to_string(service_file_path())?,
        launchd_user_environment,
    )
}

#[cfg(target_os = "macos")]
fn launchd_user_environment(key: &str) -> Option<PathBuf> {
    let output = Command::new("launchctl")
        .args(["getenv", key])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let value = output.stdout.strip_suffix(b"\n").unwrap_or(&output.stdout);
    (!value.is_empty()).then(|| PathBuf::from(std::ffi::OsStr::from_bytes(value)))
}

#[cfg(any(target_os = "macos", test))]
fn endpoint_from_launchd_plist(
    source: &str,
    manager_env: impl Fn(&str) -> Option<PathBuf>,
) -> Result<PathBuf> {
    let args = source
        .split_once("<key>ProgramArguments</key>")
        .and_then(|(_, value)| value.split_once("<array>"))
        .and_then(|(_, value)| value.split_once("</array>"))
        .map(|(value, _)| value)
        .context("installed service has no ProgramArguments")?;
    let values: Vec<String> = args
        .split("<string>")
        .skip(1)
        .filter_map(|part| part.split_once("</string>").map(|(value, _)| value))
        .map(|value| {
            value
                .replace("&lt;", "<")
                .replace("&gt;", ">")
                .replace("&quot;", "\"")
                .replace("&apos;", "'")
                .replace("&amp;", "&")
        })
        .collect();
    if let Some(socket) = values
        .windows(2)
        .find(|pair| pair[0] == "--socket")
        .map(|pair| PathBuf::from(&pair[1]))
    {
        return Ok(socket);
    }
    if !values.iter().any(|arg| arg == "--socket-activated") {
        // The original job inherited these overrides instead of persisting
        // them in the plist. Its TSINDEX_HOME layout was flat.
        if let Some(socket) = manager_env("TSINDEX_SOCKET")
            .or_else(|| manager_env("TSINDEX_HOME").map(|home| home.join("daemon.sock")))
        {
            return Ok(Path::new("/").join(socket));
        }
    }
    Ok(legacy_default_socket())
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn endpoint_from_service() -> Result<PathBuf> {
    Ok(legacy_default_socket())
}

fn manager_output(command: &mut Command, operation: &str) -> Result<()> {
    let output = command
        .output()
        .with_context(|| format!("failed to {operation}"))?;
    if !output.status.success() {
        bail!(
            "{operation} failed ({}): {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn install_platform_service(paths: &DaemonPaths, binary: &Path) -> Result<()> {
    let service = service_file_path();
    if service.is_file() && !service.with_extension("socket").exists() {
        // The legacy service was enabled directly under default.target. Remove
        // its startup link before enabling the on-demand socket definition.
        manager_output(
            Command::new("systemctl").args(["--user", "disable", LINUX_SERVICE_NAME]),
            "disable legacy user service",
        )?;
    }
    write_file_atomically(
        &service,
        &render_systemd_unit_with_home(
            binary,
            Some(&paths.socket_path),
            resolved_service_home()?.as_deref(),
        ),
    )?;
    write_file_atomically(
        &service.with_extension("socket"),
        &render_systemd_socket(binary, &paths.socket_path),
    )?;
    manager_output(
        Command::new("systemctl").args(["--user", "daemon-reload"]),
        "reload user service definitions",
    )?;
    // Only the socket is enabled at login. Its first request starts the service.
    manager_output(
        Command::new("systemctl").args(["--user", "enable", LINUX_SOCKET_NAME]),
        "enable activation socket",
    )
}

#[cfg(target_os = "macos")]
fn install_platform_service(paths: &DaemonPaths, binary: &Path) -> Result<()> {
    let path_env = env::var("PATH")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_MACOS_PATH.to_string());
    write_file_atomically(
        &service_file_path(),
        &render_launchd_plist_with_home(
            binary,
            Some(&paths.socket_path),
            &paths.log_path,
            &path_env,
            resolved_service_home()?.as_deref(),
        ),
    )
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn install_platform_service(_paths: &DaemonPaths, _binary: &Path) -> Result<()> {
    bail!("login service installation requires launchd or systemd --user")
}

#[cfg(target_os = "linux")]
fn start_os_service() -> Result<()> {
    let unit = if service_file_path().with_extension("socket").is_file() {
        LINUX_SOCKET_NAME
    } else {
        LINUX_SERVICE_NAME
    };
    manager_output(
        Command::new("systemctl").args(["--user", "start", unit]),
        "start user service endpoint",
    )
}

#[cfg(target_os = "macos")]
fn service_target() -> String {
    format!(
        "gui/{}/{}",
        crate::daemon::current_uid(),
        MACOS_SERVICE_LABEL
    )
}

#[cfg(target_os = "macos")]
fn launchd_job_loaded() -> Result<bool> {
    let loaded = Command::new("launchctl")
        .args(["print", &service_target()])
        .output()
        .context("failed to inspect launchd service")?;
    if loaded.status.success() {
        return Ok(true);
    }
    let message = String::from_utf8_lossy(&loaded.stderr);
    if message.to_lowercase().contains("could not find service") {
        return Ok(false);
    }
    bail!("cannot inspect launchd service: {}", message.trim())
}

#[cfg(target_os = "macos")]
fn start_os_service() -> Result<()> {
    // A stopped job has been booted out, so reload it to restore its socket.
    if launchd_job_loaded()? {
        if !fs::read_to_string(service_file_path())?.contains("<key>Sockets</key>") {
            manager_output(
                Command::new("launchctl").args(["kickstart", &service_target()]),
                "start legacy launchd service",
            )?;
        }
        return Ok(());
    }
    let socket = endpoint_from_service()?;
    let paths = DaemonPaths::resolve(Some(&socket))?;
    check_socket(&paths)?;
    prepare_service_dirs(&paths)?;
    let domain = format!("gui/{}", crate::daemon::current_uid());
    manager_output(
        Command::new("launchctl")
            .args(["bootstrap", &domain])
            .arg(service_file_path()),
        "bootstrap launchd service",
    )
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn start_os_service() -> Result<()> {
    bail!("login service management requires launchd or systemd --user")
}

#[cfg(target_os = "linux")]
fn stop_os_service() -> Result<()> {
    // Stop the endpoint and service together so readiness probes cannot activate
    // a replacement while the old supervisor drains its sessions.
    if service_file_path().with_extension("socket").is_file() {
        return manager_output(
            Command::new("systemctl").args([
                "--user",
                "stop",
                LINUX_SOCKET_NAME,
                LINUX_SERVICE_NAME,
            ]),
            "stop activation socket and service",
        );
    }
    // An installed legacy unit can be absent from the running manager's cache.
    // Absence needs no stop, while connection/authorization errors still fail.
    let state = Command::new("systemctl")
        .args([
            "--user",
            "show",
            "--property=LoadState",
            "--value",
            LINUX_SERVICE_NAME,
        ])
        .output()?;
    if String::from_utf8_lossy(&state.stdout).trim() == "not-found" {
        return Ok(());
    }
    if !state.status.success() {
        bail!(
            "cannot inspect legacy user service: {}",
            String::from_utf8_lossy(&state.stderr).trim()
        );
    }
    manager_output(
        Command::new("systemctl").args(["--user", "stop", LINUX_SERVICE_NAME]),
        "stop legacy user service",
    )
}

#[cfg(target_os = "macos")]
fn stop_os_service() -> Result<()> {
    if !launchd_job_loaded()? {
        return Ok(());
    }
    manager_output(
        Command::new("launchctl").args(["bootout", &service_target()]),
        "stop launchd service",
    )
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn stop_os_service() -> Result<()> {
    bail!("login service management requires launchd or systemd --user")
}

/// Stop and remove the installed service, socket definition, and endpoint record.
pub fn uninstall_service(paths: &DaemonPaths) -> Result<PathBuf> {
    let service = service_file_path();
    // An empty uninstall stays usable with a read-only or nonexistent config
    // directory. When definitions exist, serialize their removal with installs.
    let _install_lock = if service.exists()
        || service.with_extension("socket").exists()
        || service.with_extension("install.lock").exists()
    {
        Some(installation_lock()?)
    } else {
        None
    };
    let had_service = service.is_file();
    let had_socket = service.with_extension("socket").is_file();
    if had_service {
        if !installed_endpoint_matches(paths)? {
            bail!("installed service uses another socket; select its endpoint before uninstalling");
        }
        let owner = capture_running_daemon(paths, true)?;
        stop_os_service()?;
        stop_captured_daemon(paths, owner)?;
    } else if had_socket {
        stop_os_service()?;
    } else {
        // Uninstall owns the login service. A manual supervisor on this socket
        // belongs to its launcher and remains available to its existing clients.
        return Ok(service);
    }
    #[cfg(target_os = "linux")]
    manager_output(
        Command::new("systemctl").args([
            "--user",
            "disable",
            if had_socket {
                LINUX_SOCKET_NAME
            } else {
                LINUX_SERVICE_NAME
            },
        ]),
        "disable installed endpoint",
    )?;
    for file in [
        service.clone(),
        service.with_extension("socket"),
        endpoint_file(),
    ] {
        match fs::remove_file(&file) {
            Ok(()) => (),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
            Err(error) => {
                return Err(error).with_context(|| format!("failed to remove {}", file.display()));
            }
        }
    }
    #[cfg(target_os = "linux")]
    manager_output(
        Command::new("systemctl").args(["--user", "daemon-reload"]),
        "reload user service definitions",
    )?;
    Ok(service)
}

/// Start the selected endpoint through its installed manager or as a manual daemon.
pub fn start_daemon(paths: &DaemonPaths, socket_override: Option<&Path>) -> Result<DaemonStatus> {
    inspect_socket_node(paths)?;
    if installed_endpoint_matches(paths)? {
        start_os_service()?;
        return wait_for_daemon_ready(paths, START_TIMEOUT);
    }
    let client = DaemonClient::new(paths.clone());
    if let Ok(status) = client.ping() {
        return Ok(status);
    }
    check_socket(paths)?;
    spawn_detached_daemon(paths, socket_override)?;
    wait_for_daemon_ready(paths, START_TIMEOUT)
}

fn spawn_detached_daemon(paths: &DaemonPaths, _socket_override: Option<&Path>) -> Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    let binary = invocation_binary()?;
    ensure_private_dir(paths.socket_dir()?)?;
    crate::daemon::orchestrator::prepare_metadata_dir(paths)?;
    if let Some(log_dir) = paths.log_path.parent() {
        ensure_private_dir(log_dir)?;
    }
    let log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&paths.log_path)
        .with_context(|| format!("failed to open daemon log {}", paths.log_path.display()))?;
    if !log_file.metadata()?.is_file() {
        bail!("daemon log must be a regular file");
    }
    let log_err = log_file.try_clone()?;
    let mut cmd = Command::new(binary);
    cmd.arg("--socket")
        .arg(&paths.socket_path)
        .args(["daemon", "run"])
        .stdin(Stdio::null())
        .stdout(Stdio::from(log_file))
        .stderr(Stdio::from(log_err));
    crate::daemon::fds::prepare_exec(&mut cmd, None)?;
    unsafe {
        cmd.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    cmd.spawn()
        .context("failed to spawn detached tsindex daemon")?;
    Ok(())
}

fn stop_detached_daemon(paths: &DaemonPaths) -> Result<bool> {
    let owner = capture_running_daemon(paths, false)?;
    stop_captured_daemon(paths, owner)
}

/// Stop activation as well as the worker-owning service at an installed endpoint.
pub fn stop_daemon(paths: &DaemonPaths) -> Result<bool> {
    if installed_endpoint_matches(paths)? {
        let owner = capture_running_daemon(paths, true)?;
        stop_os_service()?;
        stop_captured_daemon(paths, owner)?;
        return Ok(true);
    }
    stop_detached_daemon(paths)
}

/// Keep a manager-owned listener available while its supervisor is replaced.
pub fn restart_daemon(paths: &DaemonPaths, socket_override: Option<&Path>) -> Result<DaemonStatus> {
    inspect_socket_node(paths)?;
    if installed_endpoint_matches(paths)? {
        start_os_service()?;
        #[cfg(target_os = "linux")]
        manager_output(
            Command::new("systemctl").args(["--user", "restart", LINUX_SERVICE_NAME]),
            "restart user service",
        )?;
        #[cfg(target_os = "macos")]
        {
            let owner = capture_running_daemon(paths, true)?;
            if owner.is_some()
                && fs::read_to_string(service_file_path())?.contains("<key>Sockets</key>")
            {
                // Keep the registered socket while the responsive process
                // exits. The final readiness probe activates its successor.
                stop_captured_daemon(paths, owner)?;
            } else {
                // A legacy job self-binds; an unresponsive job cannot perform
                // an IPC shutdown. In both cases launchd must replace the job.
                stop_os_service()?;
                stop_captured_daemon(paths, owner)?;
                start_os_service()?;
            }
        }
        return wait_for_daemon_ready(paths, START_TIMEOUT);
    }
    stop_detached_daemon(paths)?;
    start_daemon(paths, socket_override)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn launchd_plist_contains_expected_keys_and_escapes_xml() {
        let plist = render_launchd_plist(
            Path::new("/usr/local/bin/tsindex"),
            Some(Path::new("/tmp/a&b/daemon.sock")),
            Path::new("/Users/dev/.tsindex/daemon.log"),
            "/opt/homebrew/bin:/usr/bin",
        );
        assert!(plist.contains(MACOS_SERVICE_LABEL));
        assert!(plist.contains("<string>/usr/local/bin/tsindex</string>"));
        assert!(plist.contains("<string>daemon</string>"));
        assert!(plist.contains("<string>run</string>"));
        assert!(plist.contains("<string>/tmp/a&amp;b/daemon.sock</string>"));
        assert!(plist.contains("<key>Sockets</key>"));
        assert!(plist.contains("<key>SockPathMode</key>"));
        assert!(plist.contains("<false/>"));
        let throttle = plist
            .split("<key>ThrottleInterval</key>")
            .nth(1)
            .unwrap()
            .split("<integer>")
            .nth(1)
            .unwrap()
            .split("</integer>")
            .next()
            .unwrap()
            .parse::<u64>()
            .unwrap();
        assert!(
            START_TIMEOUT.as_secs() > throttle,
            "readiness timeout must cover launchd's restart throttle"
        );
    }

    #[test]
    fn legacy_launchd_program_arguments_preserve_xml_escaped_socket_paths() -> Result<()> {
        let legacy = r#"<plist><dict>
<key>ProgramArguments</key><array>
<string>/usr/local/bin/tsindex</string><string>daemon</string><string>run</string>
<string>--socket</string><string>/private/a&amp;b/sock&quot;name</string>
</array><key>RunAtLoad</key><true/>
<key>KeepAlive</key><dict><key>SuccessfulExit</key><false/></dict>
</dict></plist>"#;
        assert_eq!(
            endpoint_from_launchd_plist(legacy, |_| panic!("explicit socket skips launchd"))?,
            PathBuf::from("/private/a&b/sock\"name")
        );
        Ok(())
    }

    #[test]
    fn systemd_unit_quotes_paths_and_sets_restart_policy() {
        let unit = render_systemd_unit(
            Path::new("/home/dev/.cargo/bin/tsindex"),
            Some(Path::new("/run/user/1000/tsindex/daemon.sock")),
        );
        assert!(unit.contains(
            "ExecStart=\"/home/dev/.cargo/bin/tsindex\" daemon run --socket-activated --socket \"/run/user/1000/tsindex/daemon.sock\""
        ));
        assert!(unit.contains("Restart=on-failure"));
        assert!(unit.contains("KillMode=mixed"));
    }

    #[test]
    fn systemd_unit_omits_socket_flag_when_using_defaults() {
        let unit = render_systemd_unit(Path::new("/usr/bin/tsindex"), None);
        assert!(unit.contains("ExecStart=\"/usr/bin/tsindex\" daemon run --socket-activated\n"));
        assert!(!unit.contains("--socket "));
    }

    #[test]
    fn systemd_dollars_are_escaped_only_in_command_arguments() {
        let binary = Path::new("/opt/build$/tsindex");
        let socket = Path::new("/run/user/1000/$name.sock");
        let service = render_systemd_unit(binary, Some(socket));
        let activation = render_systemd_socket(binary, socket);
        assert!(service.contains("ExecStart=\"/opt/build$/tsindex\" "));
        assert!(activation.contains("ExecStartPre=\"/opt/build$/tsindex\" "));
        assert!(service.contains("--socket \"/run/user/1000/$$name.sock\""));
        assert!(activation.contains("--socket \"/run/user/1000/$$name.sock\""));
    }

    #[test]
    fn verify_service_binary_rejects_binary_without_activation_support() -> Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir_in("/tmp")?;
        let pre_activation = dir.path().join("old-tsindex");
        fs::write(
            &pre_activation,
            "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then printf 'tsindex 2.0.0\\n'; exit 0; fi\nexit 2\n",
        )?;
        fs::set_permissions(&pre_activation, fs::Permissions::from_mode(0o700))?;

        let error = verify_service_binary(&pre_activation).unwrap_err();
        assert!(
            error.to_string().contains("socket-activated"),
            "unexpected error: {error:#}"
        );

        #[cfg(target_os = "linux")]
        {
            let missing_check_socket = dir.path().join("no-check-socket");
            fs::write(
                &missing_check_socket,
                "#!/bin/sh\ncase \"$*\" in\n  --version) printf 'tsindex 2.0.0\\n'; exit 0 ;;\n  'daemon run --socket-activated --help') exit 0 ;;\n  *) exit 2 ;;\nesac\n",
            )?;
            fs::set_permissions(&missing_check_socket, fs::Permissions::from_mode(0o700))?;
            let error = verify_service_binary(&missing_check_socket).unwrap_err();
            assert!(
                error.to_string().contains("check-socket"),
                "unexpected error: {error:#}"
            );
        }
        Ok(())
    }

    #[test]
    fn socketless_legacy_systemd_unit_resolves_from_manager_environment() -> Result<()> {
        let legacy =
            "[Unit]\nAfter=default.target\n[Service]\nExecStart=\"/usr/bin/tsindex\" daemon run\n";
        let home = Path::new("/home/dev/.tsindex");

        // Invoking shell unset XDG_RUNTIME_DIR, but systemd --user supplies it.
        let socket = endpoint_from_systemd_unit(
            legacy,
            || Some("HOME=/home/dev\nXDG_RUNTIME_DIR=/run/user/1000\n".into()),
            None,
            home,
            1000,
        )?;
        assert_eq!(socket, PathBuf::from("/run/user/1000/tsindex/daemon.sock"));

        // Manager environment without XDG_RUNTIME_DIR does not inherit client shell XDG_RUNTIME_DIR.
        let socket = endpoint_from_systemd_unit(
            legacy,
            || Some("HOME=/home/dev\n".into()),
            Some("/tmp/client-runtime".into()),
            home,
            1000,
        )?;
        assert_eq!(socket, PathBuf::from("/home/dev/.tsindex/run/daemon.sock"));

        // Original socketless units placed TSINDEX_HOME sockets at `$TSINDEX_HOME/daemon.sock`.
        let with_home = "[Service]\nEnvironment=\"TSINDEX_HOME=/opt/tsindex-home\"\nExecStart=\"/usr/bin/tsindex\" daemon run\n";
        let socket = endpoint_from_systemd_unit(
            with_home,
            || Some("XDG_RUNTIME_DIR=/run/user/1000\n".into()),
            None,
            home,
            1000,
        )?;
        assert_eq!(socket, PathBuf::from("/opt/tsindex-home/daemon.sock"));

        // Socket-activated units without `--socket` still use `$TSINDEX_HOME/run/daemon.sock`.
        let socket_activated_home = "[Service]\nEnvironment=\"TSINDEX_HOME=/opt/tsindex-home\"\nExecStart=\"/usr/bin/tsindex\" daemon run --socket-activated\n";
        let socket = endpoint_from_systemd_unit(
            socket_activated_home,
            || Some("XDG_RUNTIME_DIR=/run/user/1000\n".into()),
            None,
            home,
            1000,
        )?;
        assert_eq!(socket, PathBuf::from("/opt/tsindex-home/run/daemon.sock"));

        // The newer endpoint may belong to an independent daemon. A legacy
        // service still used the flat path even when its socket is absent.
        let temp_home = tempfile::tempdir_in("/tmp")?;
        fs::create_dir_all(temp_home.path().join("run"))?;
        fs::write(temp_home.path().join("run/daemon.sock"), "")?;
        let socket = endpoint_from_systemd_unit(
            legacy,
            || Some(format!("TSINDEX_HOME={}\n", temp_home.path().display())),
            None,
            home,
            1000,
        )?;
        assert_eq!(socket, temp_home.path().join("daemon.sock"));

        // `systemctl --user show-environment` shell-quotes paths with spaces or special chars.
        let socket = endpoint_from_systemd_unit(
            legacy,
            || Some("TSINDEX_SOCKET=$'/tmp/my dir/it\\'s.sock'\n".into()),
            None,
            home,
            1000,
        )?;
        assert_eq!(socket, PathBuf::from("/tmp/my dir/it's.sock"));

        let socket = endpoint_from_systemd_unit(
            legacy,
            || Some("TSINDEX_SOCKET='/tmp/single quoted/daemon.sock'\n".into()),
            None,
            home,
            1000,
        )?;
        assert_eq!(socket, PathBuf::from("/tmp/single quoted/daemon.sock"));

        let socket = endpoint_from_systemd_unit(
            legacy,
            || Some("TSINDEX_SOCKET=\"/tmp/double quoted/\\$name.sock\"\n".into()),
            None,
            home,
            1000,
        )?;
        assert_eq!(socket, PathBuf::from("/tmp/double quoted/$name.sock"));

        let socket = endpoint_from_systemd_unit(
            legacy,
            || Some("TSINDEX_SOCKET=/tmp/escaped\\ dir/daemon.sock\n".into()),
            None,
            home,
            1000,
        )?;
        assert_eq!(socket, PathBuf::from("/tmp/escaped dir/daemon.sock"));

        let socket = endpoint_from_systemd_unit(
            legacy,
            || Some("TSINDEX_SOCKET=$'/tmp/caf\\xc3\\xa9/daemon.sock'\n".into()),
            None,
            home,
            1000,
        )?;
        assert_eq!(socket, PathBuf::from("/tmp/café/daemon.sock"));

        // Empty shell-quoted TSINDEX_SOCKET falls through to XDG_RUNTIME_DIR.
        let socket = endpoint_from_systemd_unit(
            legacy,
            || Some("TSINDEX_SOCKET=$''\nXDG_RUNTIME_DIR=$'/run/user/my 1000'\n".into()),
            None,
            home,
            1000,
        )?;
        assert_eq!(
            socket,
            PathBuf::from("/run/user/my 1000/tsindex/daemon.sock")
        );

        // Explicit --socket skips manager environment lookup entirely.
        let explicit =
            "[Service]\nExecStart=\"/usr/bin/tsindex\" daemon run --socket \"/tmp/custom.sock\"\n";
        let socket = endpoint_from_systemd_unit(
            explicit,
            || panic!("manager environment should not be queried when --socket is explicit"),
            None,
            home,
            1000,
        )?;
        assert_eq!(socket, PathBuf::from("/tmp/custom.sock"));

        Ok(())
    }

    #[test]
    fn prepare_service_dirs_recreates_missing_nested_endpoint_ancestors() -> Result<()> {
        use std::os::unix::fs::{PermissionsExt, symlink};

        let dir = tempfile::tempdir_in("/tmp")?;
        let outer = dir.path().join("outer");
        let inner = outer.join("inner");
        let socket = inner.join("daemon.sock");
        let paths = DaemonPaths::resolve(Some(&socket))?;
        assert!(!outer.exists());

        check_socket(&paths)?;
        prepare_service_dirs(&paths)?;
        assert!(inner.is_dir());
        assert_eq!(
            fs::symlink_metadata(&inner)?.permissions().mode() & 0o777,
            0o700
        );
        assert!(paths.lock_path.parent().unwrap().join("owner").is_file());

        // Symlink socket directories must still be rejected by validation.
        let target = dir.path().join("target");
        fs::create_dir(&target)?;
        fs::set_permissions(&target, fs::Permissions::from_mode(0o700))?;
        let link_dir = dir.path().join("link-dir");
        symlink(&target, &link_dir)?;
        let link_paths = DaemonPaths::resolve(Some(&link_dir.join("daemon.sock")))?;
        assert!(prepare_service_dirs(&link_paths).is_err());
        Ok(())
    }
}
