//! Login service installation (`launchd` on macOS, `systemd --user` on Linux)
//! and `start` / `stop` / `restart` control helpers for the orchestrator daemon.

use std::env;
use std::fs::{self, OpenOptions};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use serde::Serialize;

use crate::daemon::{DaemonClient, DaemonPaths, DaemonStatus};
use crate::maintenance::FileLease;
use crate::mcp::home_path;

pub const MACOS_SERVICE_LABEL: &str = "com.legalzoom.tsindex.daemon";
pub const LINUX_SERVICE_NAME: &str = "tsindex.service";
#[cfg(target_os = "macos")]
const DEFAULT_MACOS_PATH: &str = "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin";
const START_TIMEOUT: Duration = Duration::from_secs(5);
const STOP_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Serialize)]
pub struct ServiceInfo {
    pub manager: &'static str,
    pub installed: bool,
    pub service_file: PathBuf,
}

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
    let mut args = vec![
        binary.to_string_lossy().into_owned(),
        "daemon".to_string(),
        "run".to_string(),
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
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <dict>
        <key>SuccessfulExit</key>
        <false/>
    </dict>
    <key>StandardOutPath</key>
    <string>{log}</string>
    <key>StandardErrorPath</key>
    <string>{log}</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>{path_env}</string>
    </dict>
</dict>
</plist>
"#
    )
}

/// Render a Linux `systemd --user` service unit for `tsindex daemon run`.
pub fn render_systemd_unit(binary: &Path, socket: Option<&Path>) -> String {
    let mut exec_start = format!("{} daemon run", systemd_quote(&binary.to_string_lossy()));
    if let Some(socket) = socket {
        exec_start.push_str(" --socket ");
        exec_start.push_str(&systemd_quote(&socket.to_string_lossy()));
    }
    format!(
        r#"[Unit]
Description=tsindex code-navigation orchestrator daemon
After=default.target

[Service]
Type=simple
ExecStart={exec_start}
Restart=on-failure
RestartSec=2

[Install]
WantedBy=default.target
"#
    )
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

fn systemd_quote(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '%' => out.push_str("%%"),
            _ => out.push(ch),
        }
    }
    out.push('"');
    out
}

fn resolve_binary(binary_override: Option<&Path>) -> Result<PathBuf> {
    let binary = match binary_override {
        Some(path) => path.to_path_buf(),
        None => env::current_exe().context("failed to determine current tsindex executable")?,
    };
    binary
        .canonicalize()
        .with_context(|| format!("tsindex binary does not exist at {}", binary.display()))
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn write_file_atomically(path: &Path, contents: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create directory {}", parent.display()))?;
    }
    let tmp = path.with_extension(format!("tmp.{}", std::process::id()));
    fs::write(&tmp, contents).with_context(|| format!("failed to write {}", tmp.display()))?;
    fs::rename(&tmp, path).with_context(|| format!("failed to install {}", path.display()))?;
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

fn wait_for_daemon_stopped(paths: &DaemonPaths, timeout: Duration) -> Result<()> {
    let client = DaemonClient::new(paths.clone());
    let deadline = Instant::now() + timeout;
    loop {
        let ping_alive = client.ping().is_ok();
        let lock_free = !paths.lock_path.exists()
            || matches!(FileLease::try_acquire(&paths.lock_path), Ok(Some(_)));
        if !ping_alive && lock_free {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!(
                "timed out waiting for tsindex daemon at {} to stop",
                paths.socket_path.display()
            );
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn read_log_tail(log_path: &Path) -> String {
    let Ok(contents) = fs::read_to_string(log_path) else {
        return String::new();
    };
    let lines: Vec<&str> = contents.lines().collect();
    let start = lines.len().saturating_sub(15);
    lines[start..].join("\n")
}

/// Install and start the OS login service (`launchd` on macOS, `systemd --user`
/// on Linux).
pub fn install_service(
    paths: &DaemonPaths,
    socket_override: Option<&Path>,
    binary_override: Option<&Path>,
) -> Result<(ServiceInfo, DaemonStatus)> {
    let binary = resolve_binary(binary_override)?;
    if let Some(log_dir) = paths.log_path.parent() {
        fs::create_dir_all(log_dir)
            .with_context(|| format!("failed to create log directory {}", log_dir.display()))?;
    }

    // Stop any previously running daemon so the service manager owns the live instance.
    let _ = stop_daemon(paths);

    install_platform_service(paths, &binary, socket_override)
}

#[cfg(target_os = "macos")]
fn install_platform_service(
    paths: &DaemonPaths,
    binary: &Path,
    socket_override: Option<&Path>,
) -> Result<(ServiceInfo, DaemonStatus)> {
    let plist_path = service_file_path();
    let path_env = env::var("PATH")
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| DEFAULT_MACOS_PATH.to_string());
    let plist = render_launchd_plist(binary, socket_override, &paths.log_path, &path_env);
    write_file_atomically(&plist_path, &plist)?;

    let domain = format!("gui/{}", crate::daemon::current_uid());
    let service_target = format!("{domain}/{MACOS_SERVICE_LABEL}");
    let _ = Command::new("launchctl")
        .args(["bootout", &service_target])
        .status();

    let output = Command::new("launchctl")
        .args(["bootstrap", &domain])
        .arg(&plist_path)
        .output()
        .context("failed to invoke launchctl bootstrap")?;
    if !output.status.success() {
        bail!(
            "launchctl bootstrap failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let status = wait_for_daemon_ready(paths, START_TIMEOUT)?;
    Ok((inspect_service(), status))
}

#[cfg(target_os = "linux")]
fn install_platform_service(
    paths: &DaemonPaths,
    binary: &Path,
    socket_override: Option<&Path>,
) -> Result<(ServiceInfo, DaemonStatus)> {
    let unit_path = service_file_path();
    let unit = render_systemd_unit(binary, socket_override);
    write_file_atomically(&unit_path, &unit)?;

    let reload = Command::new("systemctl")
        .args(["--user", "daemon-reload"])
        .output()
        .context("failed to invoke `systemctl --user daemon-reload`")?;
    if !reload.status.success() {
        bail!(
            "systemctl --user daemon-reload failed: {}",
            String::from_utf8_lossy(&reload.stderr).trim()
        );
    }

    let enable = Command::new("systemctl")
        .args(["--user", "enable", "--now", LINUX_SERVICE_NAME])
        .output()
        .context("failed to invoke `systemctl --user enable --now`")?;
    if !enable.status.success() {
        bail!(
            "systemctl --user enable --now failed: {}",
            String::from_utf8_lossy(&enable.stderr).trim()
        );
    }

    let status = wait_for_daemon_ready(paths, START_TIMEOUT)?;
    Ok((inspect_service(), status))
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn install_platform_service(
    _paths: &DaemonPaths,
    _binary: &Path,
    _socket_override: Option<&Path>,
) -> Result<(ServiceInfo, DaemonStatus)> {
    bail!("login service installation is supported on macOS (launchd) and Linux (systemd --user)")
}

/// Stop, disable, and remove the OS login service.
pub fn uninstall_service(paths: &DaemonPaths) -> Result<PathBuf> {
    let _ = stop_daemon(paths);
    let service_file = service_file_path();
    uninstall_platform_service(&service_file)?;
    Ok(service_file)
}

#[cfg(target_os = "macos")]
fn uninstall_platform_service(service_file: &Path) -> Result<()> {
    let service_target = format!("gui/{}/{MACOS_SERVICE_LABEL}", crate::daemon::current_uid());
    let _ = Command::new("launchctl")
        .args(["bootout", &service_target])
        .status();
    if service_file.exists() {
        fs::remove_file(service_file)
            .with_context(|| format!("failed to remove LaunchAgent {}", service_file.display()))?;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn uninstall_platform_service(service_file: &Path) -> Result<()> {
    let _ = Command::new("systemctl")
        .args(["--user", "disable", "--now", LINUX_SERVICE_NAME])
        .status();
    if service_file.exists() {
        fs::remove_file(service_file)
            .with_context(|| format!("failed to remove systemd unit {}", service_file.display()))?;
    }
    let _ = Command::new("systemctl")
        .args(["--user", "daemon-reload"])
        .status();
    Ok(())
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn uninstall_platform_service(_service_file: &Path) -> Result<()> {
    Ok(())
}

/// Start the orchestrator daemon (idempotent if already running).
pub fn start_daemon(paths: &DaemonPaths, socket_override: Option<&Path>) -> Result<DaemonStatus> {
    let client = DaemonClient::new(paths.clone());
    if let Ok(status) = client.ping() {
        return Ok(status);
    }

    let use_os_service = socket_override.is_none()
        && env::var_os("TSINDEX_SOCKET").is_none_or(|v| v.is_empty())
        && env::var_os("TSINDEX_HOME").is_none_or(|v| v.is_empty())
        && inspect_service().installed;

    if use_os_service && try_start_os_service()? {
        return wait_for_daemon_ready(paths, START_TIMEOUT);
    }

    spawn_detached_daemon(paths, socket_override)?;
    wait_for_daemon_ready(paths, START_TIMEOUT)
}

#[cfg(target_os = "macos")]
fn try_start_os_service() -> Result<bool> {
    let domain = format!("gui/{}", crate::daemon::current_uid());
    let target = format!("{domain}/{MACOS_SERVICE_LABEL}");
    let kickstart = Command::new("launchctl")
        .args(["kickstart", &target])
        .status();
    if matches!(kickstart, Ok(status) if status.success()) {
        return Ok(true);
    }
    let plist = service_file_path();
    let bootstrap = Command::new("launchctl")
        .args(["bootstrap", &domain])
        .arg(&plist)
        .status();
    Ok(matches!(bootstrap, Ok(status) if status.success()))
}

#[cfg(target_os = "linux")]
fn try_start_os_service() -> Result<bool> {
    let status = Command::new("systemctl")
        .args(["--user", "start", LINUX_SERVICE_NAME])
        .status();
    Ok(matches!(status, Ok(s) if s.success()))
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn try_start_os_service() -> Result<bool> {
    Ok(false)
}

fn spawn_detached_daemon(paths: &DaemonPaths, socket_override: Option<&Path>) -> Result<()> {
    let binary = resolve_binary(None)?;
    if let Some(log_dir) = paths.log_path.parent() {
        fs::create_dir_all(log_dir)
            .with_context(|| format!("failed to create log directory {}", log_dir.display()))?;
    }
    let log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&paths.log_path)
        .with_context(|| format!("failed to open daemon log {}", paths.log_path.display()))?;
    let log_err = log_file.try_clone()?;

    let mut cmd = Command::new(binary);
    if let Some(socket) = socket_override {
        cmd.arg("--socket").arg(socket);
    }
    cmd.args(["daemon", "run"])
        .stdin(Stdio::null())
        .stdout(Stdio::from(log_file))
        .stderr(Stdio::from(log_err));

    // Detach into its own session so closing the calling terminal does not
    // send SIGHUP to the long-standing orchestrator.
    unsafe {
        cmd.pre_exec(|| {
            let _ = libc::setsid();
            Ok(())
        });
    }

    cmd.spawn()
        .context("failed to spawn detached tsindex daemon")?;
    Ok(())
}

/// Gracefully stop the running orchestrator daemon. Returns `true` if a
/// running daemon was stopped, or `false` if none was running.
pub fn stop_daemon(paths: &DaemonPaths) -> Result<bool> {
    let client = DaemonClient::new(paths.clone());
    if client.ping().is_err() {
        return Ok(false);
    }
    client.shutdown()?;
    wait_for_daemon_stopped(paths, STOP_TIMEOUT)?;
    Ok(true)
}

/// Restart the orchestrator daemon cleanly.
pub fn restart_daemon(paths: &DaemonPaths, socket_override: Option<&Path>) -> Result<DaemonStatus> {
    let _ = stop_daemon(paths)?;
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
        assert!(plist.contains("<key>RunAtLoad</key>"));
        assert!(plist.contains("<key>SuccessfulExit</key>"));
        assert!(plist.contains("<false/>"));
    }

    #[test]
    fn systemd_unit_quotes_paths_and_sets_restart_policy() {
        let unit = render_systemd_unit(
            Path::new("/home/dev/.cargo/bin/tsindex"),
            Some(Path::new("/run/user/1000/tsindex/daemon.sock")),
        );
        assert!(unit.contains(
            "ExecStart=\"/home/dev/.cargo/bin/tsindex\" daemon run --socket \"/run/user/1000/tsindex/daemon.sock\""
        ));
        assert!(unit.contains("Restart=on-failure"));
        assert!(unit.contains("WantedBy=default.target"));
    }

    #[test]
    fn systemd_unit_omits_socket_flag_when_using_defaults() {
        let unit = render_systemd_unit(Path::new("/usr/bin/tsindex"), None);
        assert!(unit.contains("ExecStart=\"/usr/bin/tsindex\" daemon run\n"));
        assert!(!unit.contains("--socket"));
    }
}
