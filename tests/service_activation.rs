//! Exercise inherited listening descriptors and the installed manager boundary.
#![cfg(unix)]

use std::fs;
#[cfg(target_os = "linux")]
use std::io::{BufRead, BufReader, Write};
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
#[cfg(target_os = "linux")]
use std::os::unix::net::{UnixListener, UnixStream};
#[cfg(target_os = "linux")]
use std::os::unix::process::CommandExt;
use std::path::Path;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};
use tempfile::TempDir;
use tsindex::daemon::{DaemonClient, DaemonPaths, SessionState};

const BIN: &str = env!("CARGO_BIN_EXE_tsindex");

fn private_scratch() -> Result<TempDir> {
    use std::os::unix::fs::PermissionsExt;
    let scratch = tempfile::tempdir_in("/tmp")?;
    fs::set_permissions(scratch.path(), fs::Permissions::from_mode(0o700))?;
    Ok(scratch)
}

struct Daemon(Child);
impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn client(socket: &Path) -> Result<DaemonClient> {
    Ok(DaemonClient::new(DaemonPaths::resolve(Some(socket))?))
}

fn wait_ready(socket: &Path) -> Result<u32> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let error = match client(socket)?.ping() {
            Ok(status) => return Ok(status.daemon_pid),
            Err(error) => error,
        };
        ensure!(
            Instant::now() < deadline,
            "activated daemon did not become ready: {error:#}"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

fn active_worker(socket: &Path, root: &Path) -> Result<(Daemon, u32)> {
    fs::create_dir(root)?;
    fs::write(root.join("lib.py"), "def supervised(): return 1\n")?;
    let proxy = Daemon(
        Command::new(BIN)
            .arg("--socket")
            .arg(socket)
            .arg("--root")
            .arg(root)
            .arg("--no-refresh")
            .args(["serve", "--mcp", "--via-daemon"])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?,
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        // Admission exposes a Starting reservation before it has a child PID.
        // Wait for this proxy's ready worker before exercising manager cleanup.
        if let Ok((active, _)) = client(socket)?.list_sessions(false)
            && let Some(session) = active.iter().find(|session| {
                session.client_pid == proxy.0.id()
                    && session.state == SessionState::Active
                    && session.worker_pid > 1
            })
        {
            return Ok((proxy, session.worker_pid));
        }
        ensure!(Instant::now() < deadline, "managed worker did not register");
        thread::sleep(Duration::from_millis(20));
    }
}

fn wait_gone(pid: u32) -> Result<()> {
    ensure!(
        pid > 1 && pid <= i32::MAX as u32,
        "expected a concrete managed process PID"
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    while unsafe { libc::kill(pid as libc::pid_t, 0) } == 0 {
        ensure!(
            Instant::now() < deadline,
            "managed process {pid} survived supervisor death"
        );
        thread::sleep(Duration::from_millis(20));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn inherit(listener: &UnixListener, socket: &Path) -> Result<Daemon> {
    let fd = listener.as_raw_fd();
    let mut command = Command::new("/bin/sh");
    command
        .args([
            "-c",
            "export LISTEN_PID=$$ LISTEN_FDS=1; exec \"$@\"",
            "activation-test",
        ])
        .arg(BIN)
        .arg("--socket")
        .arg(socket)
        .args(["daemon", "run", "--socket-activated"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());
    // Pass only descriptor 3 using the documented systemd activation protocol.
    // The shell fills LISTEN_PID before exec without post-fork allocation here.
    unsafe {
        command.pre_exec(move || {
            if libc::dup2(fd, 3) < 0 || libc::fcntl(3, libc::F_SETFD, 0) < 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    Ok(Daemon(command.spawn()?))
}

#[test]
fn required_activation_cannot_fall_back_to_binding() -> Result<()> {
    let scratch = private_scratch()?;
    let socket = scratch.path().join("d.sock");
    let output = Command::new(BIN)
        .arg("--socket")
        .arg(&socket)
        .args(["daemon", "run", "--socket-activated"])
        .env_remove("LISTEN_PID")
        .env_remove("LISTEN_FDS")
        .output()?;
    ensure!(!output.status.success(), "missing activation should fail");
    ensure!(
        !socket.exists(),
        "required activation created an unmanaged endpoint"
    );
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn activation_retains_listener_and_pending_requests_across_restart() -> Result<()> {
    let scratch = private_scratch()?;
    let socket = scratch.path().join("d.sock");
    let listener = UnixListener::bind(&socket)?;
    let mut first = inherit(&listener, &socket)?;
    let pid = wait_ready(&socket)?;
    client(&socket)?.shutdown()?;
    ensure!(first.0.wait()?.success());
    ensure!(socket.exists(), "service removed the manager's endpoint");

    // A queued connection must survive the interval without a supervisor.
    let mut pending = UnixStream::connect(&socket)?;
    pending.set_read_timeout(Some(Duration::from_secs(10)))?;
    pending.write_all(b"{\"op\":\"ping\"}\n")?;
    let mut second = inherit(&listener, &socket)?;
    let mut response = String::new();
    BufReader::new(pending).read_line(&mut response)?;
    ensure!(
        response.contains("daemon_pid"),
        "pending request was lost: {response}"
    );
    ensure!(wait_ready(&socket)? != pid);
    client(&socket)?.shutdown()?;
    ensure!(second.0.wait()?.success());
    ensure!(socket.exists());
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn activation_rejects_descriptor_for_another_endpoint() -> Result<()> {
    let scratch = private_scratch()?;
    let actual = scratch.path().join("a.sock");
    let wanted = scratch.path().join("b.sock");
    let listener = UnixListener::bind(&actual)?;
    let mut daemon = inherit(&listener, &wanted)?;
    ensure!(!daemon.0.wait()?.success());
    ensure!(actual.exists());
    ensure!(!wanted.exists());
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn installed_custom_endpoint_uses_manager_and_reports_manager_failures() -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let scratch = private_scratch()?;
    let socket = scratch.path().join("d.sock");
    let config = scratch.path().join("config/systemd/user");
    fs::create_dir_all(&config)?;
    fs::write(config.join("tsindex.service"), "[Service]\n")?;
    fs::write(config.join("tsindex.socket"), "[Socket]\n")?;
    fs::write(
        config.join("tsindex.json"),
        serde_json::to_vec(&serde_json::json!({"socket": socket}))?,
    )?;
    let bin = scratch.path().join("bin");
    fs::create_dir(&bin)?;
    let calls = scratch.path().join("calls");
    let manager = bin.join("systemctl");
    fs::write(
        &manager,
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >>\"$FAKE_MANAGER_CALLS\"\nprintf 'manager intentionally unavailable\\n' >&2\nexit 42\n",
    )?;
    fs::set_permissions(&manager, fs::Permissions::from_mode(0o700))?;
    for action in ["start", "stop", "restart", "uninstall"] {
        let output = Command::new(BIN)
            .arg("--socket")
            .arg(&socket)
            .args(["daemon", action])
            .env("XDG_CONFIG_HOME", scratch.path().join("config"))
            .env("TSINDEX_HOME", scratch.path().join("home"))
            .env("PATH", &bin)
            .env("FAKE_MANAGER_CALLS", &calls)
            .output()?;
        ensure!(
            !output.status.success(),
            "{action} concealed manager failure"
        );
        ensure!(
            String::from_utf8_lossy(&output.stderr).contains("manager intentionally unavailable")
        );
        ensure!(
            !socket.exists(),
            "{action} started an unmanaged replacement"
        );
    }
    let calls = fs::read_to_string(calls)?;
    ensure!(calls.contains("--user start tsindex.socket"));
    ensure!(calls.contains("--user stop tsindex.socket tsindex.service"));
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn login_clients_discover_installed_socket_and_preserve_explicit_isolation() -> Result<()> {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let scratch = private_scratch()?;
    let runtime = scratch.path().join("runtime");
    let config = scratch.path().join("config/systemd/user");
    fs::create_dir_all(&config)?;
    let private = scratch.path().join("private");
    fs::create_dir(&private)?;
    fs::set_permissions(&private, fs::Permissions::from_mode(0o700))?;
    let socket = private.join("custom.sock");
    fs::write(config.join("tsindex.service"), "[Service]\n")?;
    fs::write(config.join("tsindex.socket"), "[Socket]\n")?;
    fs::write(
        config.join("tsindex.json"),
        serde_json::to_vec(&serde_json::json!({
            "socket": socket, "default_socket": runtime.join("tsindex/daemon.sock"),
        }))?,
    )?;
    let bin = scratch.path().join("bin");
    fs::create_dir(&bin)?;
    let manager = bin.join("systemctl");
    fs::write(&manager, "#!/bin/sh\nprintf '%s\\n' \"$*\" >&2\nexit 42\n")?;
    fs::set_permissions(&manager, fs::Permissions::from_mode(0o700))?;
    let command = || {
        let mut cmd = Command::new(BIN);
        cmd.env("XDG_CONFIG_HOME", scratch.path().join("config"))
            .env("XDG_RUNTIME_DIR", &runtime)
            .env("HOME", scratch.path())
            .env("PATH", &bin)
            .env_remove("TSINDEX_HOME")
            .env_remove("TSINDEX_SOCKET");
        cmd
    };
    let status_socket = |cmd: &mut Command| -> Result<PathBuf> {
        let out = cmd.args(["daemon", "status", "--json"]).output()?;
        ensure!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        let status: serde_json::Value = serde_json::from_slice(&out.stdout)?;
        Ok(PathBuf::from(
            status["socket"].as_str().context("socket missing")?,
        ))
    };
    ensure!(status_socket(&mut command())? == socket);
    let standard_runtime = PathBuf::from(format!("/run/user/{}", unsafe { libc::geteuid() }));
    let endpoint = |default: &Path| -> Result<()> {
        fs::write(
            config.join("tsindex.json"),
            serde_json::to_vec(&serde_json::json!({
                "socket": socket, "default_socket": default,
            }))?,
        )?;
        Ok(())
    };
    endpoint(&standard_runtime.join("tsindex/daemon.sock"))?;
    ensure!(status_socket(command().env_remove("XDG_RUNTIME_DIR"))? == socket);
    ensure!(status_socket(&mut command())? == runtime.join("tsindex/daemon.sock"));
    endpoint(&scratch.path().join(".tsindex/run/daemon.sock"))?;
    ensure!(status_socket(command().env("XDG_RUNTIME_DIR", &standard_runtime))? == socket);
    endpoint(&runtime.join("tsindex/daemon.sock"))?;
    let independent = scratch.path().join("independent.sock");
    ensure!(status_socket(command().arg("--socket").arg(&independent))? == independent);
    ensure!(status_socket(command().env("TSINDEX_SOCKET", &independent))? == independent);
    let other_home = scratch.path().join("cache");
    ensure!(
        status_socket(command().env("TSINDEX_HOME", &other_home))?
            == other_home.join("run/daemon.sock")
    );
    let other_runtime = scratch.path().join("runtime2");
    ensure!(
        status_socket(command().env("XDG_RUNTIME_DIR", &other_runtime))?
            == other_runtime.join("tsindex/daemon.sock")
    );

    let alias = scratch.path().join("ancestor-alias");
    symlink(scratch.path(), &alias)?;
    for selected in [None, Some(alias.join("private/custom.sock"))] {
        let mut cmd = command();
        if let Some(selected) = selected {
            cmd.arg("--socket").arg(selected);
        }
        let output = cmd.args(["daemon", "stop"]).output()?;
        ensure!(!output.status.success());
        ensure!(
            String::from_utf8_lossy(&output.stderr)
                .contains("--user stop tsindex.socket tsindex.service"),
            "stop must address the actual installed manager"
        );
    }
    // A fresh client shell can launch managed MCP using the single installed
    // endpoint, even without the custom socket override used at installation.
    let _server = Daemon(
        command()
            .arg("--socket")
            .arg(&socket)
            .args(["daemon", "run"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?,
    );
    wait_ready(&socket)?;
    let root = scratch.path().join("repo");
    fs::create_dir(&root)?;
    let result = command()
        .arg("--root")
        .arg(&root)
        .args(["--no-refresh", "serve", "--mcp", "--via-daemon"])
        .stdin(Stdio::null())
        .output()?;
    ensure!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    ensure!(client(&socket)?.ping()?.total_sessions_started == 1);
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn invalid_replacement_executable_preserves_existing_installation() -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let scratch = private_scratch()?;
    let config = scratch.path().join("config/systemd/user");
    fs::create_dir_all(&config)?;
    let service = config.join("tsindex.service");
    fs::write(&service, "[Service]\n")?;
    let socket = scratch.path().join("d.sock");
    let regular = scratch.path().join("plain");
    fs::write(&regular, "regular non-executable file")?;
    let invalid_image = scratch.path().join("invalid-image");
    fs::write(&invalid_image, "not an executable format")?;
    fs::set_permissions(&invalid_image, fs::Permissions::from_mode(0o700))?;
    let not_tsindex = scratch.path().join("not-tsindex");
    fs::write(&not_tsindex, "#!/bin/sh\nprintf 'another program 1.0\\n'\n")?;
    fs::set_permissions(&not_tsindex, fs::Permissions::from_mode(0o700))?;
    for invalid in [&regular, &config, &invalid_image, &not_tsindex] {
        let output = Command::new(BIN)
            .arg("--socket")
            .arg(&socket)
            .args(["daemon", "install", "--binary"])
            .arg(invalid)
            .env("HOME", scratch.path())
            .env("XDG_CONFIG_HOME", scratch.path().join("config"))
            .env("PATH", scratch.path().join("no-manager"))
            .output()?;
        ensure!(!output.status.success());
        let error = String::from_utf8_lossy(&output.stderr);
        ensure!(
            error.contains("cannot execute")
                || error.contains("regular file")
                || error.contains("did not identify itself as tsindex"),
            "{error}"
        );
        ensure!(fs::read_to_string(&service)? == "[Service]\n");
        ensure!(!config.join("tsindex.socket").exists());
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn manager_outage_during_replacement_restores_durable_configuration() -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let scratch = private_scratch()?;
    let config = scratch.path().join("config/systemd/user");
    fs::create_dir_all(&config)?;
    let service = config.join("tsindex.service");
    let socket_unit = config.join("tsindex.socket");
    let record = config.join("tsindex.json");
    let old_socket = scratch.path().join("old.sock");
    let metadata = serde_json::to_string(&serde_json::json!({"socket": old_socket}))?;
    fs::write(&service, "[Service]\n# previous version\n")?;
    fs::write(&socket_unit, "[Socket]\n# previous version\n")?;
    fs::write(&record, &metadata)?;
    let bin = scratch.path().join("bin");
    fs::create_dir(&bin)?;
    let manager = bin.join("systemctl");
    fs::write(
        &manager,
        r#"#!/bin/sh
case "$*" in
  *'start tsindex.socket') : >"$HOME/offline" ;;
esac
if test -e "$HOME/offline"; then
  printf 'manager unavailable\n' >&2
  exit 42
fi
exit 0
"#,
    )?;
    fs::set_permissions(&manager, fs::Permissions::from_mode(0o700))?;
    let output = Command::new(BIN)
        .arg("--socket")
        .arg(scratch.path().join("replacement.sock"))
        .args(["daemon", "install"])
        .env("XDG_CONFIG_HOME", scratch.path().join("config"))
        .env("HOME", scratch.path())
        .env("PATH", &bin)
        .env_remove("TSINDEX_HOME")
        .env_remove("TSINDEX_SOCKET")
        .output()?;
    let error = String::from_utf8_lossy(&output.stderr);
    ensure!(
        !output.status.success() && error.contains("rollback was incomplete"),
        "{error}"
    );
    ensure!(scratch.path().join("offline").exists());
    ensure!(fs::read_to_string(service)? == "[Service]\n# previous version\n");
    ensure!(fs::read_to_string(socket_unit)? == "[Socket]\n# previous version\n");
    ensure!(fs::read_to_string(record)? == metadata);
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn status_and_detached_stop_distinguish_absence_from_an_unhealthy_daemon() -> Result<()> {
    let scratch = private_scratch()?;
    let socket = scratch.path().join("d.sock");
    let cli = |action: &str| -> Result<std::process::Output> {
        Ok(Command::new(BIN)
            .arg("--socket")
            .arg(&socket)
            .args(["daemon", action])
            .env("HOME", scratch.path())
            .env("XDG_CONFIG_HOME", scratch.path().join("config"))
            .env_remove("TSINDEX_HOME")
            .env_remove("TSINDEX_SOCKET")
            .output()?)
    };
    ensure!(cli("status")?.status.success());
    ensure!(cli("stop")?.status.success());
    let listener = UnixListener::bind(&socket)?;
    thread::scope(|scope| -> Result<()> {
        let server = scope.spawn(|| -> Result<()> {
            for _ in 0..2 {
                let mut event = libc::pollfd {
                    fd: listener.as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                };
                ensure!(
                    unsafe { libc::poll(&mut event, 1, 5_000) } > 0,
                    "timed out waiting for control client"
                );
                let (mut stream, _) = listener.accept()?;
                stream.set_read_timeout(Some(Duration::from_secs(2)))?;
                let mut request = String::new();
                BufReader::new(stream.try_clone()?).read_line(&mut request)?;
                ensure!(request.contains("ping"));
                stream.write_all(b"unreadable response\n")?;
            }
            Ok(())
        });
        for action in ["status", "stop"] {
            let result = cli(action)?;
            ensure!(!result.status.success());
            ensure!(
                String::from_utf8_lossy(&result.stderr)
                    .contains("could not determine whether the daemon is running")
            );
        }
        server.join().expect("control responder panicked")?;
        Ok(())
    })
}

#[cfg(target_os = "linux")]
#[test]
fn legacy_service_routes_control_and_migrates_without_endpoint_metadata() -> Result<()> {
    use std::os::unix::fs::{PermissionsExt, symlink};
    for custom in [false, true] {
        let scratch = private_scratch()?;
        let runtime = scratch.path().join("runtime");
        let socket = if custom {
            scratch.path().join("custom % socket/d.sock")
        } else {
            runtime.join("tsindex/daemon.sock")
        };
        let config = scratch.path().join("config/systemd/user");
        fs::create_dir_all(&config)?;
        let service = config.join("tsindex.service");
        let socket_arg = if custom {
            format!(
                " --socket {}",
                serde_json::to_string(&socket)?.replace('%', "%%")
            )
        } else {
            String::new()
        };
        let legacy = format!(
            "[Unit]\nAfter=default.target\n[Service]\nExecStart={} daemon run{socket_arg}\nRestart=on-failure\n[Install]\nWantedBy=default.target\n",
            serde_json::to_string(BIN)?
        );
        fs::write(&service, &legacy)?;
        let bin = scratch.path().join("bin");
        fs::create_dir(&bin)?;
        let calls = scratch.path().join("calls");
        let manager = bin.join("systemctl");
        let invoked = bin.join("tsindex-link");
        symlink(BIN, &invoked)?;
        fs::write(
            &manager,
            r#"#!/bin/sh
printf '%s\n' "$*" >>"$FAKE_MANAGER_CALLS"
case "$*" in
  *'show --property=LoadState --value tsindex.service') printf 'not-found\n' ;;
  *'start tsindex.socket')
    /bin/cp "$XDG_CONFIG_HOME/systemd/user/tsindex.service" "$HOME/candidate-service"
    /bin/cp "$XDG_CONFIG_HOME/systemd/user/tsindex.json" "$HOME/candidate-endpoint"
    printf 'intentional manager start failure\n' >&2
    exit 42 ;;
  *'start '*|*'restart '*)
    if test -f "$HOME/candidate-service"; then exit 0; fi
    printf 'intentional manager start failure\n' >&2
    exit 42 ;;
esac
"#,
        )?;
        fs::set_permissions(&manager, fs::Permissions::from_mode(0o700))?;
        let run = |action: &str| -> Result<std::process::Output> {
            let mut command = Command::new(&invoked);
            if custom {
                command.arg("--socket").arg(&socket);
            }
            command
                .args(["daemon", action])
                .env("PATH", &bin)
                .env("FAKE_MANAGER_CALLS", &calls)
                .env("XDG_CONFIG_HOME", scratch.path().join("config"))
                .env("XDG_RUNTIME_DIR", &runtime)
                .env("HOME", scratch.path())
                .env_remove("TSINDEX_HOME")
                .env_remove("TSINDEX_SOCKET");
            if custom && action == "install" {
                command
                    .current_dir(scratch.path())
                    .env("TSINDEX_HOME", "relative-cache");
            }
            Ok(command.output()?)
        };
        for action in ["start", "restart"] {
            let output = run(action)?;
            ensure!(!output.status.success());
            ensure!(
                String::from_utf8_lossy(&output.stderr)
                    .contains("intentional manager start failure"),
                "legacy {action}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        // An installed unit absent from the live manager cache is already
        // stopped. Uninstall must still remove its files and enablement links.
        ensure!(run("stop")?.status.success());
        ensure!(run("uninstall")?.status.success());
        ensure!(!service.exists());
        let before = fs::read_to_string(&calls)?;
        ensure!(before.contains("--user start tsindex.service"));
        ensure!(before.contains("--user disable tsindex.service"));
        ensure!(!before.contains("--user stop tsindex.socket"));
        ensure!(run("uninstall")?.status.success());
        ensure!(
            fs::read_to_string(&calls)? == before,
            "empty uninstall contacted manager"
        );

        fs::write(&service, &legacy)?;
        let output = run("install")?;
        ensure!(!output.status.success());
        ensure!(
            String::from_utf8_lossy(&output.stderr).contains("intentional manager start failure"),
            "legacy install: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let calls = fs::read_to_string(&calls)?;
        ensure!(calls.contains("--user disable tsindex.service"));
        ensure!(calls.contains("--user enable tsindex.socket"));
        ensure!(calls.contains("--user start tsindex.socket"));
        let definition = fs::read_to_string(scratch.path().join("candidate-service"))?;
        ensure!(
            definition.contains(&serde_json::to_string(&invoked)?),
            "the service must keep the invocation symlink across package switches"
        );
        ensure!(definition.contains("--socket-activated"));
        ensure!(!definition.contains("WantedBy=default.target"));
        if custom {
            ensure!(
                definition.contains(&format!(
                    "Environment=\"TSINDEX_HOME={}\"",
                    scratch.path().join("relative-cache").display()
                )),
                "relative TSINDEX_HOME was not persisted as an absolute path"
            );
        }
        let endpoint: serde_json::Value =
            serde_json::from_slice(&fs::read(scratch.path().join("candidate-endpoint"))?)?;
        ensure!(endpoint["socket"] == socket.to_string_lossy().as_ref());
        ensure!(
            endpoint["default_socket"]
                == runtime
                    .join("tsindex/daemon.sock")
                    .to_string_lossy()
                    .as_ref()
        );
        ensure!(
            fs::read_to_string(&service)? == legacy,
            "legacy definition was not restored"
        );
        ensure!(!config.join("tsindex.socket").exists());
        ensure!(!config.join("tsindex.json").exists());
        ensure!(calls.contains("--user disable tsindex.socket"));
        ensure!(calls.ends_with("--user enable tsindex.service\n--user start tsindex.service\n"));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn empty_uninstall_preserves_an_independent_daemon_without_a_manager() -> Result<()> {
    let scratch = private_scratch()?;
    let missing = scratch.path().join("absent-bin");
    fs::create_dir(&missing)?;
    let socket = scratch.path().join("d.sock");
    let _server = Daemon(
        Command::new(BIN)
            .arg("--socket")
            .arg(&socket)
            .args(["daemon", "run"])
            .env("HOME", scratch.path())
            .env_remove("TSINDEX_HOME")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?,
    );
    let pid = wait_ready(&socket)?;
    for _ in 0..2 {
        let output = Command::new(BIN)
            .arg("--socket")
            .arg(&socket)
            .args(["daemon", "uninstall"])
            .env("PATH", &missing)
            .env("XDG_CONFIG_HOME", scratch.path().join("config"))
            .env("HOME", scratch.path())
            .env_remove("TSINDEX_HOME")
            .env_remove("TSINDEX_SOCKET")
            .output()?;
        ensure!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    ensure!(client(&socket)?.ping()?.daemon_pid == pid);
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn manager_commands_preserve_regular_and_symlink_endpoints_before_mutations() -> Result<()> {
    use std::os::unix::fs::{PermissionsExt, symlink};
    for installed in [false, true] {
        for symlink_endpoint in [false, true] {
            for action in ["install", "start", "restart"] {
                let scratch = private_scratch()?;
                let socket = scratch.path().join("d.sock");
                let sentinel = scratch.path().join("source.txt");
                fs::write(&sentinel, "preserve this source file")?;
                if symlink_endpoint {
                    symlink(&sentinel, &socket)?;
                } else {
                    fs::write(&socket, "preserve this endpoint file")?;
                }
                let config = scratch.path().join("config/systemd/user");
                fs::create_dir_all(&config)?;
                if installed {
                    fs::write(
                        config.join("tsindex.service"),
                        "original service definition",
                    )?;
                    fs::write(config.join("tsindex.socket"), "original socket definition")?;
                    fs::write(
                        config.join("tsindex.json"),
                        serde_json::to_vec(&serde_json::json!({"socket": socket}))?,
                    )?;
                }
                let bin = scratch.path().join("bin");
                fs::create_dir(&bin)?;
                let calls = scratch.path().join("calls");
                let manager = bin.join("systemctl");
                fs::write(
                    &manager,
                    "#!/bin/sh\nprintf 'manager invoked\n' >>\"$FAKE_MANAGER_CALLS\"\nexit 42\n",
                )?;
                fs::set_permissions(&manager, fs::Permissions::from_mode(0o700))?;
                let output = Command::new(BIN)
                    .arg("--socket")
                    .arg(&socket)
                    .args(["daemon", action])
                    .env("PATH", &bin)
                    .env("FAKE_MANAGER_CALLS", &calls)
                    .env("XDG_CONFIG_HOME", scratch.path().join("config"))
                    .env("HOME", scratch.path())
                    .env_remove("TSINDEX_HOME")
                    .env_remove("TSINDEX_SOCKET")
                    .output()?;
                ensure!(
                    !output.status.success(),
                    "{action} accepted an unrelated endpoint"
                );
                ensure!(String::from_utf8_lossy(&output.stderr).contains("refusing to replace"));
                ensure!(
                    !calls.exists(),
                    "{action} invoked manager before endpoint validation"
                );
                ensure!(fs::read_to_string(&sentinel)? == "preserve this source file");
                if symlink_endpoint {
                    ensure!(fs::read_link(&socket)? == sentinel);
                } else {
                    ensure!(fs::read_to_string(&socket)? == "preserve this endpoint file");
                }
                if installed {
                    ensure!(
                        fs::read_to_string(config.join("tsindex.service"))?
                            == "original service definition"
                    );
                    ensure!(
                        fs::read_to_string(config.join("tsindex.socket"))?
                            == "original socket definition"
                    );
                } else {
                    ensure!(!config.join("tsindex.service").exists());
                    ensure!(!config.join("tsindex.socket").exists());
                }
            }
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn install_rejects_lossy_unit_paths_and_manual_control_still_requires_utf8() -> Result<()> {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;
    for name in [
        OsString::from("trailing.sock "),
        OsString::from("embedded\t.sock"),
        OsString::from("embedded\n.sock"),
        OsString::from("continuation.sock\\"),
        OsString::from_vec(b"non-utf8-\xff.sock".to_vec()),
    ] {
        let actions: &[&str] = if name.to_str().is_some() {
            // Unit-only restrictions do not apply to manually bound sockets.
            &["install"]
        } else {
            // Both modes persist the endpoint in UTF-8 JSON metadata.
            &["install", "start", "restart"]
        };
        for &action in actions {
            let scratch = private_scratch()?;
            let socket = scratch.path().join(&name);
            let config = scratch.path().join("config/systemd/user");
            fs::create_dir_all(&config)?;
            fs::write(
                config.join("tsindex.service"),
                "original service definition",
            )?;
            fs::write(
                config.join("tsindex.json"),
                serde_json::to_vec(
                    &serde_json::json!({"socket": scratch.path().join("original.sock")}),
                )?,
            )?;
            let output = Command::new(BIN)
                .arg("--socket")
                .arg(&socket)
                .args(["daemon", action])
                .env("PATH", scratch.path().join("no-manager"))
                .env("XDG_CONFIG_HOME", scratch.path().join("config"))
                .env("HOME", scratch.path())
                .env_remove("TSINDEX_HOME")
                .env_remove("TSINDEX_SOCKET")
                .output()?;
            ensure!(!output.status.success());
            ensure!(
                String::from_utf8_lossy(&output.stderr).contains("cannot be represented safely")
            );
            ensure!(
                fs::read_to_string(config.join("tsindex.service"))?
                    == "original service definition"
            );
            ensure!(!socket.exists());
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn fresh_install_preserves_an_unrelated_live_socket() -> Result<()> {
    use std::os::unix::fs::MetadataExt;
    let scratch = private_scratch()?;
    let socket = scratch.path().join("d.sock");
    let _listener = UnixListener::bind(&socket)?;
    let inode = fs::symlink_metadata(&socket)?.ino();
    let output = Command::new(BIN)
        .arg("--socket")
        .arg(&socket)
        .args(["daemon", "install"])
        .env("PATH", scratch.path().join("no-manager"))
        .env("XDG_CONFIG_HOME", scratch.path().join("config"))
        .env("HOME", scratch.path())
        .env_remove("TSINDEX_HOME")
        .env_remove("TSINDEX_SOCKET")
        .output()?;
    ensure!(!output.status.success());
    ensure!(String::from_utf8_lossy(&output.stderr).contains("live listening socket"));
    ensure!(fs::symlink_metadata(&socket)?.ino() == inode);
    ensure!(
        !scratch
            .path()
            .join("config/systemd/user/tsindex.service")
            .exists()
    );
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn installation_waits_for_legacy_daemon_exit_before_binding() -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    if let Some(socket) = std::env::var_os("TSINDEX_LEGACY_TEST_SOCKET") {
        // Emulate the initial PR #18 build: close the listener during shutdown and
        // remove its pathname later, after a bounded destructor delay.
        let socket = PathBuf::from(socket);
        let listener = UnixListener::bind(&socket)?;
        loop {
            let (mut stream, _) = listener.accept()?;
            let mut request = String::new();
            BufReader::new(stream.try_clone()?).read_line(&mut request)?;
            let request: serde_json::Value = serde_json::from_str(&request)?;
            let stopping = request["op"] == "shutdown";
            let response = if stopping {
                tsindex::daemon::DaemonResponse::Stopping
            } else {
                tsindex::daemon::DaemonResponse::Pong(tsindex::daemon::DaemonStatus {
                    daemon_pid: std::env::var("TSINDEX_LEGACY_TEST_REPORTED_PID")
                        .ok()
                        .map(|pid| pid.parse())
                        .transpose()?
                        .unwrap_or(std::process::id()),
                    version: "2.0.0".into(),
                    protocol: 1,
                    started_at: 0,
                    uptime_secs: 0,
                    socket: socket.clone(),
                    active_sessions: 0,
                    max_sessions: Some(128),
                    total_sessions_started: 0,
                    index_runtimes: 0,
                })
            };
            serde_json::to_writer(&mut stream, &response)?;
            stream.write_all(b"\n")?;
            if stopping {
                break;
            }
        }
        drop(listener);
        thread::sleep(Duration::from_millis(400));
        fs::remove_file(&socket)?;
        fs::write(socket.with_extension("cleanup"), "finished")?;
        return Ok(());
    }
    let scratch = private_scratch()?;
    let socket = scratch.path().join("d.sock");
    let mut legacy = Daemon(
        Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "installation_waits_for_legacy_daemon_exit_before_binding",
            ])
            .env("TSINDEX_LEGACY_TEST_SOCKET", &socket)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()?,
    );
    wait_ready(&socket)?;
    let bin = scratch.path().join("bin");
    fs::create_dir(&bin)?;
    let manager = bin.join("systemctl");
    fs::write(
        &manager,
        "#!/bin/sh\ncase \"$*\" in\n *'start tsindex.socket') if [ ! -f \"$LEGACY_CLEANUP_MARKER\" ]; then printf 'manager started before legacy cleanup\n' >&2; exit 72; fi; printf 'intentional manager start failure\n' >&2; exit 42;;\n *) exit 0;;\nesac\n",
    )?;
    fs::set_permissions(&manager, fs::Permissions::from_mode(0o700))?;
    let output = Command::new(BIN)
        .arg("--socket")
        .arg(&socket)
        .args(["daemon", "install"])
        .env("PATH", &bin)
        .env("LEGACY_CLEANUP_MARKER", socket.with_extension("cleanup"))
        .env("XDG_CONFIG_HOME", scratch.path().join("config"))
        .env("HOME", scratch.path())
        .env_remove("TSINDEX_HOME")
        .env_remove("TSINDEX_SOCKET")
        .output()?;
    ensure!(!output.status.success());
    ensure!(
        String::from_utf8_lossy(&output.stderr).contains("intentional manager start failure"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    ensure!(socket.with_extension("cleanup").is_file());
    ensure!(legacy.0.wait()?.success());
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn stop_tolerates_an_owner_that_exits_after_ping_but_rejects_invalid_identity() -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut exited = Command::new("/bin/sh").args(["-c", "exit 0"]).spawn()?;
    let exited_pid = exited.id();
    ensure!(exited.wait()?.success());
    for reported_pid in [exited_pid, 0] {
        let scratch = private_scratch()?;
        let socket = scratch.path().join("d.sock");
        let _server = Daemon(
            Command::new(std::env::current_exe()?)
                .args([
                    "--exact",
                    "installation_waits_for_legacy_daemon_exit_before_binding",
                ])
                .env("TSINDEX_LEGACY_TEST_SOCKET", &socket)
                .env("TSINDEX_LEGACY_TEST_REPORTED_PID", reported_pid.to_string())
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .spawn()?,
        );
        wait_ready(&socket)?;
        let config = scratch.path().join("config/systemd/user");
        fs::create_dir_all(&config)?;
        fs::write(config.join("tsindex.service"), "[Service]\n")?;
        fs::write(config.join("tsindex.socket"), "[Socket]\n")?;
        fs::write(
            config.join("tsindex.json"),
            serde_json::to_vec(&serde_json::json!({"socket": socket}))?,
        )?;
        let bin = scratch.path().join("bin");
        fs::create_dir(&bin)?;
        let manager = bin.join("systemctl");
        let calls = scratch.path().join("calls");
        fs::write(
            &manager,
            "#!/bin/sh\nprintf '%s\n' \"$*\" >>\"$FAKE_MANAGER_CALLS\"\nexit 0\n",
        )?;
        fs::set_permissions(&manager, fs::Permissions::from_mode(0o700))?;
        let output = Command::new(BIN)
            .arg("--socket")
            .arg(&socket)
            .args(["daemon", "stop"])
            .env("PATH", &bin)
            .env("FAKE_MANAGER_CALLS", &calls)
            .env("XDG_CONFIG_HOME", scratch.path().join("config"))
            .env("HOME", scratch.path())
            .env_remove("TSINDEX_HOME")
            .env_remove("TSINDEX_SOCKET")
            .output()?;
        if reported_pid == exited_pid {
            ensure!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            ensure!(
                fs::read_to_string(&calls)?.contains("--user stop tsindex.socket tsindex.service")
            );
        } else {
            ensure!(!output.status.success());
            ensure!(
                !calls.exists(),
                "invalid process identity reached the manager"
            );
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn non_utf8_install_rejects_before_writing_or_enabling_units() -> Result<()> {
    use std::ffi::OsString;
    use std::os::unix::{ffi::OsStringExt, fs::PermissionsExt};
    let scratch = private_scratch()?;
    let socket = scratch
        .path()
        .join(OsString::from_vec(b"invalid-\xff.sock".to_vec()));
    let bin = scratch.path().join("bin");
    fs::create_dir(&bin)?;
    let manager = bin.join("systemctl");
    let calls = scratch.path().join("calls");
    fs::write(
        &manager,
        "#!/bin/sh\nprintf '%s\n' \"$*\" >>\"$FAKE_MANAGER_CALLS\"\nexit 0\n",
    )?;
    fs::set_permissions(&manager, fs::Permissions::from_mode(0o700))?;
    let config = scratch.path().join("config");
    let output = Command::new(BIN)
        .arg("--socket")
        .arg(&socket)
        .args(["daemon", "install"])
        .env("PATH", &bin)
        .env("FAKE_MANAGER_CALLS", &calls)
        .env("XDG_CONFIG_HOME", &config)
        .env("HOME", scratch.path())
        .env_remove("TSINDEX_HOME")
        .env_remove("TSINDEX_SOCKET")
        .output()?;
    ensure!(!output.status.success());
    ensure!(String::from_utf8_lossy(&output.stderr).contains("cannot be represented safely"));
    ensure!(
        !calls.exists(),
        "invalid UTF-8 path reached service manager"
    );
    ensure!(
        !config.exists(),
        "invalid UTF-8 install left service definitions"
    );
    Ok(())
}

// The CLI uses the production manager name. CI explicitly runs this test in
// its own step on a disposable native runner, after the normal tests finish.
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
#[ignore = "requires a clean GitHub-hosted runner and a dedicated invocation"]
fn installed_cli_controls_native_service() -> Result<()> {
    use tsindex::daemon::service::service_file_path;

    ensure!(
        std::env::var("RUNNER_ENVIRONMENT").as_deref() == Ok("github-hosted")
            && std::env::var("TSINDEX_TEST_INSTALL").as_deref() == Ok("1"),
        "the production service name may only be exercised on a clean hosted runner"
    );
    let service = service_file_path();
    let files = [
        service.clone(),
        service.with_extension("socket"),
        service.with_extension("json"),
        service.with_extension("install.lock"),
    ];
    for file in &files {
        ensure!(
            fs::symlink_metadata(file)
                .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound),
            "service configuration already exists at {file:?}"
        );
    }
    // A globally installed or transient unit can exist even without any file
    // in this account's configuration. Verify the manager is also unoccupied.
    #[cfg(target_os = "linux")]
    for unit in ["tsindex.service", "tsindex.socket"] {
        let result = Command::new("systemctl")
            .args(["--user", "show", "-p", "LoadState", "--value", unit])
            .output()?;
        ensure!(
            String::from_utf8_lossy(&result.stdout).trim() == "not-found",
            "manager already knows {unit} or is unavailable"
        );
    }
    #[cfg(target_os = "macos")]
    {
        let target = format!(
            "gui/{}/{}",
            unsafe { libc::geteuid() },
            tsindex::daemon::service::MACOS_SERVICE_LABEL
        );
        let result = Command::new("launchctl")
            .args(["print", &target])
            .output()?;
        ensure!(
            !result.status.success()
                && String::from_utf8_lossy(&result.stderr)
                    .to_lowercase()
                    .contains("could not find service"),
            "launchd already knows this service or its GUI domain is unavailable"
        );
    }

    let scratch = private_scratch()?;
    let socket = scratch.path().join("custom.sock");
    let cli = |action: &str, explicit: bool| -> Result<serde_json::Value> {
        let mut cmd = Command::new(BIN);
        cmd.env_remove("TSINDEX_HOME").env_remove("TSINDEX_SOCKET");
        if explicit {
            cmd.arg("--socket").arg(&socket);
        }
        let result = cmd.args(["daemon", action, "--json"]).output()?;
        ensure!(
            result.status.success(),
            "daemon {action}: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        Ok(serde_json::from_slice(&result.stdout)?)
    };
    let result = (|| -> Result<()> {
        let installed = cli("install", true)?;
        let first = wait_ready(&socket)?;
        ensure!(installed["daemon"]["daemon_pid"] == first);
        let replacement = cli("install", false)?;
        let replaced = wait_ready(&socket)?;
        ensure!(replaced != first && replacement["daemon"]["daemon_pid"] == replaced);
        let restarted = cli("restart", false)?;
        let second = wait_ready(&socket)?;
        ensure!(second != replaced && restarted["daemon_pid"] == second);
        ensure!(cli("stop", false)?["stopped"] == true);
        wait_gone(second)?;
        let status = cli("status", false)?;
        ensure!(
            status["running"] == false && status["socket"] == socket.to_string_lossy().as_ref()
        );
        let started = cli("start", false)?;
        ensure!(started["daemon_pid"] == wait_ready(&socket)?);
        cli("uninstall", false)?;
        wait_gone(
            started["daemon_pid"]
                .as_u64()
                .context("missing daemon PID")?
                .try_into()?,
        )?;
        for file in &files[..3] {
            ensure!(!file.try_exists()?, "uninstall left {file:?}");
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = cli("uninstall", true);
    }
    // The persistent lock inode serializes future installs. This disposable
    // runner started without that file and has no other installation clients.
    if files[3].try_exists()? {
        fs::remove_file(&files[3])?;
    }
    result
}

struct ManagedJob {
    _scratch: TempDir,
    #[cfg(target_os = "linux")]
    name: String,
    #[cfg(target_os = "linux")]
    directory: PathBuf,
    #[cfg(target_os = "macos")]
    target: Option<String>,
    #[cfg(target_os = "macos")]
    plist: PathBuf,
}

#[cfg(target_os = "linux")]
impl Drop for ManagedJob {
    fn drop(&mut self) {
        let _ = Command::new("systemctl")
            .args([
                "--user",
                "stop",
                &format!("{}.socket", self.name),
                &format!("{}.service", self.name),
            ])
            .status();
        for suffix in ["socket", "service"] {
            let _ = fs::remove_file(self.directory.join(format!("{}.{suffix}", self.name)));
        }
        let _ = Command::new("systemctl")
            .args(["--user", "daemon-reload"])
            .status();
    }
}

#[cfg(target_os = "macos")]
impl Drop for ManagedJob {
    fn drop(&mut self) {
        if let Some(target) = self.target.as_deref() {
            let _ = Command::new("launchctl").args(["bootout", target]).output();
        }
        let _ = fs::remove_file(&self.plist);
    }
}

#[cfg(target_os = "linux")]
#[test]
fn systemd_manager_activates_isolated_service_when_available() -> Result<()> {
    use tsindex::daemon::service::{render_systemd_socket, render_systemd_unit};
    // Some development shells have no per-user service manager. Synthetic
    // descriptor tests above still exercise activation on those hosts.
    if !Command::new("systemctl")
        .args(["--user", "show-environment"])
        .output()
        .is_ok_and(|output| output.status.success())
    {
        ensure!(
            std::env::var("TSINDEX_REQUIRE_SYSTEMD").as_deref() != Ok("1"),
            "systemd user manager is required for this test run"
        );
        eprintln!("systemd user manager unavailable");
        return Ok(());
    }
    let scratch = private_scratch()?;
    let socket = scratch.path().join("d.sock");
    let name = format!(
        "tsindex-test-{}-{}",
        std::process::id(),
        scratch.path().file_name().unwrap().to_string_lossy()
    );
    let output = Command::new("systemd-path")
        .arg("user-configuration")
        .output()?;
    ensure!(output.status.success());
    let directory = PathBuf::from(String::from_utf8(output.stdout)?.trim()).join("systemd/user");
    fs::create_dir_all(&directory)?;
    let _job = ManagedJob {
        _scratch: scratch,
        name: name.clone(),
        directory: directory.clone(),
    };
    fs::write(
        directory.join(format!("{name}.service")),
        render_systemd_unit(Path::new(BIN), Some(&socket))
            .replace("tsindex.socket", &format!("{name}.socket")),
    )?;
    fs::write(
        directory.join(format!("{name}.socket")),
        render_systemd_socket(Path::new(BIN), &socket),
    )?;
    fs::write(&socket, "preserve this source file")?;
    ensure!(
        Command::new("systemctl")
            .args(["--user", "daemon-reload"])
            .status()?
            .success()
    );
    ensure!(
        !Command::new("systemctl")
            .args(["--user", "start", &format!("{name}.socket")])
            .status()?
            .success()
    );
    ensure!(
        fs::read_to_string(&socket)? == "preserve this source file",
        "manager activation replaced a regular file"
    );
    fs::remove_file(&socket)?;
    ensure!(
        Command::new("systemctl")
            .args(["--user", "reset-failed", &format!("{name}.socket")])
            .status()?
            .success()
    );
    for args in [
        vec!["--user", "daemon-reload"],
        vec!["--user", "start", &format!("{name}.socket")],
    ] {
        let status = Command::new("systemctl").args(args).status()?;
        ensure!(status.success(), "failed to load isolated service");
    }
    let first = wait_ready(&socket)?;
    let status = Command::new("systemctl")
        .args(["--user", "restart", &format!("{name}.service")])
        .status()?;
    ensure!(status.success());
    let second = wait_ready(&socket)?;
    ensure!(second != first);
    let (_proxy, worker) = active_worker(&socket, &_job._scratch.path().join("repo"))?;
    // The user manager must contain workers even when the supervisor cannot
    // execute its graceful cleanup path.
    ensure!(unsafe { libc::kill(second as libc::pid_t, libc::SIGKILL) } == 0);
    wait_gone(worker)?;
    ensure!(wait_ready(&socket)? != second);
    Ok(())
}

#[cfg(target_os = "macos")]
#[test]
fn launchd_manager_activates_isolated_service() -> Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;

    use tsindex::daemon::service::{MACOS_SERVICE_LABEL, render_launchd_plist, service_file_path};
    let scratch = private_scratch()?;
    let socket = scratch.path().join("d.sock");
    let suffix = scratch.path().file_name().unwrap().to_string_lossy();
    let name = format!(
        "com.legalzoom.tsindex.test.{}-{}",
        std::process::id(),
        suffix.trim_start_matches('.')
    );
    let service = service_file_path();
    let agents = service
        .parent()
        .context("LaunchAgent directory is unavailable")?;
    fs::create_dir_all(agents)?;
    let plist = agents.join(format!("{name}.plist"));
    let log = scratch.path().join("daemon.log");
    // Normal LaunchAgents default to Aqua. Exercise the installed service's
    // GUI domain instead of the Background user domain.
    // https://developer.apple.com/forums/thread/783780
    let domain = format!("gui/{}", unsafe { libc::geteuid() });
    let target = format!("{domain}/{name}");
    let domain_status = Command::new("launchctl")
        .args(["print", &domain])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?;
    ensure!(
        domain_status.success(),
        "launchd GUI domain {domain} is unavailable ({domain_status}) for fixture {}",
        plist.display()
    );
    let rendered = render_launchd_plist(Path::new(BIN), Some(&socket), &log, "/usr/bin:/bin")
        .replace(MACOS_SERVICE_LABEL, &name);
    // create_new protects unrelated files if a prior fixture name survives.
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&plist)?;
    let mut job = ManagedJob {
        _scratch: scratch,
        target: None,
        plist: plist.clone(),
    };
    file.write_all(rendered.as_bytes())?;
    drop(file);
    let lint = Command::new("/usr/bin/plutil")
        .args(["-lint", "--"])
        .arg(&plist)
        .output()?;
    ensure!(
        lint.status.success(),
        "invalid LaunchAgent plist {} ({}) stdout={} stderr={}",
        plist.display(),
        lint.status,
        String::from_utf8_lossy(&lint.stdout[..lint.stdout.len().min(2048)]),
        String::from_utf8_lossy(&lint.stderr[..lint.stderr.len().min(2048)])
    );
    let bootstrap = Command::new("launchctl")
        .args(["bootstrap", &domain])
        .arg(&plist)
        .output()?;
    ensure!(
        bootstrap.status.success(),
        "launchctl bootstrap {domain} {} failed ({}) stderr={}",
        plist.display(),
        bootstrap.status,
        String::from_utf8_lossy(&bootstrap.stderr[..bootstrap.stderr.len().min(4096)])
    );
    // Bootstrap success confirms ownership before arming registration cleanup.
    job.target = Some(target.clone());
    let first =
        wait_ready(&socket).with_context(|| fs::read_to_string(&log).unwrap_or_default())?;
    let (_proxy, worker) = active_worker(&socket, &job._scratch.path().join("repo"))?;
    // launchd's process-group cleanup must cover a worker when its supervisor
    // is killed before it can close the child's stdin.
    ensure!(unsafe { libc::kill(first as libc::pid_t, libc::SIGKILL) } == 0);
    wait_gone(worker)?;
    let deadline = Instant::now() + Duration::from_secs(15);
    let restarted = loop {
        if let Ok(pid) = wait_ready(&socket)
            && pid != first
        {
            break pid;
        }
        ensure!(
            Instant::now() < deadline,
            "launchd failed to reactivate the supervisor"
        );
        thread::sleep(Duration::from_millis(100));
    };
    let bootout = Command::new("launchctl")
        .args(["bootout", &target])
        .output()?;
    ensure!(
        bootout.status.success(),
        "launchctl bootout {target} failed ({}) stderr={}",
        bootout.status,
        String::from_utf8_lossy(&bootout.stderr[..bootout.stderr.len().min(4096)])
    );
    job.target = None;
    wait_gone(restarted)?;
    Ok(())
}

#[cfg(unix)]
#[test]
fn pre_activation_replacement_binary_is_rejected_before_stopping_service() -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let scratch = private_scratch()?;
    let bin = scratch.path().join("bin");
    fs::create_dir(&bin)?;
    let calls = scratch.path().join("manager-calls");
    for manager_name in ["systemctl", "launchctl"] {
        let manager = bin.join(manager_name);
        fs::write(
            &manager,
            "#!/bin/sh\nprintf \"%s\\n\" \"$*\" >>\"$FAKE_MANAGER_CALLS\"\nexit 42\n",
        )?;
        fs::set_permissions(&manager, fs::Permissions::from_mode(0o700))?;
    }
    let old_binary = scratch.path().join("pre-activation-tsindex");
    fs::write(
        &old_binary,
        "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then printf \"tsindex 2.0.0\\n\"; exit 0; fi\nexit 2\n",
    )?;
    fs::set_permissions(&old_binary, fs::Permissions::from_mode(0o700))?;

    let socket = scratch.path().join("d.sock");
    let output = Command::new(BIN)
        .arg("--socket")
        .arg(&socket)
        .args(["daemon", "install", "--binary"])
        .arg(&old_binary)
        .env("HOME", scratch.path())
        .env("XDG_CONFIG_HOME", scratch.path().join("config"))
        .env("PATH", &bin)
        .env("FAKE_MANAGER_CALLS", &calls)
        .output()?;
    ensure!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    ensure!(
        stderr.contains("socket-activated"),
        "expected activation preflight error, got: {stderr}"
    );
    ensure!(
        !calls.exists(),
        "preflight must reject incompatible binaries before invoking the service manager"
    );
    Ok(())
}

#[cfg(target_os = "macos")]
#[test]
fn macos_start_recreates_missing_nested_socket_directories_before_bootstrap() -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let scratch = private_scratch()?;
    let agents = scratch.path().join("Library/LaunchAgents");
    fs::create_dir_all(&agents)?;
    let nested_parent = scratch.path().join("removed-outer/removed-inner");
    let socket = nested_parent.join("daemon.sock");
    let plist = agents.join("com.legalzoom.tsindex.daemon.plist");
    let log = nested_parent.join(".daemon.sock.tsindex/daemon.log");
    fs::write(
        &plist,
        tsindex::daemon::service::render_launchd_plist(
            Path::new(BIN),
            Some(&socket),
            &log,
            "/usr/bin:/bin",
        ),
    )?;
    fs::write(
        agents.join("com.legalzoom.tsindex.daemon.json"),
        serde_json::to_vec(&serde_json::json!({
            "socket": socket,
            "default_socket": scratch.path().join(".tsindex/run/daemon.sock"),
        }))?,
    )?;

    let bin = scratch.path().join("bin");
    fs::create_dir(&bin)?;
    let launchctl = bin.join("launchctl");
    let observed = scratch.path().join("observed");
    fs::write(
        &launchctl,
        format!(
            r#"#!/bin/sh
case "$1" in
  print)
    printf 'Could not find service\n' >&2
    exit 113
    ;;
  bootstrap)
    if [ -d "{dir}" ] && [ -f "{owner}" ]; then
      printf 'ready\n' > "{observed}"
      exit 42
    fi
    printf 'missing-dir\n' > "{observed}"
    exit 43
    ;;
esac
exit 1
"#,
            dir = nested_parent.display(),
            owner = nested_parent.join(".daemon.sock.tsindex/owner").display(),
            observed = observed.display()
        ),
    )?;
    fs::set_permissions(&launchctl, fs::Permissions::from_mode(0o700))?;

    ensure!(!scratch.path().join("removed-outer").exists());
    let output = Command::new(BIN)
        .args(["daemon", "start"])
        .env("HOME", scratch.path())
        .env("PATH", &bin)
        .env_remove("TSINDEX_HOME")
        .env_remove("TSINDEX_SOCKET")
        .output()?;
    ensure!(!output.status.success());
    ensure!(
        fs::read_to_string(&observed)?.trim() == "ready",
        "start_os_service must recreate private socket and metadata dirs before launchctl bootstrap; stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn socketless_legacy_linux_unit_discovers_manager_runtime_socket_without_shell_xdg() -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let scratch = private_scratch()?;
    let mgr_runtime = scratch.path().join("mgr-runtime");
    let config = scratch.path().join("config/systemd/user");
    fs::create_dir_all(&config)?;
    fs::write(
        config.join("tsindex.service"),
        format!(
            "[Unit]\nAfter=default.target\n[Service]\nExecStart={} daemon run\nRestart=on-failure\n[Install]\nWantedBy=default.target\n",
            serde_json::to_string(BIN)?
        ),
    )?;
    let bin = scratch.path().join("bin");
    fs::create_dir(&bin)?;
    let manager = bin.join("systemctl");
    fs::write(
        &manager,
        format!(
            "#!/bin/sh\ncase \"$*\" in\n  *show-environment*) printf 'XDG_RUNTIME_DIR={}\\n' ;;\n  *) exit 0 ;;\nesac\n",
            mgr_runtime.display()
        ),
    )?;
    fs::set_permissions(&manager, fs::Permissions::from_mode(0o700))?;

    let output = Command::new(BIN)
        .args(["daemon", "status", "--json"])
        .env("HOME", scratch.path())
        .env("XDG_CONFIG_HOME", scratch.path().join("config"))
        .env("PATH", &bin)
        .env_remove("XDG_RUNTIME_DIR")
        .env_remove("TSINDEX_HOME")
        .env_remove("TSINDEX_SOCKET")
        .output()?;
    ensure!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let status: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    ensure!(
        status["socket"]
            == mgr_runtime
                .join("tsindex/daemon.sock")
                .to_string_lossy()
                .as_ref(),
        "expected manager runtime socket, got: {status}"
    );
    Ok(())
}
