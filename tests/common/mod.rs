#![cfg(unix)]
#![allow(dead_code)]

use std::fs::{self, File};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use tempfile::TempDir;
use tsindex::daemon::{DaemonClient, DaemonPaths, DaemonStatus};

pub const BIN: &str = env!("CARGO_BIN_EXE_tsindex");

/// Isolated orchestrator daemon running on a private temporary Unix socket.
pub struct TestDaemon {
    _dir: TempDir,
    pub socket: PathBuf,
    pub log: PathBuf,
    child: Child,
}

impl TestDaemon {
    pub fn start() -> Result<Self> {
        Self::start_with_env(&[])
    }

    pub fn start_with_env(extra_env: &[(&str, &str)]) -> Result<Self> {
        // Use `/tmp` directly so the Unix socket path stays well below macOS's
        // 104-byte `sun_path` limit even when $TMPDIR is deeply nested.
        let dir = tempfile::tempdir_in("/tmp")?;
        fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700))?;
        let socket = dir.path().join("d.sock");
        let log = dir.path().join("daemon.log");

        let mut cmd = Command::new(BIN);
        cmd.arg("--socket")
            .arg(&socket)
            .args(["daemon", "run"])
            .envs(extra_env.iter().copied())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(File::create(&log)?);
        let child = cmd.spawn().context("failed to spawn test orchestrator")?;
        let mut daemon = Self {
            _dir: dir,
            socket,
            log,
            child,
        };
        daemon.wait_ready(Duration::from_secs(5))?;
        Ok(daemon)
    }

    fn wait_ready(&mut self, timeout: Duration) -> Result<()> {
        let client = self.client()?;
        let deadline = Instant::now() + timeout;
        loop {
            if client.ping().is_ok() {
                return Ok(());
            }
            if let Some(status) = self.child.try_wait()? {
                bail!("test daemon exited early ({status}): {}", self.read_log());
            }
            if Instant::now() >= deadline {
                bail!(
                    "test daemon did not become ready at {}: {}",
                    self.socket.display(),
                    self.read_log()
                );
            }
            thread::sleep(Duration::from_millis(20));
        }
    }

    pub fn client(&self) -> Result<DaemonClient> {
        let paths = DaemonPaths::resolve(Some(&self.socket))?;
        Ok(DaemonClient::new(paths))
    }

    pub fn ping(&self) -> Result<DaemonStatus> {
        self.client()?.ping()
    }

    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    pub fn read_log(&self) -> String {
        fs::read_to_string(&self.log).unwrap_or_default()
    }

    pub fn wait_active_sessions(&self, expected: usize, timeout: Duration) -> Result<()> {
        let client = self.client()?;
        let deadline = Instant::now() + timeout;
        loop {
            if let Ok((active, _)) = client.list_sessions(false)
                && active.len() == expected
            {
                return Ok(());
            }
            if Instant::now() >= deadline {
                let actual = client.list_sessions(true).ok();
                bail!(
                    "timed out waiting for {expected} active session(s); got {actual:?}\ndaemon log:\n{}",
                    self.read_log()
                );
            }
            thread::sleep(Duration::from_millis(25));
        }
    }

    pub fn kill_hard(&mut self) -> Result<()> {
        let _ = self.child.kill();
        let _ = self.child.wait();
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.socket
    }
}

impl Drop for TestDaemon {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            if let Ok(client) = self.client() {
                let _ = client.shutdown();
            }
            let deadline = Instant::now() + Duration::from_secs(3);
            while Instant::now() < deadline {
                if self.child.try_wait().ok().flatten().is_some() {
                    return;
                }
                thread::sleep(Duration::from_millis(20));
            }
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}
