//! Opt-in ownership of one stdio connection. No process discovery or PID signals.

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
use anyhow::{Result, bail};

#[cfg(any(target_os = "linux", target_os = "macos"))]
mod unix {
    use std::collections::HashMap;
    use std::env;
    use std::fs::{self, DirBuilder, File, OpenOptions};
    use std::io::{Read, Write};
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
    use std::os::unix::net::{UnixListener, UnixStream};
    use std::path::{Component, Path, PathBuf};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::{Duration, Instant};

    use anyhow::{Context, Result, anyhow, bail};
    use serde::{Deserialize, Serialize};
    use sha2::{Digest, Sha256};

    const MAX_RECORD_BYTES: usize = 2048;
    const MAX_OWNERS: usize = 128;
    const MAX_INSTANCES: usize = 4096;
    const CLEANUP_BUDGET: Duration = Duration::from_millis(2400);
    const IO_BUDGET: Duration = Duration::from_millis(150);

    #[derive(Clone, PartialEq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Record {
        version: u8,
        instance: String,
        session_id: String,
        nonce: String,
    }

    struct Owners {
        records: HashMap<String, Record>,
        closing: bool,
    }

    struct Inner {
        dir: PathBuf,
        dir_identity: (u64, u64),
        socket_identity: (u64, u64),
        instance: String,
        nonce: String,
        owners: Mutex<Owners>,
        stop: AtomicBool,
        shutdown: Arc<AtomicBool>,
    }

    pub struct SessionLifecycle(Arc<Inner>);

    pub fn validate_session_id(id: &str) -> Result<()> {
        if id.is_empty() || id.len() > 256 || id.chars().any(char::is_control) {
            bail!("session_id must contain 1-256 bytes without control characters");
        }
        Ok(())
    }

    fn random_hex(bytes: usize) -> Result<String> {
        let mut random = vec![0; bytes];
        File::open("/dev/urandom")?.read_exact(&mut random)?;
        Ok(random.iter().map(|b| format!("{b:02x}")).collect())
    }

    fn is_hex(value: &str, len: usize) -> bool {
        value.len() == len
            && value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    }

    fn record_path(dir: &Path, session: &str) -> PathBuf {
        dir.join(format!("s-{:x}.json", Sha256::digest(session.as_bytes())))
    }

    fn identity(path: &Path) -> Result<(u64, u64)> {
        let meta = fs::symlink_metadata(path)?;
        Ok((meta.dev(), meta.ino()))
    }

    fn private_metadata(path: &Path, directory: bool) -> Result<fs::Metadata> {
        let meta = fs::symlink_metadata(path)?;
        if meta.uid() != unsafe { libc::geteuid() }
            || meta.mode() & 0o077 != 0
            || (directory && !meta.is_dir())
            || (!directory && (!meta.is_file() || meta.nlink() != 1))
        {
            bail!("unsafe session state permissions or file type");
        }
        Ok(meta)
    }

    // Reject symlinks in every component, not just the final directory. Do not
    // silently chmod an existing directory containing somebody else's data.
    fn state_dir(create: bool) -> Result<Option<PathBuf>> {
        let path = match env::var_os("TSINDEX_SESSION_DIR").filter(|v| !v.is_empty()) {
            Some(path) => PathBuf::from(path),
            None => PathBuf::from(env::var_os("HOME").ok_or_else(|| anyhow!("HOME is unset"))?)
                .join(".tsindex/sessions-v1"),
        };
        if !path.is_absolute() || path.components().any(|c| matches!(c, Component::ParentDir)) {
            bail!("session state directory must be an absolute path without '..'");
        }
        let mut current = PathBuf::new();
        for part in path.components() {
            current.push(part);
            match fs::symlink_metadata(&current) {
                Ok(meta) if !meta.is_dir() => {
                    bail!("session state path contains a symlink or non-directory")
                }
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    if !create {
                        return Ok(None);
                    }
                    match DirBuilder::new().mode(0o700).create(&current) {
                        Ok(()) => {}
                        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
                        Err(e) => return Err(e.into()),
                    }
                    if !fs::symlink_metadata(&current)?.is_dir() {
                        bail!("unsafe session state directory");
                    }
                }
                Err(e) => return Err(e.into()),
            }
        }
        private_metadata(&path, true)?;
        Ok(Some(path))
    }

    fn read_record(path: &Path) -> Result<Record> {
        private_metadata(path, false)?;
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)?;
        let meta = file.metadata()?;
        if !meta.is_file()
            || meta.nlink() != 1
            || meta.uid() != unsafe { libc::geteuid() }
            || meta.mode() & 0o077 != 0
        {
            bail!("unsafe session record");
        }
        let mut bytes = Vec::new();
        file.take((MAX_RECORD_BYTES + 1) as u64)
            .read_to_end(&mut bytes)?;
        if bytes.len() > MAX_RECORD_BYTES {
            bail!("session record exceeds size limit");
        }
        serde_json::from_slice(&bytes).map_err(|_| anyhow!("invalid session record"))
    }

    impl SessionLifecycle {
        pub fn start(shutdown: Arc<AtomicBool>) -> Result<Self> {
            let root = state_dir(true)?.expect("created state directory");
            let instance = random_hex(16)?;
            let nonce = random_hex(32)?;
            let dir = root.join(&instance);
            // Check before creating anything: sockaddr_un is short on macOS.
            let socket = dir.join("control.sock");
            if socket.as_os_str().as_bytes().len()
                >= unsafe { std::mem::zeroed::<libc::sockaddr_un>() }
                    .sun_path
                    .len()
            {
                bail!(
                    "session socket path is too long; set TSINDEX_SESSION_DIR to a shorter private directory"
                );
            }
            DirBuilder::new().mode(0o700).create(&dir)?;
            let listener = match UnixListener::bind(&socket) {
                Ok(listener) => listener,
                Err(e) => {
                    let _ = fs::remove_dir(&dir);
                    return Err(e.into());
                }
            };
            fs::set_permissions(&socket, fs::Permissions::from_mode(0o600))?;
            listener.set_nonblocking(true)?;
            let inner = Arc::new(Inner {
                dir_identity: identity(&dir)?,
                socket_identity: identity(&socket)?,
                dir,
                instance,
                nonce,
                owners: Mutex::new(Owners {
                    records: HashMap::new(),
                    closing: false,
                }),
                stop: AtomicBool::new(false),
                shutdown,
            });
            let control = inner.clone();
            thread::spawn(move || {
                while !control.stop.load(Ordering::Acquire) {
                    match listener.accept() {
                        Ok((mut stream, _)) => {
                            // macOS can inherit the listener's nonblocking mode;
                            // receive this frame with the explicit total deadline.
                            if stream.set_nonblocking(false).is_err() {
                                continue;
                            }
                            // Bad control input never reaches protocol stdout or a
                            // diagnostic containing the nonce.
                            if let Ok(bytes) = read_frame(
                                &mut stream,
                                Instant::now() + IO_BUDGET,
                                MAX_RECORD_BYTES,
                            ) && let Ok(record) = serde_json::from_slice::<Record>(&bytes)
                            {
                                let mut owners = control.owners.lock().unwrap();
                                let authenticated = record.version == 1
                                    && record.instance == control.instance
                                    && record.nonce == control.nonce;
                                if !authenticated {
                                    continue;
                                }
                                let released = owners.records.remove(&record.session_id);
                                if let Some(ref record) = released {
                                    control.remove_record(record);
                                }
                                let final_release = released.is_some() && owners.records.is_empty();
                                if final_release {
                                    owners.closing = true;
                                }
                                let reply = if released.is_some() {
                                    b"released\n".as_slice()
                                } else {
                                    b"already_released\n".as_slice()
                                };
                                let _ = stream.set_write_timeout(Some(IO_BUDGET));
                                let _ = stream.write_all(reply);
                                if final_release {
                                    control.shutdown.store(true, Ordering::Release);
                                    // An active query, DB lock, or parse may not
                                    // cooperate in time. Bound the entire process,
                                    // including the blocked reader and watcher.
                                    thread::spawn(move || {
                                        thread::sleep(Duration::from_millis(750));
                                        // Do not let filesystem cleanup or a
                                        // lock delay the hard shutdown deadline.
                                        std::process::exit(0);
                                    });
                                    break;
                                }
                            }
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(10))
                        }
                        Err(_) => break,
                    }
                }
            });
            Ok(Self(inner))
        }

        pub fn register(&self, session_id: &str) -> Result<()> {
            validate_session_id(session_id)?;
            let mut owners = self.0.owners.lock().unwrap();
            if owners.closing {
                bail!("session connection is closing");
            }
            if owners.records.contains_key(session_id) {
                return Ok(());
            }
            if owners.records.len() >= MAX_OWNERS {
                bail!("session owner limit reached");
            }
            if identity(&self.0.dir)? != self.0.dir_identity {
                bail!("session instance directory was replaced");
            }
            let record = Record {
                version: 1,
                instance: self.0.instance.clone(),
                session_id: session_id.into(),
                nonce: self.0.nonce.clone(),
            };
            let path = record_path(&self.0.dir, session_id);
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&path)
                .map_err(|_| anyhow!("could not create session owner record"))?;
            let bytes = serde_json::to_vec(&record)?;
            if file.write_all(&bytes).is_err() {
                let _ = fs::remove_file(&path);
                bail!("could not write session owner record");
            }
            owners.records.insert(session_id.into(), record);
            Ok(())
        }
    }

    impl Inner {
        fn remove_record(&self, record: &Record) {
            if identity(&self.dir).ok() == Some(self.dir_identity) {
                let path = record_path(&self.dir, &record.session_id);
                if read_record(&path).ok().as_ref() == Some(record) {
                    let _ = fs::remove_file(path);
                }
            }
        }

        fn clean_files(&self) {
            if identity(&self.dir).ok() != Some(self.dir_identity) {
                return;
            }
            for record in self.owners.lock().unwrap().records.values() {
                self.remove_record(record);
            }
            let socket = self.dir.join("control.sock");
            if identity(&socket).ok() == Some(self.socket_identity) {
                let _ = fs::remove_file(socket);
            }
            let _ = fs::remove_dir(&self.dir); // Never recurse or delete unfamiliar files.
        }
    }

    impl Drop for SessionLifecycle {
        fn drop(&mut self) {
            self.0.stop.store(true, Ordering::Release);
            self.0.shutdown.store(true, Ordering::Release);
            self.0.clean_files();
        }
    }

    fn remaining(deadline: Instant) -> Result<Duration> {
        deadline
            .checked_duration_since(Instant::now())
            .filter(|d| !d.is_zero())
            .ok_or_else(|| anyhow!("session cleanup deadline exceeded"))
    }

    fn is_not_found(error: &anyhow::Error) -> bool {
        error
            .downcast_ref::<std::io::Error>()
            .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound)
    }

    fn poll(fd: libc::c_int, events: libc::c_short, deadline: Instant) -> Result<()> {
        loop {
            let mut pfd = libc::pollfd {
                fd,
                events,
                revents: 0,
            };
            let millis = remaining(deadline)?
                .as_millis()
                .max(1)
                .min(i32::MAX as u128) as i32;
            let result = unsafe { libc::poll(&mut pfd, 1, millis) };
            if result > 0 {
                return Ok(());
            }
            if result == 0 {
                bail!("session cleanup deadline exceeded");
            }
            if std::io::Error::last_os_error().kind() != std::io::ErrorKind::Interrupted {
                bail!("session socket poll failed");
            }
        }
    }

    fn read_frame(stream: &mut UnixStream, deadline: Instant, max: usize) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        stream.set_nonblocking(true)?;
        loop {
            // On macOS, changing SO_RCVTIMEO after the peer closes can return
            // EINVAL even while its final reply is buffered. Poll the fd instead.
            poll(stream.as_raw_fd(), libc::POLLIN, deadline)?;
            let mut byte = [0];
            match stream.read(&mut byte) {
                Ok(0) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "session endpoint closed without confirmation",
                    )
                    .into());
                }
                Ok(_) => {}
                Err(e)
                    if matches!(
                        e.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                    ) =>
                {
                    continue;
                }
                Err(e) => return Err(e).context("read endpoint frame"),
            }
            if byte[0] == b'\n' {
                return Ok(bytes);
            }
            bytes.push(byte[0]);
            if bytes.len() > max {
                bail!("session control input exceeds size limit");
            }
        }
    }

    // std's UnixStream::connect has no timeout. Create a nonblocking socket
    // before connecting so even a saturated local listener cannot hang a hook.
    fn connect(path: &Path, deadline: Instant) -> Result<UnixStream> {
        let fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let stream = unsafe { UnixStream::from_raw_fd(fd) };
        stream.set_nonblocking(true)?;
        let mut addr: libc::sockaddr_un = unsafe { std::mem::zeroed() };
        addr.sun_family = libc::AF_UNIX as _;
        let path = path.as_os_str().as_bytes();
        if path.len() >= addr.sun_path.len() {
            bail!("session socket path is too long");
        }
        for (dest, source) in addr.sun_path.iter_mut().zip(path) {
            *dest = *source as _;
        }
        let result = unsafe {
            libc::connect(
                fd,
                &addr as *const _ as *const libc::sockaddr,
                std::mem::size_of_val(&addr) as _,
            )
        };
        if result != 0 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::EINPROGRESS) {
                return Err(error.into());
            }
            poll(fd, libc::POLLOUT, deadline)?;
            if let Some(error) = stream.take_error()? {
                return Err(error.into());
            }
        }
        stream.set_nonblocking(false)?;
        Ok(stream)
    }

    pub fn end_session(session_id: &str, deadline: Instant) -> Result<()> {
        validate_session_id(session_id)?;
        let Some(root) = state_dir(false)? else {
            return Ok(());
        };
        let mut failures = 0;
        for (count, entry) in fs::read_dir(root)?.enumerate() {
            remaining(deadline)?;
            if count >= MAX_INSTANCES {
                bail!("session instance scan limit exceeded");
            }
            let entry = entry?;
            let Some(instance) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            if !is_hex(&instance, 32) {
                continue;
            }
            let dir = entry.path();
            if let Err(error) = private_metadata(&dir, true) {
                if !is_not_found(&error) {
                    failures += 1;
                }
                continue;
            }
            let path = record_path(&dir, session_id);
            match fs::symlink_metadata(&path) {
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(_) => {
                    failures += 1;
                    continue;
                }
                Ok(_) => {}
            }
            let record = match read_record(&path) {
                Ok(record)
                    if record.version == 1
                        && record.instance == instance
                        && record.session_id == session_id
                        && is_hex(&record.nonce, 64) =>
                {
                    record
                }
                Err(error)
                    if is_not_found(&error)
                        || fs::symlink_metadata(&path)
                            .is_err_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
                {
                    continue;
                }
                _ => {
                    failures += 1;
                    continue;
                }
            };
            let socket = dir.join("control.sock");
            match fs::symlink_metadata(&socket) {
                Ok(meta)
                    if std::os::unix::fs::FileTypeExt::is_socket(&meta.file_type())
                        && meta.uid() == unsafe { libc::geteuid() }
                        && meta.mode() & 0o077 == 0 => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue, // Already closed.
                _ => {
                    failures += 1;
                    continue;
                }
            };
            let request_deadline = deadline.min(Instant::now() + IO_BUDGET);
            let result = (|| -> Result<()> {
                let mut stream = connect(&socket, request_deadline).context("connect endpoint")?;
                stream
                    .set_write_timeout(Some(remaining(request_deadline)?))
                    .context("configure endpoint write timeout")?;
                let mut bytes = serde_json::to_vec(&record)?;
                bytes.push(b'\n');
                stream.write_all(&bytes).context("send endpoint release")?;
                let reply = read_frame(&mut stream, request_deadline, 32)?;
                if reply != b"released" && reply != b"already_released" {
                    bail!("session release was not confirmed");
                }
                Ok(())
            })();
            if let Err(error) = result {
                // A dead instance is harmless, but do not remove its records
                // or count a connection refusal as a confirmed release.
                let already_closed = error.downcast_ref::<std::io::Error>().is_some_and(|e| {
                    matches!(
                        e.kind(),
                        std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
                    )
                });
                let lost_reply = error.downcast_ref::<std::io::Error>().is_some_and(|e| {
                    matches!(
                        e.kind(),
                        std::io::ErrorKind::UnexpectedEof
                            | std::io::ErrorKind::ConnectionReset
                            | std::io::ErrorKind::BrokenPipe
                    )
                });
                // Concurrent final release may close queued connections before
                // they receive a reply. Verify the endpoint is now closed; a
                // live server rejecting a forged nonce must still be a failure.
                let now_closed = lost_reply
                    && connect(&socket, request_deadline).err().is_some_and(|e| {
                        e.downcast_ref::<std::io::Error>().is_some_and(|e| {
                            matches!(
                                e.kind(),
                                std::io::ErrorKind::ConnectionRefused
                                    | std::io::ErrorKind::NotFound
                            )
                        })
                    });
                if !already_closed && !now_closed {
                    eprintln!("tsindex: session endpoint did not confirm release: {error:#}");
                    failures += 1;
                }
            }
        }
        if failures > 0 {
            bail!(
                "{failures} session endpoint(s) could not confirm release; no unauthenticated shutdown attempted"
            );
        }
        Ok(())
    }

    pub fn end(from_stdin: bool, session_id: Option<&str>) -> Result<()> {
        let deadline = Instant::now() + CLEANUP_BUDGET;
        let id = if from_stdin {
            let mut bytes = Vec::new();
            let mut stdin = std::io::stdin().lock();
            loop {
                poll(libc::STDIN_FILENO, libc::POLLIN, deadline)?;
                let mut chunk = [0; 4096];
                let n = stdin.read(&mut chunk)?;
                if n == 0 {
                    break;
                }
                bytes.extend_from_slice(&chunk[..n]);
                if bytes.len() > 64 * 1024 {
                    bail!("session-end event exceeds size limit");
                }
            }
            let event: serde_json::Value = serde_json::from_slice(&bytes)
                .map_err(|_| anyhow!("invalid session-end event JSON"))?;
            if event.get("hook_event_name").and_then(|v| v.as_str()) != Some("SessionEnd") {
                bail!("expected a SessionEnd event");
            }
            event
                .get("session_id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("session-end event requires session_id"))?
                .to_owned()
        } else {
            session_id
                .ok_or_else(|| anyhow!("session_id is required"))?
                .to_owned()
        };
        end_session(&id, deadline).context("session cleanup failed")
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn session_ids_are_bounded_in_utf8_bytes() {
            for id in ["", "a\n", "a\0", &"x".repeat(257), &"é".repeat(129)] {
                assert!(validate_session_id(id).is_err());
            }
            for id in ["thr_123", "session/with spaces", &"é".repeat(128)] {
                assert!(validate_session_id(id).is_ok());
            }
        }

        #[test]
        fn random_instances_and_record_names_do_not_encode_paths() -> Result<()> {
            let first = random_hex(16)?;
            assert!(is_hex(&first, 32));
            assert_ne!(first, random_hex(16)?);
            let dir = Path::new("/private/session");
            assert_eq!(record_path(dir, "../../other").parent(), Some(dir));
            assert_ne!(record_path(dir, "one"), record_path(dir, "two"));
            Ok(())
        }

        #[test]
        fn record_reads_reject_symlinks_hardlinks_and_oversize_files() -> Result<()> {
            let dir = tempfile::tempdir()?;
            let path = dir.path().join("record");
            let record = Record {
                version: 1,
                instance: "a".repeat(32),
                session_id: "one".into(),
                nonce: "b".repeat(64),
            };
            fs::write(&path, serde_json::to_vec(&record)?)?;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
            assert!(read_record(&path)? == record);
            let link = dir.path().join("link");
            std::os::unix::fs::symlink(&path, &link)?;
            assert!(read_record(&link).is_err());
            fs::remove_file(&link)?;
            fs::hard_link(&path, &link)?;
            assert!(read_record(&path).is_err());
            fs::remove_file(&link)?;
            fs::write(&path, vec![b'x'; MAX_RECORD_BYTES + 1])?;
            assert!(read_record(&path).is_err());
            Ok(())
        }

        #[test]
        fn frame_deadline_is_total_not_reset_by_dripping_bytes() -> Result<()> {
            let (mut reader, mut writer) = UnixStream::pair()?;
            let sender = thread::spawn(move || {
                for _ in 0..20 {
                    if writer.write_all(b"x").is_err() {
                        break;
                    }
                    thread::sleep(Duration::from_millis(10));
                }
            });
            let start = Instant::now();
            assert!(read_frame(&mut reader, start + Duration::from_millis(60), 100).is_err());
            assert!(start.elapsed() < Duration::from_millis(300));
            drop(reader);
            sender.join().unwrap();
            Ok(())
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
pub use unix::{SessionLifecycle, end, validate_session_id};

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub struct SessionLifecycle;

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
impl SessionLifecycle {
    pub fn start(_: std::sync::Arc<std::sync::atomic::AtomicBool>) -> Result<Self> {
        bail!("session lifecycle is supported only on Linux and macOS")
    }
    pub fn register(&self, _: &str) -> Result<()> {
        bail!("session lifecycle is unavailable")
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn validate_session_id(_: &str) -> Result<()> {
    bail!("session lifecycle is unavailable")
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn end(_: bool, _: Option<&str>) -> Result<()> {
    bail!("session lifecycle is supported only on Linux and macOS")
}
