//! Listening sockets inherited from the user's service manager.

use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::net::UnixListener;

use anyhow::{Context, Result, bail};

use crate::daemon::DaemonPaths;

/// Claim a manager-owned listener without binding or unlinking its pathname.
/// The manager retains its descriptor while this process and its children exit.
pub(crate) fn inherited_listener(paths: &DaemonPaths) -> Result<Option<UnixListener>> {
    let Some(fd) = activated_fd()? else {
        return Ok(None);
    };
    validate_listener(fd, paths).map(Some)
}

fn validate_listener(fd: OwnedFd, paths: &DaemonPaths) -> Result<UnixListener> {
    let raw = fd.as_raw_fd();
    let mut socket_type: libc::c_int = 0;
    let mut size = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    // A valid inherited descriptor can still be a connected stream or a socket
    // of another family. Validate before constructing the UnixListener wrapper.
    if unsafe {
        libc::getsockopt(
            raw,
            libc::SOL_SOCKET,
            libc::SO_TYPE,
            (&mut socket_type as *mut libc::c_int).cast(),
            &mut size,
        )
    } != 0
    {
        return Err(io::Error::last_os_error()).context("invalid activation socket");
    }
    let mut address: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let mut address_size = std::mem::size_of_val(&address) as libc::socklen_t;
    if unsafe {
        libc::getsockname(
            raw,
            (&mut address as *mut libc::sockaddr_storage).cast(),
            &mut address_size,
        )
    } != 0
    {
        return Err(io::Error::last_os_error()).context("failed to inspect activation socket");
    }
    if socket_type != libc::SOCK_STREAM
        || address.ss_family as i32 != libc::AF_UNIX
        || !is_accepting(raw).context("failed to inspect activation listener state")?
    {
        bail!("activation requires a listening Unix stream socket");
    }
    // Match sd_listen_fds: activation descriptors must never leak into workers.
    // https://www.freedesktop.org/software/systemd/man/latest/sd_listen_fds.html
    if unsafe { libc::fcntl(raw, libc::F_SETFD, libc::FD_CLOEXEC) } == -1 {
        return Err(io::Error::last_os_error()).context("failed to protect activation descriptor");
    }
    let listener = UnixListener::from(fd);
    let address = listener.local_addr()?;
    let reported = address.as_pathname().with_context(|| {
        format!(
            "activation socket has no filesystem pathname; configured endpoint {}",
            paths.socket_path.display()
        )
    })?;
    validate_endpoint_path(reported, &paths.socket_path).with_context(|| {
        format!(
            "activation socket reported {} for configured endpoint {}",
            reported.display(),
            paths.socket_path.display()
        )
    })?;
    Ok(listener)
}

fn validate_endpoint_path(reported: &std::path::Path, configured: &std::path::Path) -> Result<()> {
    use std::os::unix::fs::{FileTypeExt, MetadataExt};

    if reported.file_name().is_none() || reported.file_name() != configured.file_name() {
        bail!("activation socket filename does not match configured endpoint");
    }
    let reported_parent = reported
        .parent()
        .context("activation socket has no parent")?;
    let configured_parent = configured
        .parent()
        .context("configured socket has no parent")?;
    // Parent aliases such as /tmp and /private/tmp can name the same private
    // directory. Resolve only the parents, preserving rejection of a symlink
    // at the socket leaf instead of following it into another endpoint.
    for (socket, parent) in [(reported, reported_parent), (configured, configured_parent)] {
        crate::daemon::validate_private_dir(parent)?;
        let metadata = std::fs::symlink_metadata(socket).with_context(|| {
            format!("failed to inspect activation endpoint {}", socket.display())
        })?;
        if !metadata.file_type().is_socket() || metadata.uid() != crate::daemon::current_uid() {
            bail!(
                "activation endpoint {} must be an owned Unix socket",
                socket.display()
            );
        }
    }
    let reported_parent = std::fs::canonicalize(reported_parent)
        .context("failed to resolve activation socket parent")?;
    let configured_parent = std::fs::canonicalize(configured_parent)
        .context("failed to resolve configured socket parent")?;
    if reported_parent != configured_parent {
        bail!("activation socket parent does not match configured endpoint parent");
    }
    Ok(())
}

#[cfg(not(target_os = "macos"))]
fn is_accepting(fd: libc::c_int) -> io::Result<bool> {
    let mut accepting: libc::c_int = 0;
    let mut size = std::mem::size_of_val(&accepting) as libc::socklen_t;
    if unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_ACCEPTCONN,
            (&mut accepting as *mut libc::c_int).cast(),
            &mut size,
        )
    } != 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(accepting != 0)
}

#[cfg(target_os = "macos")]
fn is_accepting(fd: libc::c_int) -> io::Result<bool> {
    // XNU exposes SO_ACCEPTCONN in socket_info.soi_options, but does not
    // implement it as a getsockopt query. Inspect our own descriptor without
    // accepting a queued client or changing the manager's shared listener.
    // https://github.com/apple-oss-distributions/xnu/blob/f6217f891ac0bb64f3d375211650a4c1ff8ca1ea/bsd/sys/proc_info.h#L234-L240
    // https://github.com/apple-oss-distributions/xnu/blob/f6217f891ac0bb64f3d375211650a4c1ff8ca1ea/bsd/sys/proc_info.h#L503-L536
    // https://github.com/apple-oss-distributions/xnu/blob/f6217f891ac0bb64f3d375211650a4c1ff8ca1ea/bsd/kern/socket_info.c#L69-L80
    #[repr(C)]
    struct ProcFileInfo {
        _open_flags: u32,
        _status: u32,
        _offset: libc::off_t,
        _kind: i32,
        _guard_flags: u32,
    }

    #[repr(C)]
    struct SocketFdInfoPrefix {
        _file: ProcFileInfo,
        _stat: libc::vinfo_stat,
        _socket: u64,
        _pcb: u64,
        _kind: libc::c_int,
        _protocol: libc::c_int,
        _family: libc::c_int,
        options: libc::c_short,
    }

    const PROC_PIDFDSOCKETINFO: libc::c_int = 3;
    // The syscall requires room for the complete socket_fdinfo, including its
    // protocol union. Oversized aligned storage avoids duplicating that union's
    // ABI; only the documented prefix above is interpreted after a size check.
    let mut storage = [0u64; 512];
    let capacity = std::mem::size_of_val(&storage);
    let count = unsafe {
        libc::proc_pidfdinfo(
            libc::getpid(),
            fd,
            PROC_PIDFDSOCKETINFO,
            storage.as_mut_ptr().cast(),
            capacity as libc::c_int,
        )
    };
    if count <= 0 {
        return Err(io::Error::last_os_error());
    }
    if (count as usize) < std::mem::size_of::<SocketFdInfoPrefix>() || count as usize > capacity {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid activation socket inspection record size",
        ));
    }
    // SAFETY: u64 storage provides the prefix's alignment, the kernel has
    // initialized at least its full size, and all its fields have C integer ABI.
    let info = unsafe { &*storage.as_ptr().cast::<SocketFdInfoPrefix>() };
    Ok(info.options & libc::SO_ACCEPTCONN as libc::c_short != 0)
}

#[cfg(target_os = "linux")]
fn activated_fd() -> Result<Option<OwnedFd>> {
    // Implement the documented sd_listen_fds wire protocol without depending on
    // libsystemd, so the same binary remains usable outside systemd sessions.
    // https://www.freedesktop.org/software/systemd/man/latest/sd_listen_fds.html
    if std::env::var("LISTEN_PID").ok().as_deref() != Some(std::process::id().to_string().as_str())
    {
        return Ok(None);
    }
    let count = std::env::var("LISTEN_FDS")
        .context("LISTEN_PID set without LISTEN_FDS")?
        .parse::<u32>()
        .context("invalid LISTEN_FDS")?;
    if count != 1 {
        bail!("expected one activation descriptor, received {count}");
    }
    if unsafe { libc::fcntl(3, libc::F_GETFD) } == -1 {
        return Err(io::Error::last_os_error()).context("activation descriptor 3 is unavailable");
    }
    // SAFETY: the service manager transfers ownership of descriptor 3 exactly
    // once before any worker threads start.
    Ok(Some(unsafe { OwnedFd::from_raw_fd(3) }))
}

#[cfg(target_os = "macos")]
fn activated_fd() -> Result<Option<OwnedFd>> {
    unsafe extern "C" {
        fn launch_activate_socket(
            name: *const libc::c_char,
            fds: *mut *mut libc::c_int,
            count: *mut libc::size_t,
        ) -> libc::c_int;
    }
    let mut descriptors = std::ptr::null_mut();
    let mut count = 0;
    // The named entry corresponds to Sockets/Listeners in the installed plist.
    // launch_activate_socket returns errno directly and allocates the array.
    let result =
        unsafe { launch_activate_socket(c"Listeners".as_ptr(), &mut descriptors, &mut count) };
    if result == libc::ESRCH || result == libc::ENOENT {
        return Ok(None);
    }
    if result != 0 {
        return Err(io::Error::from_raw_os_error(result))
            .context("launchd socket activation failed");
    }
    if descriptors.is_null() || count == 0 {
        unsafe { libc::free(descriptors.cast()) };
        bail!("launchd returned no activation descriptors");
    }
    let fds: Vec<OwnedFd> = unsafe {
        std::slice::from_raw_parts(descriptors, count)
            .iter()
            .map(|&fd| OwnedFd::from_raw_fd(fd))
            .collect()
    };
    unsafe { libc::free(descriptors.cast()) };
    if fds.len() != 1 {
        bail!(
            "expected one launchd activation descriptor, received {}",
            fds.len()
        );
    }
    Ok(fds.into_iter().next())
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn activated_fd() -> Result<Option<OwnedFd>> {
    Ok(None)
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::{PermissionsExt, symlink};
    use std::os::unix::net::{UnixDatagram, UnixStream};
    use std::time::Duration;

    use super::*;

    fn private_scratch() -> Result<tempfile::TempDir> {
        let dir = tempfile::tempdir_in("/tmp")?;
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700))?;
        Ok(dir)
    }

    fn unbound_stream() -> Result<OwnedFd> {
        #[cfg(target_os = "linux")]
        let flags = libc::SOCK_STREAM | libc::SOCK_CLOEXEC;
        #[cfg(not(target_os = "linux"))]
        let flags = libc::SOCK_STREAM;
        let raw = unsafe { libc::socket(libc::AF_UNIX, flags, 0) };
        if raw < 0 {
            return Err(io::Error::last_os_error()).context("test socket creation failed");
        }
        // SAFETY: socket returned a fresh descriptor owned by this test.
        let fd = unsafe { OwnedFd::from_raw_fd(raw) };
        if unsafe { libc::fcntl(raw, libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
            return Err(io::Error::last_os_error()).context("test socket protection failed");
        }
        Ok(fd)
    }

    #[test]
    fn listener_validation_preserves_pending_client_and_manager_socket() -> Result<()> {
        let dir = private_scratch()?;
        let paths = DaemonPaths::resolve(Some(&dir.path().join("d.sock")))?;
        let manager = UnixListener::bind(&paths.socket_path)?;
        let mut client = UnixStream::connect(&paths.socket_path)?;
        client.write_all(b"queued request")?;
        let fd: OwnedFd = manager.try_clone()?.into();
        assert_eq!(unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFD, 0) }, 0);

        let adopted = validate_listener(fd, &paths)?;
        assert_ne!(
            unsafe { libc::fcntl(adopted.as_raw_fd(), libc::F_GETFD) } & libc::FD_CLOEXEC,
            0
        );
        // Validation must not consume the first request to prove listening.
        adopted.set_nonblocking(true)?;
        let (mut accepted, _) = adopted.accept()?;
        accepted.set_read_timeout(Some(Duration::from_secs(1)))?;
        let mut request = [0u8; 14];
        accepted.read_exact(&mut request)?;
        assert_eq!(&request, b"queued request");
        drop(adopted);
        assert!(paths.socket_path.exists());
        let _next = UnixStream::connect(&paths.socket_path)?;
        assert!(manager.accept().is_ok());
        Ok(())
    }

    #[test]
    fn listener_validation_rejects_connected_unbound_and_non_listening_streams() -> Result<()> {
        let dir = private_scratch()?;
        let paths = DaemonPaths::resolve(Some(&dir.path().join("d.sock")))?;
        let (connected, _peer) = UnixStream::pair()?;
        assert!(validate_listener(connected.into(), &paths).is_err());
        assert!(validate_listener(unbound_stream()?, &paths).is_err());

        // A bound pathname alone does not prove this socket is listening.
        let fd = unbound_stream()?;
        let mut address: libc::sockaddr_un = unsafe { std::mem::zeroed() };
        let bytes = paths.socket_path.as_os_str().as_bytes();
        assert!(bytes.len() < address.sun_path.len());
        address.sun_family = libc::AF_UNIX as libc::sa_family_t;
        for (target, byte) in address.sun_path.iter_mut().zip(bytes) {
            *target = *byte as libc::c_char;
        }
        let length = (std::mem::offset_of!(libc::sockaddr_un, sun_path) + bytes.len() + 1)
            as libc::socklen_t;
        #[cfg(target_os = "macos")]
        {
            address.sun_len = length as u8;
        }
        assert_eq!(
            unsafe {
                libc::bind(
                    fd.as_raw_fd(),
                    (&address as *const libc::sockaddr_un).cast(),
                    length,
                )
            },
            0
        );
        assert!(validate_listener(fd, &paths).is_err());
        assert!(paths.socket_path.exists());
        Ok(())
    }

    #[test]
    fn listener_validation_rejects_wrong_type_family_and_endpoint() -> Result<()> {
        let dir = private_scratch()?;
        let paths = DaemonPaths::resolve(Some(&dir.path().join("d.sock")))?;
        let datagram = UnixDatagram::bind(&paths.socket_path)?;
        assert!(validate_listener(datagram.into(), &paths).is_err());
        let tcp = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))?;
        assert!(validate_listener(tcp.into(), &paths).is_err());
        let other = dir.path().join("other.sock");
        let manager = UnixListener::bind(&other)?;
        assert!(validate_listener(manager.try_clone()?.into(), &paths).is_err());
        assert!(other.exists());
        Ok(())
    }

    #[test]
    fn listener_validation_accepts_ancestor_alias_without_consuming_a_client() -> Result<()> {
        let dir = private_scratch()?;
        let real = dir.path().join("real");
        let private = real.join("private");
        std::fs::create_dir_all(&private)?;
        std::fs::set_permissions(&private, std::fs::Permissions::from_mode(0o700))?;
        let alias = dir.path().join("alias");
        symlink(&real, &alias)?;
        let actual = private.join("d.sock");
        let configured = alias.join("private/d.sock");
        let paths = DaemonPaths::resolve(Some(&configured))?;
        let manager = UnixListener::bind(&actual)?;
        let direct_alias = dir.path().join("parent-link");
        symlink(&private, &direct_alias)?;
        let direct_paths = DaemonPaths::resolve(Some(&direct_alias.join("d.sock")))?;
        assert!(validate_listener(manager.try_clone()?.into(), &direct_paths).is_err());
        let mut client = UnixStream::connect(&configured)?;
        client.write_all(b"queued request")?;

        let adopted = validate_listener(manager.try_clone()?.into(), &paths)?;
        assert_ne!(
            unsafe { libc::fcntl(adopted.as_raw_fd(), libc::F_GETFD) } & libc::FD_CLOEXEC,
            0
        );
        adopted.set_nonblocking(true)?;
        let (mut accepted, _) = adopted.accept()?;
        accepted.set_read_timeout(Some(Duration::from_secs(1)))?;
        let mut request = [0u8; 14];
        accepted.read_exact(&mut request)?;
        assert_eq!(&request, b"queued request");
        drop(adopted);
        assert!(actual.exists());
        assert!(configured.exists());
        let _next = UnixStream::connect(&configured)?;
        assert!(manager.accept().is_ok());
        Ok(())
    }

    #[test]
    fn listener_validation_rejects_different_parents_and_symlink_socket_leaves() -> Result<()> {
        use std::os::unix::fs::FileTypeExt;

        let dir = private_scratch()?;
        let first = dir.path().join("first");
        let second = dir.path().join("second");
        for parent in [&first, &second] {
            std::fs::create_dir(parent)?;
            std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))?;
        }
        let actual = first.join("d.sock");
        let configured = second.join("d.sock");
        let manager = UnixListener::bind(&actual)?;
        let _other = UnixListener::bind(&configured)?;
        let paths = DaemonPaths::resolve(Some(&configured))?;
        let error = validate_listener(manager.try_clone()?.into(), &paths).unwrap_err();
        let diagnostic = format!("{error:#}");
        assert!(diagnostic.contains(&actual.display().to_string()));
        assert!(diagnostic.contains(&configured.display().to_string()));
        assert!(std::fs::symlink_metadata(&actual)?.file_type().is_socket());
        assert!(
            std::fs::symlink_metadata(&configured)?
                .file_type()
                .is_socket()
        );

        // Even identical pathname strings must reject a leaf replaced after
        // bind, rather than following that symlink into the other endpoint.
        std::fs::remove_file(&actual)?;
        symlink(&configured, &actual)?;
        let paths = DaemonPaths::resolve(Some(&actual))?;
        assert!(validate_listener(manager.try_clone()?.into(), &paths).is_err());
        assert!(std::fs::symlink_metadata(&actual)?.file_type().is_symlink());
        assert!(
            std::fs::symlink_metadata(&configured)?
                .file_type()
                .is_socket()
        );
        Ok(())
    }
}
