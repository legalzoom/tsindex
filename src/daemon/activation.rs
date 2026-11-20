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
    let raw = fd.as_raw_fd();
    let mut socket_type: libc::c_int = 0;
    let mut accepting: libc::c_int = 0;
    let mut size = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    // A valid inherited descriptor can still be a connected stream or a socket
    // of another family. Validate before constructing the UnixListener wrapper.
    for (option, value) in [
        (libc::SO_TYPE, &mut socket_type),
        (libc::SO_ACCEPTCONN, &mut accepting),
    ] {
        if unsafe {
            libc::getsockopt(
                raw,
                libc::SOL_SOCKET,
                option,
                (value as *mut libc::c_int).cast(),
                &mut size,
            )
        } != 0
        {
            return Err(io::Error::last_os_error()).context("invalid activation socket");
        }
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
        || accepting == 0
        || address.ss_family as i32 != libc::AF_UNIX
    {
        bail!("activation requires a listening Unix stream socket");
    }
    // Match sd_listen_fds: activation descriptors must never leak into workers.
    // https://www.freedesktop.org/software/systemd/man/latest/sd_listen_fds.html
    if unsafe { libc::fcntl(raw, libc::F_SETFD, libc::FD_CLOEXEC) } == -1 {
        return Err(io::Error::last_os_error()).context("failed to protect activation descriptor");
    }
    let listener = UnixListener::from(fd);
    if listener.local_addr()?.as_pathname() != Some(paths.socket_path.as_path()) {
        bail!(
            "activation socket does not match configured endpoint {}",
            paths.socket_path.display()
        );
    }
    Ok(Some(listener))
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
