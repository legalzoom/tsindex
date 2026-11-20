//! Descriptor inheritance across daemon and worker exec.
use std::fs;
use std::io;
use std::os::fd::{AsRawFd, BorrowedFd, RawFd};
use std::os::unix::process::CommandExt;
use std::process::Command;
#[cfg(target_os = "macos")]
use std::{mem::size_of, ptr};
#[cfg(not(target_os = "macos"))]
use std::mem;

fn set_fd_flags(fd: RawFd, flags: libc::c_int) -> io::Result<()> {
    loop {
        if unsafe { libc::fcntl(fd, libc::F_SETFD, flags) } == 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

/// Keep `inherit` open at its current number until this command has spawned.
/// Only the child changes fd flags; the daemon keeps its own descriptors CLOEXEC.
pub(super) fn prepare_exec(cmd: &mut Command, inherit: Option<BorrowedFd<'_>>) -> io::Result<()> {
    let inherit = inherit.map(|fd| fd.as_raw_fd());
    if inherit.is_some_and(|fd| fd < 3) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "runtime fd overlaps standard I/O",
        ));
    }
    let ceiling = fallback_ceiling()?;
    // Run after fork so fds opened concurrently by another daemon thread are
    // covered. Mark rather than close: Rust's spawn error pipe must work until exec.
    unsafe {
        cmd.pre_exec(move || {
            #[cfg(target_os = "linux")]
            if ceiling.is_none() {
                close_range_cloexec(3)?;
            }
            if let Some(ceiling) = ceiling {
                sweep_cloexec(ceiling)?;
            }
            if let Some(fd) = inherit {
                set_fd_flags(fd, 0)?;
            }
            Ok(())
        });
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn close_range_cloexec(first: u32) -> io::Result<()> {
    if unsafe {
        libc::syscall(
            libc::SYS_close_range,
            first,
            u32::MAX,
            libc::CLOSE_RANGE_CLOEXEC,
        )
    } < 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn fallback_ceiling() -> io::Result<Option<RawFd>> {
    #[cfg(target_os = "linux")]
    {
        // This impossible descriptor tests support without touching the parent's
        // descriptor table. Modern kernels need no procfs traversal per worker.
        match close_range_cloexec(u32::MAX) {
            Ok(()) => return Ok(None),
            Err(e)
                if matches!(
                    e.raw_os_error(),
                    Some(libc::ENOSYS | libc::EINVAL | libc::EPERM | libc::EACCES)
                ) => {}
            Err(e) => return Err(e),
        }
    }
    #[cfg(target_os = "macos")]
    let ceiling = {
        let mut ceiling: libc::c_int = 0;
        let mut size = size_of::<libc::c_int>();
        if unsafe {
            libc::sysctlbyname(
                c"kern.maxfilesperproc".as_ptr(),
                (&raw mut ceiling).cast(),
                &mut size,
                ptr::null_mut(),
                0,
            )
        } != 0
        {
            return Err(io::Error::last_os_error());
        }
        ceiling
    };
    #[cfg(not(target_os = "macos"))]
    let ceiling = {
        let mut limit: libc::rlimit = unsafe { mem::zeroed() };
        if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0 {
            return Err(io::Error::last_os_error());
        }
        // A finite hard limit bounds new descriptors in this process. Linux
        // also exposes its finite system ceiling when RLIM_INFINITY is allowed.
        if let Ok(finite) = RawFd::try_from(limit.rlim_max) {
            finite
        } else {
            #[cfg(target_os = "linux")]
            {
                fs::read_to_string("/proc/sys/fs/nr_open")?
                    .trim()
                    .parse()
                    .map_err(|_| io::Error::other("invalid kernel descriptor ceiling"))?
            }
            #[cfg(not(target_os = "linux"))]
            {
                return Err(io::Error::other("cannot bound descriptor inheritance"));
            }
        }
    };
    if ceiling < 3 {
        return Err(io::Error::other("invalid descriptor ceiling"));
    }
    // Resource limits can be lowered after descriptors were opened. Include
    // those concrete fds in the portable sweep as well as new allocations.
    #[cfg(target_os = "linux")]
    let fd_dir = "/proc/self/fd";
    #[cfg(not(target_os = "linux"))]
    let fd_dir = "/dev/fd";
    let mut ceiling = ceiling;
    for entry in fs::read_dir(fd_dir).or_else(|_| fs::read_dir("/dev/fd"))? {
        if let Some(fd) = entry?
            .file_name()
            .to_str()
            .and_then(|s| s.parse::<RawFd>().ok())
        {
            ceiling = ceiling.max(fd.saturating_add(1));
        }
    }
    Ok(Some(ceiling))
}

fn sweep_cloexec(ceiling: RawFd) -> io::Result<()> {
    for fd in 3..ceiling {
        if let Err(error) = set_fd_flags(fd, libc::FD_CLOEXEC)
            && error.raw_os_error() != Some(libc::EBADF)
        {
            return Err(error);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::fd::AsFd;
    use std::os::unix::net::UnixStream;

    #[test]
    fn exec_keeps_only_the_requested_capability_at_its_original_number() -> io::Result<()> {
        let file = fs::File::open("/dev/null")?;
        let (session, _) = UnixStream::pair()?;
        let hidden = file.as_raw_fd();
        let kept = session.as_raw_fd();
        let mut cmd = Command::new("/bin/sh");
        cmd.arg("-c").arg(format!(
            "test ! -e /dev/fd/{hidden} && test -e /dev/fd/{kept}"
        ));
        // A separate daemon thread can have an accept-to-fcntl window on Darwin.
        // Recreate its ordinary inheritance state only in this forked child.
        unsafe {
            cmd.pre_exec(move || set_fd_flags(hidden, 0));
        }
        prepare_exec(&mut cmd, Some(session.as_fd()))?;
        assert!(cmd.status()?.success());
        assert_ne!(
            unsafe { libc::fcntl(kept, libc::F_GETFD) } & libc::FD_CLOEXEC,
            0
        );
        Ok(())
    }
}
