//! Transfer a session endpoint over a private Unix socket and bound the
//! descriptors that survive an exec of a daemon, runtime, or worker.

use std::fs;
use std::io;
use std::mem::{self, size_of};
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::process::Command;
use std::ptr;

const FD_MARKER: u8 = 1;
const fn control_space(count: usize) -> usize {
    unsafe { libc::CMSG_SPACE((count * size_of::<RawFd>()) as _) as usize }
}

#[repr(C)]
struct Control<const N: usize> {
    // cmsghdr requires native alignment; a plain byte array has alignment one.
    _alignment: [libc::cmsghdr; 0],
    bytes: [u8; N],
}

impl<const N: usize> Control<N> {
    fn new() -> Self {
        Self {
            _alignment: [],
            bytes: [0; N],
        }
    }

    fn header(&mut self, io: &mut libc::iovec) -> libc::msghdr {
        let mut msg: libc::msghdr = unsafe { mem::zeroed() };
        msg.msg_iov = io;
        msg.msg_iovlen = 1;
        msg.msg_control = self.bytes.as_mut_ptr().cast();
        msg.msg_controllen = N as _;
        msg
    }
}

pub(super) fn send_fd(channel: &UnixStream, fd: BorrowedFd<'_>) -> io::Result<()> {
    let mut control = Control::<{ control_space(1) }>::new();
    let mut marker = FD_MARKER;
    let mut iov = libc::iovec {
        iov_base: (&raw mut marker).cast(),
        iov_len: 1,
    };
    let msg = control.header(&mut iov);
    // One payload byte associates exactly one descriptor transfer with this
    // stream position. The sender retains ownership until sendmsg completes.
    unsafe {
        let cmsg = libc::CMSG_FIRSTHDR(&msg);
        (*cmsg).cmsg_level = libc::SOL_SOCKET;
        (*cmsg).cmsg_type = libc::SCM_RIGHTS;
        (*cmsg).cmsg_len = libc::CMSG_LEN(size_of::<RawFd>() as _) as _;
        ptr::write_unaligned(libc::CMSG_DATA(cmsg).cast::<RawFd>(), fd.as_raw_fd());
    }
    loop {
        let n = unsafe { libc::sendmsg(channel.as_raw_fd(), &msg, libc::MSG_NOSIGNAL) };
        if n == 1 {
            return Ok(());
        }
        if n == 0 {
            return Err(io::ErrorKind::WriteZero.into());
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

pub(super) fn recv_fd(channel: &UnixStream) -> io::Result<Option<OwnedFd>> {
    // XNU installs descriptor rights before copying control data to userspace.
    // Reserve its entire UIPC_MAX_CMSG_FD (512), also covering Linux's 253,
    // so rejection of an oversized transfer can close every received fd.
    let mut control = Control::<{ control_space(512) }>::new();
    let mut marker = 0u8;
    let mut iov = libc::iovec {
        iov_base: (&raw mut marker).cast(),
        iov_len: 1,
    };
    #[cfg(target_os = "linux")]
    let flags = libc::MSG_CMSG_CLOEXEC;
    #[cfg(not(target_os = "linux"))]
    let flags = 0;
    loop {
        let mut msg = control.header(&mut iov);
        let n = unsafe { libc::recvmsg(channel.as_raw_fd(), &mut msg, flags) };
        if n < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
        if n == 0 && msg.msg_controllen == 0 {
            return Ok(None);
        }
        let mut valid = n == 1 && marker == FD_MARKER && msg.msg_flags & libc::MSG_CTRUNC == 0;
        let mut received = None;
        // recvmsg constructs the headers. Bound each payload to the storage it
        // actually returned, and take ownership of every fd before rejecting.
        #[allow(clippy::unnecessary_cast)] // Darwin uses socklen_t, Linux size_t.
        unsafe {
            let end = msg.msg_control as usize + msg.msg_controllen as usize;
            let mut cmsg = libc::CMSG_FIRSTHDR(&msg);
            while !cmsg.is_null() {
                let data = libc::CMSG_DATA(cmsg).cast::<RawFd>();
                let start = data as usize;
                let claimed = (*cmsg).cmsg_len as usize;
                let capacity = end - cmsg as usize;
                if claimed < libc::CMSG_LEN(0) as usize || start > end {
                    valid = false;
                    break;
                }
                if (*cmsg).cmsg_level == libc::SOL_SOCKET && (*cmsg).cmsg_type == libc::SCM_RIGHTS {
                    let bytes = claimed.min(capacity) - (start - cmsg as usize);
                    valid &= claimed <= capacity && bytes.is_multiple_of(size_of::<RawFd>());
                    for i in 0..bytes / size_of::<RawFd>() {
                        let fd = OwnedFd::from_raw_fd(ptr::read_unaligned(data.add(i)));
                        if received.is_some() {
                            valid = false;
                        } else {
                            received = Some(fd);
                        }
                    }
                } else {
                    valid = false;
                }
                if claimed > capacity {
                    break;
                }
                cmsg = libc::CMSG_NXTHDR(&msg, cmsg);
            }
        }
        if !valid || received.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "expected one descriptor and its marker on the runtime channel",
            ));
        }
        #[cfg(not(target_os = "linux"))]
        if let Some(fd) = &received {
            // Darwin lacks MSG_CMSG_CLOEXEC. The corresponding fork-side sweep
            // covers a concurrent worker spawn before this fcntl completes.
            set_fd_flags(fd.as_raw_fd(), libc::FD_CLOEXEC)?;
        }
        return Ok(received);
    }
}

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

// The portable fallback runs after fork, when no other thread can change the
// child's table. Obtain the actual table size or its entries without allocating
// or taking userspace locks; the precomputed ceiling remains a safe last resort.
#[cfg(target_os = "linux")]
fn child_fd_table_size() -> Option<RawFd> {
    let raw = unsafe {
        libc::open(
            c"/proc/self/status".as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };
    if raw < 0 {
        return None;
    }
    let fd = unsafe { OwnedFd::from_raw_fd(raw) };
    let mut fs: libc::statfs = unsafe { mem::zeroed() };
    if unsafe { libc::fstatfs(fd.as_raw_fd(), &mut fs) } != 0 || fs.f_type != libc::PROC_SUPER_MAGIC
    {
        return None;
    }
    let mut buf = [0u8; 2048];
    let n = unsafe { libc::read(fd.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len()) };
    let bytes = buf.get(..usize::try_from(n).ok()?)?;
    // The kernel publishes max_fds, including holes and open fds above a
    // lowered RLIMIT_NOFILE. Ignore incomplete records or a foreign PID view.
    // https://github.com/torvalds/linux/blob/master/fs/proc/array.c
    let number = |key: &[u8]| {
        bytes.split_inclusive(|b| *b == b'\n').find_map(|line| {
            let field = line.strip_suffix(b"\n")?.strip_prefix(key)?;
            std::str::from_utf8(field)
                .ok()?
                .trim()
                .parse::<RawFd>()
                .ok()
        })
    };
    if number(b"Pid:")? != unsafe { libc::getpid() } {
        return None;
    }
    number(b"FDSize:").filter(|&size| size > raw)
}

#[cfg(target_os = "macos")]
fn mark_darwin_open_fds() -> io::Result<bool> {
    // proc_pidinfo is a direct __proc_info syscall wrapper. A full bounded
    // array may be truncated, in which case use the conservative sweep below.
    // https://github.com/apple-oss-distributions/xnu/blob/main/libsyscall/wrappers/libproc/libproc.c
    let mut entries = [mem::MaybeUninit::<libc::proc_fdinfo>::uninit(); 1024];
    let bytes = std::mem::size_of_val(&entries);
    let n = unsafe {
        libc::proc_pidinfo(
            libc::getpid(),
            libc::PROC_PIDLISTFDS,
            0,
            entries.as_mut_ptr().cast(),
            bytes as _,
        )
    };
    let n = n as usize;
    if n == 0 || n >= bytes || !n.is_multiple_of(size_of::<libc::proc_fdinfo>()) {
        return Ok(false);
    }
    for entry in &entries[..n / size_of::<libc::proc_fdinfo>()] {
        let fd = unsafe { entry.assume_init_ref() }.proc_fd;
        if fd >= 3 {
            set_fd_flags(fd, libc::FD_CLOEXEC)?;
        }
    }
    Ok(true)
}

fn sweep_cloexec(ceiling: RawFd) -> io::Result<()> {
    #[cfg(target_os = "macos")]
    if mark_darwin_open_fds()? {
        return Ok(());
    }
    #[cfg(target_os = "linux")]
    let ceiling = child_fd_table_size().unwrap_or(ceiling);
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
    use std::io::{Read, Write};
    use std::os::fd::AsFd;
    use std::time::Duration;

    #[test]
    fn passed_socket_stays_connected_and_close_on_exec() -> io::Result<()> {
        let (send, receive) = UnixStream::pair()?;
        let (session, mut peer) = UnixStream::pair()?;
        send_fd(&send, session.as_fd())?;
        drop(session);
        let incoming = recv_fd(&receive)?.expect("session fd");
        assert_ne!(
            unsafe { libc::fcntl(incoming.as_raw_fd(), libc::F_GETFD) } & libc::FD_CLOEXEC,
            0
        );
        let mut socket = UnixStream::from(incoming);
        socket.write_all(b"ping")?;
        let mut bytes = [0; 4];
        peer.read_exact(&mut bytes)?;
        assert_eq!(&bytes, b"ping");
        drop(send);
        assert!(recv_fd(&receive)?.is_none());
        Ok(())
    }

    #[test]
    fn malformed_runtime_transfers_close_all_received_descriptors() -> io::Result<()> {
        let (mut send, receive) = UnixStream::pair()?;
        send.write_all(&[FD_MARKER])?;
        assert_eq!(
            recv_fd(&receive).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        let (session, mut peer) = UnixStream::pair()?;
        let mut control = Control::<{ control_space(2) }>::new();
        let mut marker = FD_MARKER;
        let mut iov = libc::iovec {
            iov_base: (&raw mut marker).cast(),
            iov_len: 1,
        };
        let msg = control.header(&mut iov);
        unsafe {
            let cmsg = libc::CMSG_FIRSTHDR(&msg);
            (*cmsg).cmsg_level = libc::SOL_SOCKET;
            (*cmsg).cmsg_type = libc::SCM_RIGHTS;
            (*cmsg).cmsg_len = libc::CMSG_LEN((size_of::<RawFd>() * 2) as _) as _;
            let fds = libc::CMSG_DATA(cmsg).cast::<RawFd>();
            ptr::write_unaligned(fds, session.as_raw_fd());
            ptr::write_unaligned(fds.add(1), session.as_raw_fd());
            assert_eq!(libc::sendmsg(send.as_raw_fd(), &msg, 0), 1);
        }
        assert_eq!(
            recv_fd(&receive).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        drop(session);
        peer.set_read_timeout(Some(Duration::from_secs(1)))?;
        assert_eq!(
            peer.read(&mut [0])?,
            0,
            "the receiver retained an extra descriptor"
        );
        Ok(())
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn fallback_sweep_handles_open_descriptors_above_a_lowered_soft_limit() -> io::Result<()> {
        let file = fs::File::open("/dev/null")?;
        let raw = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 128) };
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        let high = unsafe { OwnedFd::from_raw_fd(raw) };
        let mut cmd = Command::new("/bin/sh");
        cmd.arg("-c").arg(format!("test ! -e /dev/fd/{raw}"));
        unsafe {
            cmd.pre_exec(move || {
                let mut limit: libc::rlimit = mem::zeroed();
                if libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) != 0 {
                    return Err(io::Error::last_os_error());
                }
                limit.rlim_cur = 64;
                if libc::setrlimit(libc::RLIMIT_NOFILE, &limit) != 0 {
                    return Err(io::Error::last_os_error());
                }
                set_fd_flags(raw, 0)?;
                sweep_cloexec(raw + 1)
            });
        }
        assert!(cmd.status()?.success());
        drop(high);
        Ok(())
    }

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
