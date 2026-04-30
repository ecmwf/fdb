//! Multi-node barrier synchronization for ITT mode.
//!
//! Implements TCP-based inter-node barriers and FIFO-based intra-node barriers
//! matching the C++ fdb-hammer implementation.

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

/// Configuration for barrier synchronization.
pub struct BarrierConfig {
    /// Processes per node.
    pub ppn: u32,
    /// List of node hostnames (first is leader).
    pub nodes: Vec<String>,
    /// TCP port for inter-node barriers.
    pub port: u16,
    /// Maximum wait time for barriers.
    pub max_wait: Duration,
}

/// Perform a distributed barrier across all nodes and local processes.
///
/// # Errors
///
/// Returns an error if barrier synchronization fails.
pub fn barrier(config: &BarrierConfig) -> Result<(), Box<dyn std::error::Error>> {
    if config.nodes.is_empty() {
        return Ok(()); // No barrier needed if no nodes specified
    }

    if config.ppn == 1 {
        barrier_internode(config)
    } else {
        barrier_intranode(config)
    }
}

fn barrier_internode(config: &BarrierConfig) -> Result<(), Box<dyn std::error::Error>> {
    let hostname = hostname::get()?.to_string_lossy().to_string();

    if config.nodes.len() <= 1 {
        return Ok(()); // Single node - no barrier needed
    }

    if hostname == config.nodes[0] {
        leader_internode(config)
    } else {
        follower_internode(config)
    }
}

fn leader_internode(config: &BarrierConfig) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(("0.0.0.0", config.port))?;
    let expected = config.nodes.len() - 1;

    let mut connections = Vec::with_capacity(expected);
    for _ in 0..expected {
        let (stream, _) = listener.accept()?;
        connections.push(stream);
    }

    // Signal all followers to proceed
    for mut conn in connections {
        conn.write_all(b"END")?;
        conn.shutdown(std::net::Shutdown::Write)?;
    }

    Ok(())
}

fn follower_internode(config: &BarrierConfig) -> Result<(), Box<dyn std::error::Error>> {
    let leader = &config.nodes[0];
    let addr = format!("{leader}:{}", config.port);

    // Retry connection until timeout
    let start = std::time::Instant::now();
    let stream = loop {
        match TcpStream::connect(&addr) {
            Ok(s) => break s,
            Err(_) if start.elapsed() < config.max_wait => {
                thread::sleep(Duration::from_secs(1));
            }
            Err(e) => return Err(e.into()),
        }
    };

    let mut stream = stream;
    let mut buf = [0u8; 3];
    stream.read_exact(&mut buf)?;

    if &buf != b"END" {
        return Err("Invalid barrier signal".into());
    }

    Ok(())
}

fn barrier_intranode(config: &BarrierConfig) -> Result<(), Box<dyn std::error::Error>> {
    let run_path = get_run_path();
    let pid_file = run_path.join("fdb-hammer.pid");
    let wait_fifo = run_path.join("fdb-hammer.wait.fifo");
    let barrier_fifo = run_path.join("fdb-hammer.barrier.fifo");

    loop {
        // Try to become leader via exclusive file create
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&pid_file)
        {
            Ok(mut f) => {
                // We are the leader
                writeln!(f, "{}", std::process::id())?;
                drop(f);

                let result = run_leader_intranode(config, &wait_fifo, &barrier_fifo);
                let _ = std::fs::remove_file(&pid_file);
                return result;
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                // Check if leader is still alive
                if let Ok(contents) = std::fs::read_to_string(&pid_file)
                    && let Ok(pid) = contents.trim().parse::<i32>()
                    && unsafe { libc::kill(pid, 0) } != 0
                {
                    // Leader is dead, clean up and retry
                    let _ = std::fs::remove_file(&pid_file);
                    continue;
                }
                return run_follower_intranode(&wait_fifo, &barrier_fifo);
            }
            Err(e) => return Err(e.into()),
        }
    }
}

fn run_leader_intranode(
    config: &BarrierConfig,
    wait_fifo: &Path,
    barrier_fifo: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    // Create FIFOs
    let _ = std::fs::remove_file(wait_fifo);
    let _ = std::fs::remove_file(barrier_fifo);

    let fifo_mode = nix::sys::stat::Mode::from_bits(0o666).ok_or("Invalid FIFO mode bits")?;
    nix::unistd::mkfifo(wait_fifo, fifo_mode)?;
    nix::unistd::mkfifo(barrier_fifo, fifo_mode)?;

    // Wait for all local processes
    let mut wait_file = File::open(wait_fifo)?;
    let mut buf = [0u8; 3];
    for _ in 0..(config.ppn - 1) {
        wait_file.read_exact(&mut buf)?;
        if &buf != b"SIG" {
            return Err("Invalid wait signal".into());
        }
    }
    drop(wait_file);
    let _ = std::fs::remove_file(wait_fifo);

    // Do inter-node barrier
    let internode_result = barrier_internode(config);

    // Release local followers
    let mut barrier_file = File::create(barrier_fifo)?;
    if internode_result.is_err() {
        // Signal error to followers
        for _ in 0..(config.ppn - 1) {
            barrier_file.write_all(b"SIG")?;
        }
    }
    drop(barrier_file);
    let _ = std::fs::remove_file(barrier_fifo);

    internode_result
}

fn run_follower_intranode(
    wait_fifo: &Path,
    barrier_fifo: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    // Wait for FIFOs to exist
    while !wait_fifo.exists() {
        thread::sleep(Duration::from_millis(100));
    }

    // Spawn async task to wait for barrier (like C++ future)
    let barrier_fifo_clone = barrier_fifo.to_path_buf();
    let barrier_handle = thread::spawn(move || -> Result<(), String> {
        // Open barrier FIFO - blocks until leader opens for write
        let path_cstr = std::ffi::CString::new(barrier_fifo_clone.to_string_lossy().as_bytes())
            .map_err(|e| e.to_string())?;

        let fd = unsafe { libc::open(path_cstr.as_ptr(), libc::O_RDONLY) };
        if fd < 0 {
            return Err("Failed to open barrier FIFO".into());
        }

        let mut buf = [0u8; 3];
        let n = unsafe { libc::read(fd, buf.as_mut_ptr().cast::<libc::c_void>(), 3) };
        unsafe { libc::close(fd) };

        if n == 0 {
            Ok(()) // Normal completion - leader closed without writing
        } else if n == 3 && &buf == b"SIG" {
            Err("Inter-node barrier failed".into())
        } else {
            Err("Invalid barrier response".into())
        }
    });

    // Signal leader we're ready
    let mut wait_file = OpenOptions::new().write(true).open(wait_fifo)?;
    wait_file.write_all(b"SIG")?;
    drop(wait_file);

    // Wait for barrier result
    barrier_handle
        .join()
        .map_err(|_| "Barrier thread panicked")?
        .map_err(Into::into)
}

fn get_run_path() -> PathBuf {
    let uid = nix::unistd::getuid();
    let path = PathBuf::from(format!("/var/run/user/{uid}"));
    if path.exists() {
        path
    } else {
        std::env::temp_dir()
    }
}
