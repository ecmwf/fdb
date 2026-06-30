#![allow(clippy::doc_markdown)]
#![allow(clippy::uninlined_format_args)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::collapsible_if)]

//! FDB Hammer - Benchmark and stress test tool for FDB.
//!
//! This is a Rust port of ECMWF's C++ fdb-hammer tool, designed to reproduce
//! production workloads for testing FDB performance.
//!
//! # Usage
//!
//! ```bash
//! fdb-hammer [OPTIONS] <TEMPLATE_PATH>
//! ```
//!
//! # Modes
//!
//! - **Write mode** (default): Archives fields to FDB
//! - **Read mode** (`--read`): Reads fields from FDB
//! - **List mode** (`--list`): Lists fields in FDB
//!
//! # ITT Mode
//!
//! ITT (Instrumented Test Timing) mode enables distributed benchmarking with:
//! - Multi-node barriers (TCP-based)
//! - Step window timing (simulate model pacing)
//! - Polling for data availability (readers wait for writers)

mod barrier;

use std::fs;
use std::path::PathBuf;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::Parser;
use crossbeam_channel::{Receiver, Sender, bounded};
use rand::Rng;

use eccodes::GribHandle;
use fdb::{Fdb, Key, ListOptions, Request};

// =============================================================================
// Valid parameter IDs (from C++ fdb-hammer)
// =============================================================================

const VALID_PARAMS: &[u32] = &[
    1, 2, 3, 4, 5, 6, 7, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 21, 22, 23, 24, 25, 26, 27, 28,
    29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52,
    53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76,
    78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100,
    101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119,
    120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138,
    139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157,
    158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174,
];

// =============================================================================
// CLI Arguments
// =============================================================================

#[derive(Parser, Debug)]
#[command(name = "fdb_hammer")]
#[command(about = "FDB benchmark and stress test tool (Rust port of fdb-hammer)")]
#[allow(clippy::struct_excessive_bools)]
struct Args {
    /// Path to template GRIB file
    grib_path: PathBuf,

    /// FDB config file (YAML). If not specified, uses `FDB_HOME` env or default.
    #[arg(long)]
    config: Option<PathBuf>,

    /// Read mode (retrieve data instead of archiving)
    #[arg(long)]
    read: bool,

    /// List mode (list data instead of archiving)
    #[arg(long)]
    list: bool,

    // Request base parameters
    /// Experiment version (required)
    #[arg(long)]
    expver: String,

    /// MARS class (required)
    #[arg(long, name = "class")]
    class: String,

    /// Stream
    #[arg(long, default_value = "oper")]
    stream: String,

    /// Date (YYYYMMDD)
    #[arg(long, default_value = "20240101")]
    date: String,

    /// Time (HHMM)
    #[arg(long, default_value = "0000")]
    time: String,

    /// Type
    #[arg(long, name = "type", default_value = "fc")]
    type_: String,

    /// Level type
    #[arg(long, default_value = "sfc")]
    levtype: String,

    // Workload size
    /// Number of steps
    #[arg(long)]
    nsteps: u32,

    /// Number of levels
    #[arg(long, default_value = "0")]
    nlevels: u32,

    /// Comma-separated list of level numbers (alternative to --nlevels)
    #[arg(long, value_delimiter = ',', conflicts_with = "nlevels")]
    levels: Option<Vec<u32>>,

    /// Number of parameters
    #[arg(long)]
    nparams: u32,

    /// Number of ensemble members
    #[arg(long, default_value = "1")]
    nensembles: u32,

    // Starting values
    /// First step number
    #[arg(long, default_value = "0")]
    step: u32,

    /// First level number
    #[arg(long, default_value = "0")]
    level: u32,

    /// First ensemble member number
    #[arg(long, default_value = "1")]
    number: u32,

    // Verification
    /// Embed key digest at start/end of data for verification
    #[arg(long)]
    md_check: bool,

    /// Embed full data checksum (implies `md_check`)
    #[arg(long)]
    full_check: bool,

    /// Don't randomize field data
    #[arg(long)]
    no_randomise_data: bool,

    /// Print per-field output
    #[arg(long)]
    verbose: bool,

    // Iteration control
    /// Index (0-based) where to start iterating in level×param space
    #[arg(long, default_value = "0")]
    start_at: usize,

    /// Index (0-based) where to stop iterating in level×param space
    #[arg(long)]
    stop_at: Option<usize>,

    // Async verification
    /// Queue size for async verification worker
    #[arg(long, default_value = "10")]
    check_queue_size: usize,

    // FDB config
    /// Disable use of subtocs
    #[arg(long)]
    disable_subtocs: bool,

    // ITT mode options
    /// Enable ITT (Instrumented Test Timing) mode
    #[arg(long)]
    itt: bool,

    /// Seconds per step in ITT mode
    #[arg(long, default_value = "10")]
    step_window: u64,

    /// Random delay percentage (0-100) in ITT mode
    #[arg(long, default_value = "100")]
    random_delay: u32,

    /// Polling interval (seconds) for readers in ITT mode
    #[arg(long, default_value = "1")]
    poll_period: u64,

    /// Max polling attempts before failing in ITT mode
    #[arg(long, default_value = "200")]
    poll_max_attempts: u32,

    /// Pre-computed URIs file (skip listing in ITT read mode)
    #[arg(long)]
    uri_file: Option<PathBuf>,

    // Parallel/barrier options
    /// Processes per node
    #[arg(long, default_value = "1")]
    ppn: u32,

    /// Comma-separated list of node hostnames
    #[arg(long, value_delimiter = ',')]
    nodes: Vec<String>,

    /// Barrier TCP port
    #[arg(long, default_value = "7777")]
    barrier_port: u16,

    /// Barrier timeout (seconds)
    #[arg(long, default_value = "10")]
    barrier_max_wait: u64,

    /// Add random startup delay (0-10s)
    #[arg(long)]
    delay: bool,
}

// =============================================================================
// Verification
// =============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
enum CheckType {
    None = 0,
    MdCheck = 1,
    FullCheck = 2,
}

impl CheckType {
    const fn from_args(args: &Args) -> Self {
        if args.full_check {
            Self::FullCheck
        } else if args.md_check {
            Self::MdCheck
        } else {
            Self::None
        }
    }

    const fn header_size(self) -> usize {
        match self {
            Self::None => 0,
            Self::MdCheck => 4 + 16 + 16, // type + key_digest + unique_id
            Self::FullCheck => 4 + 16 + 16 + 16, // type + key_digest + checksum + unique_id
        }
    }

    const fn footer_size(self) -> usize {
        match self {
            Self::None | Self::FullCheck => 0,
            Self::MdCheck => 16 + 16, // key_digest + unique_id
        }
    }
}

struct Verifier {
    check_type: CheckType,
    unique_counter: u64,
    hostname: String,
}

impl Verifier {
    fn new(check_type: CheckType) -> Self {
        let hostname = hostname::get().map_or_else(
            |_| "unknown".to_string(),
            |h| h.to_string_lossy().into_owned(),
        );

        Self {
            check_type,
            unique_counter: 0,
            hostname,
        }
    }

    fn key_digest(key: &Key) -> [u8; 16] {
        use md5::{Digest, Md5};

        // Use only field-specific keys for digest (matching C++ fdb-hammer)
        // This avoids issues with optional keys like "domain" that FDB might return
        let field_keys = ["step", "levelist", "param", "number"];

        let mut entries: Vec<(&str, &str)> = key
            .entries()
            .filter(|(k, v)| field_keys.contains(k) && !v.is_empty())
            .collect();
        entries.sort_by(|a, b| a.0.cmp(b.0));

        let mut hasher = Md5::new();
        for (k, v) in &entries {
            hasher.update(k.as_bytes());
            hasher.update(b"=");
            hasher.update(v.as_bytes());
            hasher.update(b",");
        }
        hasher.finalize().into()
    }

    fn unique_digest(&mut self) -> [u8; 16] {
        use md5::{Digest, Md5};

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();

        let mut hasher = Md5::new();
        hasher.update(now.as_nanos().to_le_bytes());
        hasher.update(self.hostname.as_bytes());
        hasher.update(self.unique_counter.to_le_bytes());
        self.unique_counter += 1;

        hasher.finalize().into()
    }

    /// Embed verification data inside the GRIB message's data section.
    ///
    /// This matches the C++ fdb-hammer behavior: verification data is written
    /// into the GRIB data payload at `offset_before_data..offset_after_data`.
    #[allow(clippy::cast_possible_truncation)]
    fn embed_in_message(
        &mut self,
        key: &Key,
        message: &mut [u8],
        offset_before_data: usize,
        offset_after_data: usize,
    ) {
        if self.check_type == CheckType::None {
            return;
        }

        let data_section = &mut message[offset_before_data..offset_after_data];
        let data_len = data_section.len();

        match self.check_type {
            CheckType::None => {}

            CheckType::MdCheck => {
                let key_digest = Self::key_digest(key);
                let unique_id = self.unique_digest();

                let header_size = CheckType::MdCheck.header_size();
                let footer_size = CheckType::MdCheck.footer_size();

                if data_len >= header_size + footer_size {
                    // Write header at start of data section
                    let mut offset = 0;
                    data_section[offset..offset + 4]
                        .copy_from_slice(&(CheckType::MdCheck as u32).to_le_bytes());
                    offset += 4;
                    data_section[offset..offset + 16].copy_from_slice(&key_digest);
                    offset += 16;
                    data_section[offset..offset + 16].copy_from_slice(&unique_id);

                    // Write footer at end of data section
                    let footer_start = data_len - footer_size;
                    data_section[footer_start..footer_start + 16].copy_from_slice(&key_digest);
                    data_section[footer_start + 16..footer_start + 32].copy_from_slice(&unique_id);
                }
            }

            CheckType::FullCheck => {
                use md5::{Digest, Md5};

                let key_digest = Self::key_digest(key);
                let unique_id = self.unique_digest();

                let header_size = CheckType::FullCheck.header_size();

                if data_len >= header_size {
                    // Compute checksum over data after header (unique_id + remaining data)
                    // C++ computes: MD5(unique_id || data_after_header)
                    let checksum_data = &data_section[header_size - 16..]; // unique_id + rest
                    let checksum = Md5::digest(checksum_data);

                    // Write header at start of data section
                    let mut offset = 0;
                    data_section[offset..offset + 4]
                        .copy_from_slice(&(CheckType::FullCheck as u32).to_le_bytes());
                    offset += 4;
                    data_section[offset..offset + 16].copy_from_slice(&key_digest);
                    offset += 16;
                    data_section[offset..offset + 16].copy_from_slice(&checksum);
                    offset += 16;
                    data_section[offset..offset + 16].copy_from_slice(&unique_id);
                }
            }
        }
    }

    /// Extract and verify verification data from the GRIB data section.
    fn verify_from_message(
        &self,
        key: &Key,
        message: &[u8],
        offset_before_data: usize,
        offset_after_data: usize,
    ) -> Result<(), String> {
        if self.check_type == CheckType::None {
            return Ok(());
        }

        let data_section = &message[offset_before_data..offset_after_data];
        let header_size = self.check_type.header_size();
        let footer_size = self.check_type.footer_size();

        if data_section.len() < header_size + footer_size {
            return Err(format!(
                "Data section too short: {} bytes, need at least {}",
                data_section.len(),
                header_size + footer_size
            ));
        }

        // Read check type
        let stored_type = u32::from_le_bytes(
            data_section[0..4]
                .try_into()
                .map_err(|_| "Invalid check type bytes")?,
        );
        if stored_type != self.check_type as u32 {
            return Err(format!(
                "Check type mismatch: expected {:?}, got {}",
                self.check_type, stored_type
            ));
        }

        // Verify key digest
        let expected_key_digest = Self::key_digest(key);
        let stored_key_digest: [u8; 16] = data_section[4..20]
            .try_into()
            .map_err(|_| "Invalid key digest bytes")?;
        if stored_key_digest != expected_key_digest {
            return Err("Key digest mismatch".to_string());
        }

        match self.check_type {
            CheckType::MdCheck => {
                // Verify footer key digest matches header
                let footer_start = data_section.len() - footer_size;
                let footer_key_digest: [u8; 16] = data_section[footer_start..footer_start + 16]
                    .try_into()
                    .map_err(|_| "Invalid footer key digest bytes")?;
                if footer_key_digest != stored_key_digest {
                    return Err("Footer key digest mismatch".to_string());
                }
            }
            CheckType::FullCheck => {
                use md5::{Digest, Md5};

                // Verify data checksum
                let stored_checksum: [u8; 16] = data_section[20..36]
                    .try_into()
                    .map_err(|_| "Invalid checksum bytes")?;
                let checksum_data = &data_section[header_size - 16..]; // unique_id + rest
                let actual_checksum = Md5::digest(checksum_data);
                if stored_checksum != *actual_checksum {
                    return Err("Data checksum mismatch".to_string());
                }
            }
            CheckType::None => {}
        }

        Ok(())
    }
}

// =============================================================================
// Async Verification Worker
// =============================================================================

struct VerifyJob {
    key: Key,
    data: Vec<u8>,
}

struct AsyncVerifier {
    tx: Sender<VerifyJob>,
    worker: Option<JoinHandle<Result<(), String>>>,
}

impl AsyncVerifier {
    fn new(check_type: CheckType, queue_size: usize) -> Self {
        let (tx, rx) = bounded::<VerifyJob>(queue_size);

        let worker = thread::spawn(move || Self::verification_loop(rx, check_type));

        Self {
            tx,
            worker: Some(worker),
        }
    }

    #[allow(clippy::needless_pass_by_value)] // Receiver is moved into thread
    fn verification_loop(rx: Receiver<VerifyJob>, check_type: CheckType) -> Result<(), String> {
        let verifier = Verifier::new(check_type);

        while let Ok(job) = rx.recv() {
            // Parse GRIB to get data section offsets for verification
            let handle = GribHandle::from_bytes(&job.data)
                .map_err(|e| format!("Failed to parse GRIB: {e}"))?;

            #[allow(clippy::cast_sign_loss)]
            let offset_before = handle
                .get_long("offsetBeforeData")
                .map_err(|e| format!("Failed to get offsetBeforeData: {e}"))?
                as usize;
            #[allow(clippy::cast_sign_loss)]
            let offset_after = handle
                .get_long("offsetAfterData")
                .map_err(|e| format!("Failed to get offsetAfterData: {e}"))?
                as usize;

            verifier.verify_from_message(&job.key, &job.data, offset_before, offset_after)?;
        }

        Ok(())
    }

    /// Queue a message for verification (blocks if queue is full).
    fn verify_async(&self, key: Key, data: Vec<u8>) -> Result<(), String> {
        self.tx
            .send(VerifyJob { key, data })
            .map_err(|_| "Verification queue closed".to_string())
    }

    /// Wait for all verification to complete.
    fn finish(mut self) -> Result<(), String> {
        drop(self.tx); // Close channel

        if let Some(worker) = self.worker.take() {
            worker.join().map_err(|_| "Verification worker panicked")?
        } else {
            Ok(())
        }
    }
}

// =============================================================================
// Statistics
// =============================================================================

struct HammerStats {
    fields_processed: u64,
    bytes_processed: u64,
    start_time: Instant,
    time_before_io: Option<SystemTime>,
    time_after_io: Option<SystemTime>,
    list_attempts: u64, // For ITT read mode
}

impl HammerStats {
    fn new() -> Self {
        Self {
            fields_processed: 0,
            bytes_processed: 0,
            start_time: Instant::now(),
            time_before_io: None,
            time_after_io: None,
            list_attempts: 0,
        }
    }

    fn record_io_start(&mut self) {
        if self.time_before_io.is_none() {
            self.time_before_io = Some(SystemTime::now());
        }
    }

    fn record_io_end(&mut self) {
        self.time_after_io = Some(SystemTime::now());
    }

    const fn update(&mut self, bytes: usize) {
        self.fields_processed += 1;
        self.bytes_processed += bytes as u64;
    }

    #[allow(clippy::cast_precision_loss)]
    fn print(&self, mode: &str) {
        let duration = self.start_time.elapsed().as_secs_f64();
        let rate = if duration > 0.0 {
            self.bytes_processed as f64 / duration
        } else {
            0.0
        };

        println!("Fields {}: {}", mode, self.fields_processed);
        println!("Bytes {}: {}", mode, self.bytes_processed);
        println!("Total duration: {duration:.3}");
        println!("GRIB duration: 0.0"); // We don't have GRIB processing
        println!("{} duration: {:.3}", mode.trim_end_matches("ten"), duration);
        println!("Total rate: {rate:.0} bytes/s");
        println!("Total rate: {:.2} MB/s", rate / 1_000_000.0);

        if let Some(before) = self.time_before_io {
            let ts = before
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs_f64();
            println!("Timestamp before first IO: {ts:.6}");
        }

        if let Some(after) = self.time_after_io {
            let ts = after
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs_f64();
            println!("Timestamp after last IO: {ts:.6}");
        }
    }
}

// =============================================================================
// Configuration
// =============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Write,
    Read,
    List,
}

struct HammerConfig {
    // Request base
    expver: String,
    class: String,
    stream: String,
    date: String,
    time: String,
    type_: String,
    levtype: String,

    // Ranges
    steps: Vec<u32>,
    levels: Vec<u32>,
    params: Vec<u32>,
    members: Vec<u32>,

    // Iteration control
    start_at: usize,
    stop_at: usize,

    // Execution
    mode: Mode,
    template_data: Vec<u8>,
    check_type: CheckType,
    check_queue_size: usize,
    randomise_data: bool,
    verbose: bool,

    // ITT mode
    itt: bool,
    step_window: u64,
    random_delay: u32,
    poll_period: u64,
    poll_max_attempts: u32,
    uri_file: Option<PathBuf>,
}

impl HammerConfig {
    fn from_args(args: &Args) -> Result<Self, Box<dyn std::error::Error>> {
        // Validate nparams
        if args.nparams as usize > VALID_PARAMS.len() {
            return Err(format!(
                "nparams ({}) exceeds maximum available parameters ({})",
                args.nparams,
                VALID_PARAMS.len()
            )
            .into());
        }

        // Build ranges
        let steps: Vec<u32> = (args.step..args.step + args.nsteps).collect();

        // Parse levels - either from --levels or --nlevels
        let levels: Vec<u32> = args
            .levels
            .clone()
            .unwrap_or_else(|| (args.level..args.level + args.nlevels).collect());

        let params: Vec<u32> = VALID_PARAMS[..args.nparams as usize].to_vec();
        let members: Vec<u32> = (args.number..args.number + args.nensembles).collect();

        // Validate and set stop_at
        let nlevels = levels.len();
        let nparams = params.len();
        let total_iterations = nlevels * nparams;

        let stop_at = args
            .stop_at
            .unwrap_or_else(|| total_iterations.saturating_sub(1));
        if args.start_at >= total_iterations && total_iterations > 0 {
            return Err("--start-at exceeds level×param range".into());
        }
        if stop_at >= total_iterations && total_iterations > 0 {
            return Err("--stop-at exceeds level×param range".into());
        }
        if stop_at < args.start_at {
            return Err("--stop-at must be >= --start-at".into());
        }

        // Determine mode
        let mode = if args.list {
            Mode::List
        } else if args.read {
            Mode::Read
        } else {
            Mode::Write
        };

        // Load template data
        let template_data = fs::read(&args.grib_path)?;

        Ok(Self {
            expver: args.expver.clone(),
            class: args.class.clone(),
            stream: args.stream.clone(),
            date: args.date.clone(),
            time: args.time.clone(),
            type_: args.type_.clone(),
            levtype: args.levtype.clone(),
            steps,
            levels,
            params,
            members,
            start_at: args.start_at,
            stop_at,
            mode,
            template_data,
            check_type: CheckType::from_args(args),
            check_queue_size: args.check_queue_size,
            randomise_data: !args.no_randomise_data,
            verbose: args.verbose,
            itt: args.itt,
            step_window: args.step_window,
            random_delay: args.random_delay,
            poll_period: args.poll_period,
            poll_max_attempts: args.poll_max_attempts,
            uri_file: args.uri_file.clone(),
        })
    }

    const fn total_fields(&self) -> u64 {
        (self.steps.len() * self.members.len() * self.levels.len() * self.params.len()) as u64
    }
}

// =============================================================================
// Build request string
// =============================================================================

fn build_request(config: &HammerConfig, step: u32, member: u32) -> Request {
    let levels_str = config
        .levels
        .iter()
        .map(std::string::ToString::to_string)
        .collect::<Vec<_>>()
        .join("/");
    let params_str = config
        .params
        .iter()
        .map(std::string::ToString::to_string)
        .collect::<Vec<_>>()
        .join("/");

    Request::new()
        .with("class", &config.class)
        .with("expver", &config.expver)
        .with("stream", &config.stream)
        .with("date", &config.date)
        .with("time", &config.time)
        .with("type", &config.type_)
        .with("levtype", &config.levtype)
        .with("step", &step.to_string())
        .with("levelist", &levels_str)
        .with("param", &params_str)
        .with("number", &member.to_string())
}

// =============================================================================
// Write mode
// =============================================================================

fn run_write(fdb: &Fdb, config: &HammerConfig) -> Result<HammerStats, Box<dyn std::error::Error>> {
    let mut stats = HammerStats::new();
    let mut verifier = Verifier::new(config.check_type);
    let mut rng = rand::rng();

    // Create template GribHandle from bytes
    let template_handle = GribHandle::from_bytes(&config.template_data)?;

    println!(
        "Writing {} fields ({} steps x {} members x {} levels x {} params)",
        config.total_fields(),
        config.steps.len(),
        config.members.len(),
        config.levels.len(),
        config.params.len()
    );

    for &step in &config.steps {
        for &member in &config.members {
            for &level in &config.levels {
                for &param in &config.params {
                    // Clone the template and modify for this field
                    let mut handle = template_handle.try_clone()?;

                    // Set GRIB keys for this field (matching C++ fdb-hammer)
                    handle.set_string("expver", &config.expver)?;
                    handle.set_string("class", &config.class)?;
                    handle.set_long("step", i64::from(step))?;
                    handle.set_long("level", i64::from(level))?;
                    handle.set_long("paramId", i64::from(param))?;
                    handle.set_long("number", i64::from(member))?;

                    // Randomize values if requested
                    if config.randomise_data {
                        let size = handle.get_size("values")?;
                        let random_values: Vec<f64> =
                            (0..size).map(|_| rng.random::<f64>() * 100.0).collect();
                        handle.set_double_array("values", &random_values)?;
                    }

                    // Get data section offsets for verification embedding (like C++ fdb-hammer)
                    #[allow(clippy::cast_sign_loss)]
                    let offset_before_data = handle.get_long("offsetBeforeData")? as usize;
                    #[allow(clippy::cast_sign_loss)]
                    let offset_after_data = handle.get_long("offsetAfterData")? as usize;

                    // Get the GRIB message and embed verification data in data section
                    let mut grib_data = handle.message_copy()?;

                    // Build FDB key for this field
                    let key = Key::new()
                        .with("class", &config.class)
                        .with("expver", &config.expver)
                        .with("stream", &config.stream)
                        .with("date", &config.date)
                        .with("time", &config.time)
                        .with("type", &config.type_)
                        .with("levtype", &config.levtype)
                        .with("step", &step.to_string())
                        .with("levelist", &level.to_string())
                        .with("param", &param.to_string())
                        .with("number", &member.to_string());

                    // Embed verification data inside GRIB data section (matching C++ behavior)
                    verifier.embed_in_message(
                        &key,
                        &mut grib_data,
                        offset_before_data,
                        offset_after_data,
                    );

                    if config.verbose {
                        println!(
                            "Archiving: step={}, member={}, level={}, param={}, size={}",
                            step,
                            member,
                            level,
                            param,
                            grib_data.len()
                        );
                    }

                    stats.record_io_start();
                    fdb.archive(&key, &grib_data)?;
                    stats.record_io_end();
                    stats.update(grib_data.len());
                }
            }

            // Flush per member like C++ version
            fdb.flush()?;
        }
    }

    Ok(stats)
}

// =============================================================================
// Write mode (ITT)
// =============================================================================

fn run_write_itt(
    fdb: &Fdb,
    config: &HammerConfig,
    barrier_config: &barrier::BarrierConfig,
) -> Result<HammerStats, Box<dyn std::error::Error>> {
    let mut stats = HammerStats::new();
    let mut verifier = Verifier::new(config.check_type);
    let mut rng = rand::rng();

    // Create template GribHandle from bytes
    let template_handle = GribHandle::from_bytes(&config.template_data)?;

    println!(
        "Writing {} fields (ITT mode, step_window={}s)",
        config.total_fields(),
        config.step_window
    );

    // Initial barrier before starting
    barrier::barrier(barrier_config)?;

    // Random startup delay within step window
    #[allow(clippy::cast_precision_loss)] // Precision loss acceptable for timing
    if config.random_delay > 0 && config.step_window > 0 {
        let delay_range = config.step_window as f64 * (f64::from(config.random_delay) / 100.0);
        let delay_secs: f64 = rng.random_range(0.0..delay_range);
        thread::sleep(Duration::from_secs_f64(delay_secs));
    }

    let start = Instant::now();
    let mut step_end_due = start;

    for &step in &config.steps {
        for &member in &config.members {
            let mut iter_count = 0usize;
            for &level in &config.levels {
                if iter_count > config.stop_at {
                    break;
                }
                for &param in &config.params {
                    if iter_count > config.stop_at {
                        break;
                    }
                    if iter_count < config.start_at {
                        iter_count += 1;
                        continue;
                    }
                    iter_count += 1;

                    // Clone the template and modify for this field
                    let mut handle = template_handle.try_clone()?;

                    // Set GRIB keys for this field (matching C++ fdb-hammer)
                    handle.set_string("expver", &config.expver)?;
                    handle.set_string("class", &config.class)?;
                    handle.set_long("step", i64::from(step))?;
                    handle.set_long("level", i64::from(level))?;
                    handle.set_long("paramId", i64::from(param))?;
                    handle.set_long("number", i64::from(member))?;

                    // Randomize values if requested
                    if config.randomise_data {
                        let size = handle.get_size("values")?;
                        let random_values: Vec<f64> =
                            (0..size).map(|_| rng.random::<f64>() * 100.0).collect();
                        handle.set_double_array("values", &random_values)?;
                    }

                    // Get data section offsets for verification embedding (like C++ fdb-hammer)
                    #[allow(clippy::cast_sign_loss)]
                    let offset_before_data = handle.get_long("offsetBeforeData")? as usize;
                    #[allow(clippy::cast_sign_loss)]
                    let offset_after_data = handle.get_long("offsetAfterData")? as usize;

                    // Get the GRIB message and embed verification data in data section
                    let mut grib_data = handle.message_copy()?;

                    // Build FDB key for this field
                    let key = Key::new()
                        .with("class", &config.class)
                        .with("expver", &config.expver)
                        .with("stream", &config.stream)
                        .with("date", &config.date)
                        .with("time", &config.time)
                        .with("type", &config.type_)
                        .with("levtype", &config.levtype)
                        .with("step", &step.to_string())
                        .with("levelist", &level.to_string())
                        .with("param", &param.to_string())
                        .with("number", &member.to_string());

                    // Embed verification data inside GRIB data section (matching C++ behavior)
                    verifier.embed_in_message(
                        &key,
                        &mut grib_data,
                        offset_before_data,
                        offset_after_data,
                    );

                    if config.verbose {
                        println!(
                            "Archiving: step={}, member={}, level={}, param={}, size={}",
                            step,
                            member,
                            level,
                            param,
                            grib_data.len()
                        );
                    }

                    stats.record_io_start();
                    fdb.archive(&key, &grib_data)?;
                    stats.record_io_end();
                    stats.update(grib_data.len());
                }
            }

            // Flush per member
            fdb.flush()?;
        }

        // Sleep until step window expires
        if config.step_window > 0 {
            step_end_due += Duration::from_secs(config.step_window);
            let now = Instant::now();
            if now < step_end_due {
                thread::sleep(step_end_due - now);
            } else {
                let exceeded = now - step_end_due;
                eprintln!("Step window exceeded by {:.1}s", exceeded.as_secs_f64());
            }
        }
    }

    Ok(stats)
}

// =============================================================================
// Read mode
// =============================================================================

fn run_read(fdb: &Fdb, config: &HammerConfig) -> Result<HammerStats, Box<dyn std::error::Error>> {
    let mut stats = HammerStats::new();
    let verifier = Verifier::new(config.check_type);

    println!(
        "Reading {} fields ({} steps x {} members x {} levels x {} params)",
        config.total_fields(),
        config.steps.len(),
        config.members.len(),
        config.levels.len(),
        config.params.len()
    );

    for &step in &config.steps {
        for &member in &config.members {
            let request = build_request(config, step, member);

            // First pass: get metadata (count, keys for verification, expected sizes)
            let list_iter = fdb.list(
                &request,
                ListOptions {
                    depth: 3,
                    deduplicate: false,
                },
            )?;
            let list_items: Vec<_> = list_iter.filter_map(std::result::Result::ok).collect();

            if list_items.is_empty() {
                if config.verbose {
                    println!("No fields found for step={step}, member={member}");
                }
                continue;
            }

            let expected_bytes: u64 = list_items.iter().map(|item| item.length).sum();

            if config.verbose {
                println!(
                    "Reading {} fields for step={}, member={} (expecting {} bytes)",
                    list_items.len(),
                    step,
                    member,
                    expected_bytes
                );
            }

            // Second pass: read data using read_from_list (most efficient)
            let list_iter = fdb.list(
                &request,
                ListOptions {
                    depth: 3,
                    deduplicate: false,
                },
            )?;
            stats.record_io_start();
            let mut reader = fdb.read_from_list(list_iter, false)?;
            if config.verbose {
                println!("  Reader size: {} bytes", reader.size());
            }
            let data = reader.read_all()?;
            stats.record_io_end();

            stats.bytes_processed += data.len() as u64;

            // Verify if enabled
            if config.check_type == CheckType::None {
                stats.fields_processed += list_items.len() as u64;
            } else {
                let mut offset = 0usize;
                #[allow(clippy::cast_possible_truncation)]
                for item in &list_items {
                    let field_len = item.length as usize;
                    let field_data = if offset + field_len <= data.len() {
                        &data[offset..offset + field_len]
                    } else {
                        &[]
                    };
                    offset += field_len;

                    let key = Key::from_entries(item.full_key());
                    stats.fields_processed += 1;

                    // Parse GRIB to get data section offsets for verification
                    if let Ok(handle) = GribHandle::from_bytes(field_data) {
                        #[allow(clippy::cast_sign_loss)]
                        if let (Ok(offset_before), Ok(offset_after)) = (
                            handle.get_long("offsetBeforeData"),
                            handle.get_long("offsetAfterData"),
                        ) {
                            if let Err(e) = verifier.verify_from_message(
                                &key,
                                field_data,
                                offset_before as usize,
                                offset_after as usize,
                            ) && config.verbose
                            {
                                eprintln!("Verification error: {e}");
                            }
                        }
                    } else if config.verbose {
                        eprintln!("Failed to parse GRIB for verification");
                    }
                }
            }
        }
    }

    Ok(stats)
}

// =============================================================================
// Read mode (ITT) - with polling
// =============================================================================

fn run_read_itt(
    fdb: &Fdb,
    config: &HammerConfig,
) -> Result<HammerStats, Box<dyn std::error::Error>> {
    let mut stats = HammerStats::new();

    println!(
        "Reading fields (ITT mode, poll_period={}s, max_attempts={})",
        config.poll_period, config.poll_max_attempts
    );

    // Use async verifier if checks enabled
    let async_verifier = if config.check_type == CheckType::None {
        None
    } else {
        Some(AsyncVerifier::new(
            config.check_type,
            config.check_queue_size,
        ))
    };

    for &step in &config.steps {
        for &member in &config.members {
            let request = build_request(config, step, member);

            // Calculate expected count with start_at/stop_at
            let total_fields = config.levels.len() * config.params.len();
            let expected_count = if total_fields > 0 {
                config.stop_at.saturating_sub(config.start_at) + 1
            } else {
                0
            };

            // Poll until all fields available
            let mut attempts = 0u32;
            let list_items = loop {
                let list_iter = fdb.list(
                    &request,
                    ListOptions {
                        depth: 3,
                        deduplicate: false,
                    },
                )?;
                let items: Vec<_> = list_iter.filter_map(std::result::Result::ok).collect();

                stats.list_attempts += 1;

                if items.len() >= expected_count {
                    break items;
                }

                attempts += 1;
                if attempts >= config.poll_max_attempts {
                    return Err(format!(
                        "Polling timeout after {} attempts: expected {} fields, found {}",
                        attempts,
                        expected_count,
                        items.len()
                    )
                    .into());
                }

                if config.verbose {
                    println!(
                        "Polling attempt {}: expected {}, found {}",
                        attempts,
                        expected_count,
                        items.len()
                    );
                }

                thread::sleep(Duration::from_secs(config.poll_period));
            };

            // Read data
            let list_iter = fdb.list(
                &request,
                ListOptions {
                    depth: 3,
                    deduplicate: false,
                },
            )?;
            stats.record_io_start();
            let mut reader = fdb.read_from_list(list_iter, false)?;
            let data = reader.read_all()?;
            stats.record_io_end();

            stats.bytes_processed += data.len() as u64;

            // Queue async verification
            if let Some(ref verifier) = async_verifier {
                let mut offset = 0usize;
                #[allow(clippy::cast_possible_truncation)]
                for item in &list_items {
                    let field_len = item.length as usize;
                    if offset + field_len <= data.len() {
                        let field_data = data[offset..offset + field_len].to_vec();
                        let key = Key::from_entries(item.full_key());
                        verifier.verify_async(key, field_data)?;
                    }
                    offset += field_len;
                    stats.fields_processed += 1;
                }
            } else {
                stats.fields_processed += list_items.len() as u64;
            }
        }
    }

    // Wait for all verification to complete
    if let Some(verifier) = async_verifier {
        verifier.finish()?;
    }

    Ok(stats)
}

// =============================================================================
// Read mode (URI file) - skip listing
// =============================================================================

fn run_read_uri_file(
    fdb: &Fdb,
    config: &HammerConfig,
    uri_file: &std::path::Path,
) -> Result<HammerStats, Box<dyn std::error::Error>> {
    let mut stats = HammerStats::new();

    let contents = fs::read_to_string(uri_file)?;
    let uris: Vec<String> = contents
        .lines()
        .map(std::string::ToString::to_string)
        .collect();

    println!(
        "Reading {} URIs from file: {}",
        uris.len(),
        uri_file.display()
    );

    stats.record_io_start();
    let mut reader = fdb.read_uris(&uris, false)?;
    let data = reader.read_all()?;
    stats.record_io_end();

    stats.fields_processed = uris.len() as u64;
    stats.bytes_processed = data.len() as u64;

    // Verification with async verifier (no key verification since we don't have keys)
    if config.check_type != CheckType::None {
        // In URI file mode, we can only do data integrity check, not key check
        // because we don't have the key metadata from list
        eprintln!(
            "Warning: --md-check/--full-check has limited functionality with --uri-file (no key verification)"
        );
    }

    Ok(stats)
}

// =============================================================================
// List mode
// =============================================================================

fn run_list(fdb: &Fdb, config: &HammerConfig) -> Result<HammerStats, Box<dyn std::error::Error>> {
    let mut stats = HammerStats::new();

    println!(
        "Listing fields ({} steps x {} members)",
        config.steps.len(),
        config.members.len()
    );

    for &step in &config.steps {
        for &member in &config.members {
            let request = build_request(config, step, member);

            stats.record_io_start();
            let list_iter = fdb.list(
                &request,
                ListOptions {
                    depth: 3,
                    deduplicate: false,
                },
            )?;

            for item in list_iter {
                match item {
                    Ok(element) => {
                        stats.fields_processed += 1;
                        stats.bytes_processed += element.length;

                        if config.verbose {
                            println!(
                                "  uri={}, offset={}, length={}",
                                element.uri, element.offset, element.length
                            );
                        }
                    }
                    Err(e) => {
                        eprintln!("List error: {e}");
                    }
                }
            }

            stats.record_io_end();
        }
    }

    Ok(stats)
}

// =============================================================================
// Main
// =============================================================================

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    println!("FDB Hammer (Rust)");
    println!("FDB version: {}", fdb::version());
    println!();

    // Random startup delay (0-10 seconds)
    if args.delay {
        let mut rng = rand::rng();
        let delay = rng.random_range(0..10000);
        thread::sleep(Duration::from_millis(delay));
    }

    // Create FDB handle with optional subtoc configuration
    let fdb = if let Some(config_path) = &args.config {
        let mut config_str = fs::read_to_string(config_path)?;
        if args.disable_subtocs {
            config_str.push_str("\nuseSubToc: false\n");
        }
        Fdb::open(Some(config_str.as_str()), None)?
    } else if args.disable_subtocs {
        // Create config with subtoc disabled
        Fdb::open(Some("useSubToc: false\n"), None)?
    } else {
        Fdb::open_default()?
    };

    println!("FDB handle created: {}", fdb.name());

    // Parse configuration
    let config = HammerConfig::from_args(&args)?;

    // Create barrier configuration
    let barrier_config = barrier::BarrierConfig {
        ppn: args.ppn,
        nodes: args.nodes.clone(),
        port: args.barrier_port,
        max_wait: Duration::from_secs(args.barrier_max_wait),
    };

    println!("Template file: {}", args.grib_path.display());
    println!("Template size: {} bytes", config.template_data.len());
    println!("Mode: {:?}", config.mode);
    println!("Check type: {:?}", config.check_type);
    if config.itt {
        println!("ITT mode: enabled");
        println!("  Step window: {}s", config.step_window);
        println!("  Random delay: {}%", config.random_delay);
        if !args.nodes.is_empty() {
            println!("  Nodes: {}", args.nodes.join(", "));
            println!("  Processes per node: {}", args.ppn);
        }
    }
    println!();

    // Run appropriate mode
    let stats = match (config.mode, config.itt) {
        (Mode::Write, false) => run_write(&fdb, &config)?,
        (Mode::Write, true) => run_write_itt(&fdb, &config, &barrier_config)?,
        (Mode::Read, false) => run_read(&fdb, &config)?,
        (Mode::Read, true) => {
            if let Some(ref uri_file) = config.uri_file {
                run_read_uri_file(&fdb, &config, uri_file)?
            } else {
                run_read_itt(&fdb, &config)?
            }
        }
        (Mode::List, _) => run_list(&fdb, &config)?,
    };

    println!();
    let mode_str = match config.mode {
        Mode::Write => "written",
        Mode::Read => "read",
        Mode::List => "listed",
    };
    stats.print(mode_str);

    // ITT-specific output
    if config.itt && config.mode == Mode::Read && stats.list_attempts > 0 {
        println!("List attempts: {}", stats.list_attempts);
    }

    Ok(())
}
