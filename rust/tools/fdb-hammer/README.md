# fdb-hammer

Benchmark and stress test tool for FDB (Fields Database). Rust port of ECMWF's C++ fdb-hammer.

## Overview

fdb-hammer writes, reads, and lists meteorological fields in FDB to measure I/O performance. It supports:

- **Write mode**: Archive fields with configurable data sizes
- **Read mode**: Retrieve and optionally verify archived fields
- **List mode**: Enumerate fields matching a request
- **ITT mode**: Instrumented Test Timing for distributed benchmarks with synchronized timing windows

## Building

```bash
# From workspace root
cargo build -p fdb-hammer --release

# With system FDB (instead of vendored)
cargo build -p fdb-hammer --release --no-default-features --features system
```

## Running

Binaries work out of the box on both macOS and Linux — no
`LD_LIBRARY_PATH` / `DYLD_LIBRARY_PATH` setup needed. The build script
stamps a binary-relative RPATH so the dynamic linker finds the
vendored libraries automatically:

```bash
cd target/release
./fdb-hammer --help
```

## Quick Start with Test Config

A test configuration is included in `test_config/`. Run commands from that directory:

```bash
cd fdb/tools/fdb-hammer/test_config
```

### Write Test (150 fields)

```bash
cargo run -p fdb-hammer --release -- \
  ../../../crates/fdb/tests/fixtures/template.grib \
  --config ./config.yaml \
  --expver test --class od \
  --nsteps 10 --nlevels 5 --nparams 3
```

### Read Test with Verification

```bash
cargo run -p fdb-hammer --release -- \
  ../../../crates/fdb/tests/fixtures/template.grib \
  --config ./config.yaml \
  --expver test --class od \
  --nsteps 10 --nlevels 5 --nparams 3 \
  --read --md-check
```

### List Fields

```bash
cargo run -p fdb-hammer --release -- \
  ../../../crates/fdb/tests/fixtures/template.grib \
  --config ./config.yaml \
  --expver test --class od \
  --nsteps 10 --nlevels 5 --nparams 3 \
  --list
```

### Verbose Write

```bash
cargo run -p fdb-hammer --release -- \
  ../../../crates/fdb/tests/fixtures/template.grib \
  --config ./config.yaml \
  --expver test --class od \
  --nsteps 3 --nlevels 2 --nparams 2 \
  --verbose
```

### Clean Up

```bash
rm -rf ./root/*
```

## Usage

```bash
fdb-hammer [OPTIONS] <GRIB_PATH>
```

The `GRIB_PATH` argument specifies a template file whose size determines field data size.

### Basic Examples

```bash
# Write 10 steps × 5 levels × 3 params = 150 fields
fdb-hammer template.grib \
  --config fdb-config.yaml \
  --expver test --class od \
  --nsteps 10 --nlevels 5 --nparams 3

# Read back with MD5 verification
fdb-hammer template.grib \
  --config fdb-config.yaml \
  --expver test --class od \
  --nsteps 10 --nlevels 5 --nparams 3 \
  --read --md-check

# List fields
fdb-hammer template.grib \
  --config fdb-config.yaml \
  --expver test --class od \
  --nsteps 10 --nlevels 5 --nparams 3 \
  --list
```

### ITT Mode (Distributed Benchmarking)

ITT mode enables synchronized benchmarking across multiple nodes:

```bash
# Writer on node1 - waits for all nodes, then writes with 10s step windows
fdb-hammer template.grib \
  --config fdb-config.yaml \
  --expver test --class od \
  --nsteps 10 --nlevels 5 --nparams 3 \
  --itt --step-window 10 \
  --nodes node1,node2,node3

# Reader on node2 - polls until data available
fdb-hammer template.grib \
  --config fdb-config.yaml \
  --expver test --class od \
  --nsteps 10 --nlevels 5 --nparams 3 \
  --itt --read \
  --nodes node1,node2,node3
```

## CLI Options

### Request Parameters

| Option | Description | Default |
|--------|-------------|---------|
| `--expver <str>` | Experiment version | **required** |
| `--class <str>` | MARS class | **required** |
| `--stream <str>` | Stream | `oper` |
| `--date <str>` | Date (YYYYMMDD) | `20240101` |
| `--time <str>` | Time (HHMM) | `0000` |
| `--type <str>` | Type | `fc` |
| `--levtype <str>` | Level type | `sfc` |

### Workload Size

| Option | Description | Default |
|--------|-------------|---------|
| `--nsteps <n>` | Number of steps | **required** |
| `--nlevels <n>` | Number of levels | **required** (unless `--levels`) |
| `--levels <n,n,...>` | Explicit level list | - |
| `--nparams <n>` | Number of parameters | **required** |
| `--nensembles <n>` | Number of ensemble members | `1` |

### Starting Values

| Option | Description | Default |
|--------|-------------|---------|
| `--step <n>` | First step number | `0` |
| `--level <n>` | First level number | `0` |
| `--number <n>` | First ensemble member | `1` |

### Iteration Control

| Option | Description | Default |
|--------|-------------|---------|
| `--start-at <n>` | Start index in level×param space | `0` |
| `--stop-at <n>` | Stop index in level×param space | max |

### Mode Selection

| Option | Description |
|--------|-------------|
| (default) | Write mode |
| `--read` | Read mode |
| `--list` | List mode |

### Verification

| Option | Description | Default |
|--------|-------------|---------|
| `--md-check` | Embed key MD5 digest at data boundaries | - |
| `--full-check` | Embed full data checksum | - |
| `--check-queue-size <n>` | Async verification queue size | `10` |
| `--no-randomise-data` | Don't randomize field data | - |

### ITT Mode

| Option | Description | Default |
|--------|-------------|---------|
| `--itt` | Enable ITT mode | - |
| `--step-window <secs>` | Seconds per step (write) | `10` |
| `--random-delay <pct>` | Random startup delay percentage | `100` |
| `--poll-period <secs>` | Polling interval (read) | `1` |
| `--poll-max-attempts <n>` | Max polling attempts (read) | `200` |
| `--uri-file <path>` | Read from pre-computed URI file | - |

### Multi-Node Barriers

| Option | Description | Default |
|--------|-------------|---------|
| `--nodes <h1,h2,...>` | Comma-separated node hostnames | - |
| `--ppn <n>` | Processes per node | `1` |
| `--barrier-port <port>` | TCP port for inter-node barriers | `7777` |
| `--barrier-max-wait <s>` | Barrier timeout seconds | `10` |

### Other

| Option | Description | Default |
|--------|-------------|---------|
| `--config <path>` | FDB config YAML file | - |
| `--disable-subtocs` | Disable subtoc usage | - |
| `--delay` | Random startup delay (0-10s) | - |
| `--verbose` | Verbose output | - |

## Barrier Synchronization

### Inter-Node (TCP)

When `--nodes` is specified, processes synchronize via TCP:
1. First node in list is the leader
2. Leader listens on `--barrier-port`
3. Other nodes connect and wait for "END" signal
4. All proceed together

### Intra-Node (FIFO)

When `--ppn > 1`, processes on the same node synchronize via FIFOs:
1. First process to create PID file becomes leader
2. Leader creates FIFOs in `/var/run/user/$UID/`
3. Followers signal readiness via wait FIFO
4. Leader performs inter-node barrier, then releases followers

## Output

```
FDB Hammer (Rust)
FDB version: 5.13.2

Template file: template.grib
Template size: 2076000 bytes
Mode: Write
Check type: MdCheck

Writing 150 fields...

Fields written: 150
Bytes written: 311.4 MB
Throughput: 7.9 MB/s
Duration: 39.4s
```

## Differences from C++ Version

| Feature | Rust | C++ |
|---------|------|-----|
| GRIB manipulation | Raw bytes | eccodes library |
| Template metadata extraction | CLI args required | From GRIB file |
| Data randomization | Random bytes | Random GRIB values |
| Verification offsets | Data boundaries | GRIB data section |

For FDB I/O benchmarking, both versions produce equivalent results.
