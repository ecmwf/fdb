# Performance Benchmark Jobs

For data available in prod fdb the workflow is:

1. Create zarr replica (choose between lz4/zstd)


## Quickstart

Once the [prerequisites](#prerequisites) are in place, the full campaign is
five `sbatch` calls in order:

```bash
sbatch jobs/export_lz4.sh
sbatch jobs/export_zstd.sh
sbatch jobs/benchmark-z3fdb.sh
sbatch jobs/benchmark-zarr-lz4.sh
sbatch jobs/benchmark-zarr-zstd.sh
```

The benchmark jobs depend on the export jobs having completed. There is no
automation that enforces this — submit in order, or use `--dependency=afterok`
manually.

After the campaign completes, verify:

1. Both `$SCRATCH/zarr-store-era5-{lz4,zstd}-5/` exist and are non-empty.
2. The Sites dashboard shows ~100 uploaded logs per benchmark category.
3. `analysis/analyze_runtime.ipynb` runs end-to-end against the uploaded data.

## What each job category does

| Category | Scripts | What it measures |
|---|---|---|
| **Export** | `export_lz4.sh`, `export_zstd.sh` | FDB → Zarr conversion time and resulting store size, per codec |
| **Benchmark** | `benchmark-z3fdb.sh`, `benchmark-zarr-lz4.sh`, `benchmark-zarr-zstd.sh` | Random-access throughput as a distribution across 100 parallel tasks |
| **Profile** | `profile-z3fdb.sh` | Flamegraphs via `perf record` |
| **Compare** | `compare.sh` | Element-wise correctness between z3fdb view and exported Zarr |

The 100-task array benchmarks exist to measure *distribution*, not single-run
time. The analysis notebooks in `analysis/` compute percentiles across the
100 runs. Treat single-task timings as noise; only the aggregated distribution
is meaningful.

---

## Prerequisites

The job scripts depend on several things that are **not** in this repository.
A colleague taking this over will need to recreate or obtain each:

1. **Environment shell snippet** at `${HOME}/workspace/environment-local-build`.
   **Critical:** without this file's contents, no job will run. Sourced by every
   job script. Sets up build paths, library locations, and any module-equivalents
   for the local z3fdb build. Get the contents from the previous owner before
   handover ends, not after.

2. **Python venv** at `${HOME}/python-venvs/bench/`. Must contain the deps
   from `requirements.txt` plus a working z3fdb installation. Activate with
   `source ${HOME}/python-venvs/bench/bin/activate`.

3. **FDB data store** at `$SCRATCH/fdb-store/` with a valid `fdb_config.yaml`.
   The job scripts assume this is pre-staged. No script in this repo creates
   it — initial staging happens out-of-band.

4. **ECMWF Sites API token** at `~/.sites-token`. Used by `utils.sh` to upload
   logs and flamegraphs. Without this, uploads fail silently after each job.

5. **Sites API URL path**. `utils.sh` hardcodes the upload destination to
   `sites.ecmwf.int/ecm7593/z3fdb/...`. The `ecm7593` segment is a user ID —
   edit `utils.sh` to point to your own Sites space, or the uploads will land
   in the wrong place (or fail with auth errors).

The `z3fdb-profiler` tool itself is expected at
`${HOME}/workspace/z3fdb/fdb/tools/z3fdb-profiler/`. Adjust paths if your
clone lives elsewhere.

## Job script reference

### `export_lz4.sh` and `export_zstd.sh`

Single long-running jobs (48h walltime). Run `z3fdb-profiler export` against
`era5.yaml` to produce a Zarr v3 store under `$SCRATCH/zarr-store-era5-<codec>-5`.
Both use Blosc compression level 5.

**Destructive:** both scripts `rm -rf` the destination before exporting.

### `benchmark-z3fdb.sh`

Array job, tasks 0–99. Each task runs `z3fdb-profiler aggregate` against the
live FDB view with `--limit 10000`. Per-task logs are written and then
uploaded to the Sites API.

### `benchmark-zarr-lz4.sh`, `benchmark-zarr-zstd.sh`

Same arguments as `benchmark-z3fdb.sh` but the `aggregate` runs against the
pre-exported Zarr stores. **Will fail** if the corresponding `export_*` job
has not completed.

### `profile-z3fdb.sh`

Single job. Runs `z3fdb-profiler aggregate` under `perf record -F 500
--call-graph dwarf,16384`, then folds stacks into forward and reverse SVG
flamegraphs and uploads them to the Sites API.

### `compare.sh`

Single job. Runs `z3fdb-profiler compare` between a live view and an exported
Zarr. Long-running (48h walltime) because `compare` is O(N) element-wise.

### `utils.sh`

Shared bash library, not a job. Sourced by other scripts for the
`upload_files` function. Talks to `https://sites.ecmwf.int/...` using a
bearer token from `$SITES_TOKEN_FILE` (default: `~/.sites-token`). Uses
`curl` + `jq` for HTTP and URL encoding.

## Analysis

`analysis/analyze_runtime.ipynb` reads back the uploaded logs and computes
timing distributions across the 100 array tasks. `demo.ipynb` has interactive
exploration. `Untitled.ipynb` is exploratory and not load-bearing.

## Download helper

`download/download-era-data.py` fetches a small ERA5 test subset via
`earthkit-data`. Useful for setting up a test FDB store on a workstation, not
required on ATOS where production data is already in FDB.

---

## Known issues and gotchas

These are real problems you'll hit either when taking over or running the
campaign. Fix or work around before handover.

### Bugs

- **`compare.sh:23`**: `rm -rf ${dest}` references an undefined variable.
  With `set -ex` (not `-eu`), `${dest}` expands to empty and `rm -rf ""`
  does nothing — silent no-op. Probably meant to be `${zarr_path}` or similar.
- **`profile-z3fdb.sh:49`**: `upload_files $(date ...) **/*.svg` — the `**`
  glob requires `shopt -s globstar` to recurse, and the argument shape doesn't
  match `upload_files`. Flamegraphs may not get uploaded.
- **`export_lz4_single_param_chunk.sh`**: byte-identical to `export_lz4.sh`.
  Either rename and modify, or delete.
- **Job name typos**: `benchmar-z3fdb` (missing 'k'), `benchmark-zarr-lz`
  (truncated). Cosmetic but visible in `squeue` output.

### Operational gotchas

- **Implicit job ordering.** No `--dependency=afterok` flags. Submit exports
  before benchmarks or benchmarks will fail with "store not found" errors.
- **`$SCRATCH` is auto-cleaned on ATOS.** Re-staging the FDB store and
  re-running exports is required after long gaps between campaigns.
- **Sites API uploads can fail silently.** `upload_files` returns non-zero
  on failure but the script doesn't check. Lost uploads are invisible unless
  you watch the Sites dashboard.
- **The 48-hour walltime on exports is a generous guess, not a measured value.**
  A re-tuned campaign should measure typical export time and set a tighter limit.
- **`--qos=np` is ATOS-specific.** On a different SLURM cluster the QoS name
  will differ and the script will fail at submission.
- **The view YAML (`era5.yaml`) is hardcoded.** To benchmark a different
  view, edit the script. Parameterizing this would be a one-line change.

### Hardcoded user paths

Every job script contains paths that assume the original author's account
layout. Audit and change these for a new user:

- `${HOME}/workspace/environment-local-build` (sourced)
- `${HOME}/workspace/z3fdb/fdb/tools/z3fdb-profiler/...` (tool location)
- `${HOME}/python-venvs/bench/bin/activate` (venv)
- `utils.sh`: `sites.ecmwf.int/ecm7593/z3fdb/...` (upload destination)

## Recommended cleanups before production handover

In priority order:

1. **Fix `compare.sh:23` undefined variable** — 5 minutes.
2. **Delete or differentiate `export_lz4_single_param_chunk.sh`** — confusing
   leftover.
3. **Parameterize the Sites API URL in `utils.sh`** via env var (e.g.,
   `SITES_BASE_URL`). Then each user can set it for their account.
4. **Add an orchestration script** `run-campaign.sh` that submits all jobs
   with `--dependency=afterok` chains. Removes the implicit ordering trap.
5. **Move hardcoded paths to a single `config.sh`** sourced by every job
   script. Single point of change instead of editing every `.sh`.
6. **Add `--mail-user`** to SBATCH directives (or document setting it via
   `~/.forward`).
7. **Run `shellcheck` over all scripts** — would catch the undefined variable
   in `compare.sh` immediately.
