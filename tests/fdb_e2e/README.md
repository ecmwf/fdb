# fdb_e2e — End-to-End Test Suite

This directory contains the end-to-end (e2e) test suite for FDB. It tests the
FDB command-line tools and the `pyfdb` Python API against a real FDB
installation, exercising write, read, list, purge, wipe, hide, overlay, and
other operations across multiple FDB configuration
strategies and subtoc variants.

## Prerequisites

The FDB executables (`fdb-write`, `fdb-read`, `fdb-list`, etc.) must be
accessible via `PATH`, and the shared libraries (`libfdb5.so`, `libeckit.so`,
etc.) must be loadable by `pyfdb` via
[findlibs](https://github.com/ecmwf/findlibs).

`findlibs` locates shared libraries by searching standard paths and the
`<LIB>_DIR` family of environment variables. If your FDB installation is in a
non-standard location, set the appropriate variables before running the tests:

```sh
export ECCODES_HOME=/path/to/eccodes/install
export ECKIT_HOME=/path/to/eckit/install
export FDB5_HOME=/path/to/fdb/install
```

## Installation

```sh
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Running the Tests

Point `PATH` to the directory containing the FDB executables and run pytest:

```sh
PATH=/path/to/fdb/install/bin:$PATH pytest tests/fdb_e2e/
```
or

```sh
pytest tests/fdb_e2e/ --env PATH=/path/to/fdb/install/bin
```

To run only the tool tests (shell script based):

```sh
PATH=/path/to/fdb/install/bin:$PATH pytest tests/fdb_e2e/tool_tests/
```

To run only the Python API tests:

```sh
pytest tests/fdb_e2e/python_api/
```

### Forwarding Extra Environment Variables

Use the repeatable `--env KEY=VALUE` argument to inject additional environment
variables into all shell scripts. This is useful for overriding library search
paths or other FDB configuration at runtime without modifying the test
fixtures:

```sh
pytest tests/fdb_e2e/ --env LD_LIBRARY_PATH=/path/to/fdb/install/lib
```

Multiple `--env` flags can be combined. Variables passed this way override any
values already set by the fixtures.

## Markers

Markers allow running a specific subset of tests. They are defined at two
levels: the FDB **configuration strategy** and the **operation** being tested.

### Configuration Strategy Markers

| Marker            | Description                                                                  |
|-------------------|------------------------------------------------------------------------------|
| `simple`          | YAML config provided inline (no separate config file)                        |
| `files`           | Config loaded from `etc/fdb/` directory structure                            |
| `yaml`            | Explicit YAML config file path via `FDB_CONFIG`                              |
| `json`            | Explicit JSON config file path via `FDB_CONFIG`                              |
| `yaml_mars_disks` | YAML config using the MARS disk layout convention                            |
| `yaml_tools`      | YAML config intended for FDB tool usage patterns                             |
| `json_tools`      | JSON config intended for FDB tool usage patterns                             |

### Operation Markers

| Marker     | Description                                                          |
|------------|----------------------------------------------------------------------|
| `info`     | `fdb-info` / version information                                     |
| `hide`     | `fdb-hide` to suppress entries from listing and retrieval            |
| `grib2fdb` | `grib2fdb5` ingestion tool                                           |
| `list`     | `fdb-list` enumeration of archived fields                            |
| `purge`    | `fdb-purge` removal of hidden entries                                |
| `read`     | `fdb-read` / retrieval of archived fields                            |
| `root`     | `fdb-root` root management                                           |
| `wipe`     | `fdb-wipe` deletion of FDB subtrees                                  |
| `write`    | `fdb-write` archival with include/exclude filters                    |
| `overlay`  | `fdb-overlay` layering of FDB spaces                                 |

### Examples

Run only the `list` operation tests:

```sh
pytest tests/fdb_e2e/ -m list
```

Run only tests using the `simple` config strategy:

```sh
pytest tests/fdb_e2e/ -m simple
```

Combine markers (simple config AND list operation):

```sh
pytest tests/fdb_e2e/ -m "simple and list"
```

Run everything except the `overlay` tests:

```sh
pytest tests/fdb_e2e/ -m "not overlay"
```

## Technical Insights

### Dynamic Shell Script Parametrization

The tool tests in `tool_tests/` do not have a fixed test matrix. Instead,
`pytest_generate_tests` in `tool_tests/conftest.py` scans the
`no_subtocs/<case>/` and `subtocs/<case>/` directories at collection time and
turns every `.sh` file it finds into a separate test parameter. Adding a new
shell script to any of these directories automatically registers it as a test
case — no Python changes required.

### FDB Configuration Strategies

The top-level `conftest.py` provides 13 distinct FDB setup fixtures covering two axes:

- **Config format**: `simple` (inline YAML string), `files` (`etc/fdb/`
                     directory), `config_yaml` (explicit YAML file), `config_json` (explicit JSON
                     file), `config_yaml_mars_disks`, `config_yaml_tools`, `config_json_tools`.
- **Subtoc variant**: with and without subtocs (subtocs split FDB indices into
                      per-expver sub-TOC files).

Each test in `test_fdb_no_subtocs.py` and `test_fdb_subtocs.py` is parametrized
over all relevant setup+env fixture pairs, so a single test function covers all
configuration strategies automatically.

### Test Data Preparation

Tests do not use static pre-archived FDB stores. Instead, each test function
creates GRIB files on the fly from template files in `data/` using
`Tool.modify_metadata_source_file()`, which patches specific GRIB metadata keys
(class, expver, type, step, date, etc.) into a copy of the template. This makes
test data deterministic and independent of pre-existing FDB state.

The `function_tmp` fixture creates a fresh temporary directory per test
function, ensuring complete isolation between test runs.

### Shell Script Execution

`tool_tests/util/run_sh.py` runs shell scripts via `subprocess.Popen` with the
test-managed environment. Stdout and stderr are merged, streamed line-by-line,
and forwarded to the Python logging system (level `INFO`). On non-zero exit the
full output is logged at `ERROR` level and a `CalledProcessError` is raised,
failing the test with a clear diagnostic.

### Known Expected Failures

`overlay/wipe.sh` is marked `xfail` in both `no_subtocs/` and `subtocs/`
variants due to issue **FDB-652**. Once the issue is resolved, the `xfail`
marks and the separate overlay handling in `tool_tests/conftest.py` can be
removed and `overlay` added to the standard case list.

### Python API Tests

`python_api/list/test_list.py` exercises `pyfdb.FDB` directly (no shell scripts). It tests:
- **Counting**: exact field counts after archiving N files.
- **Masking**: `include_masked=True` returns both visible and hidden entries; default hides 
               superseded data.
- **Location metadata**: `ListElement.uri`, `offset()`, and `length()` are populated and non-empty.
- **Range requests**: date lists, param lists, levelist ranges, and relative
                      dates (e.g. `"-1"` for yesterday) are all supported in list queries.
                      Non-existent keys in the selection silently return 0 matches without errors.

### Environment Variable Forwarding

The `--env KEY=VALUE` command-line argument (defined via `pytest_addoption` in
`conftest.py`) collects extra variables into the session-scoped `cli_env`
fixture. All 13 `*_env_*` fixtures accept `cli_env` and apply
`env.update(cli_env)` as their last step, so CLI-supplied values take
precedence over fixture defaults. This makes it straightforward to point the
test suite at a different FDB installation without modifying any fixture code.
