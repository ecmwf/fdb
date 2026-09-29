---
name: "FDB Development"
description: "Use when modifying FDB sources, CMake configuration, FDB tests, Python or Rust bindings, or validating FDB changes in the ecmwf bundle. Covers linux (and macOS) builds, CTest, clang-format, and clang-tidy."
applyTo: "src/**, tests/**, python/**, rust/**"
---

# Development

## Project Boundaries

- The `fdb` project provides the FDB library, command-line tools, and optional Python and Rust bindings.
- The project depends on other libraries, such as `eckit`, `eccodes`, and `metkit`.
- Keep an FDB change within `src/` unless the dependency contract or bundle integration genuinely needs a corresponding change elsewhere.

## Configure And Build

- Use an out-of-source CMake build. Preserve an existing build tree's generator and preset; do not configure over it with a different generator.
- Enable optional FDB interfaces during configuration only when the change requires them: `-DENABLE_PYTHON_FDB_INTERFACE=ON` and `-DENABLE_PYTHON_ZARR_INTERFACE=ON`. Zarr enables the PyFDB interface as a dependency.

## Tests

- FDB tests are registered through `src/tests/`: core and tool tests are under `src/tests/fdb/`, regressions under `src/tests/regressions/`, and optional Python/Zarr coverage under `src/tests/pyfdb/`, `src/tests/z3fdb/`, and `src/tests/pychunked_data_view/`.
- Start with the most specific affected test. Use CTest against the configured bundle, for example:

  ```sh
  ctest --test-dir ./build/bundle -R 'fdb_test_api' --output-on-failure
  ctest --test-dir ./build/bundle -L remotefdb --output-on-failure
  ```

- Use `ctest --test-dir ./build/bundle -N` to find test names and `ctest --test-dir ./build/bundle --output-on-failure` for the broader suite. Some tests are deliberately absent when GRIB, tools, or Python interfaces are disabled; distinguish that configuration from a test failure.

## Validation

- Run the focused CTest selection after the affected target builds. Broaden to the relevant regression, label, or full suite when the change crosses an API, storage backend, or bundle dependency boundary.
- Format C and C++ changes with `src/.clang-format`. The FDB CI applies clang-format and ignores `third_party/`.
- The root presets generate `compile_commands.json`. After a successful build, run `src/run-clang-tidy ./build/bundle` for C++ changes when `clang-tidy`, `jq`, and GNU Parallel are available.

# Code Reviews

- Respond to review comments promptly. If you disagree with a comment, explain your reasoning and provide an alternative solution. If you accept a comment, make the change and mark it as resolved.

- When performing a code review:
  - Check that the code adheres to the project's coding standards and guidelines.
  - Check for potential bugs, memory safety issues, security vulnerabilities, and performance issues.
  - Check that the code has clear comments and explanations where necessary.
  - Check that the code is tested, and that tests cover edge cases and failure scenarios.
