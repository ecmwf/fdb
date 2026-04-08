//! Build script for fdb-sys
//!
//! Supports two build modes:
//! - `vendored` (default): Clone and build fdb5 from source using ecbuild
//! - `system`: Use `CMake` `find_package` to find system-installed fdb5
//!
//! Both modes build the CXX bridge for C++ to Rust bindings.

use std::env;
use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=src/lib.rs");
    println!("cargo:rerun-if-changed=cpp/fdb_bridge.h");
    println!("cargo:rerun-if-changed=cpp/fdb_bridge.cpp");
    println!("cargo:rerun-if-env-changed=FDB_DIR");
    println!("cargo:rerun-if-env-changed=CMAKE_PREFIX_PATH");
    println!("cargo:rerun-if-env-changed=DOCS_RS");

    // Skip build for docs.rs (rustdoc only needs Rust metadata, not C++ linkage)
    // The #[cxx::bridge] macro generates Rust types from the bridge definition itself
    if std::env::var_os("DOCS_RS").is_some() {
        return;
    }

    // Validate mutually exclusive features
    let use_system = cfg!(feature = "system");
    let use_vendored = cfg!(feature = "vendored");

    assert!(
        !(use_system && use_vendored),
        "Features `system` and `vendored` are mutually exclusive. \
         Please enable only one."
    );
    assert!(
        use_system || use_vendored,
        "Either `system` or `vendored` feature must be enabled. \
         Default should be `vendored`."
    );

    if use_system {
        build_system();
    } else {
        build_vendored();
    }
}

/// Use `CMake` `find_package` to locate a library and return (`root`, `include_dir`, `lib_dir`)
#[cfg(feature = "system")]
#[allow(clippy::too_many_lines)]
fn cmake_find_package(
    package: &str,
    version: &str,
    env_override: Option<&str>,
) -> (PathBuf, PathBuf, PathBuf) {
    use std::io::Write;
    use std::path::Path;
    use std::process::Command;

    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR not set"));

    // Check for manual override via environment variable
    if let Some(env_var) = env_override
        && let Ok(dir) = env::var(env_var)
    {
        let prefix = PathBuf::from(&dir);
        let lib_dir = if prefix.join("lib64").exists() {
            prefix.join("lib64")
        } else {
            prefix.join("lib")
        };
        return (prefix.clone(), prefix.join("include"), lib_dir);
    }

    // Create a CMake script to find the package
    let cmake_script = format!(
        r#"
cmake_minimum_required(VERSION 3.12)
project(find_{package} NONE)
find_package({package} {version} REQUIRED)
get_target_property(_include {package} INTERFACE_INCLUDE_DIRECTORIES)
get_target_property(_location {package} LOCATION)
if(_location)
    get_filename_component(_lib_dir "${{_location}}" DIRECTORY)
else()
    set(_lib_dir "${{CMAKE_PREFIX_PATH}}/lib")
endif()
message(STATUS "FOUND_ROOT=${{{package}_BASE_DIR}}")
message(STATUS "FOUND_INCLUDE=${{_include}}")
message(STATUS "FOUND_LIBDIR=${{_lib_dir}}")
"#
    );

    let cmake_dir = out_dir.join(format!("cmake_find_{}", package.to_lowercase()));
    std::fs::create_dir_all(&cmake_dir).expect("Failed to create cmake directory");

    let cmakelists = cmake_dir.join("CMakeLists.txt");
    let mut file = std::fs::File::create(&cmakelists).expect("Failed to create CMakeLists.txt");
    file.write_all(cmake_script.as_bytes())
        .expect("Failed to write CMakeLists.txt");

    let build_dir = cmake_dir.join("build");
    std::fs::create_dir_all(&build_dir).expect("Failed to create build directory");

    // Build CMAKE_PREFIX_PATH from environment
    let mut cmake_prefix = env::var("CMAKE_PREFIX_PATH").unwrap_or_default();
    if let Some(env_var) = env_override
        && let Ok(dir) = env::var(env_var)
    {
        if !cmake_prefix.is_empty() {
            cmake_prefix.push(';');
        }
        cmake_prefix.push_str(&dir);
    }

    let mut cmd = Command::new("cmake");
    cmd.current_dir(&build_dir).arg(&cmake_dir);

    if !cmake_prefix.is_empty() {
        cmd.arg(format!("-DCMAKE_PREFIX_PATH={cmake_prefix}"));
    }

    let output = cmd.output().unwrap_or_else(|e| {
        panic!(
            r"
================================================================================
Failed to run CMake to find {package}
================================================================================

Error: {e}

To fix this, try one of:

1. Install {package} development package:
   - Debian/Ubuntu: apt install lib{package_lower}-dev
   - From source: https://github.com/ecmwf/{package_lower}

2. Set CMAKE_PREFIX_PATH to the installation directory:
   export CMAKE_PREFIX_PATH=/path/to/{package_lower}:$CMAKE_PREFIX_PATH

3. Set {env_var} environment variable:
   export {env_var}=/path/to/{package_lower}

4. Use vendored build (builds from source):
   cargo build --no-default-features --features vendored
",
            package = package,
            package_lower = package.to_lowercase(),
            env_var = env_override.unwrap_or(&format!("{}_DIR", package.to_uppercase())),
            e = e
        )
    });

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        r"
================================================================================
CMake failed to find {package}
================================================================================

{stderr}

To fix this, try one of:

1. Install {package} development package:
   - Debian/Ubuntu: apt install lib{package_lower}-dev
   - From source: https://github.com/ecmwf/{package_lower}

2. Set CMAKE_PREFIX_PATH to the installation directory:
   export CMAKE_PREFIX_PATH=/path/to/{package_lower}:$CMAKE_PREFIX_PATH

3. Set {env_var} environment variable:
   export {env_var}=/path/to/{package_lower}

4. Use vendored build (builds from source):
   cargo build --no-default-features --features vendored
",
        package = package,
        package_lower = package.to_lowercase(),
        env_var = env_override.unwrap_or(&format!("{}_DIR", package.to_uppercase())),
        stderr = stderr
    );

    // Parse output (CMake message(STATUS ...) writes to stdout)
    let mut root = None;
    let mut include = None;
    let mut lib_dir = None;

    for line in stdout.lines() {
        if let Some(path) = line.strip_prefix("-- FOUND_ROOT=") {
            root = Some(PathBuf::from(path));
        } else if let Some(path) = line.strip_prefix("-- FOUND_INCLUDE=") {
            include = Some(PathBuf::from(path));
        } else if let Some(path) = line.strip_prefix("-- FOUND_LIBDIR=") {
            lib_dir = Some(PathBuf::from(path));
        }
    }

    let root = root.unwrap_or_else(|| {
        include
            .as_ref()
            .and_then(|p| p.parent())
            .map_or_else(|| PathBuf::from("/usr"), Path::to_path_buf)
    });
    let include = include.unwrap_or_else(|| root.join("include"));
    let lib_dir = lib_dir.unwrap_or_else(|| {
        if root.join("lib64").exists() {
            root.join("lib64")
        } else {
            root.join("lib")
        }
    });

    (root, include, lib_dir)
}

/// Build using system-installed fdb5 via `CMake` `find_package`
#[cfg(feature = "system")]
fn build_system() {
    let crate_dir =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set"));

    // Get dependency paths from -sys crates
    let eckit_include = env::var("DEP_ECKIT_INCLUDE")
        .expect("DEP_ECKIT_INCLUDE not set - eckit-sys must be a dependency");
    let metkit_include = env::var("DEP_METKIT_INCLUDE")
        .expect("DEP_METKIT_INCLUDE not set - metkit-sys must be a dependency");
    let eccodes_include = env::var("DEP_ECCODES_INCLUDE")
        .expect("DEP_ECCODES_INCLUDE not set - eccodes-sys must be a dependency");

    let (root, fdb_include, lib_dir) = cmake_find_package("fdb5", "5.10.0", Some("FDB_DIR"));

    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    println!("cargo:rustc-link-lib=dylib=fdb5");

    // Build the CXX bridge
    cxx_build::bridge("src/lib.rs")
        .file(crate_dir.join("cpp/fdb_bridge.cpp"))
        .include(&fdb_include)
        .include(&eckit_include)
        .include(&metkit_include)
        .include(&eccodes_include)
        .include(crate_dir.join("cpp"))
        .flag_if_supported("-std=c++17")
        .compile("fdb_sys_bridge");

    // Link to eckit and metkit (bridge uses their symbols)
    let eckit_root = env::var("DEP_ECKIT_ROOT")
        .expect("DEP_ECKIT_ROOT not set - eckit-sys must be a dependency");
    let metkit_root = env::var("DEP_METKIT_ROOT")
        .expect("DEP_METKIT_ROOT not set - metkit-sys must be a dependency");

    println!("cargo:rustc-link-search=native={eckit_root}/lib");
    println!("cargo:rustc-link-lib=dylib=eckit");
    println!("cargo:rustc-link-search=native={metkit_root}/lib");
    println!("cargo:rustc-link-lib=dylib=metkit");

    #[cfg(target_os = "linux")]
    println!("cargo:rustc-link-lib=dylib=stdc++");
    #[cfg(target_os = "macos")]
    println!("cargo:rustc-link-lib=dylib=c++");

    // Export for downstream crates
    println!("cargo:root={}", root.display());
    println!("cargo:include={}", fdb_include.display());

    // Check C++ API
    bindman_build::check_cpp_api(&fdb_include, &crate_dir.join("src/lib.rs"));
}

#[cfg(not(feature = "system"))]
fn build_system() {
    unreachable!("build_system called without system feature");
}

// Helper functions for vendored build (at module level to satisfy clippy)
#[cfg(feature = "vendored")]
const fn on_off(enabled: bool) -> &'static str {
    if enabled { "ON" } else { "OFF" }
}

#[cfg(feature = "vendored")]
fn git_clone(repo: &str, tag: &str, dest: &std::path::Path) -> PathBuf {
    use std::process::Command;

    if dest.exists() {
        return dest.to_path_buf();
    }

    eprintln!("Cloning {repo} @ {tag}...");

    run_command(
        Command::new("git").args([
            "clone",
            "--depth",
            "1",
            "--branch",
            tag,
            repo,
            dest.to_str().expect("Invalid path"),
        ]),
        &format!("git clone {repo}"),
    );

    dest.to_path_buf()
}

#[cfg(feature = "vendored")]
fn run_command(cmd: &mut std::process::Command, desc: &str) {
    eprintln!("Running: {cmd:?}");
    let status = cmd
        .status()
        .unwrap_or_else(|e| panic!("Failed to run {desc}: {e}"));
    assert!(status.success(), "{desc} failed with status: {status}");
}

#[cfg(feature = "vendored")]
fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(std::num::NonZero::get)
        .unwrap_or(4)
}

/// Build fdb5 from source using ecbuild
#[cfg(feature = "vendored")]
#[allow(clippy::too_many_lines)]
fn build_vendored() {
    use std::fs;
    use std::process::Command;

    const ECBUILD_REPO: &str = "https://github.com/ecmwf/ecbuild.git";
    const ECBUILD_TAG: &str = "3.13.1";

    const FDB_REPO: &str = "https://github.com/ecmwf/fdb.git";
    const FDB_TAG: &str = "5.19.1";

    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR not set"));
    let src_dir = out_dir.join("src");
    let build_dir = out_dir.join("build");
    let install_dir = out_dir.join("install");

    fs::create_dir_all(&src_dir).expect("Failed to create src directory");
    fs::create_dir_all(&build_dir).expect("Failed to create build directory");

    // Get dependency paths from -sys crates
    let eckit_root = env::var("DEP_ECKIT_ROOT")
        .expect("DEP_ECKIT_ROOT not set - eckit-sys must be a dependency");
    let metkit_root = env::var("DEP_METKIT_ROOT")
        .expect("DEP_METKIT_ROOT not set - metkit-sys must be a dependency");
    let eccodes_root = env::var("DEP_ECCODES_ROOT")
        .expect("DEP_ECCODES_ROOT not set - eccodes-sys must be a dependency");

    // Clone sources
    let ecbuild_src = git_clone(ECBUILD_REPO, ECBUILD_TAG, &src_dir.join("ecbuild"));
    let fdb_src = git_clone(FDB_REPO, FDB_TAG, &src_dir.join("fdb"));

    // Patch CMakeLists.txt to remove tests subdirectory (buggy when ENABLE_TESTS=OFF)
    let cmakelists = fdb_src.join("CMakeLists.txt");
    if let Ok(content) = fs::read_to_string(&cmakelists) {
        let patched = content.replace("add_subdirectory( tests )", "# add_subdirectory( tests )");
        fs::write(&cmakelists, patched).expect("failed to patch CMakeLists.txt");
    }

    let ecbuild_bin = ecbuild_src.join("bin/ecbuild");
    let num_jobs = env::var("NUM_JOBS").unwrap_or_else(|_| num_cpus().to_string());

    let cmake_prefix_path = format!("{eckit_root};{metkit_root};{eccodes_root}");

    // Build fdb
    let mut cmd = Command::new(&ecbuild_bin);
    cmd.current_dir(&build_dir)
        .arg(format!("--prefix={}", install_dir.display()))
        .arg("--")
        .arg(&fdb_src)
        .arg(format!("-DCMAKE_PREFIX_PATH={cmake_prefix_path}"))
        .arg("-DCMAKE_BUILD_TYPE=Release")
        // Always disabled (no features)
        .arg("-DENABLE_TESTS=OFF")
        .arg("-DBUILD_TESTING=OFF")
        .arg("-DENABLE_DOCS=OFF")
        .arg("-DENABLE_FDB_DOCUMENTATION=OFF")
        .arg("-DENABLE_BUILD_TOOLS=OFF")
        .arg("-DENABLE_FDB_BUILD_TOOLS=OFF")
        .arg("-DENABLE_PYTHON_ZARR_INTERFACE=OFF");

    // Core features
    cmd.arg(format!("-DENABLE_GRIB={}", on_off(cfg!(feature = "grib"))));
    cmd.arg(format!(
        "-DENABLE_TOCFDB={}",
        on_off(cfg!(feature = "tocfdb"))
    ));
    cmd.arg(format!(
        "-DENABLE_FDB_REMOTE={}",
        on_off(cfg!(feature = "fdb-remote"))
    ));

    // Storage backends
    cmd.arg(format!(
        "-DENABLE_RADOSFDB={}",
        on_off(cfg!(feature = "radosfdb"))
    ));
    cmd.arg(format!(
        "-DENABLE_LUSTRE={}",
        on_off(cfg!(feature = "lustre"))
    ));
    cmd.arg(format!(
        "-DENABLE_DAOSFDB={}",
        on_off(cfg!(feature = "daosfdb"))
    ));
    cmd.arg(format!(
        "-DENABLE_DAOS_ADMIN={}",
        on_off(cfg!(feature = "daos-admin"))
    ));
    cmd.arg(format!(
        "-DENABLE_DUMMY_DAOS={}",
        on_off(cfg!(feature = "dummy-daos"))
    ));

    // Other
    cmd.arg(format!(
        "-DENABLE_EXPERIMENTAL={}",
        on_off(cfg!(feature = "experimental"))
    ));
    cmd.arg(format!(
        "-DENABLE_SANDBOX={}",
        on_off(cfg!(feature = "sandbox"))
    ));

    // Portable install names for dynamic libraries
    // On macOS: Use @executable_path directly in install name so binaries find libs
    // without needing RPATH entries. This works because @executable_path resolves
    // at runtime to wherever the main executable is located.
    #[cfg(target_os = "macos")]
    cmd.arg("-DCMAKE_INSTALL_NAME_DIR=@executable_path/fdb_libs");

    // On Linux: Set RPATH to $ORIGIN so libraries can find each other.
    // Note: The final binary still needs its own RPATH - see emit_rpath_flags().
    #[cfg(target_os = "linux")]
    {
        cmd.arg("-DCMAKE_INSTALL_RPATH=$ORIGIN:$ORIGIN/../fdb_libs");
        cmd.arg("-DCMAKE_BUILD_WITH_INSTALL_RPATH=ON");
    }

    run_command(&mut cmd, "ecbuild configure fdb");

    run_command(
        Command::new("cmake")
            .args(["--build", ".", "--parallel", &num_jobs])
            .current_dir(&build_dir),
        "cmake build fdb",
    );

    run_command(
        Command::new("cmake")
            .args(["--install", "."])
            .current_dir(&build_dir),
        "cmake install fdb",
    );

    let include_dir = install_dir.join("include");
    let crate_dir =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set"));

    // FDB source directory contains private headers that may be needed
    let fdb_src_include = fdb_src.join("src");

    // IMPORTANT: Copy resources FIRST, then link against the copied location.
    // This ensures the link search path matches where libs will be at runtime.
    let libs_dest = copy_resources_to_output(&install_dir, &eckit_root, &metkit_root);

    // Build the CXX bridge
    cxx_build::bridge("src/lib.rs")
        .file(crate_dir.join("cpp/fdb_bridge.cpp"))
        .include(&include_dir)
        .include(&fdb_src_include)
        .include(format!("{eckit_root}/include"))
        .include(format!("{metkit_root}/include"))
        .include(format!("{eccodes_root}/include"))
        .include(crate_dir.join("cpp"))
        .flag_if_supported("-std=c++17")
        .compile("fdb_sys_bridge");

    // Link against the copied location in target directory
    println!("cargo:rustc-link-search=native={}", libs_dest.display());
    println!("cargo:rustc-link-lib=dylib=fdb5");
    println!("cargo:rustc-link-lib=dylib=eckit");
    println!("cargo:rustc-link-lib=dylib=metkit");

    #[cfg(target_os = "linux")]
    println!("cargo:rustc-link-lib=dylib=stdc++");
    #[cfg(target_os = "macos")]
    println!("cargo:rustc-link-lib=dylib=c++");

    // Export for downstream crates (still point to install dir for headers)
    println!("cargo:root={}", install_dir.display());
    println!("cargo:include={}", include_dir.display());

    // Emit RPATH flags for runtime library discovery
    emit_rpath_flags();

    // Check C++ API
    bindman_build::check_cpp_api(&fdb_src_include, &crate_dir.join("src/lib.rs"));
}

#[cfg(not(feature = "vendored"))]
fn build_vendored() {
    unreachable!("build_vendored called without vendored feature");
}

/// Emit RPATH linker flags for portable binaries
#[cfg(feature = "vendored")]
fn emit_rpath_flags() {
    // Relative rpath pointing to libs directory next to binary
    #[cfg(target_os = "linux")]
    {
        println!("cargo:rustc-link-arg=-Wl,-rpath,$ORIGIN/fdb_libs");
        println!("cargo:rustc-link-arg=-Wl,-rpath,$ORIGIN");
    }

    #[cfg(target_os = "macos")]
    {
        println!("cargo:rustc-link-arg=-Wl,-rpath,@executable_path/fdb_libs");
        println!("cargo:rustc-link-arg=-Wl,-rpath,@executable_path");
    }
}

/// Copy libraries to target directory for portable binaries.
/// Returns the path to the libs directory where libraries were copied.
/// This MUST be called BEFORE `emit_link_directives` so we link against the copied location.
#[cfg(feature = "vendored")]
fn copy_resources_to_output(
    fdb_install_dir: &std::path::Path,
    eckit_root: &str,
    metkit_root: &str,
) -> PathBuf {
    use std::path::Path;

    let out_dir = env::var("OUT_DIR").expect("OUT_DIR not set");
    // Navigate from OUT_DIR to target/<profile>/
    // OUT_DIR is typically: target/<profile>/build/<crate>-<hash>/out
    let target_dir = Path::new(&out_dir)
        .ancestors()
        .nth(3)
        .expect("Could not determine target directory for resource copying");

    // Copy dynamic libraries to target directory FIRST (before linking)
    let libs_dest = target_dir.join("fdb_libs");
    std::fs::create_dir_all(&libs_dest).expect("Failed to create fdb_libs directory");

    // Helper to copy library files from a directory
    let copy_libs = |lib_dir: &Path, name: &str| {
        if !lib_dir.exists() {
            return;
        }

        for entry in std::fs::read_dir(lib_dir).into_iter().flatten().flatten() {
            let path = entry.path();
            let file_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");

            // Match .so, .dylib, and versioned .so.X files
            let is_shared_lib = std::path::Path::new(file_name)
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("dylib"))
                || file_name.contains(".so")
                || path.extension().is_some_and(|ext| ext == "so");

            if is_shared_lib {
                let dest = libs_dest.join(file_name);
                if let Err(e) = std::fs::copy(&path, &dest) {
                    eprintln!("Warning: Failed to copy {}: {e}", path.display());
                }
            }
        }
        eprintln!("Copied {name} libraries to {}", libs_dest.display());
    };

    // Get library directories
    let fdb_lib_dir = if fdb_install_dir.join("lib64").exists() {
        fdb_install_dir.join("lib64")
    } else {
        fdb_install_dir.join("lib")
    };

    let eckit_lib_dir = Path::new(eckit_root).join("lib");
    let metkit_lib_dir = Path::new(metkit_root).join("lib");

    // Copy all libraries
    copy_libs(&fdb_lib_dir, "fdb5");
    copy_libs(&eckit_lib_dir, "eckit");
    copy_libs(&metkit_lib_dir, "metkit");

    // Export resource directory name for runtime discovery
    println!("cargo:rustc-env=FDB_LIBS_DIR=fdb_libs");

    libs_dest.clone()
}
