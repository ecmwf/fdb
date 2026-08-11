//! Build script for fdb-sys
//!
//! Supports two build modes:
//! - `vendored` (default): Clone and build fdb5 from source using ecbuild
//! - `system`: Use `CMake` `find_package` to find system-installed fdb5
//!
//! Both modes build the CXX bridge for C++ to Rust bindings.

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=src/lib.rs");
    println!("cargo:rerun-if-changed=cpp/FdbBridge.h");
    println!("cargo:rerun-if-changed=cpp/Types.h");
    println!("cargo:rerun-if-changed=cpp/Key.h");
    println!("cargo:rerun-if-changed=cpp/Key.cc");
    println!("cargo:rerun-if-changed=cpp/Library.h");
    println!("cargo:rerun-if-changed=cpp/Library.cc");
    println!("cargo:rerun-if-changed=cpp/FdbHandle.h");
    println!("cargo:rerun-if-changed=cpp/FdbHandle.cc");
    println!("cargo:rerun-if-changed=cpp/ListIteratorHandle.h");
    println!("cargo:rerun-if-changed=cpp/ListIteratorHandle.cc");
    println!("cargo:rerun-if-changed=cpp/DumpIteratorHandle.h");
    println!("cargo:rerun-if-changed=cpp/DumpIteratorHandle.cc");
    println!("cargo:rerun-if-changed=cpp/StatusIteratorHandle.h");
    println!("cargo:rerun-if-changed=cpp/StatusIteratorHandle.cc");
    println!("cargo:rerun-if-changed=cpp/WipeIteratorHandle.h");
    println!("cargo:rerun-if-changed=cpp/WipeIteratorHandle.cc");
    println!("cargo:rerun-if-changed=cpp/PurgeIteratorHandle.h");
    println!("cargo:rerun-if-changed=cpp/PurgeIteratorHandle.cc");
    println!("cargo:rerun-if-changed=cpp/StatsIteratorHandle.h");
    println!("cargo:rerun-if-changed=cpp/StatsIteratorHandle.cc");
    println!("cargo:rerun-if-changed=cpp/ControlIteratorHandle.h");
    println!("cargo:rerun-if-changed=cpp/ControlIteratorHandle.cc");
    println!("cargo:rerun-if-changed=cpp/MessageArchiverWrapper.h");
    println!("cargo:rerun-if-changed=cpp/MessageArchiverWrapper.cc");
    println!("cargo:rerun-if-env-changed=FDB_DIR");
    println!("cargo:rerun-if-env-changed=CMAKE_PREFIX_PATH");
    println!("cargo:rerun-if-env-changed=DOCS_RS");

    if bindman_utils::is_docs_rs() {
        return;
    }

    bindman_utils::validate_build_mode(cfg!(feature = "system"), cfg!(feature = "vendored"));

    generate_exceptions();

    if cfg!(feature = "system") {
        build_system();
    } else {
        build_vendored();
    }
}

/// Generate `fdb_exceptions.{h,rs}` for fdb-sys's cxx bridge.
///
/// fdb-sys does not introduce its own exception subclasses (the higher-level
/// `fdb` crate maps `cxx::Exception` directly), so the `own` list is empty
/// and we inherit C++ catch blocks from upstream `-sys` crates (eckit-sys,
/// metkit-sys) via [`bindman_build::collect_dep_exception_sources`].
fn generate_exceptions() {
    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR not set"));
    let inherited = bindman_build::collect_dep_exception_sources();

    bindman_build::generate_exception_bridge(&bindman_build::ExceptionBridgeConfig {
        primary_namespace: "fdb",
        out_dir: &out_dir,
        own: &[],
        inherited: &inherited,
    });
}

/// Build using system-installed fdb5 via `CMake` `find_package`
#[cfg(feature = "system")]
fn build_system() {
    use std::env;
    use std::path::PathBuf;

    let crate_dir =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set"));

    // Get dependency paths from -sys crates
    let eckit_include = env::var("DEP_ECKIT_SYS_INCLUDE")
        .expect("DEP_ECKIT_SYS_INCLUDE not set - eckit-sys must be a dependency");
    let eckit_cpp_dir = env::var("DEP_ECKIT_SYS_CPP_DIR")
        .expect("DEP_ECKIT_SYS_CPP_DIR not set - eckit-sys must be a dependency");
    let metkit_include = env::var("DEP_METKIT_SYS_INCLUDE")
        .expect("DEP_METKIT_SYS_INCLUDE not set - metkit-sys must be a dependency");
    let metkit_cpp_dir = env::var("DEP_METKIT_SYS_CPP_DIR")
        .expect("DEP_METKIT_SYS_CPP_DIR not set - metkit-sys must be a dependency");
    let eccodes_include = env::var("DEP_ECCODES_SYS_INCLUDE")
        .expect("DEP_ECCODES_SYS_INCLUDE not set - eccodes-sys must be a dependency");
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR not set"));

    let (root, fdb_include, lib_dir) = bindman_utils::cmake_find_package("fdb5", "5.10.0");

    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    println!("cargo:rustc-link-lib=dylib=fdb5");

    // Build the CXX bridge
    cxx_build::bridge("src/lib.rs")
        .file(crate_dir.join("cpp/Key.cc"))
        .file(crate_dir.join("cpp/Library.cc"))
        .file(crate_dir.join("cpp/FdbHandle.cc"))
        .file(crate_dir.join("cpp/ListIteratorHandle.cc"))
        .file(crate_dir.join("cpp/DumpIteratorHandle.cc"))
        .file(crate_dir.join("cpp/StatusIteratorHandle.cc"))
        .file(crate_dir.join("cpp/WipeIteratorHandle.cc"))
        .file(crate_dir.join("cpp/PurgeIteratorHandle.cc"))
        .file(crate_dir.join("cpp/StatsIteratorHandle.cc"))
        .file(crate_dir.join("cpp/ControlIteratorHandle.cc"))
        .file(crate_dir.join("cpp/MessageArchiverWrapper.cc"))
        .include(&fdb_include)
        .include(&eckit_include)
        .include(&eckit_cpp_dir) // for EckitBridge.h
        .include(&metkit_include)
        .include(&metkit_cpp_dir) // for MetkitBridge.h
        .include(&eccodes_include)
        .include(crate_dir.join("cpp"))
        .include(&out_dir) // for fdb_exceptions.h (generated)
        .std("c++17")
        .compile("fdb_sys_bridge");

    // Link to eckit and metkit (bridge uses their symbols)
    let eckit_root = env::var("DEP_ECKIT_SYS_ROOT")
        .expect("DEP_ECKIT_SYS_ROOT not set - eckit-sys must be a dependency");
    let metkit_root = env::var("DEP_METKIT_SYS_ROOT")
        .expect("DEP_METKIT_SYS_ROOT not set - metkit-sys must be a dependency");
    let eccodes_root = env::var("DEP_ECCODES_SYS_ROOT")
        .expect("DEP_ECCODES_SYS_ROOT not set - eccodes-sys must be a dependency");

    println!("cargo:rustc-link-search=native={eckit_root}/lib");
    println!("cargo:rustc-link-lib=dylib=eckit");
    println!("cargo:rustc-link-search=native={metkit_root}/lib");
    println!("cargo:rustc-link-lib=dylib=metkit");
    bindman_utils::link_cpp_stdlib();

    // Re-publish each dependency's install lib dir so the downstream
    // `fdb` crate's build script can emit matching absolute rpath
    // entries on the final binary. `rustc-link-arg` emitted by a
    // library crate's build.rs does not reach binaries that link the
    // crate, so the rpath flags have to come from `fdb/build.rs`.
    println!("cargo:system_fdb5_lib={}", lib_dir.display());
    println!("cargo:system_eckit_lib={eckit_root}/lib");
    println!("cargo:system_metkit_lib={metkit_root}/lib");
    println!("cargo:system_eccodes_lib={eccodes_root}/lib");

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

/// Build fdb5 from source using ecbuild
#[cfg(feature = "vendored")]
#[allow(clippy::too_many_lines)]
fn build_vendored() {
    use std::env;
    use std::fs;
    use std::path::PathBuf;
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
    let eckit_root = env::var("DEP_ECKIT_SYS_ROOT")
        .expect("DEP_ECKIT_SYS_ROOT not set - eckit-sys must be a dependency");
    let metkit_root = env::var("DEP_METKIT_SYS_ROOT")
        .expect("DEP_METKIT_SYS_ROOT not set - metkit-sys must be a dependency");
    let eccodes_root = env::var("DEP_ECCODES_SYS_ROOT")
        .expect("DEP_ECCODES_SYS_ROOT not set - eccodes-sys must be a dependency");

    // Clone sources
    let ecbuild_src = bindman_utils::git_clone(ECBUILD_REPO, ECBUILD_TAG, &src_dir.join("ecbuild"));
    let fdb_src = bindman_utils::git_clone(FDB_REPO, FDB_TAG, &src_dir.join("fdb"));

    // Patch CMakeLists.txt to remove tests subdirectory (buggy when ENABLE_TESTS=OFF)
    let cmakelists = fdb_src.join("CMakeLists.txt");
    if let Ok(content) = fs::read_to_string(&cmakelists) {
        let patched = content.replace("add_subdirectory( tests )", "# add_subdirectory( tests )");
        fs::write(&cmakelists, patched).expect("failed to patch CMakeLists.txt");
    }

    let ecbuild_bin = ecbuild_src.join("bin/ecbuild");
    let num_jobs = bindman_utils::build_parallelism();

    let cmake_prefix_path = format!("{eckit_root};{metkit_root};{eccodes_root}");

    // Build fdb
    let mut cmd = Command::new(&ecbuild_bin);
    cmd.current_dir(&build_dir)
        .arg(format!("--prefix={}", install_dir.display()))
        .arg("--")
        .arg(&fdb_src)
        .arg(format!("-DCMAKE_PREFIX_PATH={cmake_prefix_path}"))
        .arg(format!(
            "-DCMAKE_BUILD_TYPE={}",
            bindman_utils::cmake_build_type()
        ))
        // Always disabled (no features)
        .arg("-DENABLE_TESTS=OFF")
        .arg("-DBUILD_TESTING=OFF")
        .arg("-DENABLE_DOCS=OFF")
        .arg("-DENABLE_FDB_DOCUMENTATION=OFF")
        .arg("-DENABLE_BUILD_TOOLS=OFF")
        .arg("-DENABLE_FDB_BUILD_TOOLS=OFF")
        .arg("-DENABLE_PYTHON_ZARR_INTERFACE=OFF");

    // Core features
    cmd.arg(format!(
        "-DENABLE_GRIB={}",
        bindman_utils::on_off(cfg!(feature = "grib"))
    ));
    cmd.arg(format!(
        "-DENABLE_TOCFDB={}",
        bindman_utils::on_off(cfg!(feature = "tocfdb"))
    ));
    cmd.arg(format!(
        "-DENABLE_FDB_REMOTE={}",
        bindman_utils::on_off(cfg!(feature = "fdb-remote"))
    ));

    // Storage backends
    cmd.arg(format!(
        "-DENABLE_RADOSFDB={}",
        bindman_utils::on_off(cfg!(feature = "radosfdb"))
    ));
    cmd.arg(format!(
        "-DENABLE_LUSTRE={}",
        bindman_utils::on_off(cfg!(feature = "lustre"))
    ));
    cmd.arg(format!(
        "-DENABLE_DAOSFDB={}",
        bindman_utils::on_off(cfg!(feature = "daosfdb"))
    ));
    cmd.arg(format!(
        "-DENABLE_DAOS_ADMIN={}",
        bindman_utils::on_off(cfg!(feature = "daos-admin"))
    ));
    cmd.arg(format!(
        "-DENABLE_DUMMY_DAOS={}",
        bindman_utils::on_off(cfg!(feature = "dummy-daos"))
    ));

    // Other
    cmd.arg(format!(
        "-DENABLE_EXPERIMENTAL={}",
        bindman_utils::on_off(cfg!(feature = "experimental"))
    ));
    cmd.arg(format!(
        "-DENABLE_SANDBOX={}",
        bindman_utils::on_off(cfg!(feature = "sandbox"))
    ));

    // Use @rpath install names — the leaf binary sets rpaths via bindman_utils::emit_rpaths()
    #[cfg(target_os = "macos")]
    cmd.arg("-DCMAKE_INSTALL_NAME_DIR=@rpath");

    bindman_utils::run_command(&mut cmd, "ecbuild configure fdb");

    bindman_utils::run_command(
        Command::new("cmake")
            .args(["--build", ".", "--parallel", &num_jobs])
            .current_dir(&build_dir),
        "cmake build fdb",
    );

    bindman_utils::run_command(
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

    let eckit_cpp_dir = env::var("DEP_ECKIT_SYS_CPP_DIR")
        .expect("DEP_ECKIT_SYS_CPP_DIR not set - eckit-sys must be a dependency");
    let metkit_cpp_dir = env::var("DEP_METKIT_SYS_CPP_DIR")
        .expect("DEP_METKIT_SYS_CPP_DIR not set - metkit-sys must be a dependency");

    // Build the CXX bridge
    cxx_build::bridge("src/lib.rs")
        .file(crate_dir.join("cpp/Key.cc"))
        .file(crate_dir.join("cpp/Library.cc"))
        .file(crate_dir.join("cpp/FdbHandle.cc"))
        .file(crate_dir.join("cpp/ListIteratorHandle.cc"))
        .file(crate_dir.join("cpp/DumpIteratorHandle.cc"))
        .file(crate_dir.join("cpp/StatusIteratorHandle.cc"))
        .file(crate_dir.join("cpp/WipeIteratorHandle.cc"))
        .file(crate_dir.join("cpp/PurgeIteratorHandle.cc"))
        .file(crate_dir.join("cpp/StatsIteratorHandle.cc"))
        .file(crate_dir.join("cpp/ControlIteratorHandle.cc"))
        .file(crate_dir.join("cpp/MessageArchiverWrapper.cc"))
        .include(&include_dir)
        .include(&fdb_src_include)
        .include(format!("{eckit_root}/include"))
        .include(&eckit_cpp_dir) // for EckitBridge.h
        .include(format!("{metkit_root}/include"))
        .include(&metkit_cpp_dir) // for MetkitBridge.h
        .include(format!("{eccodes_root}/include"))
        .include(crate_dir.join("cpp"))
        .include(&out_dir) // for fdb_exceptions.h (generated)
        .std("c++17")
        .compile("fdb_sys_bridge");

    // Link against the install directory
    let fdb_lib_dir = bindman_utils::resolve_lib_dir(&install_dir);
    println!("cargo:rustc-link-search=native={}", fdb_lib_dir.display());
    println!("cargo:rustc-link-lib=dylib=fdb5");
    bindman_utils::link_cpp_stdlib();

    // Export for downstream crates (still point to install dir for headers)
    println!("cargo:root={}", install_dir.display());
    println!("cargo:include={}", include_dir.display());

    // Check C++ API
    bindman_build::check_cpp_api(&fdb_src_include, &crate_dir.join("src/lib.rs"));
}

#[cfg(not(feature = "vendored"))]
fn build_vendored() {
    unreachable!("build_vendored called without vendored feature");
}
