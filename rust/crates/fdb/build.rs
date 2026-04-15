//! Build script for fdb crate.
//!
//! Emits RPATH linker flags so binaries can find dynamic libraries
//! at runtime without setting `LD_LIBRARY_PATH`/`DYLD_LIBRARY_PATH`.
//!
//! Two layouts are supported:
//!
//! - **Vendored** (default): dynamic libs are copied into
//!   `fdb_libs/` and `eccodes_libs/` subdirectories next to the
//!   final binary. The rpath entries are binary-relative
//!   (`@executable_path/fdb_libs` on macOS, `$ORIGIN/fdb_libs` on
//!   Linux), so the binary is portable as long as the user ships
//!   those two directories alongside it.
//!
//! - **System**: libraries live wherever `find_package` resolved
//!   them (e.g. `/usr/lib`, `/opt/.../lib`, or a custom prefix).
//!   `fdb-sys`'s build script re-publishes each dependency's lib dir
//!   via `cargo:system_*_lib` metadata keys, and we emit an
//!   absolute rpath entry for each one so the binary still loads
//!   without `LD_LIBRARY_PATH` / `DYLD_LIBRARY_PATH`.
fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    bindman_utils::emit_rpath_flags(&["fdb_libs", "eccodes_libs"]);

    // When fdb-sys is in system mode, it re-publishes each
    // dependency's install lib dir so we can stamp matching
    // absolute rpath entries onto the final binary. The vendored
    // build leaves these unset, so this block is a no-op there.
    for key in [
        "DEP_FDB_SYS_SYSTEM_FDB5_LIB",
        "DEP_FDB_SYS_SYSTEM_ECKIT_LIB",
        "DEP_FDB_SYS_SYSTEM_METKIT_LIB",
        "DEP_FDB_SYS_SYSTEM_ECCODES_LIB",
    ] {
        println!("cargo:rerun-if-env-changed={key}");
        if let Ok(lib_dir) = std::env::var(key) {
            println!("cargo:rustc-link-arg=-Wl,-rpath,{lib_dir}");
        }
    }
}
