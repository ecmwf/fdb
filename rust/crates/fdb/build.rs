//! Build script for fdb crate.
//!
//! Emits RPATH linker flags so binaries can find dynamic libraries
//! at runtime without setting `LD_LIBRARY_PATH`/`DYLD_LIBRARY_PATH`.

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    bindman_utils::emit_rpath_flags(&["fdb_libs", "eccodes_libs"]);
}
