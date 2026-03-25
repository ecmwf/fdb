//! Build script for fdb crate.
//!
//! Emits RPATH linker flags so binaries can find dynamic libraries
//! at runtime without setting LD_LIBRARY_PATH/DYLD_LIBRARY_PATH.

fn main() {
    println!("cargo:rerun-if-changed=build.rs");

    // Emit RPATH flags for portable binaries
    // These apply to binaries, tests, and examples that depend on fdb

    #[cfg(target_os = "linux")]
    {
        // $ORIGIN = directory containing the executable
        println!("cargo:rustc-link-arg=-Wl,-rpath,$ORIGIN/fdb_libs");
        println!("cargo:rustc-link-arg=-Wl,-rpath,$ORIGIN/eccodes_libs");
        println!("cargo:rustc-link-arg=-Wl,-rpath,$ORIGIN");
        eprintln!("fdb build.rs: Emitting Linux RPATH flags");
    }

    #[cfg(target_os = "macos")]
    {
        // @executable_path = directory containing the executable
        println!("cargo:rustc-link-arg=-Wl,-rpath,@executable_path/fdb_libs");
        println!("cargo:rustc-link-arg=-Wl,-rpath,@executable_path/eccodes_libs");
        println!("cargo:rustc-link-arg=-Wl,-rpath,@executable_path");
        eprintln!("fdb build.rs: Emitting macOS RPATH flags");
    }
}
