fn main() {
    bindman_utils::reexport_dep_root("FDB_SYS");
    // Rpaths for this crate's own test binaries; consumers emit their own.
    bindman_utils::emit_rpaths();
    // System-mode transitive lib dirs re-published by fdb-sys.
    for (key, dir) in std::env::vars() {
        if key.starts_with("DEP_FDB_SYS_SYSTEM_") && key.ends_with("_LIB") {
            println!("cargo:rustc-link-arg=-Wl,-rpath,{dir}");
        }
    }
}
