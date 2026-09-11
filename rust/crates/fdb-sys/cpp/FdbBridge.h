// fdb C++ bridge for Rust FFI — umbrella header pulled in by the
// cxx-generated bridge (`include!("FdbBridge.h")` in lib.rs) and by
// downstream `-sys` crates. Real declarations live in the per-topic headers
// below.
#pragma once

// Note: the auto-generated `rust::behavior::trycatch` lives in
// `fdb_exceptions.h`, which is included by each per-topic `.cc` directly
// (not from this header). Downstream `-sys` crates have their own generated
// `<ns>_exceptions.h` and must not see fdb's transitively through here, or
// they would have two `trycatch` specializations in one translation unit.

#include "ControlIteratorHandle.h"
#include "DumpIteratorHandle.h"
#include "FdbHandle.h"
#include "Key.h"
#include "Library.h"
#include "ListIteratorHandle.h"
#include "MessageArchiverWrapper.h"
#include "PurgeIteratorHandle.h"
#include "StatsIteratorHandle.h"
#include "StatusIteratorHandle.h"
#include "Types.h"
#include "WipeIteratorHandle.h"
