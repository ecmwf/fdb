// fdb C++ bridge — forward declarations for the cxx-shared data structs.
//
// All structs are defined on the Rust side via the cxx bridge in `lib.rs`;
// this header just exposes them to the C++ wrapper code that consumes them.
#pragma once

namespace fdb_bridge {

//----------------------------------------------------------------------------------------------------------------------

struct KeyValue;
struct KeyData;
struct ListElementData;
struct CompactListingData;
struct AxisEntry;
struct FdbStatsData;
struct DumpElementData;
struct StatusElementData;
struct WipeElementData;
struct PurgeElementData;
struct IndexStatsData;
struct DbStatsData;
struct StatsElementData;
struct ControlElementData;

//----------------------------------------------------------------------------------------------------------------------

// Forward declarations for Rust-side opaque boxes; defined in `lib.rs`.
struct FlushCallbackBox;
struct ArchiveCallbackBox;
struct ReaderBox;

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb_bridge
