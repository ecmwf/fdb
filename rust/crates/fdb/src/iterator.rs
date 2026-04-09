//! FDB iterator wrappers.

use fdb_sys::UniquePtr;

use crate::error::Result;

// =============================================================================
// Helper to convert KeyValue vectors
// =============================================================================

fn key_values_to_vec(kv: Vec<fdb_sys::KeyValue>) -> Vec<(String, String)> {
    kv.into_iter().map(|kv| (kv.key, kv.value)).collect()
}

// =============================================================================
// ListIterator
// =============================================================================

/// An iterator over FDB list results.
pub struct ListIterator {
    handle: UniquePtr<fdb_sys::ListIteratorHandle>,
    exhausted: bool,
}

impl ListIterator {
    /// Create a new iterator from a cxx handle.
    pub(crate) const fn new(handle: UniquePtr<fdb_sys::ListIteratorHandle>) -> Self {
        Self {
            handle,
            exhausted: false,
        }
    }

    /// Access the underlying iterator handle (for `read_list_iterator`).
    pub(crate) fn inner_mut(&mut self) -> std::pin::Pin<&mut fdb_sys::ListIteratorHandle> {
        self.handle.pin_mut()
    }
}

impl Iterator for ListIterator {
    type Item = Result<ListElement>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }
        match self.handle.pin_mut().hasNext() {
            Ok(false) => {
                self.exhausted = true;
                return None;
            }
            Err(e) => {
                self.exhausted = true;
                return Some(Err(e.into()));
            }
            Ok(true) => {}
        }

        match self.handle.pin_mut().next() {
            Ok(data) => Some(Ok(ListElement::from_cxx(data))),
            Err(e) => {
                self.exhausted = true;
                Some(Err(e.into()))
            }
        }
    }
}

// SAFETY: ListIterator can be sent to another thread because:
// 1. The C++ fdb5::ListIterator contains a snapshot of index data taken at construction
// 2. It does not hold references back to the FDB handle after creation
// 3. Access is exclusive via &mut self (Pin<&mut> in the FFI layer)
// 4. The iterator has no thread-local state or thread-affine resources
#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl Send for ListIterator {}

/// A list element returned by the iterator.
///
/// Contains location information and metadata keys at different levels.
#[derive(Debug, Clone)]
pub struct ListElement {
    /// URI of the resource containing this element.
    pub uri: String,
    /// Byte offset within the resource.
    pub offset: u64,
    /// Length in bytes of the element data.
    pub length: u64,
    /// Timestamp (Unix epoch seconds).
    pub timestamp: i64,
    /// Database-level key entries.
    pub db_key: Vec<(String, String)>,
    /// Index-level key entries.
    pub index_key: Vec<(String, String)>,
    /// Datum-level key entries.
    pub datum_key: Vec<(String, String)>,
}

impl ListElement {
    /// Create from the cxx list element data.
    fn from_cxx(data: fdb_sys::ListElementData) -> Self {
        Self {
            uri: data.uri,
            offset: data.offset,
            length: data.length,
            timestamp: data.timestamp,
            db_key: key_values_to_vec(data.db_key),
            index_key: key_values_to_vec(data.index_key),
            datum_key: key_values_to_vec(data.datum_key),
        }
    }

    /// Get the full key as a combined map of all levels.
    #[must_use]
    pub fn full_key(&self) -> Vec<(String, String)> {
        let mut key =
            Vec::with_capacity(self.db_key.len() + self.index_key.len() + self.datum_key.len());
        key.extend(self.db_key.iter().cloned());
        key.extend(self.index_key.iter().cloned());
        key.extend(self.datum_key.iter().cloned());
        key
    }
}

// =============================================================================
// DumpIterator
// =============================================================================

/// An iterator over FDB dump results.
pub struct DumpIterator {
    handle: UniquePtr<fdb_sys::DumpIteratorHandle>,
    exhausted: bool,
}

impl DumpIterator {
    /// Create a new iterator from a cxx handle.
    pub(crate) const fn new(handle: UniquePtr<fdb_sys::DumpIteratorHandle>) -> Self {
        Self {
            handle,
            exhausted: false,
        }
    }
}

impl Iterator for DumpIterator {
    type Item = Result<DumpElement>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }
        match self.handle.pin_mut().hasNext() {
            Ok(false) => {
                self.exhausted = true;
                return None;
            }
            Err(e) => {
                self.exhausted = true;
                return Some(Err(e.into()));
            }
            Ok(true) => {}
        }

        match self.handle.pin_mut().next() {
            Ok(data) => Some(Ok(DumpElement {
                content: data.content,
            })),
            Err(e) => {
                self.exhausted = true;
                Some(Err(e.into()))
            }
        }
    }
}

// SAFETY: DumpIterator can be sent to another thread because:
// 1. The C++ fdb5::DumpIterator contains a snapshot of dump data taken at construction
// 2. It does not hold references back to the FDB handle after creation
// 3. Access is exclusive via &mut self (Pin<&mut> in the FFI layer)
// 4. The iterator has no thread-local state or thread-affine resources
#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl Send for DumpIterator {}

/// A dump element containing database structure information.
#[derive(Debug, Clone)]
pub struct DumpElement {
    /// String representation of the dump element.
    pub content: String,
}

// =============================================================================
// StatusIterator
// =============================================================================

/// An iterator over FDB status results.
pub struct StatusIterator {
    handle: UniquePtr<fdb_sys::StatusIteratorHandle>,
    exhausted: bool,
}

impl StatusIterator {
    /// Create a new iterator from a cxx handle.
    pub(crate) const fn new(handle: UniquePtr<fdb_sys::StatusIteratorHandle>) -> Self {
        Self {
            handle,
            exhausted: false,
        }
    }
}

impl Iterator for StatusIterator {
    type Item = Result<StatusElement>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }
        match self.handle.pin_mut().hasNext() {
            Ok(false) => {
                self.exhausted = true;
                return None;
            }
            Err(e) => {
                self.exhausted = true;
                return Some(Err(e.into()));
            }
            Ok(true) => {}
        }

        match self.handle.pin_mut().next() {
            Ok(data) => Some(Ok(StatusElement {
                location: data.location,
                status: key_values_to_vec(data.status),
            })),
            Err(e) => {
                self.exhausted = true;
                Some(Err(e.into()))
            }
        }
    }
}

// SAFETY: StatusIterator can be sent to another thread because:
// 1. The C++ fdb5::StatusIterator contains a snapshot of status data taken at construction
// 2. It does not hold references back to the FDB handle after creation
// 3. Access is exclusive via &mut self (Pin<&mut> in the FFI layer)
// 4. The iterator has no thread-local state or thread-affine resources
#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl Send for StatusIterator {}

/// A status element containing database location and status information.
#[derive(Debug, Clone)]
pub struct StatusElement {
    /// Path/location of the database.
    pub location: String,
    /// Status information as key-value pairs.
    pub status: Vec<(String, String)>,
}

// =============================================================================
// WipeIterator
// =============================================================================

/// An iterator over FDB wipe results.
pub struct WipeIterator {
    handle: UniquePtr<fdb_sys::WipeIteratorHandle>,
    exhausted: bool,
}

impl WipeIterator {
    /// Create a new iterator from a cxx handle.
    pub(crate) const fn new(handle: UniquePtr<fdb_sys::WipeIteratorHandle>) -> Self {
        Self {
            handle,
            exhausted: false,
        }
    }
}

impl Iterator for WipeIterator {
    type Item = Result<WipeElement>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }
        match self.handle.pin_mut().hasNext() {
            Ok(false) => {
                self.exhausted = true;
                return None;
            }
            Err(e) => {
                self.exhausted = true;
                return Some(Err(e.into()));
            }
            Ok(true) => {}
        }

        match self.handle.pin_mut().next() {
            Ok(data) => Some(Ok(WipeElement {
                content: data.content,
            })),
            Err(e) => {
                self.exhausted = true;
                Some(Err(e.into()))
            }
        }
    }
}

// SAFETY: WipeIterator can be sent to another thread because:
// 1. The C++ fdb5::WipeIterator contains a snapshot of wipe data taken at construction
// 2. It does not hold references back to the FDB handle after creation
// 3. Access is exclusive via &mut self (Pin<&mut> in the FFI layer)
// 4. The iterator has no thread-local state or thread-affine resources
#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl Send for WipeIterator {}

/// A wipe element describing data that was or would be wiped.
#[derive(Debug, Clone)]
pub struct WipeElement {
    /// String representation of the wiped element.
    pub content: String,
}

// =============================================================================
// PurgeIterator
// =============================================================================

/// An iterator over FDB purge results.
pub struct PurgeIterator {
    handle: UniquePtr<fdb_sys::PurgeIteratorHandle>,
    exhausted: bool,
}

impl PurgeIterator {
    /// Create a new iterator from a cxx handle.
    pub(crate) const fn new(handle: UniquePtr<fdb_sys::PurgeIteratorHandle>) -> Self {
        Self {
            handle,
            exhausted: false,
        }
    }
}

impl Iterator for PurgeIterator {
    type Item = Result<PurgeElement>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }
        match self.handle.pin_mut().hasNext() {
            Ok(false) => {
                self.exhausted = true;
                return None;
            }
            Err(e) => {
                self.exhausted = true;
                return Some(Err(e.into()));
            }
            Ok(true) => {}
        }

        match self.handle.pin_mut().next() {
            Ok(data) => Some(Ok(PurgeElement {
                content: data.content,
            })),
            Err(e) => {
                self.exhausted = true;
                Some(Err(e.into()))
            }
        }
    }
}

// SAFETY: PurgeIterator can be sent to another thread because:
// 1. The C++ fdb5::PurgeIterator contains a snapshot of purge data taken at construction
// 2. It does not hold references back to the FDB handle after creation
// 3. Access is exclusive via &mut self (Pin<&mut> in the FFI layer)
// 4. The iterator has no thread-local state or thread-affine resources
#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl Send for PurgeIterator {}

/// A purge element describing data that was or would be purged.
#[derive(Debug, Clone)]
pub struct PurgeElement {
    /// String representation of the purged element.
    pub content: String,
}

// =============================================================================
// StatsIterator
// =============================================================================

/// An iterator over FDB stats results.
pub struct StatsIterator {
    handle: UniquePtr<fdb_sys::StatsIteratorHandle>,
    exhausted: bool,
}

impl StatsIterator {
    /// Create a new iterator from a cxx handle.
    pub(crate) const fn new(handle: UniquePtr<fdb_sys::StatsIteratorHandle>) -> Self {
        Self {
            handle,
            exhausted: false,
        }
    }
}

impl Iterator for StatsIterator {
    type Item = Result<StatsElement>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }
        match self.handle.pin_mut().hasNext() {
            Ok(false) => {
                self.exhausted = true;
                return None;
            }
            Err(e) => {
                self.exhausted = true;
                return Some(Err(e.into()));
            }
            Ok(true) => {}
        }

        match self.handle.pin_mut().next() {
            Ok(data) => Some(Ok(StatsElement {
                index_statistics: IndexStats {
                    fields_count: data.index_statistics.fields_count,
                    fields_size: data.index_statistics.fields_size,
                    duplicates_count: data.index_statistics.duplicates_count,
                    duplicates_size: data.index_statistics.duplicates_size,
                    report: data.index_statistics.report,
                },
                db_statistics: DbStats {
                    report: data.db_statistics.report,
                },
            })),
            Err(e) => {
                self.exhausted = true;
                Some(Err(e.into()))
            }
        }
    }
}

// SAFETY: StatsIterator can be sent to another thread because:
// 1. The C++ fdb5::StatsIterator contains a snapshot of stats data taken at construction
// 2. It does not hold references back to the FDB handle after creation
// 3. Access is exclusive via &mut self (Pin<&mut> in the FFI layer)
// 4. The iterator has no thread-local state or thread-affine resources
#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl Send for StatsIterator {}

/// Index-level statistics — mirrors `fdb5::IndexStats`.
///
/// Bundles the four numeric accessors upstream exposes
/// (`fieldsCount` / `fieldsSize` / `duplicatesCount` / `duplicatesSize`)
/// plus the captured `report()` text.
#[derive(Debug, Clone)]
pub struct IndexStats {
    /// Number of fields covered by this index.
    pub fields_count: u64,
    /// Total size in bytes of those fields.
    pub fields_size: u64,
    /// Number of duplicate (masked) entries.
    pub duplicates_count: u64,
    /// Total size in bytes of the duplicate entries.
    pub duplicates_size: u64,
    /// Captured `fdb5::IndexStats::report()` output — the same text
    /// `fdb-stats --details` prints for the index portion.
    pub report: String,
}

/// Database-level statistics — mirrors `fdb5::DbStats`.
///
/// Upstream's `DbStats` is fully content-opaque; the only public
/// readable accessor is `report(std::ostream&)`. The captured report
/// text is therefore the only thing this binding can surface — same
/// rule the C++ tools play by.
#[derive(Debug, Clone)]
pub struct DbStats {
    /// Captured `fdb5::DbStats::report()` output — the same text
    /// `fdb-stats --details` prints for the database portion.
    pub report: String,
}

/// A stats element — mirrors `fdb5::StatsElement`.
#[derive(Debug, Clone)]
pub struct StatsElement {
    /// Index-level statistics for this database.
    pub index_statistics: IndexStats,
    /// Database-level statistics for this database.
    pub db_statistics: DbStats,
}

// =============================================================================
// ControlIterator
// =============================================================================

/// An iterator over FDB control results.
pub struct ControlIterator {
    handle: UniquePtr<fdb_sys::ControlIteratorHandle>,
    exhausted: bool,
}

impl ControlIterator {
    /// Create a new iterator from a cxx handle.
    pub(crate) const fn new(handle: UniquePtr<fdb_sys::ControlIteratorHandle>) -> Self {
        Self {
            handle,
            exhausted: false,
        }
    }
}

impl Iterator for ControlIterator {
    type Item = Result<ControlElement>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }
        match self.handle.pin_mut().hasNext() {
            Ok(false) => {
                self.exhausted = true;
                return None;
            }
            Err(e) => {
                self.exhausted = true;
                return Some(Err(e.into()));
            }
            Ok(true) => {}
        }

        match self.handle.pin_mut().next() {
            Ok(data) => Some(Ok(ControlElement {
                location: data.location,
                identifiers: data.identifiers,
            })),
            Err(e) => {
                self.exhausted = true;
                Some(Err(e.into()))
            }
        }
    }
}

// SAFETY: ControlIterator can be sent to another thread because:
// 1. The C++ fdb5::ControlIterator contains a snapshot of control data taken at construction
// 2. It does not hold references back to the FDB handle after creation
// 3. Access is exclusive via &mut self (Pin<&mut> in the FFI layer)
// 4. The iterator has no thread-local state or thread-affine resources
#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl Send for ControlIterator {}

/// A control element describing database control state.
#[derive(Debug, Clone)]
pub struct ControlElement {
    /// Location of the database.
    pub location: String,
    /// Control identifiers enabled for this database.
    pub identifiers: Vec<fdb_sys::ControlIdentifier>,
}

// =============================================================================
// MoveIterator
// =============================================================================

/// An iterator over FDB move results.
pub struct MoveIterator {
    handle: UniquePtr<fdb_sys::MoveIteratorHandle>,
    exhausted: bool,
}

impl MoveIterator {
    /// Create a new iterator from a cxx handle.
    pub(crate) const fn new(handle: UniquePtr<fdb_sys::MoveIteratorHandle>) -> Self {
        Self {
            handle,
            exhausted: false,
        }
    }
}

impl Iterator for MoveIterator {
    type Item = Result<MoveElement>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }
        match self.handle.pin_mut().hasNext() {
            Ok(false) => {
                self.exhausted = true;
                return None;
            }
            Err(e) => {
                self.exhausted = true;
                return Some(Err(e.into()));
            }
            Ok(true) => {}
        }

        match self.handle.pin_mut().next() {
            Ok(data) => Some(Ok(MoveElement {
                source: data.source,
                destination: data.destination,
            })),
            Err(e) => {
                self.exhausted = true;
                Some(Err(e.into()))
            }
        }
    }
}

// SAFETY: MoveIterator can be sent to another thread because:
// 1. The C++ fdb5::MoveIterator contains a snapshot of move data taken at construction
// 2. It does not hold references back to the FDB handle after creation
// 3. Access is exclusive via &mut self (Pin<&mut> in the FFI layer)
// 4. The iterator has no thread-local state or thread-affine resources
#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl Send for MoveIterator {}

/// A move element describing data relocation.
#[derive(Debug, Clone)]
pub struct MoveElement {
    /// Source location.
    pub source: String,
    /// Destination location.
    pub destination: String,
}
