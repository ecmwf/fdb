//! FDB key wrapper.

/// A key for FDB archive operations.
///
/// Keys are used to identify data when archiving to FDB.
///
/// Internally a `Key` wraps an `fdb_sys::KeyData` directly, so handing it to
/// the cxx bridge is a borrow rather than a copy — the only allocations are
/// the original string `push`es done by the builder.
///
/// # Example
///
/// ```
/// use fdb::Key;
///
/// let key = Key::new()
///     .with("class", "od")
///     .with("expver", "0001")
///     .with("stream", "oper");
/// ```
#[derive(Debug, Clone, Default)]
pub struct Key {
    inner: fdb_sys::KeyData,
}

impl Key {
    /// Create a new empty key.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a key from a vector of key-value pairs. Consumes the input
    /// without per-string cloning.
    #[must_use]
    pub fn from_entries(entries: Vec<(String, String)>) -> Self {
        Self {
            inner: fdb_sys::KeyData {
                entries: entries
                    .into_iter()
                    .map(|(key, value)| fdb_sys::KeyValue { key, value })
                    .collect(),
            },
        }
    }

    /// Add a key-value pair to the key (builder pattern).
    #[must_use]
    pub fn with(mut self, name: &str, value: &str) -> Self {
        self.inner.entries.push(fdb_sys::KeyValue {
            key: name.to_string(),
            value: value.to_string(),
        });
        self
    }

    /// Add a key-value pair to the key (mutable reference).
    pub fn add(&mut self, name: &str, value: &str) -> &mut Self {
        self.inner.entries.push(fdb_sys::KeyValue {
            key: name.to_string(),
            value: value.to_string(),
        });
        self
    }

    /// Get the number of entries in the key.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.inner.entries.len()
    }

    /// Check if the key is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.inner.entries.is_empty()
    }

    /// Iterate over the key entries as `(name, value)` pairs.
    pub fn entries(&self) -> impl Iterator<Item = (&str, &str)> {
        self.inner
            .entries
            .iter()
            .map(|kv| (kv.key.as_str(), kv.value.as_str()))
    }

    /// Borrow the underlying cxx representation. Zero-copy.
    pub(crate) const fn to_cxx(&self) -> &fdb_sys::KeyData {
        &self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_creation() {
        let key = Key::new();
        assert!(key.is_empty());
    }

    #[test]
    fn test_key_builder() {
        let key = Key::new().with("class", "od").with("expver", "0001");
        assert_eq!(key.len(), 2);
        let first = key.entries().next().expect("key has at least one entry");
        assert_eq!(first, ("class", "od"));
    }

    #[test]
    fn test_key_add() {
        let mut key = Key::new();
        key.add("class", "od").add("expver", "0001");
        assert_eq!(key.len(), 2);
    }
}
