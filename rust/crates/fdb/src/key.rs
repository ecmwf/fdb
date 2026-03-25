//! FDB key wrapper.

/// A key for FDB archive operations.
///
/// Keys are used to identify data when archiving to FDB.
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
    entries: Vec<(String, String)>,
}

impl Key {
    /// Create a new empty key.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a key from a vector of key-value pairs.
    #[must_use]
    pub const fn from_entries(entries: Vec<(String, String)>) -> Self {
        Self { entries }
    }

    /// Add a key-value pair to the key (builder pattern).
    #[must_use]
    pub fn with(mut self, name: &str, value: &str) -> Self {
        self.entries.push((name.to_string(), value.to_string()));
        self
    }

    /// Add a key-value pair to the key (mutable reference).
    pub fn add(&mut self, name: &str, value: &str) -> &mut Self {
        self.entries.push((name.to_string(), value.to_string()));
        self
    }

    /// Get the number of entries in the key.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if the key is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get the entries as a slice.
    #[must_use]
    pub fn entries(&self) -> &[(String, String)] {
        &self.entries
    }

    /// Convert to the cxx `KeyData` type.
    #[must_use]
    pub(crate) fn to_cxx(&self) -> fdb_sys::KeyData {
        fdb_sys::KeyData {
            entries: self
                .entries
                .iter()
                .map(|(k, v)| fdb_sys::KeyValue {
                    key: k.clone(),
                    value: v.clone(),
                })
                .collect(),
        }
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
        assert_eq!(key.entries()[0], ("class".to_string(), "od".to_string()));
    }

    #[test]
    fn test_key_add() {
        let mut key = Key::new();
        key.add("class", "od").add("expver", "0001");
        assert_eq!(key.len(), 2);
    }
}
