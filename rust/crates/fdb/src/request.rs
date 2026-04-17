//! FDB request wrapper.

use std::str::FromStr;

use indexmap::IndexMap;

use crate::error::{Error, Result};

/// A request for FDB list/retrieve operations.
///
/// Requests specify which fields to list or retrieve from FDB. Each MARS
/// key maps to exactly one value list — setting the same key twice
/// replaces the earlier list (last write wins). Insertion order is
/// preserved for predictable rendering via [`Self::to_request_string`].
///
/// # Example
///
/// ```
/// use fdb::Request;
///
/// let request = Request::new()
///     .with("class", "od")
///     .with("expver", "0001")
///     .with_values("step", &["0", "6", "12"]);
/// ```
#[derive(Debug, Clone, Default)]
pub struct Request {
    entries: IndexMap<String, Vec<String>>,
}

impl Request {
    /// Create a new empty request.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set a single value for a key (builder pattern).
    ///
    /// If the key already exists, its value list is replaced — **last
    /// write wins**. MARS requests have at most one value list per key,
    /// so silently keeping two separate entries for the same key would
    /// produce an invalid request string (`class=od,class=rd`).
    #[must_use]
    pub fn with(self, name: &str, value: &str) -> Self {
        self.with_values(name, &[value])
    }

    /// Set multiple values for a key (builder pattern).
    ///
    /// If the key already exists, its value list is replaced.
    #[must_use]
    pub fn with_values(mut self, name: &str, values: &[&str]) -> Self {
        self.set(name, values);
        self
    }

    /// Set a single value for a key (mutable reference).
    ///
    /// Same "last write wins" semantics as [`Self::with`].
    pub fn add(&mut self, name: &str, value: &str) -> &mut Self {
        self.add_values(name, &[value])
    }

    /// Set multiple values for a key (mutable reference).
    ///
    /// Same "last write wins" semantics as [`Self::with_values`].
    pub fn add_values(&mut self, name: &str, values: &[&str]) -> &mut Self {
        self.set(name, values);
        self
    }

    /// Shared implementation for the builder / mutable APIs. `IndexMap::insert`
    /// replaces the value in place if the key already exists (preserving
    /// its position), otherwise appends a new entry.
    fn set(&mut self, name: &str, values: &[&str]) {
        let vs: Vec<String> = values.iter().map(ToString::to_string).collect();
        self.entries.insert(name.to_string(), vs);
    }

    /// Get the number of entries in the request.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if the request is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Iterate the request entries in insertion order.
    pub fn entries(&self) -> impl Iterator<Item = (&str, &[String])> + '_ {
        self.entries.iter().map(|(k, v)| (k.as_str(), v.as_slice()))
    }

    /// Convert to MARS request string format.
    ///
    /// Format: `key1=val1/val2,key2=val3,...`
    #[must_use]
    pub fn to_request_string(&self) -> String {
        self.entries
            .iter()
            .map(|(k, vs)| format!("{}={}", k, vs.join("/")))
            .collect::<Vec<_>>()
            .join(",")
    }
}

impl FromStr for Request {
    type Err = Error;

    /// Parse a MARS request string using metkit's parser and expansion
    /// machinery.
    ///
    /// Handles the full MARS language: `key=val1/val2` lists, `to`/`by`
    /// ranges (e.g. `step=0/to/24/by/3`), type expansion, optional fields,
    /// etc. Internally calls into the C++ bridge so the *exact same* parser
    /// is used here as for `Fdb::list`/`retrieve`/etc.
    ///
    /// # Errors
    ///
    /// Returns an `Error` if metkit can't parse the request, with the
    /// underlying eckit/metkit message attached.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use fdb::Request;
    ///
    /// let request: Request = "class=od,step=0/to/12/by/3".parse()?;
    /// assert_eq!(request.len(), 2);
    /// # Ok::<(), fdb::Error>(())
    /// ```
    fn from_str(s: &str) -> Result<Self> {
        let parsed = fdb_sys::parse_mars_request(s)?;
        let mut entries = IndexMap::with_capacity(parsed.params.len());
        for param in parsed.params {
            entries.insert(param.key, param.values);
        }
        Ok(Self { entries })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_creation() {
        let request = Request::new();
        assert!(request.is_empty());
    }

    #[test]
    fn test_request_builder() {
        let request = Request::new()
            .with("class", "od")
            .with("expver", "0001")
            .with_values("step", &["0", "6", "12"]);

        assert_eq!(request.len(), 3);
    }

    #[test]
    fn test_request_add() {
        let mut request = Request::new();
        request.add("class", "od").add("expver", "0001");
        assert_eq!(request.len(), 2);
    }

    #[test]
    fn test_request_string() {
        let request = Request::new()
            .with("class", "od")
            .with_values("step", &["0", "6"]);

        assert_eq!(request.to_request_string(), "class=od,step=0/6");
    }

    /// Setting a key that already exists must replace the previous value
    /// list — MARS has one value list per key, so producing
    /// `class=od,class=rd` would be malformed.
    #[test]
    fn test_request_with_last_write_wins() {
        let request = Request::new().with("class", "od").with("class", "rd");

        assert_eq!(request.len(), 1);
        assert_eq!(request.to_request_string(), "class=rd");
    }

    /// Multi-value overrides follow the same rule: the whole list is
    /// replaced, not merged.
    #[test]
    fn test_request_with_values_last_write_wins() {
        let request = Request::new()
            .with_values("step", &["0", "6"])
            .with_values("step", &["12", "18"]);

        assert_eq!(request.len(), 1);
        assert_eq!(request.to_request_string(), "step=12/18");
    }

    /// The mutable `add` / `add_values` APIs share the override semantics
    /// with their builder counterparts.
    #[test]
    fn test_request_add_last_write_wins() {
        let mut request = Request::new();
        request.add("class", "od");
        request.add("class", "rd");
        request.add_values("step", &["0", "6"]);
        request.add_values("step", &["12"]);

        assert_eq!(request.len(), 2);
        assert_eq!(request.to_request_string(), "class=rd,step=12");
    }

    /// Replacing a key in place must keep it in its original position,
    /// so the rendered MARS string is stable across overrides.
    #[test]
    fn test_request_override_preserves_insertion_order() {
        let request = Request::new()
            .with("class", "od")
            .with("expver", "0001")
            .with("class", "rd");

        assert_eq!(request.to_request_string(), "class=rd,expver=0001");
    }

    #[test]
    fn test_request_from_str() {
        let request: Request = "class=od,expver=0001"
            .parse()
            .expect("metkit should parse a trivial request");
        // Each key the user typed should be present after parsing.
        let keys: Vec<&str> = request.entries().map(|(k, _)| k).collect();
        assert!(keys.contains(&"class"));
        assert!(keys.contains(&"expver"));
    }

    #[test]
    fn test_request_from_str_with_to_by_range() {
        // The whole point of routing through metkit: `to`/`by` should expand
        // into a flat value list rather than being treated as literal strings.
        let request: Request = "class=od,expver=0001,step=0/to/12/by/3"
            .parse()
            .expect("metkit should parse a to/by range");
        let step_values: Vec<String> = request
            .entries()
            .find(|(k, _)| *k == "step")
            .map(|(_, vs)| vs.to_vec())
            .expect("step key should be present");
        // step=0/to/12/by/3 expands to [0, 3, 6, 9, 12].
        assert_eq!(step_values, vec!["0", "3", "6", "9", "12"]);
    }

    #[test]
    fn test_request_from_str_invalid() {
        // Garbage that even metkit can't make sense of should be a parse error,
        // not a silent empty Request.
        let result: Result<Request> = "this is not a mars request".parse();
        assert!(result.is_err(), "expected parse failure, got {result:?}");
    }
}
