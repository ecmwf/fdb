//! FDB request wrapper.

use std::str::FromStr;

use crate::error::{Error, Result};

/// A request for FDB list/retrieve operations.
///
/// Requests specify which fields to list or retrieve from FDB.
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
    entries: Vec<(String, Vec<String>)>,
}

impl Request {
    /// Create a new empty request.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a single value for a key (builder pattern).
    #[must_use]
    pub fn with(self, name: &str, value: &str) -> Self {
        self.with_values(name, &[value])
    }

    /// Add multiple values for a key (builder pattern).
    #[must_use]
    pub fn with_values(mut self, name: &str, values: &[&str]) -> Self {
        self.entries.push((
            name.to_string(),
            values.iter().map(|s| (*s).to_string()).collect(),
        ));
        self
    }

    /// Add a single value for a key (mutable reference).
    pub fn add(&mut self, name: &str, value: &str) -> &mut Self {
        self.add_values(name, &[value])
    }

    /// Add multiple values for a key (mutable reference).
    pub fn add_values(&mut self, name: &str, values: &[&str]) -> &mut Self {
        self.entries.push((
            name.to_string(),
            values.iter().map(|s| (*s).to_string()).collect(),
        ));
        self
    }

    /// Get the number of entries in the request.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if the request is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get the entries as a slice.
    #[must_use]
    pub fn entries(&self) -> &[(String, Vec<String>)] {
        &self.entries
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
        let entries = parsed
            .params
            .into_iter()
            .map(|p| (p.key, p.values))
            .collect();
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

    #[test]
    fn test_request_from_str() {
        let request: Request = "class=od,expver=0001"
            .parse()
            .expect("metkit should parse a trivial request");
        // Each key the user typed should be present after parsing.
        let keys: Vec<&str> = request.entries().iter().map(|(k, _)| k.as_str()).collect();
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
            .iter()
            .find(|(k, _)| k == "step")
            .map(|(_, vs)| vs.clone())
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
