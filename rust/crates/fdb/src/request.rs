//! FDB request wrapper.

use std::str::FromStr;

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
    type Err = std::convert::Infallible;

    /// Parse a MARS request string.
    ///
    /// Format: `key1=val1/val2,key2=val3,...`
    ///
    /// # Example
    ///
    /// ```
    /// use fdb::Request;
    ///
    /// let request: Request = "class=od,step=0/6/12".parse().unwrap();
    /// assert_eq!(request.len(), 2);
    /// ```
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut req = Self::new();
        for part in s.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            if let Some((k, v)) = part.split_once('=') {
                let values: Vec<&str> = v.split('/').map(str::trim).collect();
                req = req.with_values(k.trim(), &values);
            }
        }
        Ok(req)
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
        let request: Request = "class=od,expver=0001".parse().unwrap();
        assert_eq!(request.len(), 2);
    }

    #[test]
    fn test_request_from_str_with_values() {
        let request: Request = "class=od,step=0/6/12".parse().unwrap();
        assert_eq!(request.len(), 2);
        assert_eq!(request.to_request_string(), "class=od,step=0/6/12");
    }

    #[test]
    fn test_request_roundtrip() {
        let original = Request::new()
            .with("class", "od")
            .with_values("step", &["0", "6", "12"]);
        let string = original.to_request_string();
        let parsed: Request = string.parse().unwrap();
        assert_eq!(parsed.to_request_string(), string);
    }
}
