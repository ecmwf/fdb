//! Error handling for FDB.

/// Error type for FDB operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Internal programming error in the C++ library (`eckit::SeriousBug`).
    #[error("serious bug: {0}")]
    SeriousBug(String),

    /// User-caused error (`eckit::UserError`).
    #[error("user error: {0}")]
    UserError(String),

    /// Invalid parameter passed to C++ library (`eckit::BadParameter`).
    #[error("bad parameter: {0}")]
    BadParameter(String),

    /// Feature not implemented (`eckit::NotImplemented`).
    #[error("not implemented: {0}")]
    NotImplemented(String),

    /// Index or range out of bounds (`eckit::OutOfRange`).
    #[error("out of range: {0}")]
    OutOfRange(String),

    /// File operation error (`eckit::FileError`).
    #[error("file error: {0}")]
    FileError(String),

    /// Assertion failed in C++ library (`eckit::AssertionFailed`).
    #[error("assertion failed: {0}")]
    AssertionFailed(String),

    /// Generic eckit exception.
    #[error("eckit error: {0}")]
    Eckit(String),

    /// Generic error from the FDB C++ library.
    #[error("fdb error: {0}")]
    Fdb(String),

    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Data size exceeds platform capacity.
    #[error("data size exceeds platform capacity: {0}")]
    SizeOverflow(#[from] std::num::TryFromIntError),
}

/// Result type alias for FDB operations.
pub type Result<T> = std::result::Result<T, Error>;

impl From<fdb_sys::Exception> for Error {
    #[allow(clippy::option_if_let_else)]
    fn from(e: fdb_sys::Exception) -> Self {
        let msg = e.what();

        // Parse prefixes added by rust::behavior::trycatch
        if let Some(rest) = msg.strip_prefix("ECKIT_SERIOUS_BUG: ") {
            Self::SeriousBug(rest.to_string())
        } else if let Some(rest) = msg.strip_prefix("ECKIT_USER_ERROR: ") {
            Self::UserError(rest.to_string())
        } else if let Some(rest) = msg.strip_prefix("ECKIT_BAD_PARAMETER: ") {
            Self::BadParameter(rest.to_string())
        } else if let Some(rest) = msg.strip_prefix("ECKIT_NOT_IMPLEMENTED: ") {
            Self::NotImplemented(rest.to_string())
        } else if let Some(rest) = msg.strip_prefix("ECKIT_OUT_OF_RANGE: ") {
            Self::OutOfRange(rest.to_string())
        } else if let Some(rest) = msg.strip_prefix("ECKIT_FILE_ERROR: ") {
            Self::FileError(rest.to_string())
        } else if let Some(rest) = msg.strip_prefix("ECKIT_ASSERTION_FAILED: ") {
            Self::AssertionFailed(rest.to_string())
        } else if let Some(rest) = msg.strip_prefix("ECKIT: ") {
            Self::Eckit(rest.to_string())
        } else {
            Self::Fdb(msg.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper to create a mock exception-like message
    #[allow(clippy::option_if_let_else)]
    fn convert_message(msg: &str) -> Error {
        // Simulate what From<Exception> does by parsing the prefix
        msg.strip_prefix("ECKIT_SERIOUS_BUG: ").map_or_else(
            || {
                if let Some(rest) = msg.strip_prefix("ECKIT_USER_ERROR: ") {
                    Error::UserError(rest.to_string())
                } else if let Some(rest) = msg.strip_prefix("ECKIT_BAD_PARAMETER: ") {
                    Error::BadParameter(rest.to_string())
                } else if let Some(rest) = msg.strip_prefix("ECKIT_NOT_IMPLEMENTED: ") {
                    Error::NotImplemented(rest.to_string())
                } else if let Some(rest) = msg.strip_prefix("ECKIT_OUT_OF_RANGE: ") {
                    Error::OutOfRange(rest.to_string())
                } else if let Some(rest) = msg.strip_prefix("ECKIT_FILE_ERROR: ") {
                    Error::FileError(rest.to_string())
                } else if let Some(rest) = msg.strip_prefix("ECKIT_ASSERTION_FAILED: ") {
                    Error::AssertionFailed(rest.to_string())
                } else if let Some(rest) = msg.strip_prefix("ECKIT: ") {
                    Error::Eckit(rest.to_string())
                } else {
                    Error::Fdb(msg.to_string())
                }
            },
            |rest| Error::SeriousBug(rest.to_string()),
        )
    }

    #[test]
    fn test_serious_bug_prefix() {
        let err = convert_message("ECKIT_SERIOUS_BUG: something went wrong");
        assert!(matches!(err, Error::SeriousBug(msg) if msg == "something went wrong"));
    }

    #[test]
    fn test_user_error_prefix() {
        let err = convert_message("ECKIT_USER_ERROR: invalid input");
        assert!(matches!(err, Error::UserError(msg) if msg == "invalid input"));
    }

    #[test]
    fn test_bad_parameter_prefix() {
        let err = convert_message("ECKIT_BAD_PARAMETER: param must be positive");
        assert!(matches!(err, Error::BadParameter(msg) if msg == "param must be positive"));
    }

    #[test]
    fn test_not_implemented_prefix() {
        let err = convert_message("ECKIT_NOT_IMPLEMENTED: feature X");
        assert!(matches!(err, Error::NotImplemented(msg) if msg == "feature X"));
    }

    #[test]
    fn test_out_of_range_prefix() {
        let err = convert_message("ECKIT_OUT_OF_RANGE: index 10 out of bounds");
        assert!(matches!(err, Error::OutOfRange(msg) if msg == "index 10 out of bounds"));
    }

    #[test]
    fn test_file_error_prefix() {
        let err = convert_message("ECKIT_FILE_ERROR: cannot open file");
        assert!(matches!(err, Error::FileError(msg) if msg == "cannot open file"));
    }

    #[test]
    fn test_assertion_failed_prefix() {
        let err = convert_message("ECKIT_ASSERTION_FAILED: x > 0");
        assert!(matches!(err, Error::AssertionFailed(msg) if msg == "x > 0"));
    }

    #[test]
    fn test_generic_eckit_prefix() {
        let err = convert_message("ECKIT: some eckit error");
        assert!(matches!(err, Error::Eckit(msg) if msg == "some eckit error"));
    }

    #[test]
    fn test_no_prefix_falls_through() {
        let err = convert_message("plain error message");
        assert!(matches!(err, Error::Fdb(msg) if msg == "plain error message"));
    }

    #[test]
    fn test_std_exception_no_prefix() {
        let err = convert_message("std::runtime_error message");
        assert!(matches!(err, Error::Fdb(msg) if msg == "std::runtime_error message"));
    }
}
