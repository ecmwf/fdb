//! FDB data reader wrapper.

use std::io::{Read, Seek, SeekFrom};

use fdb_sys::UniquePtr;

use crate::error::Result;

/// A reader for data retrieved from FDB.
///
/// Implements [`std::io::Read`] and [`std::io::Seek`] for standard I/O operations.
pub struct DataReader {
    handle: UniquePtr<fdb_sys::DataReaderHandle>,
}

impl DataReader {
    /// Create a new data reader from a cxx handle.
    pub(crate) fn new(mut handle: UniquePtr<fdb_sys::DataReaderHandle>) -> Result<Self> {
        handle.pin_mut().open()?;
        Ok(Self { handle })
    }

    /// Get the total size of the data in bytes.
    #[must_use]
    pub fn size(&self) -> u64 {
        self.handle.size()
    }

    /// Get the current read position.
    #[must_use]
    pub fn tell(&self) -> u64 {
        self.handle.tell()
    }

    /// Seek to a position in the data.
    ///
    /// # Errors
    ///
    /// Returns an error if seeking fails.
    pub fn seek_to(&mut self, pos: u64) -> Result<()> {
        self.handle.pin_mut().seek(pos)?;
        Ok(())
    }

    /// Read all data into a vector.
    ///
    /// # Errors
    ///
    /// Returns an error if reading fails or if the data size exceeds platform capacity.
    pub fn read_all(&mut self) -> Result<Vec<u8>> {
        let size = usize::try_from(self.size())?;
        let mut buf = vec![0u8; size];
        let mut total_read = 0;

        while total_read < size {
            let n = self.handle.pin_mut().read(&mut buf[total_read..])?;
            if n == 0 {
                break;
            }
            total_read += n;
        }

        buf.truncate(total_read);
        Ok(buf)
    }

    /// Close the data reader.
    ///
    /// # Errors
    ///
    /// Returns an error if closing fails.
    pub fn close(&mut self) -> Result<()> {
        self.handle.pin_mut().close()?;
        Ok(())
    }
}

impl Read for DataReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.handle
            .pin_mut()
            .read(buf)
            .map_err(|e| std::io::Error::other(e.to_string()))
    }
}

impl Seek for DataReader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let new_pos = match pos {
            SeekFrom::Start(offset) => offset,
            SeekFrom::End(offset) => {
                let size = i64::try_from(self.size())
                    .map_err(|_| std::io::Error::other("file size exceeds i64::MAX"))?;
                let new = size
                    .checked_add(offset)
                    .ok_or_else(|| std::io::Error::other("seek position overflow"))?;
                if new < 0 {
                    return Err(std::io::Error::other("seek to negative position"));
                }
                new.cast_unsigned()
            }
            SeekFrom::Current(offset) => {
                let current = i64::try_from(self.tell())
                    .map_err(|_| std::io::Error::other("current position exceeds i64::MAX"))?;
                let new = current
                    .checked_add(offset)
                    .ok_or_else(|| std::io::Error::other("seek position overflow"))?;
                if new < 0 {
                    return Err(std::io::Error::other("seek to negative position"));
                }
                new.cast_unsigned()
            }
        };

        self.handle
            .pin_mut()
            .seek(new_pos)
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        Ok(new_pos)
    }
}

impl Drop for DataReader {
    fn drop(&mut self) {
        let _ = self.handle.pin_mut().close();
    }
}

// SAFETY: The underlying C++ DataHandle is accessed through &mut self only.
#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl Send for DataReader {}
