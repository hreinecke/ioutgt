//! NVMe/TCP header and data digests: CRC32C (Castagnoli), little-endian
//! on the wire, computed incrementally for streamed payloads.

/// Incremental CRC32C accumulator for data digests (DDGST).
#[derive(Debug, Clone, Copy)]
pub struct Crc32c {
    state: u32,
}

impl Default for Crc32c {
    fn default() -> Self {
        Self::new()
    }
}

impl Crc32c {
    /// Fresh accumulator (initial state per RFC 3720 CRC32C).
    pub fn new() -> Self {
        Crc32c { state: 0 }
    }

    /// Fold more payload bytes into the digest.
    pub fn update(&mut self, data: &[u8]) {
        self.state = crc32c::crc32c_append(self.state, data);
    }

    /// Final digest value (compare with the wire's little-endian u32).
    pub fn finalize(self) -> u32 {
        self.state
    }
}

/// One-shot digest of a complete buffer (header digests).
pub fn crc32c(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}
