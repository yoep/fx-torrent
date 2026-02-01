use std::borrow::Cow;

/// Checks whether both bit positions derived from the first 4 bytes of `k` are set.
pub fn has_bits(k: &[u8], bits: &[u8]) -> bool {
    if k.len() < 4 || bits.is_empty() {
        return false;
    }
    let bit_len = bits.len() * 8;

    let idx1 = u16::from_le_bytes([k[0], k[1]]) as usize % bit_len;
    let idx2 = u16::from_le_bytes([k[2], k[3]]) as usize % bit_len;

    let b1 = bits[idx1 / 8] & (1u8 << (idx1 & 7)) != 0;
    let b2 = bits[idx2 / 8] & (1u8 << (idx2 & 7)) != 0;
    b1 && b2
}

/// Sets both bit positions derived from the first 4 bytes of `k`.
pub fn set_bits(k: &[u8], bits: &mut [u8]) {
    if k.len() < 4 || bits.is_empty() {
        return;
    }
    let bit_len = bits.len() * 8;

    let idx1 = u16::from_le_bytes([k[0], k[1]]) as usize % bit_len;
    let idx2 = u16::from_le_bytes([k[2], k[3]]) as usize % bit_len;

    bits[idx1 / 8] |= 1u8 << (idx1 & 7);
    bits[idx2 / 8] |= 1u8 << (idx2 & 7);
}

/// Counts how many bits are not set (i.e., zero bits) in `bits`.
pub fn count_zero_bits(bits: &[u8]) -> usize {
    const BITCOUNT: [u8; 16] = [
        // 0000, 0001, 0010, 0011, 0100, 0101, 0110, 0111,
        // 1000, 1001, 1010, 1011, 1100, 1101, 1110, 1111
        4, 3, 3, 2, 3, 2, 2, 1, 3, 2, 2, 1, 2, 1, 1, 0,
    ];

    bits.iter()
        .map(|&b| BITCOUNT[(b & 0x0f) as usize] as usize + BITCOUNT[(b >> 4) as usize] as usize)
        .sum()
}

#[derive(Debug)]
pub struct BloomFilter<const N: usize> {
    bits: [u8; N],
}

impl<const N: usize> BloomFilter<N> {
    /// Create a new bloom filter with `N` bits.
    /// The bits are initialized to zero.
    pub fn new() -> Self {
        Self { bits: [0; N] }
    }

    /// Clear all bits in the filter.
    pub fn clear(&mut self) {
        self.bits.fill(0);
    }

    /// Check if the given key is present in the filter.
    /// Returns `true` if the key is present, `false` otherwise.
    pub fn find(&self, key: &[u8]) -> bool {
        has_bits(key, &self.bits)
    }

    /// Insert the given key into the filter.
    pub fn insert(&mut self, key: impl AsRef<[u8]>) {
        set_bits(key.as_ref(), &mut self.bits);
    }

    /// Returns a byte slice representing the filter's bits.
    pub fn as_bytes(&self) -> &[u8; N] {
        &self.bits
    }

    /// Returns a string representing the filter's bits.
    pub fn as_str(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(&self.bits)
    }
}

impl<const N: usize> Default for BloomFilter<N> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha1::{Digest, Sha1};

    #[test]
    fn test_has_bits() {
        let mut bloom_filter = BloomFilter::<32>::new();
        let k1 = Sha1::digest("test1".as_bytes()).to_vec();
        let k2 = Sha1::digest("test2".as_bytes()).to_vec();
        let k3 = Sha1::digest("test3".as_bytes()).to_vec();
        let k4 = Sha1::digest("test4".as_bytes()).to_vec();

        // verify that all keys are not present in the filter
        assert!(
            !bloom_filter.find(&k1),
            "k1 should not be present in the filter"
        );
        assert!(
            !bloom_filter.find(&k2),
            "k2 should not be present in the filter"
        );
        assert!(
            !bloom_filter.find(&k3),
            "k3 should not be present in the filter"
        );
        assert!(
            !bloom_filter.find(&k4),
            "k4 should not be present in the filter"
        );

        // set k1
        bloom_filter.insert(&k1);
        assert!(bloom_filter.find(&k1), "k1 should be present in the filter");
        assert!(
            !bloom_filter.find(&k2),
            "k2 should not be present in the filter"
        );
        assert!(
            !bloom_filter.find(&k3),
            "k3 should not be present in the filter"
        );
        assert!(
            !bloom_filter.find(&k4),
            "k4 should not be present in the filter"
        );

        // set k4
        bloom_filter.insert(&k4);
        assert!(bloom_filter.find(&k1), "k1 should be present in the filter");
        assert!(
            !bloom_filter.find(&k2),
            "k2 should not be present in the filter"
        );
        assert!(
            !bloom_filter.find(&k3),
            "k3 should not be present in the filter"
        );
        assert!(bloom_filter.find(&k4), "k4 should be present in the filter");
    }

    #[test]
    fn test_count_zeroes() {
        let mut bits = [0x00u8, 0xff, 0x55, 0xaa];
        assert_eq!(count_zero_bits(&bits), 16);

        let t = [4u8, 0, 4, 0];
        set_bits(&t, &mut bits);

        assert_eq!(count_zero_bits(&bits), 15);

        let compare = [0x10u8, 0xff, 0x55, 0xaa];
        assert_eq!(bits, compare);
    }
}
