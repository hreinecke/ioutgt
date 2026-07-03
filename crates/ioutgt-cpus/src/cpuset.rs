//! Growable CPU bitset with the mask operations the grouping and the
//! topology reader rely on.

use std::fmt;
use std::io;

const BITS_PER_WORD: usize = u64::BITS as usize;

/// A set of CPU ids backed by a growable bit vector.
///
/// Sets grow on demand; all binary operations
/// accept operands of different lengths.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct CpuSet {
    words: Vec<u64>,
}

impl CpuSet {
    /// The empty set.
    pub fn new() -> CpuSet {
        CpuSet { words: Vec::new() }
    }

    /// Add `cpu` to the set.
    pub fn set(&mut self, cpu: usize) {
        let word = cpu / BITS_PER_WORD;
        if word >= self.words.len() {
            self.words.resize(word + 1, 0);
        }
        self.words[word] |= 1u64 << (cpu % BITS_PER_WORD);
    }

    /// Remove `cpu` from the set.
    pub fn clear(&mut self, cpu: usize) {
        if let Some(word) = self.words.get_mut(cpu / BITS_PER_WORD) {
            *word &= !(1u64 << (cpu % BITS_PER_WORD));
        }
    }

    /// Whether `cpu` is in the set.
    pub fn test(&self, cpu: usize) -> bool {
        self.words
            .get(cpu / BITS_PER_WORD)
            .is_some_and(|w| w & (1u64 << (cpu % BITS_PER_WORD)) != 0)
    }

    /// Number of CPUs in the set.
    pub fn weight(&self) -> usize {
        self.words.iter().map(|w| w.count_ones() as usize).sum()
    }

    /// Whether the set is empty.
    pub fn is_empty(&self) -> bool {
        self.words.iter().all(|&w| w == 0)
    }

    /// Lowest CPU in the set.
    pub fn first(&self) -> Option<usize> {
        self.iter().next()
    }

    /// Highest CPU in the set.
    pub fn last(&self) -> Option<usize> {
        self.words
            .iter()
            .enumerate()
            .rev()
            .find(|&(_, &w)| w != 0)
            .map(|(i, &w)| i * BITS_PER_WORD + (BITS_PER_WORD - 1 - w.leading_zeros() as usize))
    }

    /// Iterate over CPUs in the set in ascending order.
    pub fn iter(&self) -> impl Iterator<Item = usize> + '_ {
        self.words.iter().enumerate().flat_map(|(i, &w)| {
            (0..BITS_PER_WORD)
                .filter(move |b| w & (1u64 << b) != 0)
                .map(move |b| i * BITS_PER_WORD + b)
        })
    }

    /// `self & other`.
    pub fn and(&self, other: &CpuSet) -> CpuSet {
        CpuSet {
            words: self
                .words
                .iter()
                .zip(&other.words)
                .map(|(a, b)| a & b)
                .collect(),
        }
    }

    /// `self | other`.
    pub fn or(&self, other: &CpuSet) -> CpuSet {
        let mut words = vec![0u64; self.words.len().max(other.words.len())];
        for (i, w) in words.iter_mut().enumerate() {
            *w = self.words.get(i).copied().unwrap_or(0) | other.words.get(i).copied().unwrap_or(0);
        }
        CpuSet { words }
    }

    /// `self & !other`.
    pub fn andnot(&self, other: &CpuSet) -> CpuSet {
        CpuSet {
            words: self
                .words
                .iter()
                .enumerate()
                .map(|(i, a)| a & !other.words.get(i).copied().unwrap_or(0))
                .collect(),
        }
    }

    /// Whether the sets share any CPU.
    pub fn intersects(&self, other: &CpuSet) -> bool {
        self.words.iter().zip(&other.words).any(|(a, b)| a & b != 0)
    }

    /// Parse the sysfs cpulist format, e.g. `"0-3,8,10-11"`. An empty
    /// (or whitespace-only) string is the empty set.
    pub fn from_cpulist(s: &str) -> io::Result<CpuSet> {
        let bad = |part: &str| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid cpulist range {part:?}"),
            )
        };
        let mut set = CpuSet::new();
        for part in s.trim().split(',').filter(|p| !p.is_empty()) {
            let (lo, hi) = match part.split_once('-') {
                Some((lo, hi)) => (lo, hi),
                None => (part, part),
            };
            let lo: usize = lo.parse().map_err(|_| bad(part))?;
            let hi: usize = hi.parse().map_err(|_| bad(part))?;
            if lo > hi {
                return Err(bad(part));
            }
            for cpu in lo..=hi {
                set.set(cpu);
            }
        }
        Ok(set)
    }
}

impl FromIterator<usize> for CpuSet {
    fn from_iter<T: IntoIterator<Item = usize>>(iter: T) -> CpuSet {
        let mut set = CpuSet::new();
        for cpu in iter {
            set.set(cpu);
        }
        set
    }
}

/// Formats as a cpulist (`0-3,8`), the inverse of [`CpuSet::from_cpulist`].
impl fmt::Display for CpuSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        let mut run: Option<(usize, usize)> = None;
        let mut flush = |f: &mut fmt::Formatter<'_>, (lo, hi): (usize, usize)| {
            let sep = if first { "" } else { "," };
            first = false;
            if lo == hi {
                write!(f, "{sep}{lo}")
            } else {
                write!(f, "{sep}{lo}-{hi}")
            }
        };
        for cpu in self.iter() {
            run = match run {
                Some((lo, hi)) if cpu == hi + 1 => Some((lo, cpu)),
                Some(done) => {
                    flush(f, done)?;
                    Some((cpu, cpu))
                }
                None => Some((cpu, cpu)),
            };
        }
        if let Some(done) = run {
            flush(f, done)?;
        }
        Ok(())
    }
}

impl fmt::Debug for CpuSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CpuSet({self})")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_ops() {
        let mut a = CpuSet::new();
        assert!(a.is_empty());
        assert_eq!(a.first(), None);
        a.set(1);
        a.set(64);
        a.set(65);
        assert!(a.test(64) && !a.test(63));
        assert_eq!(a.weight(), 3);
        assert_eq!(a.first(), Some(1));
        assert_eq!(a.last(), Some(65));
        assert_eq!(a.iter().collect::<Vec<_>>(), vec![1, 64, 65]);

        a.clear(64);
        assert_eq!(a.weight(), 2);
        a.clear(1000); // out of range is a no-op
        let b: CpuSet = [0, 1, 65].into_iter().collect();
        assert_eq!(a.and(&b).iter().collect::<Vec<_>>(), vec![1, 65]);
        assert_eq!(a.or(&b).iter().collect::<Vec<_>>(), vec![0, 1, 65]);
        assert_eq!(b.andnot(&a).iter().collect::<Vec<_>>(), vec![0]);
        assert!(a.intersects(&b));
        assert!(!CpuSet::new().intersects(&b));
    }

    #[test]
    fn cpulist_round_trip() {
        for s in ["", "0", "0-3", "0-3,8,10-11", "1,3,5"] {
            let set = CpuSet::from_cpulist(s).unwrap();
            assert_eq!(set.to_string(), s, "round-trip of {s:?}");
        }
        assert_eq!(CpuSet::from_cpulist(" 0-2\n").unwrap().weight(), 3);
        assert!(CpuSet::from_cpulist("3-1").is_err());
        assert!(CpuSet::from_cpulist("x").is_err());
    }
}
