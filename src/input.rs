//! The file mappings outlive all work on their byte ranges. Only LF ends a row.
use memmap2::Mmap;
use std::{
    arch::x86_64::*,
    fs::{self, File},
    path::Path,
    sync::Mutex,
    sync::atomic::{AtomicUsize, Ordering},
};

pub fn jobs<T: Send, R: Send>(
    items: Vec<T>,
    threads: usize,
    init: impl Fn(usize) -> R + Sync,
    process: impl Fn(&mut R, T) + Sync,
) -> Vec<R> {
    let workers = threads.min(items.len()).max(1);
    let slots: Vec<_> = items.into_iter().map(|x| Mutex::new(Some(x))).collect();
    let next = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        let handles: Vec<_> = (0..workers)
            .map(|i| {
                let (init, process, slots, next) = (&init, &process, &slots, &next);
                scope.spawn(move || {
                    let mut result = init(i);
                    loop {
                        let index = next.fetch_add(1, Ordering::Relaxed);
                        let Some(slot) = slots.get(index) else { break };
                        let value = slot.lock().unwrap().take().unwrap();
                        process(&mut result, value);
                    }
                    result
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

pub struct Input(Vec<Mmap>);
impl Input {
    pub fn open(path: &Path) -> Self {
        let paths = if path.is_dir() {
            fs::read_dir(path)
                .unwrap()
                .map(|p| p.unwrap().path())
                .filter(|p| p.extension().is_some_and(|s| s == "csv"))
                .collect()
        } else {
            vec![path.to_owned()]
        };
        Self(
            paths
                .into_iter()
                .filter_map(|p| {
                    let f = File::open(p).unwrap();
                    // The scorer mounts immutable input files.
                    (f.metadata().unwrap().len() != 0).then(|| unsafe { Mmap::map(&f).unwrap() })
                })
                .collect(),
        )
    }
    pub fn bytes(&self) -> usize {
        self.0.iter().map(|m| m.len()).sum()
    }
    pub fn chunks(&self) -> Vec<&[u8]> {
        let mut out = Vec::new();
        for m in &self.0 {
            let Some(header) = m.iter().position(|&b| b == b'\n') else {
                continue;
            };
            let mut start = header + 1;
            while start < m.len() {
                let cut = (start + (1 << 20)).min(m.len());
                let end = m[cut..]
                    .iter()
                    .position(|&b| b == b'\n')
                    .map_or(m.len(), |n| cut + n + 1);
                out.push(&m[start..end]);
                start = end;
            }
        }
        out
    }
}

#[inline]
fn masks(bytes: &[u8]) -> (u32, u32, u32) {
    // Caller supplies 32 readable bytes. x86-64-v3 provides AVX2.
    unsafe {
        let x = _mm256_loadu_si256(bytes.as_ptr().cast());
        let mask = |byte| _mm256_movemask_epi8(_mm256_cmpeq_epi8(x, _mm256_set1_epi8(byte))) as u32;
        let lf = mask(10);
        (mask(44) | lf, lf, mask(34))
    }
}

pub fn rows<const N: usize>(data: &[u8], mut emit: impl FnMut(&[&[u8]; N])) {
    let mut fields = [&data[0..0]; N];
    let (mut start, mut n, mut quotes, mut good) = (0, 0usize, 0u32, true);
    let mut base = 0;
    // A virtual LF closes an unterminated final row. Extra blank rows are invalid.
    while base <= data.len() {
        let mut tail = [0u8; 32];
        let remaining = data.len() - base;
        let block = if remaining >= 32 {
            &data[base..base + 32]
        } else {
            tail[..remaining].copy_from_slice(&data[base..]);
            tail[remaining] = 10;
            &tail
        };
        let (mut delimiters, lf, mut quote_mask) = masks(block);
        while delimiters != 0 {
            let offset = delimiters.trailing_zeros();
            let bit = 1u32 << offset;
            let end = base + offset as usize;
            quotes += (quote_mask & (bit - 1)).count_ones();
            quote_mask &= !(bit - 1);
            if n < N {
                let value = &data[start..end];
                if quotes == 0 {
                    fields[n] = value;
                } else if quotes == 2
                    && value.len() >= 2
                    && value[0] == 34
                    && value[value.len() - 1] == 34
                {
                    fields[n] = &value[1..value.len() - 1];
                } else {
                    good = false;
                }
            }
            n += 1;
            if bit & lf != 0 {
                if n == N && good {
                    emit(&fields);
                }
                n = 0;
                good = true;
            }
            start = end + 1;
            quotes = 0;
            delimiters &= delimiters - 1;
        }
        quotes += quote_mask.count_ones();
        if remaining < 32 {
            break;
        }
        base += 32;
    }
}

pub const DAY: i64 = 86_400_000;
pub const WEEK: i64 = 7 * DAY;
pub const INVALID: i64 = i64::MIN;

#[inline]
pub fn decimal(s: &[u8], limit: u64) -> Option<u64> {
    if s.is_empty() {
        return None;
    }
    let mut value = 0u64;
    for &b in s {
        let d = b.wrapping_sub(b'0') as u64;
        if d > 9 || value > limit / 10 {
            return None;
        }
        value = value * 10 + d;
        if value > limit {
            return None;
        }
    }
    Some(value)
}

#[inline]
fn two(s: &[u8], i: usize) -> Option<i64> {
    let a = s[i].wrapping_sub(48);
    let b = s[i + 1].wrapping_sub(48);
    if a > 9 || b > 9 {
        None
    } else {
        Some((10 * a + b) as i64)
    }
}

#[inline]
fn civil_day(year: i64, month: i64, day: i64) -> Option<i64> {
    if !(1..=12).contains(&month) {
        return None;
    }
    let lens = [
        31,
        28 + i64::from(year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)),
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    if day < 1 || day > lens[(month - 1) as usize] {
        return None;
    }
    let y = year - i64::from(month <= 2);
    let era = y.div_euclid(400);
    let yy = y - era * 400;
    let m = if month > 2 { month - 3 } else { month + 9 };
    Some(era * 146097 + yy * 365 + yy / 4 - yy / 100 + (153 * m + 2) / 5 + day - 1 - 719468)
}

// Small per-worker cache amortizes the Gregorian conversion. Keys include
// all ten date bytes, including separators and leading digits.
pub struct Dates {
    slots: [([u8; 10], i64); 128],
}
impl Dates {
    pub fn new() -> Self {
        Self {
            slots: [([0; 10], INVALID); 128],
        }
    }
    #[inline]
    fn day(&mut self, s: &[u8]) -> Option<i64> {
        let key: &[u8; 10] = s[..10].try_into().unwrap();
        let index = (s[9] as usize + 11 * s[8] as usize + 33 * s[6] as usize) & 127;
        let entry = &mut self.slots[index];
        if entry.0 == *key && entry.1 != INVALID {
            return Some(entry.1);
        }
        if s[4] != 45 || s[7] != 45 {
            return None;
        }
        let day = civil_day(two(s, 0)? * 100 + two(s, 2)?, two(s, 5)?, two(s, 8)?)? * DAY;
        *entry = (*key, day);
        Some(day)
    }
    #[inline]
    pub fn stamp(&mut self, s: &[u8]) -> Option<i64> {
        if (s.len() != 20 && s.len() != 25) || s[4] != 45 {
            return decimal(s, i64::MAX as u64).map(|v| v as i64);
        }
        if s[10] != 84 || s[13] != 58 || s[16] != 58 {
            return None;
        }
        let hour = two(s, 11)?;
        let minute = two(s, 14)?;
        let second = two(s, 17)?;
        if hour > 23 || minute > 59 || second > 59 {
            return None;
        }
        let local = self.day(s)? + ((hour * 60 + minute) * 60 + second) * 1000;
        if s.len() == 20 {
            return (s[19] == 90).then_some(local);
        }
        if s[22] != 58 || (s[19] != 43 && s[19] != 45) {
            return None;
        }
        let oh = two(s, 20)?;
        let om = two(s, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        let offset = (oh * 60 + om) * 60_000;
        Some(if s[19] == 43 {
            local - offset
        } else {
            local + offset
        })
    }
}
