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

#[target_feature(enable = "avx2")]
unsafe fn masks(bytes: &[u8]) -> (u32, u32, u32) {
    // Caller supplies 32 readable bytes after AVX2 dispatch.
    unsafe {
        let x = _mm256_loadu_si256(bytes.as_ptr().cast());
        let mask = |byte| _mm256_movemask_epi8(_mm256_cmpeq_epi8(x, _mm256_set1_epi8(byte))) as u32;
        let lf = mask(10);
        (mask(44) | lf, lf, mask(34))
    }
}

pub fn rows<const N: usize>(data: &[u8], emit: impl FnMut(&[&[u8]; N])) {
    use std::sync::OnceLock;
    static FAST: OnceLock<bool> = OnceLock::new();
    if *FAST.get_or_init(|| {
        is_x86_feature_detected!("avx2")
            && is_x86_feature_detected!("bmi2")
            && is_x86_feature_detected!("popcnt")
            && std::env::var_os("SHOPPING_FORCE_SCALAR").is_none()
    }) {
        unsafe { rows_vector(data, emit) }
    } else {
        rows_body::<N, false>(data, emit)
    }
}

#[target_feature(enable = "avx2,bmi2,popcnt")]
unsafe fn rows_vector<const N: usize>(mut data: &[u8], mut emit: impl FnMut(&[&[u8]; N])) {
    // Two 64-byte mask words cover a typical record. The chunk parser below
    // handles arbitrarily long records and the unpadded end of the mapping.
    while data.len() >= 128 {
        let (s0, l0, q0) = unsafe { mask64(data) };
        let (seps, lf, quotes) = if l0 != 0 {
            (s0 as u128, l0 as u128, q0 as u128)
        } else {
            let (s1, l1, q1) = unsafe { mask64(&data[64..]) };
            if l1 == 0 {
                let end = data[128..]
                    .iter()
                    .position(|&v| v == 10)
                    .map_or(data.len(), |p| 129 + p);
                rows_body::<N, true>(&data[..end], &mut emit);
                data = &data[end..];
                continue;
            }
            (
                s0 as u128 | ((s1 as u128) << 64),
                (l1 as u128) << 64,
                q0 as u128 | ((q1 as u128) << 64),
            )
        };
        let end = lf.trailing_zeros() as usize;
        let before = (lf & lf.wrapping_neg()) - 1;
        let mut ds = seps & (before | (before + 1));
        let q = quotes & before;
        // Each quote must touch exactly one field boundary. Opening and
        // closing quote flags must also match for each field, in order.
        if ds.count_ones() == N as u32 && q & !(((ds << 1) | 1) ^ (ds >> 1)) == 0 {
            let extract = |v: u128| unsafe {
                _pext_u64(v as u64, ds as u64)
                    | (_pext_u64((v >> 64) as u64, (ds >> 64) as u64) << (ds as u64).count_ones())
            };
            let mut wraps = ((extract(q >> 1) << 1) | (q as u64 & 1)) & ((1 << N) - 1);
            if wraps == extract(q << 1) {
                let mut fields = [&data[..0]; N];
                let mut a = 0;
                for field in &mut fields {
                    let b = ds.trailing_zeros() as usize;
                    let trim = wraps as usize & 1;
                    // Masks above prove each trimmed field has two distinct quotes.
                    *field = &data[a + trim..b - trim];
                    a = b + 1;
                    ds &= ds - 1;
                    wraps >>= 1;
                }
                emit(&fields);
            }
        }
        data = &data[end + 1..];
    }
    rows_body::<N, true>(data, emit);
}

#[inline]
#[target_feature(enable = "avx2")]
unsafe fn mask64(data: &[u8]) -> (u64, u64, u64) {
    let (a, b, c) = unsafe { masks(data) };
    let (d, e, f) = unsafe { masks(&data[32..]) };
    (
        a as u64 | ((d as u64) << 32),
        b as u64 | ((e as u64) << 32),
        c as u64 | ((f as u64) << 32),
    )
}

#[inline(always)]
fn rows_body<const N: usize, const FAST: bool>(data: &[u8], mut emit: impl FnMut(&[&[u8]; N])) {
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
        let (mut delimiters, lf, mut quote_mask) = if FAST {
            unsafe { masks(block) }
        } else {
            let mut comma = 0;
            let mut lf = 0;
            let mut quote = 0;
            for (i, &v) in block.iter().enumerate() {
                match v {
                    44 => comma |= 1 << i,
                    10 => lf |= 1 << i,
                    34 => quote |= 1 << i,
                    _ => (),
                }
            }
            (comma | lf, lf, quote)
        };
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
    // Ingest time is guaranteed to be a valid, whole-second UTC timestamp.
    #[inline]
    pub fn ingest(&mut self, s: &[u8]) -> i64 {
        let hour = 10 * s[11] as i64 + s[12] as i64 - 528;
        let minute = 10 * s[14] as i64 + s[15] as i64 - 528;
        let second = 10 * s[17] as i64 + s[18] as i64 - 528;
        self.day(s).unwrap() + ((hour * 60 + minute) * 60 + second) * 1000
    }
    #[inline]
    fn epoch(s: &[u8]) -> Option<i64> {
        // Decode a full eight-digit word with three pairwise multiply-adds.
        // Other lengths, including arbitrary leading zeros, use checked decimal.
        if s.len() == 13 {
            let w = u64::from_le_bytes(s[..8].try_into().unwrap());
            let raw = w ^ 0x3030303030303030;
            if (raw & 0xf0f0f0f0f0f0f0f0) != 0
                || (raw.wrapping_add(0x0606060606060606) & 0x1010101010101010) != 0
            {
                return None;
            }
            let pairs = (raw.wrapping_mul(10) + (raw >> 8)) & 0x00ff00ff00ff00ff;
            let quads = (pairs.wrapping_mul(100) + (pairs >> 16)) & 0x0000ffff0000ffff;
            let high = (quads * 10000 + (quads >> 32)) as u32;
            let mut low = 0;
            for &v in &s[8..] {
                let d = v.wrapping_sub(48);
                if d > 9 {
                    return None;
                }
                low = low * 10 + d as i64;
            }
            return Some(high as i64 * 100000 + low);
        }
        decimal(s, i64::MAX as u64).map(|v| v as i64)
    }
    #[inline]
    pub fn stamp(&mut self, s: &[u8]) -> Option<i64> {
        if (s.len() != 20 && s.len() != 25) || s[4] != 45 {
            return Self::epoch(s);
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

#[cfg(test)]
mod tests {
    use super::*;
    fn baseline<const N: usize>(data: &[u8]) -> Vec<Vec<Vec<u8>>> {
        let mut records = Vec::new();
        for line in data.split(|&c| c == 10) {
            let mut fields = Vec::new();
            for field in line.split(|&c| c == 44) {
                let raw = if field.len() >= 2
                    && field.first() == Some(&34)
                    && field.last() == Some(&34)
                {
                    &field[1..field.len() - 1]
                } else {
                    field
                };
                if raw.contains(&34) {
                    fields.clear();
                    break;
                }
                fields.push(raw.to_vec());
            }
            if fields.len() == N {
                records.push(fields);
            }
        }
        records
    }
    fn check<const N: usize>(data: &[u8]) {
        let expected = baseline::<N>(data);
        let mut fast = Vec::new();
        rows::<N>(data, |f| {
            fast.push(f.iter().map(|v| v.to_vec()).collect::<Vec<_>>())
        });
        assert_eq!(fast, expected);
        let mut scalar = Vec::new();
        rows_body::<N, false>(data, |f| {
            scalar.push(f.iter().map(|v| v.to_vec()).collect::<Vec<_>>())
        });
        assert_eq!(scalar, expected);
    }
    #[test]
    fn quotes_and_delimiters_at_vector_boundaries() {
        let mut data = Vec::new();
        for n in 0..160 {
            let prefix = vec![b'x'; n];
            for suffix in [
                b",b,c,d\n".as_slice(),
                b"\",b,c,d\n",
                b",\"\",\"c\",d\n",
                b",\",c,d\n",
                b",x\"y,c,d\n",
                b",a,b,c,d,e,f\n",
            ] {
                data.extend_from_slice(&prefix);
                data.extend_from_slice(suffix);
            }
        }
        data.extend_from_slice(b"\"a\",b,c,d");
        check::<2>(&data);
        check::<4>(&data);
        check::<7>(&data);
    }
    #[test]
    fn epoch_word_parser_matches_checked_digits() {
        let mut date = Dates::new();
        for i in 0..13 {
            let mut input = *b"1791071890000";
            for v in 0..=255u8 {
                input[i] = v;
                assert_eq!(
                    date.stamp(&input),
                    decimal(&input, i64::MAX as u64).map(|n| n as i64)
                );
            }
        }
    }
}
