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
    jobs_finish(items, threads, init, process, |_, state| state)
}
pub fn jobs_finish<T: Send, R: Send, S: Send>(
    items: Vec<T>,
    threads: usize,
    init: impl Fn(usize) -> R + Sync,
    process: impl Fn(&mut R, T) + Sync,
    finish: impl Fn(usize, R) -> S + Sync,
) -> Vec<S> {
    let workers = threads.min(items.len()).max(1);
    let slots: Vec<_> = items.into_iter().map(|x| Mutex::new(Some(x))).collect();
    let next = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        let handles: Vec<_> = (0..workers)
            .map(|i| {
                let (init, process, finish, slots, next) =
                    (&init, &process, &finish, &slots, &next);
                scope.spawn(move || {
                    let mut result = init(i);
                    loop {
                        let index = next.fetch_add(1, Ordering::Relaxed);
                        let Some(slot) = slots.get(index) else { break };
                        let value = slot.lock().unwrap().take().unwrap();
                        process(&mut result, value);
                    }
                    finish(i, result)
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

struct Source {
    mapping: Mmap,
    buffered: Option<File>,
}

pub struct Input {
    files: Vec<Source>,
    read_ahead: usize,
    release_window: usize,
    step: usize,
    populate_chunks: bool,
    large_first: bool,
    stripe: usize,
}

const STEP: usize = 2 << 20;
const OVERLAP: usize = 4096;

pub struct ReadBuffer(Option<memmap2::MmapMut>);
impl ReadBuffer {
    pub fn new() -> Self {
        Self(None)
    }

    fn bytes(&mut self, size: usize) -> Option<&mut [u8]> {
        if self.0.as_ref().is_none_or(|buf| buf.len() < size) {
            let buf = memmap2::MmapMut::map_anon(size).ok()?;
            let _ = buf.advise(memmap2::Advice::HugePage);
            self.0 = Some(buf);
        }
        Some(&mut self.0.as_mut()?[..size])
    }
}

fn mib(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
        .min(64) << 20
}

fn is_cold(mapping: &Mmap) -> bool {
    if mapping.len() < STEP {
        return false;
    }
    // Sample distinct cache pages without faulting them in. A failed probe
    // leaves the scan on the ordinary mapped reader.
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page <= 0 {
        return false;
    }
    let page = page as usize;
    [1, 2, 3].into_iter().filter(|&quarter| {
        let offset = (mapping.len() / 4 * quarter) / page * page;
        let mut resident = 0;
        let ptr = unsafe { mapping.as_ptr().add(offset) };
        let status = unsafe { libc::mincore(ptr.cast_mut().cast(), page, &mut resident) };
        status == 0 && resident & 1 == 0
    }).count() >= 2
}

impl Input {
    pub fn open(path: &Path) -> Self {
        Self::open_mode(path, false)
    }

    pub fn open_for_scan(path: &Path) -> Self {
        Self::open_mode(path, true)
    }

    fn open_mode(path: &Path, scan: bool) -> Self {
        use std::os::fd::AsRawFd;
        let mut paths = if path.is_dir() {
            fs::read_dir(path)
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .filter(|path| path.extension().is_some_and(|ext| ext == "csv"))
                .collect::<Vec<_>>()
        } else {
            vec![path.to_owned()]
        };
        paths.sort_unstable();
        let strategy = std::env::var("SHOPPING_MAP_POLICY").unwrap_or_default();
        let huge = scan && strategy == "huge";
        let populate = scan && strategy == "populate";
        let populate_chunks = scan && strategy == "parallel";
        let stripe = if scan { mib("SHOPPING_INPUT_STRIPE_MIB", 0) } else { 0 };
        let large_first = scan && (stripe != 0 || std::env::var_os("SHOPPING_INPUT_LARGE_FIRST").is_some());
        let mut mappings = Vec::with_capacity(paths.len());
        for path in paths {
            let file = File::open(path).unwrap();
            if file.metadata().unwrap().len() != 0 {
                // The scorer mounts immutable input files.
                let mut options = memmap2::MmapOptions::new();
                if populate {
                    options.populate();
                }
                let mapping = unsafe { options.map(&file).unwrap() };
                mappings.push((file, mapping));
            }
        }
        if large_first {
            mappings.sort_by_key(|(_, map)| std::cmp::Reverse(map.len()));
        }
        let cold = scan
            && mappings.iter().max_by_key(|(_, map)| map.len())
                .is_some_and(|(_, map)| is_cold(map));
        let buffered_step = if scan && (cold || std::env::var_os("SHOPPING_BUFFER_ALWAYS").is_some()) {
            mib("SHOPPING_BUFFER_MIB", 0)
        } else {
            0
        };
        let read_ahead = if cold || std::env::var_os("SHOPPING_WARM_SKIP_PREFETCH").is_none() {
            mib("SHOPPING_READAHEAD_MIB", 8)
        } else {
            0
        };
        Self {
            files: mappings.into_iter().map(|(file, mapping)| {
                let _ = mapping.advise(memmap2::Advice::Sequential);
                if huge || (scan && cold && strategy == "huge-cold") {
                    let _ = mapping.advise(memmap2::Advice::HugePage);
                }
                let buffered = (buffered_step != 0).then(|| {
                    unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_SEQUENTIAL) };
                    file
                });
                Source { mapping, buffered }
            }).collect(),
            read_ahead,
            release_window: mib("SHOPPING_RELEASE_MIB", 8),
            step: if buffered_step == 0 && scan {
                mib("SHOPPING_CHUNK_MIB", STEP >> 20).max(1 << 20)
            } else if buffered_step == 0 {
                STEP
            } else {
                buffered_step
            },
            populate_chunks,
            large_first,
            stripe,
        }
    }

    pub fn bytes(&self) -> usize {
        self.files.iter().map(|source| source.mapping.len()).sum()
    }

    pub fn release(self) {
        self.release_pages();
    }

    pub fn release_pages(&self) {
        // Clear private page-table entries in smaller ranges to limit how
        // long the process's mapping lock is held while reducers run.
        let window = self.release_window;
        for source in &self.files {
            let file = &source.mapping;
            if window != 0 && source.buffered.is_none() && file.len() > window {
                for off in (0..file.len()).step_by(window) {
                    // The read-only scanner has joined and cannot use this range.
                    unsafe {
                        let _ = file.unchecked_advise_range(
                            memmap2::UncheckedAdvice::DontNeed, off, (file.len() - off).min(window),
                        );
                    }
                }
            }
        }
    }

    pub fn prefetch(&self, lane: usize, lanes: usize) {
        use std::os::fd::AsRawFd;
        let window = self.read_ahead;
        if window == 0 {
            return;
        }
        for source in self.files.iter().skip(lane).step_by(lanes) {
            let file = &source.mapping;
            for off in (0..file.len()).step_by(window) {
                let len = (file.len() - off).min(window);
                if let Some(fd) = &source.buffered {
                    unsafe {
                        libc::posix_fadvise(fd.as_raw_fd(), off as i64, len as i64, libc::POSIX_FADV_WILLNEED);
                    }
                } else {
                    let _ = file.advise_range(memmap2::Advice::WillNeed, off, len);
                }
            }
        }
    }

    pub fn chunks(&self) -> Vec<Chunk<'_>> {
        let mut chunks: Vec<_> = self.files.iter().flat_map(|source| {
            let file = &source.mapping;
            (0..file.len()).step_by(self.step).map(|start| Chunk {
                file,
                buffered: source.buffered.as_ref(),
                start,
                populate: self.populate_chunks,
                end: (start + self.step).min(file.len()),
            })
        }).collect();
        if self.stripe != 0 {
            chunks.sort_by_key(|chunk| (
                chunk.start / self.stripe,
                std::cmp::Reverse(chunk.file.len()),
                chunk.start,
            ));
        } else if self.large_first {
            chunks.sort_by_key(|chunk| std::cmp::Reverse(chunk.end - chunk.start));
        }
        chunks
    }
}

#[derive(Clone, Copy)]
pub struct Chunk<'a> {
    file: &'a [u8],
    buffered: Option<&'a File>,
    start: usize,
    end: usize,
    populate: bool,
}
impl<'a> Chunk<'a> {
    pub fn full_bytes<'b>(&'b self, buf: &'b mut ReadBuffer) -> &'b [u8] {
        use std::os::unix::fs::FileExt;
        if let Some(fd) = self.buffered {
            if let Some(bytes) = buf.bytes(self.end - self.start) {
                if fd.read_exact_at(bytes, self.start as u64).is_ok() {
                    return bytes;
                }
            }
        }
        self.populate_range();
        &self.file[self.start..self.end]
    }

    pub fn read<'b>(&'b self, buf: &'b mut ReadBuffer) -> &'b [u8] {
        use std::os::unix::fs::FileExt;
        if let Some(fd) = self.buffered {
            let remaining = self.file.len() - self.start;
            let body = self.end - self.start;
            let wanted = remaining.min(body + OVERLAP);
            if let Some(bytes) = buf.bytes(wanted) {
                if fd.read_at(bytes, self.start as u64).ok() == Some(wanted) {
                    if let Some(rows) = owned_records(bytes, body, wanted == remaining) {
                        return rows;
                    }
                }
            }
        }
        // A short read or a boundary row longer than the overlap uses mmap.
        self.records()
    }

    pub fn records(self) -> &'a [u8] {
        self.populate_range();
        owned_records(&self.file[self.start..], self.end - self.start, true).unwrap()
    }

    fn populate_range(&self) {
        if self.populate {
            // The mapping base and chunk starts are page-aligned. The tail
            // can be short because the kernel rounds the advised size up.
            let range = &self.file[self.start..self.end];
            unsafe {
                let _ = libc::madvise(range.as_ptr().cast_mut().cast(), range.len(),
                                      libc::MADV_POPULATE_READ);
            }
        }
    }
}

// A row belongs to the chunk containing its preceding LF. The virtual start of
// each file has no LF, so the header is skipped. The last row may end at EOF.
fn owned_records(bytes: &[u8], body: usize, at_eof: bool) -> Option<&[u8]> {
    let Some(header) = bytes[..body].iter().position(|&c| c == b'\n') else {
        return Some(&[]);
    };
    let stop = match bytes[body..].iter().position(|&c| c == b'\n') {
        Some(extra) => body + extra + 1,
        None if at_eof => bytes.len(),
        None => return None,
    };
    Some(&bytes[header + 1..stop])
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
        let (mut lo, mut hi, qlo, qhi, end) = if l0 != 0 {
            let live = l0 ^ l0.wrapping_sub(1);
            (s0 & live, 0, q0 & live, 0, l0.trailing_zeros() as usize)
        } else {
            let (a, b, c) = unsafe { masks(&data[64..]) };
            let (s1, l1, q1) = if b != 0 {
                (a as u64, b as u64, c as u64)
            } else {
                let (d, e, f) = unsafe { masks(&data[96..]) };
                (
                    a as u64 | ((d as u64) << 32),
                    b as u64 | ((e as u64) << 32),
                    c as u64 | ((f as u64) << 32),
                )
            };
            if l1 == 0 {
                let end = data[128..]
                    .iter()
                    .position(|&b| b == 10)
                    .map_or(data.len(), |p| 129 + p);
                rows_body::<N, true>(&data[..end], &mut emit);
                data = &data[end..];
                continue;
            }
            let live = l1 ^ l1.wrapping_sub(1);
            (
                s0,
                s1 & live,
                q0,
                q1 & live,
                64 + l1.trailing_zeros() as usize,
            )
        };
        let nlo = lo.count_ones();
        if nlo + hi.count_ones() == N as u32 {
            let mut wraps = 0;
            let mut valid = true;
            if qlo | qhi != 0 {
                let left_lo = (lo << 1) | 1;
                let right_lo = (lo >> 1) | (hi << 63);
                let left_hi = (hi << 1) | (lo >> 63);
                let right_hi = hi >> 1;
                valid = (qlo & !(left_lo ^ right_lo)) | (qhi & !(left_hi ^ right_hi)) == 0;
                let gather = |l, h| unsafe { _pext_u64(l, lo) | (_pext_u64(h, hi) << nlo) };
                wraps = ((gather((qlo >> 1) | (qhi << 63), qhi >> 1) << 1) | (qlo & 1))
                    & ((1 << N) - 1);
                valid &= wraps == gather(qlo << 1, (qhi << 1) | (qlo >> 63));
            }
            if valid {
                let mut fields = [&data[..0]; N];
                let mut start = 0;
                // The quote masks prove that each pair belongs to one nonempty
                // field. Each bit below denotes a real separator in this row.
                macro_rules! field {
                    ($i:expr,$m:ident,$base:expr) => {{
                        let at = $base + $m.trailing_zeros() as usize;
                        let trim = ((wraps >> $i) & 1) as usize;
                        fields[$i] = unsafe { data.get_unchecked(start + trim..at - trim) };
                        start = at + 1;
                        $m &= $m - 1;
                    }};
                }
                // Keep the low and high bit scans independent on long records.
                if nlo as usize == N {
                    for i in 0..N {
                        field!(i, lo, 0);
                    }
                } else if N == 7 && nlo == 4 {
                    for i in 0..4 {
                        field!(i, lo, 0);
                    }
                    for i in 4..7 {
                        field!(i, hi, 64);
                    }
                } else if N == 7 && nlo == 3 {
                    for i in 0..3 {
                        field!(i, lo, 0);
                    }
                    for i in 3..7 {
                        field!(i, hi, 64);
                    }
                } else {
                    for i in 0..nlo as usize {
                        field!(i, lo, 0);
                    }
                    for i in nlo as usize..N {
                        field!(i, hi, 64);
                    }
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
    #[inline(always)]
    fn day(&mut self, s: &[u8]) -> Option<i64> {
        let key: &[u8; 10] = s[..10].try_into().unwrap();
        let index = (s[9] as usize + 11 * s[8] as usize + 33 * s[6] as usize) & 127;
        let entry = &mut self.slots[index];
        if entry.0 == *key && entry.1 != INVALID {
            return Some(entry.1);
        }
        Self::fill_day(key, entry)
    }
    #[cold]
    fn fill_day(key: &[u8; 10], entry: &mut ([u8; 10], i64)) -> Option<i64> {
        if key[4] != 45 || key[7] != 45 {
            return None;
        }
        let day = civil_day(
            two(key, 0)? * 100 + two(key, 2)?,
            two(key, 5)?,
            two(key, 8)?,
        )? * DAY;
        *entry = (*key, day);
        Some(day)
    }
    #[inline(always)]
    pub fn event_times(&mut self, event: &[u8], ingest: &[u8]) -> (Option<i64>, i64) {
        let day = self.day(ingest).unwrap();
        let arrival = day + Self::hms::<false>(ingest).unwrap();
        let instant = if event.len() == 20
            && event[10] == b'T'
            && event[19] == b'Z'
            && event[..10] == ingest[..10]
        {
            Self::hms::<true>(event).map(|millis| day + millis)
        } else {
            self.stamp(event)
        };
        (instant, arrival)
    }
    #[inline(always)]
    fn hms<const CHECK: bool>(s: &[u8]) -> Option<i64> {
        let word =
            u64::from_le_bytes(s[11..19].try_into().unwrap()) ^ u64::from_le_bytes(*b"00:00:00");
        const DIGITS: u64 = 0x0f0f000f0f000f0f;
        if CHECK
            && (word & !DIGITS != 0
                || word.wrapping_add(0x0606000606000606) & 0x1010001010001010 != 0)
        {
            return None;
        }
        let pairs = (word.wrapping_mul(10).wrapping_add(word >> 8)) & 0xff0000ff0000ff;
        if CHECK && pairs.wrapping_add((68u64 << 48) | (68u64 << 24) | 104) & 0x80000080000080 != 0
        {
            return None;
        }
        let seconds = (pairs & 255) * 3600 + ((pairs >> 24) & 255) * 60 + (pairs >> 48);
        Some(seconds as i64 * 1000)
    }
    #[inline(always)]
    fn digits8(w: u64) -> Option<u32> {
        let raw = w ^ 0x3030303030303030;
        if (raw & 0xf0f0f0f0f0f0f0f0) != 0
            || (raw.wrapping_add(0x0606060606060606) & 0x1010101010101010) != 0
        {
            return None;
        }
        let pairs = (raw.wrapping_mul(10).wrapping_add(raw >> 8)) & 0x00ff00ff00ff00ff;
        let quads = (pairs.wrapping_mul(100).wrapping_add(pairs >> 16)) & 0x0000ffff0000ffff;
        Some(quads.wrapping_mul(10000).wrapping_add(quads >> 32) as u32)
    }
    #[inline(always)]
    fn epoch(s: &[u8]) -> Option<i64> {
        // Decode a full eight-digit word with three pairwise multiply-adds.
        // Other lengths, including arbitrary leading zeros, use checked decimal.
        if s.len() == 13 {
            let high = Self::digits8(u64::from_le_bytes(s[..8].try_into().unwrap()))?;
            let last = u64::from_le_bytes(s[5..].try_into().unwrap());
            let low = Self::digits8((last & !0xffffff) | 0x303030)?;
            return Some(high as i64 * 100000 + low as i64);
        }
        decimal(s, i64::MAX as u64).map(|v| v as i64)
    }
    #[inline(always)]
    pub fn stamp(&mut self, s: &[u8]) -> Option<i64> {
        if (s.len() != 20 && s.len() != 25) || s[4] != 45 {
            return Self::epoch(s);
        }
        if s[10] != 84 {
            return None;
        }
        let local = self.day(s)? + Self::hms::<true>(s)?;
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
    fn lazy_ranges_preserve_physical_rows() {
        for file in [
            &b"header\nabc\nx\nlast"[..],
            b"h\n\n\n",
            b"header only",
            b"",
            b"a long header spans chunks\nthen a really long row\nshort\n",
            b"\n\n",
        ] {
            let expected = file
                .iter()
                .position(|&b| b == 10)
                .map_or(&file[0..0], |n| &file[n + 1..]);
            for step in 1..=file.len() + 1 {
                let got: Vec<_> = (0..file.len())
                    .step_by(step)
                    .flat_map(|start| {
                        Chunk {
                            buffered: None,
                            populate: false,
                            file,
                            start,
                            end: (start + step).min(file.len()),
                        }
                        .records()
                    })
                    .copied()
                    .collect();
                assert_eq!(got, expected, "step {step}");
            }
        }
    }
    #[test]
    fn bounded_reads_preserve_long_boundary_rows_and_eof() {
        let mut bytes = b"header\n".to_vec();
        bytes.extend(std::iter::repeat_n(b'x', OVERLAP + 64));
        bytes.extend_from_slice(b"\nfirst\nlast");
        for step in [1, 15, 64, OVERLAP, bytes.len()] {
            let mut actual = Vec::new();
            for start in (0..bytes.len()).step_by(step) {
                let body = step.min(bytes.len() - start);
                let wanted = (body + 8).min(bytes.len() - start);
                let block = &bytes[start..start + wanted];
                let eof = start + wanted == bytes.len();
                let rows = owned_records(block, body, eof)
                    .unwrap_or_else(|| owned_records(&bytes[start..], body, true).unwrap());
                actual.extend_from_slice(rows);
            }
            assert_eq!(actual, &bytes[7..]);
        }
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
    fn time_of_day_word_validation() {
        let mut word = *b"2001-02-03T12:34:56Z";
        for i in 11..19 {
            let old = word[i];
            for byte in 0..=255 {
                word[i] = byte;
                let expected = (|| {
                    if word[13] != b':' || word[16] != b':' {
                        return None;
                    }
                    let h = two(&word, 11)?;
                    let m = two(&word, 14)?;
                    let s = two(&word, 17)?;
                    (h < 24 && m < 60 && s < 60).then_some(((h * 60 + m) * 60 + s) * 1000)
                })();
                assert_eq!(Dates::hms::<true>(&word), expected);
            }
            word[i] = old;
        }
    }
    #[test]
    fn variable_length_rows_match_scalar_oracle() {
        let mut state = 5u32;
        let mut next = || {
            state = state.wrapping_mul(1664525).wrapping_add(1013904223);
            state
        };
        let mut buf = Vec::new();
        for _ in 0..6000 {
            let n = 2 + next() as usize % 8;
            for field in 0..n {
                if field > 0 {
                    buf.push(b',');
                }
                let quoted = next() % 3 == 0;
                if quoted {
                    buf.push(b'"');
                }
                for _ in 0..next() % 32 {
                    buf.push(if next() % 97 == 0 { b'"' } else { b'x' });
                }
                if quoted {
                    buf.push(b'"');
                }
            }
            buf.push(10);
        }
        check::<2>(&buf);
        check::<4>(&buf);
        check::<7>(&buf);
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
