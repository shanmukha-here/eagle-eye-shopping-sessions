//! Fixed-width projection for common CSV spellings. The exact reader owns
//! all rows that fail the shape, clock, or field checks.
use crate::input::{DAY, Dates, INVALID, WEEK};
use crate::keys::{Keys, mix, pack_ninth};
use crate::{ARRIVAL_BIAS, Candidate, Catalog, Click, Detail, EventSink, EventTime, SIDE};
use std::arch::x86_64::*;

pub(super) fn available() -> bool {
    is_x86_feature_detected!("avx2") && std::env::var_os("SHOPPING_FORCE_SCALAR").is_none()
}

type Ptr = *const u8;

#[inline(always)]
unsafe fn word(p: Ptr) -> u64 {
    unsafe { p.cast::<u64>().read_unaligned() }
}

/// Each narrow calendar lookup compares all ten bytes. A wider window, or
/// a date outside the seeded range, is handled by the incumbent.
pub(super) struct Calendar {
    days: [(u64, u16, i64); 256],
}
impl Calendar {
    pub(super) fn new(start: i64, end: i64) -> Self {
        let mut days = [(0, 0, INVALID); 256];
        let from = start.div_euclid(DAY) - 9;
        let to = end.div_euclid(DAY) + 3;
        if to - from <= 62 {
            for day in from..=to {
                let (y, m, d) = civil(day);
                if !(1..=9999).contains(&y) {
                    continue;
                }
                let text = format!("{y:04}-{m:02}-{d:02}");
                let bytes = text.as_bytes();
                let head = u64::from_le_bytes(bytes[..8].try_into().unwrap());
                let tail = u16::from_le_bytes(bytes[8..10].try_into().unwrap());
                days[index(head, tail)] = (head, tail, day * DAY);
            }
        }
        Self { days }
    }

    #[inline(always)]
    unsafe fn day(&self, p: Ptr) -> i64 {
        unsafe {
            let head = word(p);
            let tail = p.add(8).cast::<u16>().read_unaligned();
            let (a, b, ms) = *self.days.get_unchecked(index(head, tail));
            if a == head && b == tail { ms } else { INVALID }
        }
    }
}

#[inline(always)]
fn index(head: u64, tail: u16) -> usize {
    let month = (head >> 48) as usize;
    (((month & 3) << 6) | ((tail as usize & 3) << 4) | (tail as usize >> 8 & 15)) & 255
}

fn civil(days: i64) -> (i64, i64, i64) {
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    (y + i64::from(m <= 2), m, d)
}

/// At least 128 readable bytes follow the row start. These offsets are
/// bounded by the fixed widths of the candidate fields.
#[inline(always)]
unsafe fn id_field(p: Ptr, delimiter: u8, key: &mut u64, encode: bool) -> Ptr {
    unsafe {
        let q = usize::from(p.read() == b'"');
        let v = p.add(q);
        let raw = word(v);
        const HIGH: u64 = 0x8080808080808080;
        // Bytes at least ASCII '0' cannot be a comma, quote, LF or NUL.
        // Set the high bit in each lane before subtracting to prevent borrows.
        let low = (raw | HIGH).wrapping_sub(0x3030303030303030);
        if (raw | !low) & HIGH != 0 {
            return std::ptr::null();
        }
        let terminator = if q == 0 { delimiter } else { b'"' };
        let ninth = v.add(8).read();
        let wide = ninth != terminator;
        let width = 8 + usize::from(wide);
        if v.add(width).read() != terminator
            || v.add(width + q).read() != delimiter
            || (wide && ninth.wrapping_sub(b'0') > 127 - b'0')
        {
            return std::ptr::null();
        }
        if encode {
            *key = raw.swap_bytes() | pack_ninth(if wide { ninth } else { 0 });
        }
        v.add(width + q + 1)
    }
}

#[inline(always)]
unsafe fn two_ids<const FIRST: bool, const SECOND: bool>(
    p: Ptr,
    end_field: u8,
    first: &mut u64,
    second: &mut u64,
) -> Ptr {
    unsafe {
        if p.add(9).read() == b',' && p.add(19).read() == end_field {
            let raw = _mm256_loadu_si256(p.cast());
            let ordinary =
                _mm256_movemask_epi8(_mm256_cmpgt_epi8(raw, _mm256_set1_epi8(b'/' as i8)));
            const ID_BYTES: i32 = 0x1ff | (0x1ff << 10);
            if ordinary & ID_BYTES == ID_BYTES {
                if FIRST {
                    *first = word(p).swap_bytes() | pack_ninth(p.add(8).read());
                }
                if SECOND {
                    *second = word(p.add(10)).swap_bytes() | pack_ninth(p.add(18).read());
                }
                return p.add(20);
            }
        }
        let right = id_field(p, b',', first, FIRST);
        if right.is_null() {
            right
        } else {
            id_field(right, end_field, second, SECOND)
        }
    }
}

#[inline(always)]
unsafe fn suffix(field: Ptr, width: usize, quotes: usize, delimiter: u8) -> Ptr {
    unsafe {
        let at = field.add(width);
        // Both bytes are readable at every use on the guarded raw path.
        let found = at.cast::<u16>().read_unaligned();
        let mask = 0xffu16 | ((quotes as u16).wrapping_neg() << 8);
        let plain = u16::from(delimiter);
        let quoted = (plain << 8) | u16::from(b'"');
        let expected = if quotes == 0 { plain } else { quoted };
        if (found ^ expected) & mask == 0 {
            at.add(quotes + 1)
        } else {
            std::ptr::null()
        }
    }
}

#[inline(always)]
unsafe fn hms<const CHECK: bool>(p: Ptr) -> i64 {
    unsafe {
        let x = word(p.add(11)) ^ u64::from_le_bytes(*b"00:00:00");
        let valid_digits = (x & !0x0f0f000f0f000f0f
            | x.wrapping_add(0x0606000606000606) & 0x1010001010001010)
            == 0;
        let pairs = x.wrapping_mul(10).wrapping_add(x >> 8) & 0xff0000ff0000ff;
        let valid_ranges =
            pairs.wrapping_add((68u64 << 48) | (68u64 << 24) | 104) & 0x80000080000080 == 0;
        let time =
            ((pairs & 255) * 3600 + ((pairs >> 24) & 255) * 60 + (pairs >> 48)) as i64 * 1000;
        if CHECK && !(valid_digits & valid_ranges) {
            INVALID
        } else {
            time
        }
    }
}

/// Decode two already framed HH:MM:SS fields. The two 64-bit lanes use
/// the same digit, separator, and range proof as the scalar decoder.
#[inline(always)]
unsafe fn paired_hms(a: Ptr, b: Ptr) -> Option<(i64, i64)> {
    unsafe {
        let raw = _mm_set_epi64x(word(b.add(11)) as i64, word(a.add(11)) as i64);
        let x = _mm_xor_si128(raw, _mm_set1_epi64x(i64::from_le_bytes(*b"00:00:00")));
        let digit_bits = _mm_set1_epi64x(0x0f0f000f0f000f0f);
        let bad_letters = _mm_andnot_si128(digit_bits, x);
        let bad_digits = _mm_and_si128(
            _mm_add_epi64(x, _mm_set1_epi64x(0x0606000606000606)),
            _mm_set1_epi64x(0x1010001010001010),
        );
        let ten = _mm_add_epi64(_mm_slli_epi64::<3>(x), _mm_slli_epi64::<1>(x));
        let pairs = _mm_and_si128(
            _mm_add_epi64(ten, _mm_srli_epi64::<8>(x)),
            _mm_set1_epi64x(0xff0000ff0000ff),
        );
        let invalid_range = _mm_and_si128(
            _mm_add_epi64(pairs, _mm_set1_epi64x((68 << 48) | (68 << 24) | 104)),
            _mm_set1_epi64x(0x80000080000080),
        );
        let bad = _mm_or_si128(_mm_or_si128(bad_letters, bad_digits), invalid_range);
        if _mm_testz_si128(bad, bad) == 0 {
            return None;
        }
        let words = _mm_shuffle_epi8(
            pairs,
            _mm_setr_epi8(0, -1, 3, -1, 6, -1, -1, -1, 8, -1, 11, -1, 14, -1, -1, -1),
        );
        let partial = _mm_madd_epi16(words, _mm_setr_epi16(3600, 60, 1, 0, 3600, 60, 1, 0));
        let totals = _mm_hadd_epi32(partial, partial);
        Some((
            i64::from(_mm_cvtsi128_si32(totals)) * 1000,
            i64::from(_mm_extract_epi32::<1>(totals)) * 1000,
        ))
    }
}

/// A 16-byte readable span is guaranteed by the row decoder. Only the
/// first thirteen lanes are part of the timestamp.
#[inline(always)]
unsafe fn epoch_millis(p: Ptr) -> i64 {
    unsafe {
        let bytes = _mm_loadu_si128(p.cast());
        let digits = _mm_sub_epi8(bytes, _mm_set1_epi8(b'0' as i8));
        // Unsigned digits > 9 clamp to 9, including wrapped values below '0'.
        let exact = _mm_cmpeq_epi8(digits, _mm_min_epu8(digits, _mm_set1_epi8(9)));
        if _mm_movemask_epi8(exact) & 0x1fff != 0x1fff {
            return INVALID;
        }
        let digits = _mm_shuffle_epi8(
            digits, _mm_setr_epi8(0, 1, 2, 3, 4, 5, 6, 7, -1, -1, -1, 8, 9, 10, 11, 12),
        );
        let pairs = _mm_maddubs_epi16(digits, _mm_set1_epi16(0x010a));
        let quads = _mm_madd_epi16(pairs, _mm_set1_epi32(0x00010064));
        let words = _mm_packus_epi32(quads, quads);
        let parts = _mm_madd_epi16(words, _mm_set1_epi32(0x00012710));
        let parts = _mm_cvtsi128_si64(parts) as u64;
        (i64::from(parts as u32) * 100_000) + (parts >> 32) as i64
    }
}

#[inline(always)]
unsafe fn stamp<const WIDTH: usize>(p: Ptr, calendar: &Calendar) -> i64 {
    unsafe {
        if WIDTH == 13 {
            return epoch_millis(p);
        }
        let day = calendar.day(p);
        let time = hms::<true>(p);
        if (day == INVALID) | (time == INVALID) | (p.add(10).read() != b'T') {
            return INVALID;
        }
        if WIDTH == 20 {
            return if p.add(19).read() == b'Z' {
                day + time
            } else {
                INVALID
            };
        }
        offset_millis(p).map_or(INVALID, |offset| day + time + offset)
    }
}

#[inline(always)]
unsafe fn offset_millis(p: Ptr) -> Option<i64> {
    unsafe {
        let op = p.add(19).read();
        let z0 = p.add(20).read().wrapping_sub(b'0');
        let z1 = p.add(21).read().wrapping_sub(b'0');
        let z2 = p.add(23).read().wrapping_sub(b'0');
        let z3 = p.add(24).read().wrapping_sub(b'0');
        if !matches!(op, b'+' | b'-')
            || p.add(22).read() != b':'
            || z0 > 9
            || z1 > 9
            || z2 > 9
            || z3 > 9
        {
            return None;
        }
        let hours = z0 * 10 + z1;
        let minutes = z2 * 10 + z3;
        if hours > 23 || minutes > 59 {
            return None;
        }
        let offset = (i64::from(hours) * 60 + i64::from(minutes)) * 60_000;
        Some(if op == b'+' { -offset } else { offset })
    }
}

#[inline(always)]
unsafe fn next_lf(p: Ptr, limit: Ptr) -> Ptr {
    unsafe {
        let available = limit.offset_from(p) as usize;
        if available >= 128 {
            let lf = _mm256_set1_epi8(10);
            for offset in [0, 32, 64, 96] {
                let v = _mm256_loadu_si256(p.add(offset).cast());
                let mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, lf)) as u32;
                if mask != 0 {
                    return p.add(offset + mask.trailing_zeros() as usize);
                }
            }
        }
        let end = libc::memchr(p.cast(), 10, available);
        if end.is_null() { limit } else { end.cast() }
    }
}

#[inline(always)]
unsafe fn next_lf64(p: Ptr, limit: Ptr) -> Ptr {
    unsafe {
        let lf = _mm256_set1_epi8(10);
        let available = limit.offset_from(p) as usize;
        for offset in [0, 64] {
            if available < offset + 64 {
                break;
            }
            let block = p.add(offset);
            let lo = _mm256_loadu_si256(block.cast());
            let hi = _mm256_loadu_si256(block.add(32).cast());
            let low = _mm256_movemask_epi8(_mm256_cmpeq_epi8(lo, lf)) as u32;
            let high = _mm256_movemask_epi8(_mm256_cmpeq_epi8(hi, lf)) as u32;
            let ends = u64::from(low) | (u64::from(high) << 32);
            if ends != 0 {
                return block.add(ends.trailing_zeros() as usize);
            }
        }
        let end = libc::memchr(p.cast(), 10, available);
        if end.is_null() { limit } else { end.cast() }
    }
}

/// The two tail fields share one vector load. Its live prefix excludes LF.
/// Purchases go through the precise price/category reader.
#[inline(always)]
unsafe fn view_tail(p: Ptr, end: Ptr) -> bool {
    unsafe {
        let q = usize::from(p.read() == b'"');
        let t = p.add(q);
        let kind = t.cast::<u32>().read_unaligned();
        if kind != u32::from_le_bytes(*b"view") && kind != u32::from_le_bytes(*b"cart") {
            return false;
        }
        let product = suffix(t, 4, q, b',');
        if product.is_null() || end < product {
            return false;
        }
        let n = end.offset_from(product) as u32;
        if n > 32 {
            return false;
        }
        let mask = u32::MAX.checked_shr(32 - n).unwrap_or(0);
        let data = _mm256_loadu_si256(product.cast());
        let sep =
            (_mm256_movemask_epi8(_mm256_cmpeq_epi8(data, _mm256_set1_epi8(44))) as u32) & mask;
        let qmask =
            (_mm256_movemask_epi8(_mm256_cmpeq_epi8(data, _mm256_set1_epi8(34))) as u32) & mask;
        let cut = sep.trailing_zeros();
        let before_sep = sep.wrapping_sub(1);
        let qp = qmask & before_sep;
        let qa = qmask & !before_sep & !sep;
        let product_quotes = (cut >= 3) & (qp == (1 | 1u32.wrapping_shl(cut.wrapping_sub(1))));
        let price_quotes =
            (n > cut + 2) & (qa == ((sep << 1) | 1u32.wrapping_shl(n.wrapping_sub(1))));
        (sep.count_ones() == 1)
            & (cut != 0)
            & ((qp == 0) | product_quotes)
            & ((qa == 0) | price_quotes)
    }
}

#[derive(Clone, Copy)]
struct State<'a> {
    clock: &'a Calendar,
    start: i64,
    end: i64,
    // ARRIVAL_BIAS - start / 1000, the per-row offset base.
    bias: i64,
    catalog: &'a Catalog,
    keys: &'a Keys,
}

#[inline(always)]
unsafe fn decode_event<const LEVEL: usize, T: EventTime>(
    time: i64,
    received: Ptr,
    tail: Ptr,
    end: Ptr,
    state: State<'_>,
    id: u64,
    user: u64,
    emit: &mut (impl EventSink<T> + ?Sized),
) -> bool {
    unsafe {
        if time == INVALID {
            return false;
        }
        if LEVEL == 4 {
            std::hint::black_box(time);
            return true;
        }
        let day = state.clock.day(received);
        let receive_ms = hms::<true>(received);
        if (day == INVALID)
            | (receive_ms == INVALID)
            | (received.add(10).read() != b'T')
            | (received.add(19).read() != b'Z')
        {
            return false;
        }
        let arrival = day + receive_ms;
        if LEVEL == 5 {
            std::hint::black_box(arrival);
            return true;
        }
        if !view_tail(tail, end) {
            return false;
        }
        let offset = arrival / 1000 + state.bias;
        if (offset as u64) >= u64::from(SIDE) {
            return false;
        }
        let valid = time >= state.start && time < state.end && time >= arrival - DAY;
        let record = Candidate {
            id,
            user: if valid { user } else { 0 },
            arrival: offset as u32,
            t: T::pack(if valid { time } else { INVALID }, state.start),
        };
        if LEVEL >= 7 {
            emit.push(record, None);
        } else {
            std::hint::black_box(record);
        }
        true
    }
}

#[inline(always)]
unsafe fn event<const LEVEL: usize, T: EventTime>(
    p: Ptr,
    end: Ptr,
    state: State<'_>,
    emit: &mut (impl EventSink<T> + ?Sized),
) -> bool {
    unsafe {
        let (mut id, mut user) = (0, 0);
        let clock = if LEVEL == 1 {
            two_ids::<false, false>(p, b',', &mut id, &mut user)
        } else if LEVEL == 2 {
            two_ids::<true, false>(p, b',', &mut id, &mut user)
        } else {
            two_ids::<true, true>(p, b',', &mut id, &mut user)
        };
        if clock.is_null() {
            return false;
        }
        let q = usize::from(clock.read() == b'"');
        let ts = clock.add(q);
        let width = if ts.add(4).read() != b'-' {
            13
        } else if ts.add(19).read() == b'Z' {
            20
        } else {
            25
        };
        let arrival = suffix(ts, width, q, b',');
        if arrival.is_null() {
            return false;
        }
        let q = usize::from(arrival.read() == b'"');
        let received = arrival.add(q);
        let tail = suffix(received, 20, q, b',');
        if tail.is_null() || tail > end {
            return false;
        }
        if LEVEL <= 1 {
            std::hint::black_box(tail);
            return true;
        }
        let id = mix(id);
        let user = if LEVEL >= 3 { mix(user) } else { 0 };
        if LEVEL < 4 {
            std::hint::black_box((id, user, ts, received));
            return true;
        }
        emit.prefetch_user(user);
        let time = match width {
            13 => stamp::<13>(ts, state.clock),
            20 => stamp::<20>(ts, state.clock),
            _ => stamp::<25>(ts, state.clock),
        };
        decode_event::<LEVEL, T>(time, received, tail, end, state, id, user, emit)
    }
}

#[target_feature(enable = "avx2")]
unsafe fn event_loop<const LEVEL: usize, T: EventTime>(
    data: &[u8],
    date: &mut Dates,
    keys: &Keys,
    catalog: &Catalog,
    state: State<'_>,
    emit: &mut (impl EventSink<T> + ?Sized),
) -> [usize; 2] {
    unsafe {
        let limit = data.as_ptr().add(data.len());
        let mut p = data.as_ptr();
        let mut count = [0, 0];
        while p < limit {
            let end = next_lf(p, limit);
            let following = end.add(usize::from(end < limit));
            if LEVEL == 0 {
                std::hint::black_box(end);
                count[0] += 1;
            } else if limit.offset_from(p) >= 128
                && end.offset_from(p) >= 50
                && event::<LEVEL, T>(p, end, state, emit)
            {
                count[0] += 1;
            } else {
                count[1] += 1;
                if LEVEL == 7 {
                    let row = std::slice::from_raw_parts(p, following.offset_from(p) as usize);
                    crate::events(row, date, keys, catalog, state.start, state.end, emit);
                }
            }
            p = following;
        }
        count
    }
}

#[inline(always)]
unsafe fn bounded_key(p: Ptr, len: usize) -> (u64, bool) {
    unsafe {
        let q = usize::from(p.read() == b'"');
        let width = len.wrapping_sub(q * 2);
        let raw = word(p.add(q));
        let ninth = p.add(q + 8).read();
        let low = (raw | 0x8080808080808080).wrapping_sub(0x3030303030303030);
        let bad = (raw | !low) & 0x8080808080808080 != 0;
        let nine = width == 9;
        let quotes = (q == 0) | (p.add(len.saturating_sub(1)).read() == b'"');
        let valid = quotes
            & ((width == 8) | nine)
            & !bad
            & (!nine | (ninth.wrapping_sub(b'0') <= 127 - b'0'));
        // The ninth-byte table accepts any seven-bit value. The validation
        // above rejects non-ASCII bytes before this key is used.
        let extra = if nine { ninth & 127 } else { 0 };
        (raw.swap_bytes() | pack_ninth(extra), valid)
    }
}

/// Frame the two leading ID fields from the comma mask of the first 32
/// bytes. Plain and quoted IDs of eight or nine bytes take one path; any
/// other row returns a null cursor for the slower readers. At least 128
/// readable bytes follow `p`.
#[inline(always)]
unsafe fn id_pair(p: Ptr) -> (u64, u64, Ptr) {
    unsafe {
        let commas = _mm256_set1_epi8(b',' as i8);
        let bytes = _mm256_loadu_si256(p.cast());
        let head = _mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes, commas)) as u32;
        let first = head.trailing_zeros();
        let rest = head & head.wrapping_sub(1);
        let second = rest.trailing_zeros();
        // Bytes from '0' to 127, the only ones an ID may contain.
        let ordinary = _mm256_movemask_epi8(_mm256_cmpgt_epi8(bytes, _mm256_set1_epi8(b'/' as i8))) as u32;
        let quotes = _mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes, _mm256_set1_epi8(b'"' as i8))) as u32;
        let left = first.wrapping_add(1);
        let q1 = quotes & 1;
        let q2 = quotes.wrapping_shr(left) & 1;
        let w1 = first.wrapping_sub(2 * q1);
        let w2 = second.wrapping_sub(left).wrapping_sub(2 * q2);
        let s1 = q1;
        let s2 = left.wrapping_add(q2);
        let span = |width: u32, at: u32| (1u32.wrapping_shl(width).wrapping_sub(1)).wrapping_shl(at);
        let need = span(w1, s1) | span(w2, s2);
        // Content bytes are ordinary, so a quoted field has only its two
        // quotes; the closing ones remain to be proved.
        let closers = q1.wrapping_shl(first.wrapping_sub(1)) | q2.wrapping_shl(second.wrapping_sub(1));
        let ok = (second < 32)
            & (w1.wrapping_sub(8) <= 1)
            & (w2.wrapping_sub(8) <= 1)
            & (ordinary & need == need)
            & (quotes & closers == closers);
        let key = |at: u32, width: u32| {
            let v = p.add(at as usize & 31);
            let ninth = if width == 9 { v.add(8).read() } else { 0 };
            word(v).swap_bytes() | pack_ninth(ninth & 127)
        };
        (key(s1, w1), key(s2, w2), if ok { p.add(second as usize + 1) } else { std::ptr::null() })
    }
}

#[inline(always)]
unsafe fn event_prefix<T: EventTime>(
    p: Ptr,
    end: Ptr,
    state: State<'_>,
    out: &mut (impl EventSink<T> + ?Sized),
) -> bool {
    unsafe {
        let commas = _mm256_set1_epi8(b',' as i8);
        let head = _mm256_loadu_si256(p.cast());
        let next = _mm256_loadu_si256(p.add(32).cast());
        let a = _mm256_movemask_epi8(_mm256_cmpeq_epi8(head, commas)) as u32;
        let b = _mm256_movemask_epi8(_mm256_cmpeq_epi8(next, commas)) as u32;
        let mask = u64::from(a) | u64::from(b) << 32;
        let first = mask.trailing_zeros() as usize;
        let mask = mask & mask.wrapping_sub(1);
        let second = mask.trailing_zeros() as usize;
        let mask = mask & mask.wrapping_sub(1);
        let third = mask.trailing_zeros() as usize;
        if third == 64 || p.add(third) >= end {
            return false;
        }
        let (id, id_ok) = bounded_key(p, first);
        let (user, user_ok) = bounded_key(p.add(first + 1), second - first - 1);
        let clock = p.add(second + 1);
        let q = usize::from(clock.read() == b'"');
        let ts = clock.add(q);
        let len = (third - second - 1).wrapping_sub(q * 2);
        let closed = (q == 0) | (p.add(third - 1).read() == b'"');
        let arrival = p.add(third + 1);
        let q = usize::from(arrival.read() == b'"');
        let received = arrival.add(q);
        let tail = suffix(received, 20, q, b',');
        if !(id_ok & user_ok & closed) || tail.is_null() || tail > end {
            return false;
        }
        let id = mix(id);
        let user = mix(user);
        out.prefetch_user(user);
        let time = match len {
            13 => stamp::<13>(ts, state.clock),
            20 => stamp::<20>(ts, state.clock),
            25 => stamp::<25>(ts, state.clock),
            _ => return false,
        };
        decode_event::<7, T>(time, received, tail, end, state, id, user, out)
    }
}

// Prove the row from field to field and find its LF in the tail load.
// An unsupported shape leaves the cursor untouched for the existing reader.
#[inline(always)]
unsafe fn event_tail_scan<T: EventTime>(
    p: Ptr,
    limit: Ptr,
    state: State<'_>,
    keys: &Keys,
    emit: &mut (impl EventSink<T> + ?Sized),
) -> Ptr {
    unsafe {
        if limit.offset_from(p) < 128 {
            return std::ptr::null();
        }
        let (id, user, clock) = id_pair(p);
        if clock.is_null() {
            return event_opaque_tail(p, limit, state, keys, emit);
        }
        let Some(tail) = checked_event_tail(clock, limit, state) else {
            return std::ptr::null();
        };
        let valid = tail.in_window(state);
        emit.push(Candidate {
            id: mix(id),
            user: if valid { mix(user) } else { 0 },
            arrival: tail.offset,
            t: T::pack(if valid { tail.time } else { INVALID }, state.start),
        }, tail.sale(valid));
        tail.next
    }
}

struct CheckedEventTail {
    time: i64,
    arrival_ms: i64,
    offset: u32,
    next: Ptr,
    // Purchase price, or zero for a view or cart. A purchase with an
    // invalid price carries an invalid time, which makes the copy invalid.
    cents: u32,
    category: u32,
}

impl CheckedEventTail {
    #[inline(always)]
    fn in_window(&self, state: State<'_>) -> bool {
        self.time >= state.start && self.time < state.end && self.time >= self.arrival_ms - DAY
    }

    /// The side record of a valid purchase, as the exact reader stores it.
    #[inline(always)]
    fn sale(&self, valid: bool) -> Option<Detail> {
        (valid && self.cents != 0).then(|| Detail {
            arrival: self.arrival_ms / 1000,
            cents: self.cents,
            category: self.category,
            opaque: None,
        })
    }
}

/// Digits only, 1 to 10,000,000, as `input::decimal` with the nonzero rule.
/// Zero means an invalid price. Longer spellings, which can only be valid
/// with leading zeros, return None for the exact reader.
#[inline(always)]
unsafe fn price(p: Ptr, len: usize) -> Option<u32> {
    unsafe {
        if len > 8 {
            return None;
        }
        let mut value = 0u32;
        for i in 0..len {
            let d = p.add(i).read().wrapping_sub(b'0');
            if d > 9 {
                return Some(0);
            }
            value = value * 10 + u32::from(d);
        }
        Some(if value > 10_000_000 { 0 } else { value })
    }
}

// The tail reads at most 90 bytes after the first timestamp, including the
// unaligned vector containing its row ending.
#[inline(always)]
unsafe fn checked_event_tail(clock: Ptr, limit: Ptr, state: State<'_>) -> Option<CheckedEventTail> {
    unsafe { event_tail_body::<false>(clock, limit, state) }
}

// Purchases take their own instance, so price and category code stays out
// of the view and cart path.
#[inline(never)]
unsafe fn purchase_tail(clock: Ptr, limit: Ptr, state: State<'_>) -> Option<CheckedEventTail> {
    unsafe { event_tail_body::<true>(clock, limit, state) }
}

#[inline(always)]
unsafe fn event_tail_body<const PURCHASE: bool>(clock: Ptr, limit: Ptr, state: State<'_>) -> Option<CheckedEventTail> {
    unsafe {
        if limit.offset_from(clock) < 96 {
            return None;
        }
        let q = usize::from(clock.read() == b'"');
        let ts = clock.add(q);
        let width = if ts.add(4).read() != b'-' { 13 }
            else if ts.add(19).read() == b'Z' { 20 }
            else { 25 };
        let arrival = suffix(ts, width, q, b',');
        if arrival.is_null() { return None; }
        let q = usize::from(arrival.read() == b'"');
        let received = arrival.add(q);
        let tail = suffix(received, 20, q, b',');
        if tail.is_null() { return None; }
        let q = usize::from(tail.read() == b'"');
        let kind = tail.add(q);
        let kind_word = kind.cast::<u32>().read_unaligned();
        if PURCHASE {
            if kind_word != u32::from_le_bytes(*b"purc")
                || kind.add(4).cast::<u32>().read_unaligned() != u32::from_le_bytes(*b"hase")
            {
                return None;
            }
        } else if kind_word != u32::from_le_bytes(*b"view") && kind_word != u32::from_le_bytes(*b"cart") {
            return if kind_word == u32::from_le_bytes(*b"purc") { purchase_tail(clock, limit, state) } else { None };
        }
        let product = suffix(kind, if PURCHASE { 8 } else { 4 }, q, b',');
        if product.is_null() { return None; }
        let bytes = _mm256_loadu_si256(product.cast());
        let mask = |c| _mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes, _mm256_set1_epi8(c))) as u32;
        let lf = mask(b'\n' as i8);
        if lf == 0 { return None; }
        let width_tail = lf.trailing_zeros();
        // All earlier fields are proved below and cannot contain an LF.
        let live = lf ^ lf.wrapping_sub(1);
        let live = live >> 1;
        let separators = mask(b',' as i8) & live;
        let quote_bits = mask(b'"' as i8) & live;
        let cut = separators.trailing_zeros();
        // A quoted field has quotes on both of its ends and none inside, so
        // the opening quotes fix the only valid quote mask. Field quoting
        // varies row by row, so the proof uses no branches.
        let product_quoted = quote_bits & 1;
        let price_quoted = (separators << 1) & quote_bits;
        let expected = (product_quoted.wrapping_neg() & (1 | 1u32.wrapping_shl(cut.wrapping_sub(1))))
            | price_quoted
            | (u32::from(price_quoted != 0) << (width_tail.wrapping_sub(1) & 31));
        let framing_bad = (separators.count_ones() != 1) | (cut == 0)
            | (quote_bits != expected)
            | ((product_quoted != 0) & (cut < 3))
            | ((price_quoted != 0) & (width_tail <= cut + 2));
        if framing_bad {
            return None;
        }
        let day = state.clock.day(received);
        if day == INVALID || received.add(10).read() != b'T' || received.add(19).read() != b'Z' {
            return None;
        }
        let (time, arrived) = if width == 13 {
            let time = epoch_millis(ts);
            let arrival_clock = hms::<true>(received);
            if time == INVALID || arrival_clock == INVALID {
                return None;
            }
            (time, day + arrival_clock)
        } else {
            let (event_clock, arrival_clock) = paired_hms(ts, received)?;
            if ts.add(10).read() != b'T' {
                return None;
            }
            let same_day = word(ts) == word(received)
                && ts.add(8).cast::<u16>().read_unaligned()
                    == received.add(8).cast::<u16>().read_unaligned();
            let event_day = if same_day { day } else { state.clock.day(ts) };
            if event_day == INVALID {
                return None;
            }
            let zone = if width == 25 { offset_millis(ts)? } else { 0 };
            (event_day + event_clock + zone, day + arrival_clock)
        };
        let offset = arrived / 1000 + state.bias;
        if offset as u64 >= u64::from(SIDE) {
            return None;
        }
        let (mut cents, mut category) = (0, 0);
        if PURCHASE {
            // The masks above proved both field spans and their quotes.
            let (cut, width_tail) = (cut as usize, width_tail as usize);
            let quoted = usize::from(price_quoted != 0);
            cents = price(product.add(cut + 1 + quoted), width_tail - cut - 1 - 2 * quoted)?;
            if cents != 0 {
                let quoted = usize::from(product_quoted != 0);
                let name = std::slice::from_raw_parts(product.add(quoted), cut - 2 * quoted);
                category = state.catalog.products.get(state.keys, name).unwrap_or(state.catalog.unknown);
            }
        }
        Some(CheckedEventTail {
            time: if PURCHASE && cents == 0 { INVALID } else { time },
            arrival_ms: arrived,
            offset: offset as u32,
            next: product.add(width_tail as usize + 1),
            cents,
            category,
        })
    }
}

// Frame an ID field using only the available bytes. Wider fields take the
// general scanner.
#[inline]
unsafe fn read_opaque_id<'a>(p: Ptr, limit: Ptr) -> Option<(&'a [u8], Ptr)> {
    unsafe {
        if limit.offset_from(p) < 64 {
            return None;
        }
        let comma = _mm256_set1_epi8(b',' as i8);
        let quote = _mm256_set1_epi8(b'"' as i8);
        let lf = _mm256_set1_epi8(b'\n' as i8);
        let first = _mm256_loadu_si256(p.cast());
        let extract = |bytes, value| u64::from(
            _mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes, value)) as u32,
        );
        let mut commas = extract(first, comma);
        let mut quotes = extract(first, quote);
        let mut lines = extract(first, lf);
        if commas == 0 {
            let second = _mm256_loadu_si256(p.add(32).cast());
            commas = extract(second, comma) << 32;
            quotes |= extract(second, quote) << 32;
            lines |= extract(second, lf) << 32;
        }
        let end = commas.trailing_zeros() as usize;
        if end == 0 || end == 64 {
            return None;
        }
        let active = (1_u64 << end) - 1;
        if lines & active != 0 {
            return None;
        }
        let quotes = quotes & active;
        let field = if quotes == 0 {
            std::slice::from_raw_parts(p, end)
        } else if end >= 3 && quotes == 1 | (1_u64 << (end - 1)) {
            std::slice::from_raw_parts(p.add(1), end - 2)
        } else {
            return None;
        };
        Some((field, p.add(end + 1)))
    }
}

#[inline(never)]
unsafe fn event_opaque_tail<T: EventTime>(
    p: Ptr,
    limit: Ptr,
    state: State<'_>,
    keys: &Keys,
    emit: &mut (impl EventSink<T> + ?Sized),
) -> Ptr {
    unsafe {
        let Some((id, next)) = read_opaque_id(p, limit) else { return std::ptr::null(); };
        let Some((user, clock)) = read_opaque_id(next, limit) else { return std::ptr::null(); };
        let Some(tail) = checked_event_tail(clock, limit, state) else { return std::ptr::null(); };
        let valid = tail.in_window(state);
        // Long user names would miss in the shared name table. The sink
        // prefetches and resolves them in batches instead.
        let defer = valid && user.len() > 9 && emit.defers_users();
        let mut row = Candidate {
            id: 0,
            user: if valid && !defer { mix(keys.key(user)) } else { 0 },
            arrival: tail.offset,
            t: T::pack(if valid { tail.time } else { INVALID }, state.start),
        };
        let sale = tail.sale(valid);
        if id.len() > 9 {
            emit.push_opaque(row, sale.unwrap_or(Detail {
                arrival: tail.arrival_ms / 1000,
                cents: 0,
                category: 0,
                opaque: None,
            }), id, keys);
        } else {
            row.id = mix(keys.key(id));
            emit.push(row, sale);
        }
        if defer {
            emit.defer_user(crate::opaque_names::hash(user), user, keys);
        }
        tail.next
    }
}

const ROW_PREFETCH: usize = 4096;

#[target_feature(enable = "avx2")]
unsafe fn event_cursors<const N: usize, const PREFIX: bool, const TAIL_SCAN: bool, const COUNT: bool, T: EventTime>(
    data: &[u8],
    date: &mut Dates,
    keys: &Keys,
    catalog: &Catalog,
    state: State<'_>,
    emit: &mut (impl EventSink<T> + ?Sized),
) -> [usize; 2] {
    unsafe {
        let base = data.as_ptr();
        let limit = base.add(data.len());
        let mut cursor = [base; N];
        let mut ends = [limit; N];
        for i in 1..N {
            let at = base.add(data.len() * i / N);
            let lf = next_lf64(at, limit);
            cursor[i] = lf.add(usize::from(lf < limit));
            ends[i - 1] = cursor[i];
        }
        let mut count = [0, 0];
        loop {
            let mut active = false;
            for i in 0..N {
                let p = cursor[i];
                if p >= ends[i] {
                    continue;
                }
                active = true;
                // The hardware streamer stops at 4 KiB page boundaries, so
                // each new page of a mapped file would stall the decoder.
                // Prefetches past the mapping cannot fault.
                _mm_prefetch::<_MM_HINT_T0>(p.wrapping_add(ROW_PREFETCH).cast());
                if TAIL_SCAN {
                    let following = event_tail_scan::<T>(p, ends[i], state, keys, emit);
                    if !following.is_null() {
                        if COUNT { count[0] += 1; }
                        cursor[i] = following;
                        continue;
                    }
                }
                let end = next_lf64(p, ends[i]);
                let following = end.add(usize::from(end < ends[i]));
                if limit.offset_from(p) >= 128
                    && end.offset_from(p) >= 50
                    && if PREFIX {
                        event_prefix::<T>(p, end, state, emit)
                    } else {
                        event::<7, T>(p, end, state, emit)
                    }
                {
                    if COUNT { count[0] += 1; }
                } else {
                    if COUNT { count[1] += 1; }
                    let row = std::slice::from_raw_parts(p, following.offset_from(p) as usize);
                    crate::events(row, date, keys, catalog, state.start, state.end, emit);
                }
                cursor[i] = following;
            }
            if !active {
                return count;
            }
        }
    }
}

pub(super) fn events<T: EventTime>(
    data: &[u8],
    date: &mut Dates,
    keys: &Keys,
    catalog: &Catalog,
    clock: &Calendar,
    start: i64,
    end: i64,
    emit: &mut (impl EventSink<T> + ?Sized),
    level: usize,
    count_rows: bool,
) -> [usize; 2] {
    let state = State { clock, start, end, bias: ARRIVAL_BIAS - start / 1000, catalog, keys };
    // The caller has checked AVX2. Keep the probe checkpoints consistent.
    unsafe {
        if level == 7 {
            if std::env::var_os("SHOPPING_LF_IN_TAIL").is_some() {
                return if count_rows {
                    event_cursors::<1, true, true, true, T>(data, date, keys, catalog, state, emit)
                } else {
                    event_cursors::<1, true, true, false, T>(data, date, keys, catalog, state, emit)
                };
            }
            if std::env::var_os("SHOPPING_MASKED_PREFIX").is_some() {
                return event_cursors::<1, true, false, true, T>(data, date, keys, catalog, state, emit);
            }
            match std::env::var("SHOPPING_ROW_CURSORS").as_deref() {
                Ok("1") => {
                    return event_cursors::<1, false, false, true, T>(data, date, keys, catalog, state, emit);
                }
                Ok("2") => {
                    return event_cursors::<2, false, false, true, T>(data, date, keys, catalog, state, emit);
                }
                Ok("3") => {
                    return event_cursors::<3, false, false, true, T>(data, date, keys, catalog, state, emit);
                }
                _ => {}
            }
        }
        match level {
            0 => event_loop::<0, T>(data, date, keys, catalog, state, emit),
            1 => event_loop::<1, T>(data, date, keys, catalog, state, emit),
            2 => event_loop::<2, T>(data, date, keys, catalog, state, emit),
            3 => event_loop::<3, T>(data, date, keys, catalog, state, emit),
            4 => event_loop::<4, T>(data, date, keys, catalog, state, emit),
            5 => event_loop::<5, T>(data, date, keys, catalog, state, emit),
            6 => event_loop::<6, T>(data, date, keys, catalog, state, emit),
            _ => event_loop::<7, T>(data, date, keys, catalog, state, emit),
        }
    }
}

#[inline(always)]
unsafe fn click_tail(
    campaign: Ptr,
    end: Ptr,
    state: State<'_>,
    catalog: &Catalog,
) -> Option<(i64, u32)> {
    unsafe {
        // The shortest accepted tail contains c_0, a comma, and 13 digits.
        if end.offset_from(campaign) < 17 {
            return None;
        }
        let q = usize::from(campaign.read() == b'"');
        let label = campaign.add(q);
        if label.cast::<u16>().read_unaligned() != u16::from_le_bytes(*b"c_") {
            return None;
        }
        let digits = label.add(2);
        let mut len = 0;
        let mut number = 0usize;
        while len < 7 {
            let digit = digits.add(len).read().wrapping_sub(b'0');
            if digit > 9 {
                break;
            }
            len += 1;
            number = number * 10 + usize::from(digit);
        }
        if len == 0
            || (len > 1 && digits.read() == b'0')
            || number >= catalog.campaigns.values.len()
        {
            return None;
        }
        let code = *catalog.campaigns.values.get_unchecked(number);
        if code == u32::MAX {
            return None;
        }
        let clock = suffix(label, 2 + len, q, b',');
        if clock.is_null() {
            return None;
        }
        let q = usize::from(clock.read() == b'"');
        let stamp_ptr = clock.add(q);
        if stamp_ptr > end {
            return None;
        }
        let width = end.offset_from(stamp_ptr) as usize;
        if width < q || (q == 1 && end.sub(1).read() != b'"') {
            return None;
        }
        let time = match width - q {
            13 => stamp::<13>(stamp_ptr, state.clock),
            20 => stamp::<20>(stamp_ptr, state.clock),
            25 => stamp::<25>(stamp_ptr, state.clock),
            _ => return None,
        };
        if time == INVALID {
            return None;
        }
        Some((time, code))
    }
}

#[inline(always)]
unsafe fn click(
    p: Ptr,
    end: Ptr,
    limit: Ptr,
    state: State<'_>,
    keys: &Keys,
    catalog: &Catalog,
    out: &mut Vec<Click>,
    source: Option<crate::input::FieldSource<'_>>,
    users: &mut LongUsers,
) -> bool {
    unsafe {
        let (id, user, campaign) = id_pair(p);
        if campaign.is_null() {
            return click_opaque_tail(p, end, limit, state, keys, catalog, out, source, users);
        }
        let Some((time, code)) = click_tail(campaign, end, state, catalog) else {
            return false;
        };
        if time >= state.start - WEEK && time < state.end {
            out.push(Click {
                u: mix(user),
                t: time,
                id,
                campaign: code,
            });
        }
        true
    }
}

#[cold]
unsafe fn click_opaque_tail(
    p: Ptr,
    end: Ptr,
    limit: Ptr,
    state: State<'_>,
    keys: &Keys,
    catalog: &Catalog,
    out: &mut Vec<Click>,
    source: Option<crate::input::FieldSource<'_>>,
    users: &mut LongUsers,
) -> bool {
    unsafe {
        let Some((id, next)) = read_opaque_id(p, limit) else { return false; };
        let Some((user, campaign)) = read_opaque_id(next, limit) else { return false; };
        let Some((time, code)) = click_tail(campaign, end, state, catalog) else {
            return false;
        };
        if time >= state.start - WEEK && time < state.end {
            let long = user.len() > 9;
            out.push(Click {
                u: if long { 0 } else { mix(keys.key(user)) },
                t: time,
                id: keys.click_key(id, source),
                campaign: code,
            });
            if long {
                users.defer(out.len() - 1, user, keys, out);
            }
        }
        true
    }
}

// Long click users wait here for a batched name-table lookup, as in the
// event store. Spans point into the chunk being parsed.
pub(super) struct LongUsers(Vec<(usize, u64, Ptr, usize)>);

impl LongUsers {
    const BATCH: usize = 32;

    #[inline(always)]
    unsafe fn defer(&mut self, row: usize, user: &[u8], keys: &Keys, out: &mut [Click]) {
        let hash = crate::opaque_names::hash(user);
        keys.prefetch_slot(hash);
        self.0.push((row, hash, user.as_ptr(), user.len()));
        if self.0.len() == Self::BATCH {
            unsafe { self.resolve(keys, out) };
        }
    }

    #[inline(never)]
    unsafe fn resolve(&mut self, keys: &Keys, out: &mut [Click]) {
        for &(_, hash, _, _) in &self.0 {
            keys.prefetch_name(hash);
        }
        for &(row, hash, name, len) in &self.0 {
            let name = unsafe { std::slice::from_raw_parts(name, len) };
            out[row].u = mix(keys.intern_hashed(name, hash));
        }
        self.0.clear();
    }
}

pub(super) fn clicks(
    data: &[u8],
    date: &mut Dates,
    keys: &Keys,
    catalog: &Catalog,
    clock: &Calendar,
    start: i64,
    end: i64,
    out: &mut Vec<Click>,
    source: Option<crate::input::FieldSource<'_>>,
    fast: bool,
) {
    if fast {
        // The caller selected AVX2 before enabling this scanner.
        unsafe { click_loop(data, date, keys, catalog, State { start, end, bias: ARRIVAL_BIAS - start / 1000, clock, catalog, keys }, out, source) }
    } else {
        crate::clicks(data, date, keys, catalog, start, end, out, source);
    }
}

#[target_feature(enable = "avx2")]
unsafe fn click_loop(
    data: &[u8],
    date: &mut Dates,
    keys: &Keys,
    catalog: &Catalog,
    state: State<'_>,
    out: &mut Vec<Click>,
    source: Option<crate::input::FieldSource<'_>>,
) {
    unsafe {
        let limit = data.as_ptr().add(data.len());
        let mut p = data.as_ptr();
        let mut users = LongUsers(Vec::with_capacity(LongUsers::BATCH));
        while p < limit {
            let end = next_lf(p, limit);
            let next = end.add(usize::from(end < limit));
            if limit.offset_from(p) < 128
                || end.offset_from(p) < 32
                || !click(p, end, limit, state, keys, catalog, out, source, &mut users)
            {
                let row = std::slice::from_raw_parts(p, next.offset_from(p) as usize);
                crate::clicks(row, date, keys, catalog, state.start, state.end, out, source);
            }
            p = next;
        }
        users.resolve(keys, out);
    }
}

pub(super) fn links(data: &[u8], keys: &Keys, fast: bool, mut pair: impl FnMut(u64, u64)) {
    if fast {
        // The caller selected AVX2 before enabling this scanner.
        unsafe {
            link_loop(data, keys, &mut pair);
        }
    } else {
        crate::links(data, keys, pair);
    }
}

#[target_feature(enable = "avx2")]
unsafe fn link_loop(data: &[u8], keys: &Keys, pair: &mut impl FnMut(u64, u64)) {
    unsafe {
        let limit = data.as_ptr().add(data.len());
        let mut p = data.as_ptr();
        while p < limit {
            let end = next_lf(p, limit);
            let next = end.add(usize::from(end < limit));
            if limit.offset_from(p) >= 128 && end.offset_from(p) >= 17 {
                // Frame the first field from the comma mask; the second runs
                // to the LF. Field proofs reject commas and quotes inside.
                let commas = _mm256_set1_epi8(b',' as i8);
                let head = _mm256_movemask_epi8(_mm256_cmpeq_epi8(_mm256_loadu_si256(p.cast()), commas)) as u32;
                let first = head.trailing_zeros() as usize;
                let width = end.offset_from(p) as usize;
                let framed = (first < 32) & (width <= 32);
                let (a, a_ok) = bounded_key(p, first);
                let (b, b_ok) = bounded_key(p.add(first + 1), if framed { width - first - 1 } else { 0 });
                if framed & a_ok & b_ok {
                    pair(a, b);
                    p = next;
                    continue;
                }
            }
            let row = std::slice::from_raw_parts(p, next.offset_from(p) as usize);
            crate::links(row, keys, &mut *pair);
            p = next;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{INVALID, epoch_millis};

    #[test]
    fn epoch_millis_requires_thirteen_ascii_digits() {
        for millis in [0, 1, 86_400_001, 1_707_724_800_123, 9_999_999_999_999] {
            let mut bytes = format!("{millis:013}").into_bytes();
            // The SIMD load can see following CSV bytes but never parses them.
            bytes.extend_from_slice(b"\x22,\xff");
            assert_eq!(unsafe { epoch_millis(bytes.as_ptr()) }, millis);
            for i in 0..13 {
                let saved = bytes[i];
                for bad in [0, b'/', b':', b' ', b'\x22', 0xff] {
                    bytes[i] = bad;
                    assert_eq!(unsafe { epoch_millis(bytes.as_ptr()) }, INVALID);
                }
                bytes[i] = saved;
            }
        }
    }
}
