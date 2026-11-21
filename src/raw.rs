//! Fixed-width projection for common CSV spellings. The exact reader owns
//! all rows that fail the shape, clock, or field checks.
use crate::input::{DAY, Dates, INVALID, WEEK};
use crate::keys::{Keys, mix, pack_ninth};
use crate::{ARRIVAL_BIAS, Candidate, Catalog, Click, EventSink, EventTime, SIDE};
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
        if (quotes == 0 || at.read() == b'"') && at.add(quotes).read() == delimiter {
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
        if CHECK
            && (x & !0x0f0f000f0f000f0f != 0
                || x.wrapping_add(0x0606000606000606) & 0x1010001010001010 != 0)
        {
            return INVALID;
        }
        let pairs = x.wrapping_mul(10).wrapping_add(x >> 8) & 0xff0000ff0000ff;
        if CHECK && pairs.wrapping_add((68u64 << 48) | (68u64 << 24) | 104) & 0x80000080000080 != 0
        {
            return INVALID;
        }
        ((pairs & 255) * 3600 + ((pairs >> 24) & 255) * 60 + (pairs >> 48)) as i64 * 1000
    }
}

#[inline(always)]
fn digits8(word: u64) -> u64 {
    let raw = word ^ 0x3030303030303030;
    if raw & 0xf0f0f0f0f0f0f0f0 != 0
        || raw.wrapping_add(0x0606060606060606) & 0x1010101010101010 != 0
    {
        return u64::MAX;
    }
    let pairs = raw.wrapping_mul(10).wrapping_add(raw >> 8) & 0x00ff00ff00ff00ff;
    let quads = pairs.wrapping_mul(100).wrapping_add(pairs >> 16) & 0x0000ffff0000ffff;
    (quads.wrapping_mul(10000).wrapping_add(quads >> 32)) & 0xffff_ffff
}

#[inline(always)]
unsafe fn stamp<const WIDTH: usize>(p: Ptr, calendar: &Calendar) -> i64 {
    unsafe {
        if WIDTH == 13 {
            let hi = digits8(word(p));
            let lo = digits8((word(p.add(5)) & !0xff_ffff) | 0x30_3030);
            return if hi == u64::MAX || lo == u64::MAX {
                INVALID
            } else {
                (hi * 100_000 + lo) as i64
            };
        }
        let day = calendar.day(p);
        let time = hms::<true>(p);
        if day == INVALID || time == INVALID || p.add(10).read() != b'T' {
            return INVALID;
        }
        if WIDTH == 20 {
            return if p.add(19).read() == b'Z' {
                day + time
            } else {
                INVALID
            };
        }
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
            return INVALID;
        }
        let hours = z0 * 10 + z1;
        let minutes = z2 * 10 + z3;
        if hours > 23 || minutes > 59 {
            return INVALID;
        }
        let offset = (i64::from(hours) * 60 + i64::from(minutes)) * 60_000;
        day + time + if op == b'+' { -offset } else { offset }
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
        if sep == 0 || sep & (sep - 1) != 0 {
            return false;
        }
        let qmask =
            (_mm256_movemask_epi8(_mm256_cmpeq_epi8(data, _mm256_set1_epi8(34))) as u32) & mask;
        let cut = sep.trailing_zeros();
        if cut == 0 {
            return false;
        }
        let qp = qmask & (sep - 1);
        if qp != 0 && (cut < 3 || qp != 1 | (1 << (cut - 1))) {
            return false;
        }
        let qa = qmask & !((sep - 1) | sep);
        qa == 0 || (n > cut + 2 && qa == (sep << 1) | (1 << (n - 1)))
    }
}

#[derive(Clone, Copy)]
struct State<'a> {
    clock: &'a Calendar,
    start: i64,
    end: i64,
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
        if day == INVALID || received.add(10).read() != b'T' || received.add(19).read() != b'Z' {
            return false;
        }
        let receive_ms = hms::<true>(received);
        if receive_ms == INVALID {
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
        let offset = arrival / 1000 - state.start / 1000 + ARRIVAL_BIAS;
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
) -> [usize; 2] {
    let state = State { clock, start, end };
    // Each fixed path uses the same bounds and vector dispatch.
    unsafe {
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
unsafe fn click(
    p: Ptr,
    end: Ptr,
    state: State<'_>,
    catalog: &Catalog,
    out: &mut Vec<Click>,
) -> bool {
    unsafe {
        let (mut id, mut user) = (0, 0);
        let campaign = two_ids::<true, true>(p, b',', &mut id, &mut user);
        if campaign.is_null() {
            return false;
        }
        let q = usize::from(campaign.read() == b'"');
        let label = campaign.add(q);
        if label.cast::<u16>().read_unaligned() != u16::from_le_bytes(*b"c_") {
            return false;
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
            return false;
        }
        let code = *catalog.campaigns.values.get_unchecked(number);
        if code == u32::MAX {
            return false;
        }
        let clock = suffix(label, 2 + len, q, b',');
        if clock.is_null() {
            return false;
        }
        let q = usize::from(clock.read() == b'"');
        let stamp_ptr = clock.add(q);
        if stamp_ptr > end {
            return false;
        }
        let width = end.offset_from(stamp_ptr) as usize;
        if width < q || (q == 1 && end.sub(1).read() != b'"') {
            return false;
        }
        let time = match width - q {
            13 => stamp::<13>(stamp_ptr, state.clock),
            20 => stamp::<20>(stamp_ptr, state.clock),
            25 => stamp::<25>(stamp_ptr, state.clock),
            _ => return false,
        };
        if time == INVALID {
            return false;
        }
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

pub(super) fn clicks(
    data: &[u8],
    date: &mut Dates,
    keys: &Keys,
    catalog: &Catalog,
    clock: &Calendar,
    start: i64,
    end: i64,
    out: &mut Vec<Click>,
    fast: bool,
) {
    if fast {
        // The caller selected AVX2 before enabling this scanner.
        unsafe { click_loop(data, date, keys, catalog, State { start, end, clock }, out) }
    } else {
        crate::clicks(data, date, keys, catalog, start, end, out);
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
) {
    unsafe {
        let limit = data.as_ptr().add(data.len());
        let mut p = data.as_ptr();
        while p < limit {
            let end = next_lf(p, limit);
            let next = end.add(usize::from(end < limit));
            if limit.offset_from(p) < 128
                || end.offset_from(p) < 32
                || !click(p, end, state, catalog, out)
            {
                let row = std::slice::from_raw_parts(p, next.offset_from(p) as usize);
                crate::clicks(row, date, keys, catalog, state.start, state.end, out);
            }
            p = next;
        }
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
                let (mut a, mut b) = (0, 0);
                if two_ids::<true, true>(p, b'\n', &mut a, &mut b) == next {
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
