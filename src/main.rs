#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;
mod aggregate;
mod compact;
mod input;
mod keys;
mod opaque_names;
mod output;
mod pool;
mod pages;
mod raw;

use aggregate::{Click, Event, SmallEvent, Stats, Stored, Totals};
use input::{DAY, Dates, INVALID, Input, WEEK, jobs, rows};
use keys::{Aliases, IntMap, Key, Keys, People, mix};
use parquet::data_type::ByteArray;
use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
    time::Instant,
};

fn columns<T>(n: usize) -> Vec<Vec<T>> {
    (0..n).map(|_| Vec::new()).collect()
}

// Shards sized from a row estimate rarely grow, so pushes avoid copying.
fn reserved<T>(n: usize, rows: usize) -> Vec<Vec<T>> {
    let each = rows / n.max(1);
    let each = if each == 0 { 0 } else { each + each / 4 + 16 };
    (0..n).map(|_| Vec::with_capacity(each)).collect()
}

/// One dedup worker's events for every person shard, in fixed segments.
/// Segments are carved from parse buffers the worker has already reduced,
/// so most output lands in faulted-in memory instead of fresh pages the
/// kernel must zero. Segments are never freed: the process ends in `_exit`.
struct Shards<E> {
    segments: Vec<Vec<*mut E>>,
    lanes: Vec<Lane<E>>,
    spare: Vec<*mut E>,
}

/// A shard's length and, inside a segment, its next free slot, kept
/// together so the common push reads one entry.
struct Lane<E> {
    head: *mut E,
    len: usize,
}

// The raw segments are owned by exactly one worker until `into_parts`.
unsafe impl<E: Send> Send for Shards<E> {}

const SEGMENT_SHIFT: u32 = 10;
const SEGMENT: usize = 1 << SEGMENT_SHIFT;

impl<E: Copy> Shards<E> {
    fn new(n: usize) -> Self {
        Self {
            segments: (0..n).map(|_| Vec::new()).collect(),
            lanes: (0..n).map(|_| Lane { head: std::ptr::null_mut(), len: 0 }).collect(),
            spare: Vec::new(),
        }
    }

    fn count(&self) -> usize {
        self.lanes.len()
    }

    #[inline(always)]
    fn len(&self, shard: usize) -> usize {
        self.lanes[shard].len
    }

    #[inline(always)]
    fn push(&mut self, shard: usize, value: E) {
        let lane = &mut self.lanes[shard];
        if lane.len & (SEGMENT - 1) == 0 {
            return self.push_first(shard, value);
        }
        // Inside a segment the head is the slot for row `len`.
        unsafe {
            lane.head.write(value);
            lane.head = lane.head.add(1);
        }
        lane.len += 1;
    }

    #[inline(never)]
    fn push_first(&mut self, shard: usize, value: E) {
        let len = self.lanes[shard].len;
        if len >> SEGMENT_SHIFT == self.segments[shard].len() {
            let piece = self.piece();
            self.segments[shard].push(piece);
        }
        // The segment for this row exists and holds SEGMENT values.
        let slot = self.segments[shard][len >> SEGMENT_SHIFT];
        unsafe { slot.write(value) };
        self.lanes[shard] = Lane { head: unsafe { slot.add(1) }, len: len + 1 };
    }

    #[inline(always)]
    fn at(&mut self, shard: usize, row: usize) -> &mut E {
        assert!(row < self.lanes[shard].len);
        unsafe { &mut *self.segments[shard][row >> SEGMENT_SHIFT].add(row & (SEGMENT - 1)) }
    }

    fn swap_remove(&mut self, shard: usize, row: usize) {
        let last = self.lanes[shard].len - 1;
        let value = *self.at(shard, last);
        *self.at(shard, row) = value;
        let slot = self.segments[shard][last >> SEGMENT_SHIFT];
        // `head` is read only when `last` is inside a segment.
        self.lanes[shard] = Lane { head: slot.wrapping_add(last & (SEGMENT - 1)), len: last };
    }

    #[cold]
    #[inline(never)]
    fn piece(&mut self) -> *mut E {
        if let Some(piece) = self.spare.pop() {
            return piece;
        }
        const SLAB: usize = 64;
        let mut slab = std::mem::ManuallyDrop::new(Vec::<E>::with_capacity(SEGMENT * SLAB));
        let base = slab.as_mut_ptr();
        self.spare.extend((1..SLAB).map(|i| unsafe { base.add(i * SEGMENT) }));
        base
    }

    /// Keep a reduced buffer's memory for later segments. Beyond a few
    /// tasks' worth of spare segments, buffers go back to the allocator for
    /// the person stage.
    fn recycle<T>(&mut self, buffer: Vec<T>) {
        const SPARE: usize = 256;
        let step = SEGMENT * size_of::<E>();
        let bytes = buffer.capacity() * size_of::<T>();
        if align_of::<T>() < align_of::<E>() || bytes < step || self.spare.len() >= SPARE {
            return;
        }
        let mut buffer = std::mem::ManuallyDrop::new(buffer);
        let base = buffer.as_mut_ptr().cast::<u8>();
        self.spare.extend((0..bytes / step).map(|i| unsafe { base.add(i * step).cast::<E>() }));
    }

    fn into_parts(self) -> Vec<Vec<&'static mut [E]>> {
        self.segments
            .into_iter()
            .zip(self.lanes.into_iter().map(|lane| lane.len))
            .map(|(segments, len)| {
                segments
                    .into_iter()
                    .enumerate()
                    .filter_map(|(i, piece)| {
                        let n = len.saturating_sub(i * SEGMENT).min(SEGMENT);
                        // Rows below `len` were written; segments are leaked.
                        (n > 0).then(|| unsafe { std::slice::from_raw_parts_mut(piece, n) })
                    })
                    .collect()
            })
            .collect()
    }
}

fn transpose<T>(lanes: Vec<Vec<T>>, n: usize) -> Vec<Vec<T>> {
    let mut partitions = columns(n);
    for lane in lanes {
        for (out, part) in partitions.iter_mut().zip(lane) {
            out.push(part);
        }
    }
    partitions
}

struct Numbered {
    values: Vec<u32>,
    other: IntMap<u32>,
    prefix: &'static [u8],
}
impl Numbered {
    fn new(prefix: &'static [u8], len: usize) -> Self {
        Self {
            values: vec![u32::MAX; (len * 2 + 1).min(1 << 20)],
            other: IntMap::default(),
            prefix,
        }
    }
    #[inline(always)]
    fn index(&self, s: &[u8]) -> Option<usize> {
        let digits = s.strip_prefix(self.prefix)?;
        if digits.is_empty() || digits.len() > 7 || (digits.len() > 1 && digits[0] == b'0') {
            return None;
        }
        let mut result = 0usize;
        for b in digits {
            let d = b.wrapping_sub(b'0');
            if d > 9 {
                return None;
            }
            result = result * 10 + d as usize;
        }
        (result < self.values.len()).then_some(result)
    }
    fn insert(&mut self, keys: &Keys, name: &[u8], val: u32) {
        if let Some(i) = self.index(name) {
            self.values[i] = val;
        } else {
            self.other.insert(keys.key(name), val);
        }
    }
    #[inline(always)]
    fn get(&self, keys: &Keys, name: &[u8]) -> Option<u32> {
        if let Some(i) = self.index(name) {
            let v = self.values[i];
            (v != u32::MAX).then_some(v)
        } else if self.other.is_empty() {
            None
        } else {
            self.other.get(&keys.key(name)).copied()
        }
    }
}

struct Catalog {
    products: Numbered,
    categories: Vec<ByteArray>,
    unknown: u32,
    campaigns: Numbered,
    channels: Vec<(ByteArray, ByteArray)>,
}
impl Catalog {
    fn read(data: &Path, keys: &Keys) -> Self {
        // Products take a provisional category number in first-seen order;
        // after the read, numbers map to positions in the sorted names.
        let input = Input::open(&data.join("products.csv"));
        let lines = input.chunks().iter().map(|chunk| chunk.records().iter().filter(|&&c| c == b'\n').count() + 1).sum();
        let mut products = Numbered::new(b"p", lines);
        let mut seen = std::collections::HashMap::<Vec<u8>, u32, _>::with_hasher(ahash::RandomState::with_seeds(1, 2, 3, 4));
        let mut names = vec![b"unknown".to_vec()];
        seen.insert(b"unknown".to_vec(), 0);
        for chunk in input.chunks() {
            rows::<2>(chunk.records(), |v| {
                let category = match seen.get(v[1]) {
                    Some(&number) => number,
                    None => {
                        let number = names.len() as u32;
                        names.push(v[1].to_vec());
                        seen.insert(v[1].to_vec(), number);
                        number
                    }
                };
                products.insert(keys, v[0], category);
            });
        }
        let mut order: Vec<u32> = (0..names.len() as u32).collect();
        order.sort_unstable_by(|&a, &b| names[a as usize].cmp(&names[b as usize]));
        let mut position = vec![0; names.len()];
        for (at, &number) in order.iter().enumerate() {
            position[number as usize] = at as u32;
        }
        for value in products.values.iter_mut().chain(products.other.values_mut()) {
            if *value != u32::MAX {
                *value = position[*value as usize];
            }
        }
        let unknown = position[0];
        let names: Vec<Vec<u8>> = order.iter().map(|&number| std::mem::take(&mut names[number as usize])).collect();
        let mut campaign_pairs = Vec::new();
        let mut channels = vec![(ByteArray::from("organic"), ByteArray::from("organic"))];
        for chunk in Input::open(&data.join("campaigns.csv")).chunks() {
            rows::<2>(chunk.records(), |v| {
                campaign_pairs.push((v[0].to_vec(), channels.len() as u32));
                channels.push((ByteArray::from(v[0]), ByteArray::from(v[1])));
            });
        }
        let mut campaigns = Numbered::new(b"c_", campaign_pairs.len());
        for (id, index) in campaign_pairs {
            campaigns.insert(keys, &id, index);
        }
        Self {
            products,
            unknown,
            categories: names.into_iter().map(ByteArray::from).collect(),
            campaigns,
            channels,
        }
    }
}

// Most rows have no sale and an ingest date near the reporting window.
const SIDE: u32 = 1 << 31;
const ARRIVAL_BIAS: i64 = (SIDE / 2) as i64;
/// Compact nonempty identifier span. Unusually large arena spans keep
/// their full endpoints in a private side vector.
#[derive(Clone, Copy)]
struct IdSpan(std::num::NonZeroU64);

impl IdSpan {
    const FILE: u64 = 1 << 63;
    const WIDE: u64 = 1 << 62;
    const LENGTH_BITS: u32 = 30;

    fn file(handle: u64) -> Self {
        Self(std::num::NonZeroU64::new(Self::FILE | handle).unwrap())
    }

    fn arena(start: usize, len: usize, wide: &mut Vec<(usize, usize)>) -> Self {
        let bits = if start <= u32::MAX as usize && len < 1 << Self::LENGTH_BITS {
            ((start as u64) << Self::LENGTH_BITS) | len as u64
        } else {
            let at = wide.len() as u64;
            assert!(at < Self::WIDE);
            wide.push((start, start + len));
            Self::WIDE | at
        };
        Self(std::num::NonZeroU64::new(bits).unwrap())
    }

    fn bytes<'a, T>(self, bucket: &'a Bucket<T>, source: &'a Input) -> &'a [u8] {
        let bits = self.0.get();
        if bits & Self::FILE != 0 {
            return source.mapped_field(bits ^ Self::FILE);
        }
        if bits & Self::WIDE != 0 {
            let (start, end) = bucket.wide[(bits ^ Self::WIDE) as usize];
            return &bucket.ids[start..end];
        }
        let start = (bits >> Self::LENGTH_BITS) as usize;
        let len = (bits & ((1 << Self::LENGTH_BITS) - 1)) as usize;
        &bucket.ids[start..start + len]
    }
}

#[derive(Clone, Copy)]
struct Detail {
    arrival: i64,
    cents: u32,
    category: u32,
    opaque: Option<IdSpan>,
}
struct Bucket<T> {
    rows: Vec<Candidate<T>>,
    extra: Vec<Detail>,
    ids: Vec<u8>,
    wide: Vec<(usize, usize)>,
}
trait EventTime: Copy + Send {
    fn pack(t: i64, start: i64) -> Self;
    fn unpack(self, start: i64) -> Option<i64>;
}
impl EventTime for u32 {
    #[inline(always)]
    fn pack(t: i64, start: i64) -> Self {
        if t == INVALID {
            u32::MAX
        } else {
            (t - start) as u32
        }
    }
    #[inline(always)]
    fn unpack(self, start: i64) -> Option<i64> {
        (self != u32::MAX).then(|| start + self as i64)
    }
}
impl EventTime for i64 {
    #[inline(always)]
    fn pack(t: i64, _: i64) -> Self {
        t
    }
    #[inline(always)]
    fn unpack(self, _: i64) -> Option<i64> {
        (self != INVALID).then_some(self)
    }
}
#[derive(Clone, Copy)]
struct Candidate<T> {
    id: Key,
    user: Key,
    arrival: u32,
    t: T,
}

trait EventSink<T: EventTime> {
    #[inline(always)]
    fn prefetch_user(&self, _key: Key) {}

    fn push(&mut self, row: Candidate<T>, detail: Option<Detail>);

    fn push_opaque(&mut self, mut row: Candidate<T>, detail: Detail, id: &[u8], keys: &Keys) {
        row.id = mix(keys.key(id));
        self.push(row, Some(detail));
    }

    /// Whether `defer_user` may be used instead of keying a long user ID.
    #[inline(always)]
    fn defers_users(&self) -> bool {
        false
    }

    /// Set the user of the row pushed last to `mix(keys.key(user))` for a
    /// name longer than nine bytes. `hash` is its `opaque_names::hash`.
    /// The bytes must stay readable until the sink is dropped.
    fn defer_user(&mut self, _hash: u64, _user: &[u8], _keys: &Keys) {
        unreachable!("this sink keys users immediately")
    }
}

// A long user ID waiting for its batched name-table lookup.
struct PendingUser {
    bin: u32,
    row: u32,
    hash: u64,
    name: *const u8,
    len: usize,
}

const USER_BATCH: usize = 32;

/// Where a bucket's next row goes. The heads of all buckets are dense, so
/// the scatter's bookkeeping stays in L1 beside the rows being written.
struct RowHead<T> {
    rows: *mut Candidate<T>,
    len: u32,
    cap: u32,
}

struct EventStore<'a, T> {
    bins: &'a mut [Bucket<T>],
    // Rows below each head's length are written; the bucket vectors take
    // those lengths back when the store drops.
    heads: Vec<RowHead<T>>,
    mask: usize,
    source: Option<input::FieldSource<'a>>,
    keys: &'a Keys,
    last_bin: usize,
    pending: Vec<PendingUser>,
}

impl<'a, T> EventStore<'a, T> {
    fn new(bins: &'a mut [Bucket<T>], source: Option<input::FieldSource<'a>>, keys: &'a Keys) -> Self {
        assert!(bins.len().is_power_of_two());
        let heads = bins.iter_mut().map(|bin| RowHead::of(&mut bin.rows)).collect();
        Self { mask: bins.len() - 1, bins, heads, source, keys, last_bin: 0, pending: Vec::with_capacity(USER_BATCH) }
    }

    #[cold]
    #[inline(never)]
    fn grow(&mut self, bin: usize, row: Candidate<T>) {
        let rows = &mut self.bins[bin].rows;
        // The head's rows are initialized, and its length never exceeds the capacity.
        unsafe { rows.set_len(self.heads[bin].len as usize) };
        rows.push(row);
        self.heads[bin] = RowHead::of(rows);
    }

    // Slots were prefetched when each name was deferred. Fetch the published
    // names together, then intern and patch the rows.
    #[inline(never)]
    fn resolve_users(&mut self) {
        for user in &self.pending {
            self.keys.prefetch_name(user.hash);
        }
        for user in &self.pending {
            // Each span is a field of the chunk this sink is parsing.
            let name = unsafe { std::slice::from_raw_parts(user.name, user.len) };
            let key = mix(self.keys.intern_hashed(name, user.hash));
            let head = &self.heads[user.bin as usize];
            assert!(user.row < head.len);
            unsafe { (*head.rows.add(user.row as usize)).user = key };
        }
        self.pending.clear();
    }
}

impl<T> Drop for EventStore<'_, T> {
    fn drop(&mut self) {
        if !self.pending.is_empty() {
            self.resolve_users();
        }
        for (bin, head) in self.bins.iter_mut().zip(&self.heads) {
            // The store wrote every row below the head's length in place.
            unsafe { bin.rows.set_len(head.len as usize) };
        }
    }
}

impl<T> RowHead<T> {
    fn of(rows: &mut Vec<Candidate<T>>) -> Self {
        Self {
            rows: rows.as_mut_ptr(),
            len: u32::try_from(rows.len()).unwrap(),
            cap: u32::try_from(rows.capacity()).unwrap_or(u32::MAX),
        }
    }
}

impl<T> EventStore<'_, T> {

    #[inline(always)]
    fn bucket(&mut self, id: Key) -> &mut Bucket<T> {
        // new() establishes the power-of-two length, which cannot change.
        unsafe { self.bins.get_unchecked_mut(id as usize & self.mask) }
    }
}

impl<T: EventTime> EventSink<T> for EventStore<'_, T> {
    #[inline(always)]
    fn push(&mut self, mut row: Candidate<T>, detail: Option<Detail>) {
        let index = row.id as usize & self.mask;
        self.last_bin = index;
        if let Some(detail) = detail {
            let bin = self.bucket(row.id);
            row.arrival = SIDE | bin.extra.len() as u32;
            bin.extra.push(detail);
        }
        // new() made one head per bucket, and the mask is below their count.
        let head = unsafe { self.heads.get_unchecked_mut(index) };
        if head.len == head.cap {
            return self.grow(index, row);
        }
        unsafe { head.rows.add(head.len as usize).write(row) };
        head.len += 1;
    }

    #[inline(always)]
    fn defers_users(&self) -> bool {
        true
    }

    #[inline(always)]
    fn defer_user(&mut self, hash: u64, user: &[u8], keys: &Keys) {
        debug_assert!(std::ptr::eq(keys, self.keys));
        self.keys.prefetch_slot(hash);
        let bin = self.last_bin;
        self.pending.push(PendingUser {
            bin: bin as u32,
            row: self.heads[bin].len - 1,
            hash,
            name: user.as_ptr(),
            len: user.len(),
        });
        if self.pending.len() == USER_BATCH {
            self.resolve_users();
        }
    }

    fn push_opaque(&mut self, mut row: Candidate<T>, mut detail: Detail, id: &[u8], _: &Keys) {
        row.id = crate::opaque_names::hash(id);
        detail.opaque = Some(if let Some(handle) = self.source.and_then(|source| source.field_handle(id)) {
            IdSpan::file(handle)
        } else {
            let bin = self.bucket(row.id);
            let start = bin.ids.len();
            bin.ids.extend_from_slice(id);
            IdSpan::arena(start, id.len(), &mut bin.wide)
        });
        self.push(row, Some(detail));
    }
}

#[cold]
#[inline(never)]
fn events<T: EventTime>(
    chunk: &[u8],
    dates: &mut Dates,
    keys: &Keys,
    catalog: &Catalog,
    start: i64,
    end: i64,
    partitions: &mut (impl EventSink<T> + ?Sized),
) {
    rows::<7>(
        chunk,
        #[inline(always)]
        |f| {
            if f[0].is_empty() {
                return;
            }
            // mix is a permutation of all 64-bit keys (odd multipliers and
            // reversible XOR shifts). Its full value remains an exact event ID.
            let opaque = f[0].len() > 9;
            let id = if opaque { 0 } else { mix(keys.key(f[0])) };
            let (instant, ingest) = dates.event_times(f[2], f[3]);
            let mut cents = 0;
            let mut category = 0;
            let mut candidate = Candidate {
                id,
                user: 0,
                arrival: 0,
                t: T::pack(INVALID, start),
            };
            // Even a semantically invalid earliest copy must win deduplication.
            if let Some(t) = instant.filter(|&t| t >= start && t < end && t >= ingest - DAY) {
                if !f[1].is_empty() && !f[5].is_empty() {
                    let ok = match f[4] {
                        b"view" | b"cart" => true,
                        b"purchase" => match input::decimal(f[6], 10_000_000).filter(|&p| p != 0) {
                            Some(price) => {
                                cents = price as u32;
                                category =
                                    catalog.products.get(keys, f[5]).unwrap_or(catalog.unknown);
                                true
                            }
                            None => false,
                        },
                        _ => false,
                    };
                    if ok {
                        candidate.t = T::pack(t, start);
                        candidate.user = mix(keys.key(f[1]));
                    }
                }
            }
            let arrival = ingest / 1000;
            let offset = arrival - start / 1000 + ARRIVAL_BIAS;
            candidate.arrival = offset as u32;
            let detail = Detail {
                arrival,
                cents,
                category,
                opaque: None,
            };
            if opaque {
                partitions.push_opaque(candidate, detail, f[0], keys);
            } else {
                partitions.push(candidate, (cents != 0 || (offset as u64) >= u64::from(SIDE)).then_some(detail));
            }
        },
    );
}

#[derive(Default)]
struct Routes {
    repeats: IntMap<u64>,
    overflow: IntMap<(i64, u64)>,
    opaque: hashbrown::HashTable<OpaqueWinner>,
    retired: Vec<u64>,
    seen: Vec<u64>,
    collisions: Vec<u64>,
}

impl Routes {
    // Zero is a winner with no valid output. The remaining handles are
    // stable indices into this worker's destination vectors.
    #[inline(always)]
    fn handle(shard: usize, row: usize) -> u64 {
        1 + ((row as u64) << 9 | shard as u64)
    }

    #[inline(always)]
    fn index(handle: u64) -> (usize, usize) {
        let i = handle - 1;
        (i as usize & 511, (i >> 9) as usize)
    }

    fn remove_retired<E: Copy>(&mut self, out: &mut Shards<E>) {
        if self.retired.is_empty() {
            return;
        }
        self.retired.sort_unstable();
        for handle in self.retired.drain(..).rev() {
            let (shard, row) = Self::index(handle);
            out.swap_remove(shard, row);
        }
    }
}

#[derive(Clone, Copy)]
struct OpaqueWinner {
    part: usize,
    row: usize,
}

impl OpaqueWinner {
    fn candidate<T: Copy>(self, parts: &[Bucket<T>]) -> Candidate<T> {
        parts[self.part].rows[self.row]
    }

    fn detail<T: Copy>(self, parts: &[Bucket<T>]) -> Detail {
        let row = self.candidate(parts);
        parts[self.part].extra[(row.arrival ^ SIDE) as usize]
    }

    fn id<'a, T: Copy>(self, parts: &'a [Bucket<T>], source: &'a Input) -> &'a [u8] {
        self.detail(parts).opaque.unwrap().bytes(&parts[self.part], source)
    }
}

fn winners<T: EventTime, E: Stored, const PAIRED_BITS: bool>(
    parts: Vec<Bucket<T>>,
    source: &Input,
    people: &Aliases,
    out: &mut Shards<E>,
    start: i64,
    group: usize,
    routes: &mut Routes,
) {
    let count = parts.iter().map(|bin| bin.rows.len()).sum::<usize>();
    if count == 0 {
        return;
    }
    // First find unique hash positions in a cache-resident bitmap. Only IDs
    // at repeated positions require a full-key minimum-ingest reduction.
    let bits = (count * 16).max(64).next_power_of_two();
    routes.seen.clear();
    routes.collisions.clear();
    routes.seen.resize(if PAIRED_BITS { 0 } else { bits / 64 }, 0);
    routes.collisions.resize(bits / if PAIRED_BITS { 32 } else { 64 }, 0);
    let (seen, collisions) = (&mut routes.seen, &mut routes.collisions);
    let index = |key: u64| (key >> 20) as usize & (bits - 1);
    for e in parts.iter().flat_map(|bin| &bin.rows) {
        let i = index(e.id);
        if PAIRED_BITS {
            let first = 1u64 << (2 * (i % 32));
            let word = &mut collisions[i / 32];
            *word |= first | ((*word & first) << 1);
        } else {
            let mask = 1u64 << (i % 64);
            collisions[i / 64] |= seen[i / 64] & mask;
            seen[i / 64] |= mask;
        }
    }
    routes.repeats.clear();
    routes.overflow.clear();
    routes.opaque.clear();
    routes.repeats.reserve(count / 8);
    let shard_mask = out.count() - 1;
    let bias = start / 1000 - ARRIVAL_BIAS;
    for (part, bin) in parts.iter().enumerate() {
        for (block_number, block) in bin.rows.chunks(group).enumerate() {
            let mut need_alias = people.prefetch_mask(block, |row| row.user);
            for (within_block, &row) in block.iter().enumerate() {
                let possible = need_alias & 1 != 0;
                need_alias >>= 1;
                let name = || if possible { people.canonical(row.user) } else { row.user };
                let bit = (row.id >> 20) as usize & (bits - 1);
                let repeat = if PAIRED_BITS {
                    collisions[bit / 32] & (2u64 << (2 * (bit % 32))) != 0
                } else {
                    collisions[bit / 64] & (1u64 << (bit % 64)) != 0
                };
                let side = row.arrival & SIDE != 0;
                let detail = if side {
                    bin.extra[(row.arrival ^ SIDE) as usize]
                } else {
                    Detail { arrival: i64::from(row.arrival) + bias, cents: 0, category: 0, opaque: None }
                };
                let route = |u, t| E::encode(Event { u, t, cents: detail.cents, cat: detail.category }, start);
                if !repeat {
                    if let Some(t) = row.t.unpack(start) {
                        let u = name();
                        out.push(u as usize & shard_mask, route(u, t));
                    }
                    continue;
                }
                if let Some(span) = detail.opaque {
                    use hashbrown::hash_table::Entry;
                    let id = span.bytes(bin, source);
                    let candidate = OpaqueWinner { part, row: block_number * group + within_block };
                    match routes.opaque.entry(
                        row.id,
                        |winner| crate::opaque_names::same(winner.id(&parts, source), id),
                        |winner| winner.candidate(&parts).id,
                    ) {
                        Entry::Vacant(entry) => { entry.insert(candidate); }
                        Entry::Occupied(mut entry) => {
                            if detail.arrival < entry.get().detail(&parts).arrival {
                                *entry.get_mut() = candidate;
                            }
                        }
                    }
                    continue;
                }
                // Normal ingest times fit in a 32-bit interval around
                // the manifest. Large route vectors and outlying clocks use
                // full-width entries for the affected IDs.
                const EMPTY: u64 = u64::MAX;
                const WIDE: u64 = !(u32::MAX as u64);
                let base = start / 1000 - (1i64 << 31);
                let state = routes.repeats.entry(row.id).or_insert(EMPTY);
                let (old_arrival, old) = match *state {
                    EMPTY => (i64::MAX, 0),
                    WIDE => routes.overflow[&row.id],
                    compact => (base + (compact >> 32) as i64, compact & u32::MAX as u64),
                };
                if detail.arrival >= old_arrival {
                    continue;
                }
                let new = if let Some(t) = row.t.unpack(start) {
                    let u = if old != 0 {
                        let (shard, at) = Routes::index(old);
                        let previous = *out.at(shard, at).user();
                        if previous == row.user { previous } else { name() }
                    } else {
                        name()
                    };
                    let shard = u as usize & shard_mask;
                    let (previous, at) = if old == 0 { (usize::MAX, 0) } else { Routes::index(old) };
                    if previous == shard {
                        *out.at(shard, at) = route(u, t);
                        old
                    } else {
                        if old != 0 {
                            routes.retired.push(old);
                        }
                        let handle = Routes::handle(shard, out.len(shard));
                        out.push(shard, route(u, t));
                        handle
                    }
                } else {
                    if old != 0 {
                        routes.retired.push(old);
                    }
                    0
                };
                let delta = (detail.arrival - base) as u64;
                if delta < u32::MAX as u64 && new <= u32::MAX as u64 {
                    *state = delta << 32 | new;
                } else {
                    routes.overflow.insert(row.id, (detail.arrival, new));
                    *state = WIDE;
                }
            }
        }
    }
    for &winner in &routes.opaque {
        let row = winner.candidate(&parts);
        if let Some(t) = row.t.unpack(start) {
            let detail = winner.detail(&parts);
            let u = people.canonical(row.user);
            out.push(u as usize & shard_mask, E::encode(
                Event { u, t, cents: detail.cents, cat: detail.category }, start,
            ));
        }
    }
    for bin in parts {
        out.recycle(bin.rows);
        out.recycle(bin.extra);
    }
}

fn links(data: &[u8], keys: &Keys, mut pair: impl FnMut(Key, Key)) {
    rows::<2>(data, |f| {
        if !f[0].is_empty() && !f[1].is_empty() {
            pair(keys.key(f[0]), keys.key(f[1]));
        }
    });
}

fn clicks(
    data: &[u8],
    date: &mut Dates,
    keys: &Keys,
    catalog: &Catalog,
    start: i64,
    end: i64,
    out: &mut Vec<Click>,
    source: Option<input::FieldSource<'_>>,
) {
    rows::<4>(
        data,
        #[inline(always)]
        |f| {
            if f[0].is_empty() || f[1].is_empty() {
                return;
            }
            let Some(t) = date.stamp(f[3]).filter(|&t| t >= start - WEEK && t < end) else {
                return;
            };
            let Some(campaign) = catalog.campaigns.get(keys, f[2]) else {
                return;
            };
            out.push(Click {
                u: mix(keys.key(f[1])),
                t,
                id: keys.click_key(f[0], source),
                campaign,
            });
        },
    );
}

fn read_people(data: &Path, keys: &Keys, threads: usize, fast: bool) -> Aliases {
    let links = Input::open(&data.join("links"));
    if links.bytes() >= (1 << 20) && links.bytes() / 8 <= (1 << 26) && threads >= 3 {
        let graph = keys::Graph::with_capacity(links.bytes() / 10);
        let ok = jobs(
            links.chunks(),
            3,
            |_| true,
            |ok, chunk| {
                if *ok {
                    // Components do not depend on join order, so pairs wait in
                    // small batches while their vertex slots load.
                    const BATCH: usize = 32;
                    let mut batch = [(0, 0); BATCH];
                    let mut held = 0;
                    let mut good = true;
                    raw::links(chunk.records(), keys, fast, |a, b| {
                        graph.prefetch(a);
                        graph.prefetch(b);
                        batch[held] = (a, b);
                        held += 1;
                        if held == BATCH {
                            for &(a, b) in &batch {
                                good &= graph.join(keys, a, b).is_some();
                            }
                            held = 0;
                        }
                    });
                    for &(a, b) in &batch[..held] {
                        good &= graph.join(keys, a, b).is_some();
                    }
                    *ok = good;
                }
            },
        );
        if ok.into_iter().all(|x| x) {
            return graph.freeze(threads);
        }
    }
    let mut people = People::default();
    people.reserve(links.bytes() / 22);
    for chunk in links.chunks() {
        raw::links(chunk.records(), keys, fast, |a, b| people.join(keys, a, b));
    }
    drop(links);
    people.freeze()
}

fn usage() -> (f64, f64, i64, i64, i64) {
    let mut raw = std::mem::MaybeUninit::<libc::rusage>::uninit();
    let status = unsafe { libc::getrusage(libc::RUSAGE_SELF, raw.as_mut_ptr()) };
    assert_eq!(status, 0);
    let raw = unsafe { raw.assume_init() };
    let seconds = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 * 1e-6;
    (
        seconds(raw.ru_utime),
        seconds(raw.ru_stime),
        raw.ru_minflt,
        raw.ru_majflt,
        raw.ru_maxrss,
    )
}

fn io_boundary() -> Option<u128> {
    env::var_os("SHOPPING_IO_TRACE").map(|_| {
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    })
}
fn io_mark(name: &str, value: u128) {
    eprintln!("boundary={name} realtime_ns={value}");
}
fn main() {
    let data = PathBuf::from(env::var_os("DATA_DIR").unwrap_or_else(|| "/data".into()));
    let out = PathBuf::from(env::var_os("OUTPUT_DIR").unwrap_or_else(|| "/output".into()));
    let threads = env::var("NCPU")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(8)
        .clamp(1, 64);
    if env::var_os("SHOPPING_PHASE_USAGE").is_some() {
        eprintln!("start_unix_ns={}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos());
    }
    let mut mark = Instant::now();
    let mut last = env::var_os("SHOPPING_PHASE_USAGE").map(|_| usage());
    let stage = |name: &str| {
        let wall = mark.elapsed().as_secs_f64();
        if let Some(before) = last {
            let now = usage();
            let stage = name.replace(' ', "_");
            eprintln!(
                "stage={stage} wall_s={wall:.6} user_s={:.6} system_s={:.6} minor={} major={} rss_kib={}",
                now.0 - before.0,
                now.1 - before.1,
                now.2 - before.2,
                now.3 - before.3,
                now.4,
            );
            last = Some(now);
        } else {
            eprintln!("{name} {wall:.3}s");
        }
        mark = Instant::now();
    };
    let start_read = io_boundary();
    let manifest_bytes = fs::read(data.join("manifest.json")).unwrap();
    if let Some(begin) = start_read {
        io_mark("manifest_read_begin", begin);
        io_mark("manifest_read_end", io_boundary().unwrap());
    }
    let manifest: serde_json::Value = serde_json::from_slice(&manifest_bytes).unwrap();
    let mut dates = Dates::new();
    let mut bound = |key: &str| {
        dates
            .stamp(manifest[key].as_str().unwrap().as_bytes())
            .unwrap()
    };
    let (start, end) = (bound("window_start"), bound("window_end"));
    fs::create_dir_all(out.join("user_daily")).unwrap();
    fs::create_dir_all(out.join("campaign_daily")).unwrap();
    let event_bytes = csv_bytes(&data.join("events"));
    huge_page_policy(event_bytes + csv_bytes(&data.join("clicks")) + csv_bytes(&data.join("links")));
    // Only the 32-bit clock layout is reserved ahead; the parser checks the
    // type and the sizes before it takes a reservation.
    let prepared = (end.saturating_sub(start) <= u32::MAX as i64)
        .then(|| Prepared::spawn::<u32>(event_bytes, csv_bytes(&data.join("clicks")), threads));
    let mut keys = Keys::with_event_bytes(event_bytes);
    let catalog = Catalog::read(&data, &keys);
    if env::var_os("SHOPPING_DEFER_CLICK_IDS").is_some() {
        keys.click_input = Some(Input::open_for_scan(&data.join("clicks")));
    }
    if end.saturating_sub(start) <= u32::MAX as i64
        && catalog.categories.len() <= 256
        && env::var_os("SHOPPING_COMPACT_RECORDS").is_some()
    {
        compact::pipeline(&data, &out, threads, start, end, keys, catalog, stage);
    }
    if end.saturating_sub(start) <= u32::MAX as i64 {
        if catalog.categories.len() <= 256 {
            pipeline::<u32, SmallEvent>(&data, &out, threads, start, end, keys, catalog, prepared, stage);
        } else {
            pipeline::<u32, Event>(&data, &out, threads, start, end, keys, catalog, prepared, stage);
        }
    } else {
        pipeline::<i64, Event>(&data, &out, threads, start, end, keys, catalog, prepared, stage);
    }
}

#[inline]
fn route_clicks(people: &Aliases, clicks: &[Click], shards: &mut [Vec<Click>], group: usize) {
    let mask = shards.len() - 1;
    for block in clicks.chunks(group) {
        let mut need_alias = people.prefetch_mask(block, |click| click.u);
        for click in block {
            let u = if need_alias & 1 != 0 { people.canonical(click.u) } else { click.u };
            need_alias >>= 1;
            shards[u as usize & mask].push(Click { u, ..*click });
        }
    }
}

fn csv_bytes(folder: &Path) -> usize {
    fs::read_dir(folder).unwrap().map(|entry| {
        let entry = entry.unwrap();
        if entry.path().extension().is_some_and(|extension| extension == "csv") {
            entry.metadata().unwrap().len() as usize
        } else {
            0
        }
    }).sum()
}

/// Huge pages make the in-memory stages faster, but on a host without room
/// for the input's page cache plus about 0.7 times its size in tables,
/// huge-page faults compact memory and evicted input is read again (lab,
/// 60M: 2.3-3.3 s with huge pages, 2.0-2.1 s without). Turn them off for
/// the process when reclaimable memory is under three times the input.
const ZAP_RANGES: usize = 1024;
// Range count, bytes per part, parts, ready, done, abandoned, then the ranges.
static ZAP: [std::sync::atomic::AtomicUsize; 2 * ZAP_RANGES + 6] =
    [const { std::sync::atomic::AtomicUsize::new(0) }; 2 * ZAP_RANGES + 6];

/// Exit frees every page of the process on one thread, which takes 0.1-0.3 s
/// at 60M with small pages. Before exiting, release the large private
/// anonymous regions (allocator arenas and reservations) on this thread and
/// every idle pool worker. Once all of them have reached the barrier, none
/// reads the heap again: each uses only its stack and `ZAP`, and the workers
/// sleep until the caller ends the process.
fn release_before_exit(pool: &pool::Pool) {
    use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
    let lanes = pool.lanes();
    let threads = fs::read_dir("/proc/self/task").map(|tasks| tasks.count()).ok();
    if lanes == 0 || threads != Some(lanes + 1) || env::var_os("SHOPPING_KEEP_AT_EXIT").is_some() {
        return;
    }
    let Ok(maps) = fs::read_to_string("/proc/self/maps") else { return };
    let (mut count, mut total) = (0, 0);
    for line in maps.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        // Private writable mappings without a file. Allocators name theirs
        // [anon:...]; [stack] and [heap] are excluded.
        let range = match fields[..] {
            [range, "rw-p", _, _, "0"] => range,
            [range, "rw-p", _, _, "0", name] if name.starts_with("[anon:") => range,
            _ => continue,
        };
        let Some((start, end)) = range.split_once('-') else { continue };
        let (Ok(start), Ok(end)) = (usize::from_str_radix(start, 16), usize::from_str_radix(end, 16)) else { continue };
        if end - start >= 32 << 20 && count < ZAP_RANGES {
            ZAP[6 + 2 * count].store(start, Relaxed);
            ZAP[7 + 2 * count].store(end - start, Relaxed);
            count += 1;
            total += end - start;
        }
    }
    drop(maps);
    if env::var_os("SHOPPING_PHASE_USAGE").is_some() {
        eprintln!("exit release: {count} regions, {total} bytes, {} parts", lanes + 1);
    }
    ZAP[0].store(count, Relaxed);
    // Page freeing contends in the kernel: on the lab, 4 parts finish in
    // 75-83 ms, 9 parts in 86-90 ms and 1 part in 123-156 ms.
    let zappers = (lanes + 1).min(4);
    ZAP[1].store(total.div_ceil(zappers).next_multiple_of(2 << 20), Relaxed);
    ZAP[2].store(lanes + 1, Release);
    fn zap(part: usize) {
        let (count, share) = (ZAP[0].load(Relaxed), ZAP[1].load(Relaxed));
        let (begin, end) = (part * share, (part + 1) * share);
        let mut offset = 0;
        for i in 0..count {
            let (start, len) = (ZAP[6 + 2 * i].load(Relaxed), ZAP[7 + 2 * i].load(Relaxed));
            let (lo, hi) = (begin.max(offset), end.min(offset + len));
            if lo < hi {
                unsafe { libc::madvise((start + lo - offset) as *mut libc::c_void, hi - lo, libc::MADV_DONTNEED) };
            }
            offset += len;
        }
    }
    // Returns whether every part arrived, and so may release its share.
    fn barrier(deadline: Option<Instant>) -> bool {
        let parts = ZAP[2].load(Acquire);
        ZAP[3].fetch_add(1, Release);
        while ZAP[3].load(Acquire) < parts {
            if ZAP[5].load(Acquire) != 0 {
                return false;
            }
            if deadline.is_some_and(|deadline| Instant::now() > deadline) {
                ZAP[5].store(1, Release);
                return ZAP[3].load(Acquire) >= parts;
            }
            std::hint::spin_loop();
        }
        true
    }
    fn worker(part: usize) -> ! {
        if barrier(None) {
            zap(part);
        }
        ZAP[4].fetch_add(1, Release);
        loop {
            unsafe { libc::pause() };
        }
    }
    let sent = pool.detach(|lane| worker(lane + 1));
    // A missing worker abandons the release instead of delaying the exit.
    if sent == lanes && barrier(Some(Instant::now() + std::time::Duration::from_millis(100))) {
        zap(0);
        while ZAP[4].load(Acquire) < lanes {
            std::hint::spin_loop();
        }
    }
}

fn huge_page_policy(input_bytes: usize) {
    let available = || -> Option<usize> {
        let info = fs::read_to_string("/proc/meminfo").ok()?;
        let line = info.lines().find(|line| line.starts_with("MemAvailable:"))?;
        let host = line.split_whitespace().nth(1)?.parse::<usize>().ok()? << 10;
        let group = (|| {
            let limit = fs::read_to_string("/sys/fs/cgroup/memory.max").ok()?.trim().parse::<usize>().ok()?;
            let used = fs::read_to_string("/sys/fs/cgroup/memory.current").ok()?.trim().parse::<usize>().ok()?;
            Some(limit.saturating_sub(used))
        })();
        Some(group.map_or(host, |group| group.min(host)))
    };
    let room = available();
    let off = match env::var("SHOPPING_THP").as_deref() {
        Ok("on") => false,
        Ok("off") => true,
        _ => room.is_some_and(|room| room < input_bytes.saturating_mul(3)),
    };
    if off {
        // Affects every later fault of this process, including mappings
        // already advised for huge pages.
        unsafe { libc::prctl(libc::PR_SET_THP_DISABLE, 1, 0, 0, 0) };
        input::TIGHT_MEMORY.store(true, std::sync::atomic::Ordering::Relaxed);
    }
    if env::var_os("SHOPPING_PHASE_USAGE").is_some() {
        eprintln!("huge pages {} for {input_bytes} input bytes, {room:?} available", if off { "off" } else { "on" });
    }
}

fn bucket_count(event_bytes: usize) -> usize {
    let bucket_bytes = env::var("SHOPPING_BUCKET_KB").ok()
        .and_then(|value| value.parse::<usize>().ok()).unwrap_or(6 << 10) << 10;
    ((event_bytes / bucket_bytes).max(1)).next_power_of_two().min(4096)
}

fn person_shard_count(buckets: usize) -> usize {
    buckets.min(env::var("SHOPPING_PERSON_SHARDS").ok()
        .and_then(|value| value.parse::<usize>().ok()).unwrap_or(512).next_power_of_two())
}

/// Per-worker parse reservations: event buckets and routed click shards.
#[derive(Clone, Copy, PartialEq)]
struct Layout {
    buckets: usize,
    rows: usize,
    shards: usize,
    clicks: usize,
}

impl Layout {
    fn new(event_bytes: usize, click_bytes: usize, threads: usize) -> Self {
        let buckets = bucket_count(event_bytes);
        Self {
            buckets,
            rows: event_bytes / 64 / threads / buckets,
            shards: person_shard_count(buckets),
            // Generated click rows average more than 50 bytes.
            clicks: click_bytes / 50 / threads,
        }
    }

    fn reserve<T>(self) -> (Vec<Bucket<T>>, Vec<Vec<Click>>) {
        let share = populate_share();
        let buckets = (0..self.buckets)
            .map(|_| {
                let bucket = Bucket { rows: Vec::with_capacity(self.rows), extra: Vec::with_capacity(self.rows / EXTRA_SHARE), ids: Vec::new(), wide: Vec::new() };
                populate(&bucket.rows, share);
                populate(&bucket.extra, share);
                bucket
            })
            .collect();
        let clicks = reserved::<Click>(self.shards, self.clicks);
        if env::var_os("SHOPPING_POPULATE_CLICKS").is_some() {
            clicks.iter().for_each(|shard| populate(shard, share));
        }
        (buckets, clicks)
    }
}

/// Percent of each parse reservation to fault in on the helper threads.
/// Without huge pages, demand faults on 4 KiB pages interrupt the parse
/// lanes; generated rows fill about two thirds of the reservations.
fn populate_share() -> usize {
    env::var("SHOPPING_POPULATE_ROWS").ok().and_then(|v| v.parse().ok()).unwrap_or(
        if input::TIGHT_MEMORY.load(std::sync::atomic::Ordering::Relaxed) { 70 } else { 0 },
    )
}

fn populate<T>(v: &Vec<T>, percent: usize) {
    let bytes = v.capacity() * size_of::<T>() * percent / 100;
    let start = v.as_ptr() as usize;
    let first = (start + 4095) & !4095;
    let end = (start + bytes) & !4095;
    if end > first {
        // Advice only: a kernel without MADV_POPULATE_WRITE leaves the
        // pages to fault on first use as before.
        unsafe { libc::madvise(first as *mut libc::c_void, end - first, libc::MADV_POPULATE_WRITE) };
    }
}

// Purchases and long event IDs carry a detail row; reserving for an eighth
// of the rows avoids most regrowth copies across the buckets.
const EXTRA_SHARE: usize = 8;

type Reservation = Box<dyn std::any::Any + Send>;

/// Parse reservations built on helper threads while the main thread reads
/// the catalog and maps the inputs, so their first-touch page zeroing is
/// off the serial startup path.
struct Prepared {
    layout: Layout,
    ready: std::sync::Mutex<Vec<std::thread::JoinHandle<Reservation>>>,
}

impl Prepared {
    fn spawn<T: Send + 'static>(event_bytes: usize, click_bytes: usize, threads: usize) -> Self {
        let layout = Layout::new(event_bytes, click_bytes, threads);
        // One lane reserves its own memory, which leaves a CPU for the
        // serial startup.
        let ready = (0..threads - 1)
            .map(|_| std::thread::spawn(move || Box::new(layout.reserve::<T>()) as Reservation))
            .collect();
        Self { layout, ready: std::sync::Mutex::new(ready) }
    }

    fn take<T: 'static>(&self, layout: Layout) -> Option<(Vec<Bucket<T>>, Vec<Vec<Click>>)> {
        if layout != self.layout {
            return None;
        }
        let handle = self.ready.lock().unwrap().pop()?;
        handle.join().ok()?.downcast().ok().map(|reserved| *reserved)
    }
}

fn pipeline<T: EventTime + 'static, E: Stored + 'static>(
    data: &Path,
    out: &Path,
    threads: usize,
    start: i64,
    end: i64,
    keys: Keys,
    catalog: Catalog,
    prepared: Option<Prepared>,
    mut stage: impl FnMut(&str),
) {
    let raw = Input::open_for_scan(&data.join("events"));
    let borrow_events = env::var_os("SHOPPING_BORROW_EVENT_IDS").is_some() && raw.prepare_revisit();
    // Each valid event consumes at least six bytes for separators.
    let narrow = raw.bytes() / 6 <= i32::MAX as usize;
    let n = bucket_count(raw.bytes());
    let people = std::sync::Mutex::new(None);
    let clock = raw::Calendar::new(start, end);
    let fast = raw::available() && env::var_os("SHOPPING_RAW_EVENTS").is_some();
    let raw_aux = fast && env::var_os("SHOPPING_RAW_NO_AUX").is_none();
    let probe = env::var_os("SHOPPING_PROBE_EVENTS").is_some();
    let level = env::var("SHOPPING_RAW_FIELD")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(7);
    let level = if probe { level } else { 7 };
    enum Scan<'a> {
        Links,
        Events(input::Chunk<'a>),
        Clicks(input::Chunk<'a>),
    }
    let mut tasks = Vec::new();
    if !probe {
        tasks.push(Scan::Links);
    }
    let borrowed_click_ids = keys.click_input.as_ref().is_some_and(Input::prepare_revisit);
    let local_click_input = keys.click_input.is_none().then(|| Input::open_for_scan(&data.join("clicks")));
    let click_input = keys.click_input.as_ref().or(local_click_input.as_ref()).unwrap();
    if env::var_os("SHOPPING_PHASE_USAGE").is_some() {
        eprintln!("mapped opaque IDs events={borrow_events} clicks={borrowed_click_ids}");
    }
    let early_clicks = env::var_os("SHOPPING_EARLY_CLICKS").is_some();
    let interleave = env::var_os("SHOPPING_INTERLEAVE_CLICKS").is_some();
    let person_shards = person_shard_count(n);
    let group = env::var("SHOPPING_PREFETCH_GROUP").ok()
        .and_then(|value| value.parse::<usize>().ok()).unwrap_or(32).clamp(1, 128);
    let mut click_chunks = click_input.chunks().into_iter();
    for (i, chunk) in raw.chunks().into_iter().enumerate() {
        tasks.push(Scan::Events(chunk));
        if !probe && interleave && i & 3 == 3 {
            if let Some(clicks) = click_chunks.next() {
                tasks.push(Scan::Clicks(clicks));
            }
        }
    }
    if !probe {
        tasks.extend(click_chunks.map(Scan::Clicks));
    }
    let use_pool = env::var_os("SHOPPING_REUSE_THREADS").is_some();
    std::thread::scope(|pool_scope| {
        let pool = pool::Pool::new(pool_scope, threads, use_pool);
        let (raw, click_input, keys, catalog, clock, people) =
            (&raw, click_input, &keys, &catalog, &clock, &people);
        let layout = Layout::new(raw.bytes(), click_input.bytes(), threads);
        let prepared = &prepared;
    let lanes = std::thread::scope(|scope| {
        let readers = threads.min(4);
        for lane in 0..readers {
            let raw = &raw;
            let clicks = &click_input;
            scope.spawn(move || {
                raw.prefetch(lane, readers);
                clicks.prefetch(lane, readers);
            });
        }
        pool.jobs(
            tasks,
            move |_| {
                let (buckets, shards) = prepared.as_ref().and_then(|p| p.take::<T>(layout))
                    .unwrap_or_else(|| layout.reserve());
                (
                    Dates::new(),
                    buckets,
                    input::ReadBuffer::new(),
                    Vec::<Vec<Click>>::new(),
                    [0usize; 2],
                    shards,
                    None::<std::sync::Arc<Aliases>>,
                    0usize,
                )
            },
            move |s, task| match task {
                Scan::Events(chunk) => {
                    let (data, source) = if borrow_events {
                        (chunk.records(), Some(chunk.field_source()))
                    } else {
                        (chunk.read(&mut s.2), None)
                    };
                    let mut sink = EventStore::new(&mut s.1, source, keys);
                    if fast {
                        let seen = raw::events(
                            data,
                            &mut s.0,
                            &keys,
                            &catalog,
                            &clock,
                            start,
                            end,
                            &mut sink,
                            level,
                            probe,
                        );
                        s.4[0] += seen[0];
                        s.4[1] += seen[1];
                    } else {
                        events(data, &mut s.0, &keys, &catalog, start, end, &mut sink);
                    }
                }
                Scan::Clicks(chunk) => {
                    let mut pending = Vec::with_capacity(chunk.len() / 48 + 16);
                    let (bytes, source) = if borrowed_click_ids {
                        (chunk.records(), Some(chunk.field_source()))
                    } else {
                        (chunk.read(&mut s.2), None)
                    };
                    raw::clicks(
                        bytes,
                        &mut s.0,
                        &keys,
                        &catalog,
                        &clock,
                        start,
                        end,
                        &mut pending,
                        source,
                        raw_aux,
                    );
                    if early_clicks {
                        if s.6.is_none() {
                            s.6 = people.lock().unwrap().clone();
                        }
                        if let Some(aliases) = &s.6 {
                            s.7 += pending.len();
                            route_clicks(aliases, &pending, &mut s.5, group);
                        } else {
                            s.3.push(pending);
                        }
                    } else {
                        s.3.push(pending);
                    }
                }
                Scan::Links => {
                    let frozen = read_people(data, keys, threads, raw_aux);
                    *people.lock().unwrap() = Some(std::sync::Arc::new(frozen));
                }
            },
            |_, state| state,
        )
    });
    if probe {
        stage("parse_events");
        let hit = lanes.iter().map(|s| s.4[0]).sum::<usize>();
        let miss = lanes.iter().map(|s| s.4[1]).sum::<usize>();
        let rows = lanes
            .iter()
            .flat_map(|s| &s.1)
            .map(|b| b.rows.len())
            .sum::<usize>();
        eprintln!("probe=events fast={hit} fallback={miss} emitted={rows}");
        unsafe { libc::_exit(0) }
    }
    let people = people.lock().unwrap().take().unwrap();
    let mut parts = columns(n);
    let mut pending = Vec::new();
    let mut pre_routed = Vec::with_capacity(lanes.len());
    let mut earlier = 0;
    for (_, lane, _, clicks, _, done, _, n_clicks) in lanes {
        pre_routed.push(done);
        earlier += n_clicks;
        pending.extend(clicks);
        for (part, bucket) in parts.iter_mut().zip(lane) {
            part.push(bucket);
        }
    }
    // Every parser has joined. Release the input page tables while workers
    // reduce events, so the main thread need not unmap gigabytes at a barrier.
    let release_inputs = pool_scope.spawn(move || {
        if !borrow_events {
            raw.release_pages();
        }
        if !borrowed_click_ids {
            click_input.release_pages();
        }
    });
    if early_clicks && env::var_os("SHOPPING_PHASE_USAGE").is_some() {
        eprintln!("clicks routed in parse={earlier}");
    }
    stage("event and click parsing, identity graph");
    enum Work<T> {
        Copies(Vec<Bucket<T>>),
        Clicks(Vec<Click>),
    }
    let pending_clicks = pending.iter().map(Vec::len).sum::<usize>();
    let mut copies = parts.into_iter();
    let mut chunks = pending.into_iter();
    // One two-bit word per 32 positions marks first and repeated IDs, so the
    // collision pass touches half the cache lines of two separate bitmaps.
    let paired_bits = env::var_os("SHOPPING_SPLIT_BITS").is_none();
    let mut tasks = Vec::with_capacity(copies.len() + chunks.len());
    while copies.len() != 0 || chunks.len() != 0 {
        if let Some(part) = copies.next() {
            tasks.push(Work::Copies(part));
        }
        if let Some(chunk) = chunks.next() {
            tasks.push(Work::Clicks(chunk));
        }
    }
    let lanes = pool.jobs(
        tasks,
        move |_| (
            Shards::<E>::new(person_shards),
            reserved(person_shards, pending_clicks / threads),
            Routes::default(),
        ),
        move |s, task| match task {
            Work::Copies(part) => {
                if paired_bits {
                    winners::<T, E, true>(part, raw, &people, &mut s.0, start, group, &mut s.2);
                } else {
                    winners::<T, E, false>(part, raw, &people, &mut s.0, start, group, &mut s.2);
                }
            }
            Work::Clicks(chunk) => route_clicks(&people, &chunk, &mut s.1, group),
        },
        |_, (mut events, clicks, mut routes)| {
            routes.remove_retired(&mut events);
            (events.into_parts(), clicks)
        },
    );
    let (events, mut clicks): (Vec<_>, Vec<_>) = lanes.into_iter().unzip();
    clicks.extend(pre_routed);
    let event_parts: Vec<Vec<&mut [E]>> = transpose(events, person_shards)
        .into_iter()
        .map(|lanes| lanes.into_iter().flatten().collect())
        .collect();
    let click_parts = transpose(clicks, person_shards);
    release_inputs.join().unwrap();
    stage("event dedup and click routing");
    let mut work: Vec<_> = event_parts.into_iter().zip(click_parts).collect();
    if env::var_os("SHOPPING_HEAVIEST_FIRST").is_some() {
        work.sort_unstable_by_key(|(events, clicks)| std::cmp::Reverse(
            events.iter().map(|s| s.len()).sum::<usize>() + clicks.iter().map(Vec::len).sum::<usize>(),
        ));
    }
    enum Person<E: 'static> {
        Rows(Vec<&'static mut [E]>, Vec<Vec<Click>>),
        Release(usize),
    }
    // Workers that finish their people early unmap the event input in
    // parallel, so process exit does not tear it down on one thread.
    const RELEASE_PARTS: usize = 16;
    let mut work: Vec<_> = work.into_iter().map(|(es, cs)| Person::Rows(es, cs)).collect();
    if borrow_events {
        work.extend((0..RELEASE_PARTS).map(Person::Release));
    }
    let partial = pool.jobs(
        work,
        move |i| {
            let mut totals = Totals::new(catalog.categories.len(), catalog.channels.len(), start, end);
            totals.writer = Some(output::UserWriter::open(
                &out.join(format!("user_daily/part-{i}.parquet")), &catalog.categories, narrow, true,
            ));
            totals
        },
        move |local, task| match task {
            Person::Rows(es, cs) => local.partition(keys, es, cs),
            Person::Release(part) => raw.release_part(part, RELEASE_PARTS),
        },
        |_, mut totals| {
            totals.writer.take().unwrap().close();
            totals.days()
        },
    );
    stage("person sorts, aggregation and user parquet");
    if borrowed_click_ids {
        // Attribution has finished with the mapped click IDs. Unmap them on
        // every worker instead of on one thread during process exit.
        pool.jobs((0..threads).collect(), |_| (), move |_, part| click_input.release_part(part, threads), |_, _| ());
    }
    finish(out, catalog, partial, || release_before_exit(&pool), stage)
    })
}

fn finish(
    out: &Path,
    catalog: &Catalog,
    partial: Vec<BTreeMap<i32, Vec<Stats>>>,
    release: impl FnOnce(),
    mut stage: impl FnMut(&str),
) -> ! {
    let mut all = BTreeMap::new();
    for part in partial {
        for (day, stats) in part {
            let acc = all
                .entry(day)
                .or_insert_with(|| vec![Stats::default(); catalog.channels.len()]);
            for (a, b) in acc.iter_mut().zip(stats) {
                a.add(b);
            }
        }
    }
    output::campaigns(
        &out.join("campaign_daily/part-0.parquet"),
        all,
        &catalog.channels,
    );
    if let Some(after) = io_boundary() {
        io_mark("last_parquet_write_returned", after);
    }
    stage("parquet");
    if env::var_os("SHOPPING_DUMP_MAPS").is_some() {
        eprintln!("{}", fs::read_to_string("/proc/self/smaps_rollup").unwrap_or_default());
        if let Ok(maps) = fs::read_to_string("/proc/self/smaps") {
            let mut big = Vec::new();
            let mut head = String::new();
            for line in maps.lines() {
                if line.contains('-') && !line.starts_with(|c: char| c.is_ascii_uppercase()) {
                    head = line.to_string();
                } else if let Some(rss) = line.strip_prefix("Rss:") {
                    let kb: u64 = rss.trim().trim_end_matches(" kB").trim().parse().unwrap_or(0);
                    if kb >= 16384 { big.push((kb, head.clone())); }
                }
            }
            big.sort_unstable_by(|a, b| b.cmp(a));
            for (kb, head) in big.iter().take(20) { eprintln!("{kb:>9} kB {head}"); }
        }
    }
    if env::var_os("SHOPPING_PHASE_USAGE").is_some() {
        eprintln!("exit_unix_ns={}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos());
    }
    // Both Parquet footers have been closed and all output workers joined.
    let report = env::var_os("SHOPPING_PHASE_USAGE").is_some();
    release();
    if report {
        // The heap may be released: format on the stack and write directly.
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_nanos());
        let mut line = *b"released_unix_ns=00000000000000000000\n";
        let mut value = now;
        for digit in line[17..37].iter_mut().rev() {
            *digit = b'0' + (value % 10) as u8;
            value /= 10;
        }
        unsafe { libc::write(2, line.as_ptr().cast(), line.len()) };
    }
    unsafe extern "C" {
        fn _exit(status: std::ffi::c_int) -> !;
    }
    unsafe { _exit(0) }
}
