//! Event keys have their own stream. After copy reduction only the dense
//! person, time and packed purchase fields remain.
use crate::{
    ARRIVAL_BIAS, Candidate, Catalog, Detail, EventSink, Keys, SIDE, Totals, events, finish,
    input::{self, Dates, Input},
    keys::{self, Aliases, IntMap, Key},
    output, raw, read_people, transpose,
};
use std::{
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
};

#[derive(Default)]
struct Overflow {
    ids: IntMap<u32>,
    names: Vec<Key>,
}

#[repr(C)]
struct Slot {
    key: AtomicU64,
    root: u32,
    padding: u32,
}

pub(crate) struct DensePeople {
    _arena: memmap2::MmapMut,
    base: std::ptr::NonNull<Slot>,
    len: usize,
    overflow: Mutex<Overflow>,
}
// The owned arena outlives base. Root fields are written only in the serial
// constructor. Shared inserts mutate AtomicU64 keys or lock the spill.
unsafe impl Send for DensePeople {}
unsafe impl Sync for DensePeople {}

impl DensePeople {
    fn new(original: &Aliases, bytes: usize, divisor: usize) -> Self {
        let entries = original.entries().count();
        let n = (bytes / divisor + entries * 4 + 16)
            .next_power_of_two()
            .min(1 << 25);
        let mut slots = memmap2::MmapMut::map_anon(n * std::mem::size_of::<Slot>()).unwrap();
        let _ = slots.advise(memmap2::Advice::HugePage);
        let base = std::ptr::NonNull::new(slots.as_mut_ptr().cast()).unwrap();
        let people = Self {
            _arena: slots,
            base,
            len: n,
            overflow: Mutex::default(),
        };
        for (_, root) in original.entries() {
            people.index(root);
        }
        for (alias, root) in original.entries() {
            let id = people.index(alias) as usize;
            let root = people.index(root);
            if id < people.table().len() {
                // Construction has exclusive access. All later root reads
                // happen after workers receive this finished table.
                unsafe { (*people.base.as_ptr().add(id)).root = root + 1 };
            }
        }
        people
    }

    #[inline(always)]
    fn table(&self) -> &[Slot] {
        // The arena is page-aligned and zero is valid for every Slot field.
        // Concurrent key insertions use the AtomicU64 field.
        unsafe { std::slice::from_raw_parts(self.base.as_ptr(), self.len) }
    }

    #[inline]
    fn index(&self, key: Key) -> u32 {
        debug_assert_ne!(key, 0);
        let table = self.table();
        let mask = table.len() - 1;
        let mut idx = key as usize & mask;
        for _ in 0..128 {
            let entry = &table[idx].key;
            let old = entry.load(Relaxed);
            if old == key {
                return idx as u32;
            }
            if old == 0 {
                match entry.compare_exchange(0, key, Relaxed, Relaxed) {
                    Ok(_) => return idx as u32,
                    Err(current) if current == key => return idx as u32,
                    _ => {}
                }
            }
            idx = (idx + 1) & mask;
        }
        let mut spill = self.overflow.lock().unwrap();
        if let Some(&idx) = spill.ids.get(&key) {
            return idx;
        }
        let idx = (self.table().len() + spill.names.len()) as u32;
        spill.ids.insert(key, idx);
        spill.names.push(key);
        idx
    }

    #[inline]
    fn person(&self, key: Key, aliases: &Aliases) -> u32 {
        let id = self.index(key);
        if let Some(slot) = self.table().get(id as usize) {
            return if slot.root == 0 { id } else { slot.root - 1 };
        }
        // Rare clustered identifiers can overflow the fixed slot array.
        self.index(aliases.canonical(key))
    }

    pub(crate) fn key(&self, person: u32) -> Key {
        let i = person as usize;
        let mixed = if let Some(k) = self.table().get(i) {
            k.key.load(Relaxed)
        } else {
            self.overflow.lock().unwrap().names[i - self.table().len()]
        };
        keys::unmix(mixed)
    }

    fn count(&self) -> usize {
        self.table().len() + self.overflow.lock().unwrap().names.len()
    }
}

#[derive(Clone, Copy, Default)]
#[repr(C)]
pub(crate) struct Event {
    pub person: u32,
    pub ts: u32,
    pub purchase: u32,
}

#[derive(Clone, Copy, Default)]
#[repr(C)]
pub(crate) struct Click {
    pub t: i64,
    pub id: Key,
    pub person: u32,
    pub campaign: u32,
}

#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct Line<T, const N: usize> {
    rows: [T; N],
}

struct Lines<T, const N: usize> {
    blocks: Vec<Line<T, N>>,
    pending: Line<T, N>,
    count: usize,
    stream: bool,
}

pub(crate) struct Output<T, const N: usize> {
    linear: Vec<T>,
    lines: Option<Box<Lines<T, N>>>,
}

// SAFETY: implement only for initialized, padding-free plain records.
pub(crate) unsafe trait DenseRow: Copy + Default {}
unsafe impl DenseRow for Event {}
unsafe impl DenseRow for Click {}

impl<T: DenseRow, const N: usize> Output<T, N> {
    fn new(combine: usize, fast: bool) -> Self {
        Self {
            linear: Vec::new(),
            lines: (combine != 0).then(|| {
                Box::new(Lines {
                    blocks: Vec::new(),
                    pending: Line {
                        rows: [T::default(); N],
                    },
                    count: 0,
                    stream: combine == 2 && fast,
                })
            }),
        }
    }

    #[inline(always)]
    fn push(&mut self, row: T) {
        let Some(lines) = &mut self.lines else {
            self.linear.push(row);
            return;
        };
        lines.pending.rows[lines.count] = row;
        lines.count += 1;
        if lines.count == N {
            if lines.stream {
                if lines.blocks.len() == lines.blocks.capacity() {
                    unsafe { std::arch::x86_64::_mm_sfence() };
                    lines.blocks.reserve(1);
                }
                let n = lines.blocks.len();
                // DenseRow is private and implemented only by the two
                // padding-free records above. Used instantiations fill whole
                // cache lines and the allocator aligns them to 64 bytes.
                let size = std::mem::size_of::<Line<T, N>>();
                assert_eq!(size, N * std::mem::size_of::<T>());
                unsafe {
                    stream_bytes(
                        lines.blocks.as_mut_ptr().add(n).cast(),
                        (&lines.pending as *const Line<T, N>).cast(),
                        size / 32,
                    );
                    lines.blocks.set_len(n + 1);
                }
            } else {
                lines.blocks.push(lines.pending);
            }
            lines.count = 0;
        }
    }
}

impl<T, const N: usize> Output<T, N> {
    pub(crate) fn len(&self) -> usize {
        self.linear.len()
            + self
                .lines
                .as_ref()
                .map_or(0, |lines| lines.blocks.len() * N + lines.count)
    }
    pub(crate) fn walk(&mut self, mut f: impl FnMut(&mut T)) {
        if let Some(lines) = &mut self.lines {
            for frame in &mut lines.blocks {
                for row in &mut frame.rows {
                    f(row);
                }
            }
            for row in &mut lines.pending.rows[..lines.count] {
                f(row);
            }
        } else {
            for row in &mut self.linear {
                f(row);
            }
        }
    }
}

#[target_feature(enable = "avx2")]
unsafe fn stream_bytes(
    out: *mut std::arch::x86_64::__m256i,
    from: *const std::arch::x86_64::__m256i,
    words: usize,
) {
    use std::arch::x86_64::*;
    for i in 0..words {
        unsafe { _mm256_stream_si256(out.add(i), _mm256_load_si256(from.add(i))) };
    }
}

#[derive(Clone, Copy, Default)]
#[repr(C)]
struct Payload {
    person: u32,
    ts: u32,
    arrival: u32,
}

// Sixteen records give two whole key cache lines and three payload
// cache lines. The key pass does not read the payload lines.
#[derive(Clone, Copy, Default)]
#[repr(C, align(64))]
struct Frame {
    ids: [Key; 16],
    rows: [Payload; 16],
}

struct Frames {
    output: Vec<Frame>,
    pending: Frame,
    count: usize,
    stream: bool,
}

impl Frames {
    fn push(&mut self, id: Key, row: Payload) {
        self.pending.ids[self.count] = id;
        self.pending.rows[self.count] = row;
        self.count += 1;
        if self.count == 16 {
            if self.stream {
                let dest = self.output.spare_capacity_mut();
                if dest.is_empty() {
                    self.fence();
                    self.output.reserve(1);
                }
                let n = self.output.len();
                // A Frame is exactly five aligned cache lines. Only these
                // slots are subsequently exposed as initialized.
                unsafe {
                    stream_frame(self.output.as_mut_ptr().add(n), &self.pending);
                    self.output.set_len(n + 1);
                }
            } else {
                self.output.push(self.pending);
            }
            self.count = 0;
        }
    }

    fn fence(&self) {
        if self.stream {
            unsafe { std::arch::x86_64::_mm_sfence() };
        }
    }
}

#[target_feature(enable = "avx2")]
unsafe fn stream_frame(out: *mut Frame, from: &Frame) {
    use std::arch::x86_64::*;
    let src = (from as *const Frame).cast::<__m256i>();
    let dst = out.cast::<__m256i>();
    // Frame has 320 initialized bytes and both pointers are 64-byte aligned.
    for i in 0..10 {
        unsafe { _mm256_stream_si256(dst.add(i), _mm256_load_si256(src.add(i))) };
    }
}

pub(crate) struct ParseArena {
    _map: memmap2::MmapMut,
    base: std::ptr::NonNull<u8>,
    bytes: usize,
}

// Each partition receives a unique, disjoint lane once. It can be read
// after its writer joins. The sorter requires exclusive arena ownership.
unsafe impl Send for ParseArena {}
unsafe impl Sync for ParseArena {}
impl ParseArena {
    fn new(partitions: usize, capacity: usize) -> Arc<Self> {
        let bytes = partitions * capacity * 20;
        let mut map = memmap2::MmapMut::map_anon(bytes).unwrap();
        let _ = map.advise(memmap2::Advice::HugePage);
        let base = std::ptr::NonNull::new(map.as_mut_ptr()).unwrap();
        Arc::new(Self {
            _map: map,
            base,
            bytes,
        })
    }

    pub(crate) fn words(&mut self, n: usize) -> Option<&mut [u64]> {
        if n > self.bytes / 8 {
            return None;
        }
        // mmap is page-aligned and initialized even in untouched slots.
        // No partition can still reference this arena after unwrapping Arc.
        Some(unsafe { std::slice::from_raw_parts_mut(self.base.as_ptr().cast(), n) })
    }
}

struct ArenaPart {
    arena: Arc<ParseArena>,
    offset: usize,
    limit: usize,
    used: usize,
}
impl ArenaPart {
    fn addresses(&self) -> (*mut Key, *mut Payload) {
        // The allocator gives this partition limit*20 disjoint bytes.
        unsafe {
            let base = self.arena.base.as_ptr().add(self.offset);
            (base.cast(), base.add(self.limit * 8).cast())
        }
    }
    fn push(&mut self, key: Key, row: Payload) -> bool {
        if self.used == self.limit {
            return false;
        }
        let (ids, rows) = self.addresses();
        // Only this partition accessor can write these slots.
        unsafe {
            ids.add(self.used).write(key);
            rows.add(self.used).write(row);
        }
        self.used += 1;
        true
    }
    fn split(&self) -> (&[Key], &[Payload]) {
        let (ids, rows) = self.addresses();
        // Both streams expose only the initialized prefix.
        unsafe {
            (
                std::slice::from_raw_parts(ids, self.used),
                std::slice::from_raw_parts(rows, self.used),
            )
        }
    }
}

struct Part {
    ids: Vec<Key>,
    rows: Vec<Payload>,
    extra: Vec<Detail>,
    frames: Option<Box<Frames>>,
    arena: Option<ArenaPart>,
}
impl Part {
    fn new(cap: usize, combine: usize, fast: bool, arena: Option<ArenaPart>) -> Self {
        let direct = if combine == 0 && arena.is_none() {
            cap
        } else {
            0
        };
        let frames = (combine != 0).then(|| {
            Box::new(Frames {
                output: Vec::with_capacity(cap.div_ceil(16)),
                pending: Frame::default(),
                count: 0,
                stream: combine == 2 && fast,
            })
        });
        Self {
            ids: Vec::with_capacity(direct),
            rows: Vec::with_capacity(direct),
            extra: Vec::new(),
            frames,
            arena,
        }
    }

    #[inline(always)]
    fn push(&mut self, id: Key, row: Payload) {
        if let Some(frames) = &mut self.frames {
            frames.push(id, row);
        } else {
            if let Some(lane) = &mut self.arena {
                if lane.push(id, row) {
                    return;
                }
            }
            self.ids.push(id);
            self.rows.push(row);
        }
    }

    fn len(&self) -> usize {
        if let Some(frames) = &self.frames {
            frames.output.len() * 16 + frames.count
        } else {
            self.ids.len() + self.arena.as_ref().map_or(0, |part| part.used)
        }
    }

    fn payload(&self, index: usize) -> Payload {
        if let Some(frames) = &self.frames {
            let (frame, offset) = (index / 16, index % 16);
            return frames.output.get(frame).unwrap_or(&frames.pending).rows[offset];
        }
        if let Some(part) = &self.arena {
            if index < part.used {
                return part.split().1[index];
            }
            return self.rows[index - part.used];
        }
        self.rows[index]
    }

    fn keys_and_rows(&self) -> impl Iterator<Item = (Key, Payload)> + '_ {
        let (keys, rows) = self
            .arena
            .as_ref()
            .map_or((&[][..], &[][..]), ArenaPart::split);
        keys.iter()
            .copied()
            .zip(rows.iter().copied())
            .chain(self.ids.iter().copied().zip(self.rows.iter().copied()))
            .chain(self.frames.iter().flat_map(|frames| {
                frames
                    .output
                    .iter()
                    .flat_map(|f| f.ids.iter().copied().zip(f.rows.iter().copied()))
                    .chain(
                        frames.pending.ids[..frames.count]
                            .iter()
                            .copied()
                            .zip(frames.pending.rows[..frames.count].iter().copied()),
                    )
            }))
    }

    fn ids(&self) -> impl Iterator<Item = Key> + '_ {
        let keys = self.arena.as_ref().map_or(&[][..], |part| part.split().0);
        keys.iter()
            .copied()
            .chain(self.ids.iter().copied())
            .chain(self.frames.iter().flat_map(|frames| {
                frames
                    .output
                    .iter()
                    .flat_map(|f| f.ids.iter().copied())
                    .chain(frames.pending.ids[..frames.count].iter().copied())
            }))
    }
}

struct LocalNames {
    cache: Vec<(Key, u32)>,
}

impl LocalNames {
    fn new(size: usize) -> Self {
        Self {
            cache: vec![(0, 0); size],
        }
    }

    #[inline(always)]
    fn get(&mut self, key: Key, people: &DensePeople, aliases: &Aliases) -> u32 {
        if self.cache.is_empty() {
            return people.person(key, aliases);
        }
        let mask = self.cache.len() - 1;
        let entry = &mut self.cache[(key >> 16) as usize & mask];
        if entry.0 != key {
            *entry = (key, people.person(key, aliases));
        }
        entry.1
    }
}

struct Emitter<'a> {
    bins: Vec<Part>,
    people: &'a DensePeople,
    aliases: &'a Aliases,
    names: LocalNames,
    prefetch: bool,
}
impl EventSink<u32> for Emitter<'_> {
    #[inline(always)]
    fn prefetch_user(&self, key: Key) {
        if self.prefetch {
            let slot = key as usize & (self.people.table().len() - 1);
            unsafe {
                let base = self.people.table().as_ptr().add(slot);
                std::arch::x86_64::_mm_prefetch(base.cast(), std::arch::x86_64::_MM_HINT_T0);
            }
        }
    }

    #[inline(always)]
    fn push(&mut self, row: Candidate<u32>, detail: Option<Detail>) {
        let bin = row.id as usize & (self.bins.len() - 1);
        let part = &mut self.bins[bin];
        let arrival = if let Some(detail) = detail {
            let index = part.extra.len() as u32;
            part.extra.push(detail);
            SIDE | index
        } else {
            row.arrival
        };
        let person = if row.t == u32::MAX {
            0
        } else {
            self.names.get(row.user, self.people, self.aliases)
        };
        part.push(
            row.id,
            Payload {
                person,
                ts: row.t,
                arrival,
            },
        );
    }
}

#[inline]
fn arrival(row: Payload, part: &Part) -> i64 {
    if row.arrival & SIDE == 0 {
        i64::from(row.arrival)
    } else {
        part.extra[(row.arrival ^ SIDE) as usize].arrival
    }
}

#[inline]
fn send(row: Payload, part: &Part, out: &mut [Output<Event, 16>]) {
    if row.ts == u32::MAX {
        return;
    }
    let purchase = if row.arrival & SIDE != 0 {
        let d = part.extra[(row.arrival ^ SIDE) as usize];
        (d.category << 24) | d.cents
    } else {
        0
    };
    let shard = row.person as usize & (out.len() - 1);
    out[shard].push(Event {
        person: row.person,
        ts: row.ts,
        purchase,
    });
}

#[derive(Clone, Copy, Default)]
struct CopySlot {
    key: Key,
    lane: u32,
    row: u32,
}

// Load is at most one half. Full-key comparisons keep this reducer exact
// even when every event falls in the same low hash prefix.
fn reduce_direct(
    parts: &[Part],
    out: &mut [Output<Event, 16>],
    table: &mut Vec<CopySlot>,
    origin: i64,
) {
    let count = parts.iter().map(Part::len).sum::<usize>();
    if count == 0 {
        return;
    }
    let cap = (count * 2).next_power_of_two();
    table.resize(cap, CopySlot::default());
    let mask = cap - 1;
    let bias = origin / 1000 - ARRIVAL_BIAS;
    let ingest = |row: Payload, part: &Part| {
        arrival(row, part) + if row.arrival & SIDE == 0 { bias } else { 0 }
    };
    for (lane, part) in parts.iter().enumerate() {
        for (row, (key, new)) in part.keys_and_rows().enumerate() {
            let mut i = (key >> 20) as usize & mask;
            while table[i].key != 0 && table[i].key != key {
                i = (i + 1) & mask;
            }
            let slot = &mut table[i];
            if slot.key == key {
                let current = &parts[slot.lane as usize];
                if ingest(new, part) >= ingest(current.payload(slot.row as usize), current) {
                    continue;
                }
            }
            *slot = CopySlot {
                key,
                lane: lane as u32,
                row: row as u32,
            };
        }
    }
    for slot in table {
        if slot.key != 0 {
            let part = &parts[slot.lane as usize];
            send(part.payload(slot.row as usize), part, out);
            slot.key = 0;
        }
    }
}

fn reduce(
    parts: Vec<Part>,
    out: &mut [Output<Event, 16>],
    scratch: &mut Vec<u64>,
    origin: i64,
    bits_per_row: usize,
    reuse: bool,
) {
    let n = parts.iter().map(|p| p.len()).sum::<usize>();
    let bits = (n * bits_per_row).max(64).next_power_of_two();
    let mut seen = vec![0u64; bits / 64];
    let mut repeated = vec![0u64; bits / 64];
    let index = |id: Key| (id >> 20) as usize & (bits - 1);
    for part in &parts {
        for id in part.ids() {
            let bit = index(id);
            let mask = 1 << (bit % 64);
            repeated[bit / 64] |= seen[bit / 64] & mask;
            seen[bit / 64] |= mask;
        }
    }
    let bias = origin / 1000 - ARRIVAL_BIAS;
    let mut copies = IntMap::<(usize, Payload, i64)>::default();
    copies.reserve(n / 8);
    for (which, part) in parts.iter().enumerate() {
        for (key, row) in part.keys_and_rows() {
            let bit = index(key);
            if repeated[bit / 64] & (1 << (bit % 64)) == 0 {
                send(row, part, out);
                continue;
            }
            let offset = if row.arrival & SIDE == 0 { bias } else { 0 };
            let t = arrival(row, part) + offset;
            copies
                .entry(key)
                .and_modify(|old| {
                    if t < old.2 {
                        *old = (which, row, t);
                    }
                })
                .or_insert((which, row, t));
        }
    }
    for (which, row, _) in copies.into_values() {
        send(row, &parts[which], out);
    }
    if reuse {
        for part in parts {
            if part.ids.capacity() > scratch.capacity() {
                *scratch = part.ids;
            }
        }
    }
}

pub(crate) fn pipeline(
    data: &Path,
    out: &Path,
    threads: usize,
    start: i64,
    end: i64,
    keys: Keys,
    catalog: Catalog,
    mut stage: impl FnMut(&str),
) -> ! {
    let input = Input::open_for_scan(&data.join("events"));
    let click_input = Input::open_for_scan(&data.join("clicks"));
    let setting = |key, default| {
        std::env::var(key)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(default)
    };
    let partitions: usize = setting(
        "SHOPPING_EVENT_PARTITIONS",
        ((input.bytes() / (2 << 20)).max(1)).next_power_of_two(),
    )
    .clamp(1, 4096)
    .next_power_of_two();
    let shards: usize = setting("SHOPPING_PERSON_PARTITIONS", 256)
        .clamp(1, 512)
        .next_power_of_two();
    let divisor = setting("SHOPPING_DENSE_DIVISOR", 768).clamp(16, 4096);
    let combine = setting("SHOPPING_SW_COMBINE", 0).min(2);
    let combine_out = setting("SHOPPING_PERSON_COMBINE", 0).min(2);
    let arena = setting("SHOPPING_PARSE_ARENA", 0).min(2);
    let backing = setting("SHOPPING_ARENA_DIVISOR", 64).clamp(32, 128);
    let reuse = setting("SHOPPING_REUSE_SCRATCH", 0) != 0;
    let direct = setting("SHOPPING_DEDUP_DIRECT", 0) != 0;
    let bits = setting("SHOPPING_DEDUP_BITS", 16).clamp(2, 64);
    let cache = setting("SHOPPING_NAME_CACHE", 0).min(4096);
    let cache = if cache == 0 {
        0
    } else {
        cache.next_power_of_two()
    };
    let prefetch = setting("SHOPPING_PREFETCH_DENSE", 0) != 0;
    let clock = raw::Calendar::new(start, end);
    let fast = raw::available();
    let aliases = read_people(data, &keys, threads, fast);
    let people = DensePeople::new(&aliases, input.bytes() + click_input.bytes(), divisor);
    stage("identity_dense");
    enum Scan<'a> {
        Events(input::Chunk<'a>),
        Clicks(input::Chunk<'a>),
    }
    let tasks = input
        .chunks()
        .into_iter()
        .map(Scan::Events)
        .chain(click_input.chunks().into_iter().map(Scan::Clicks))
        .collect();
    let lanes = std::thread::scope(|scope| {
        for lane in 0..threads.min(4) {
            let (input, clicks) = (&input, &click_input);
            scope.spawn(move || {
                input.prefetch(lane, threads.min(4));
                clicks.prefetch(lane, threads.min(4));
            });
        }
        let cap = input.bytes() / 72 / partitions / threads;
        let arena_cap =
            ((input.bytes() / backing / partitions / threads).max(32)).next_multiple_of(32);
        input::jobs_finish(
            tasks,
            threads,
            |_| {
                let memory =
                    (arena != 0 && combine == 0).then(|| ParseArena::new(partitions, arena_cap));
                (
                    Emitter {
                        bins: (0..partitions)
                            .map(|part| {
                                Part::new(
                                    cap,
                                    combine,
                                    fast,
                                    memory.as_ref().map(|region| ArenaPart {
                                        arena: Arc::clone(region),
                                        offset: part * arena_cap * 20,
                                        limit: arena_cap,
                                        used: 0,
                                    }),
                                )
                            })
                            .collect(),
                        people: &people,
                        aliases: &aliases,
                        names: LocalNames::new(cache),
                        prefetch,
                    },
                    Dates::new(),
                    input::ReadBuffer::new(),
                    (0..shards)
                        .map(|_| Output::<Click, 8>::new(combine_out, fast))
                        .collect::<Vec<_>>(),
                    Vec::new(),
                    memory,
                )
            },
            |(emit, dates, buffer, clicks, temporary, _), task| match task {
                Scan::Events(chunk) => {
                    let bytes = chunk.read(buffer);
                    if fast {
                        raw::events(bytes, dates, &keys, &catalog, &clock, start, end, emit, 7, false);
                    } else {
                        events(bytes, dates, &keys, &catalog, start, end, emit);
                    }
                }
                Scan::Clicks(chunk) => {
                    raw::clicks(
                        chunk.read(buffer),
                        dates,
                        &keys,
                        &catalog,
                        &clock,
                        start,
                        end,
                        temporary,
                        None,
                        fast,
                    );
                    for click in temporary.drain(..) {
                        let person = emit.names.get(click.u, &people, &aliases);
                        clicks[person as usize & (shards - 1)].push(Click {
                            t: click.t,
                            id: click.id,
                            campaign: click.campaign,
                            person,
                        });
                    }
                }
            },
            |_, lane| {
                if let Some(frame) = lane.0.bins.first().and_then(|part| part.frames.as_ref()) {
                    frame.fence();
                }
                if combine_out == 2 && fast {
                    unsafe { std::arch::x86_64::_mm_sfence() };
                }
                lane
            },
        )
    });
    let (mut parts, mut click_lanes) = (super::columns(partitions), Vec::new());
    let mut arenas = Vec::new();
    for (emit, _, _, clicks, _, arena) in lanes {
        arenas.push(arena);
        for (part, lane) in parts.iter_mut().zip(emit.bins) {
            part.push(lane);
        }
        click_lanes.push(clicks);
    }
    let release = std::thread::spawn(move || {
        input.release();
        click_input.release();
    });
    stage("compact_scan");
    let reducers = input::jobs_finish(
        parts,
        threads,
        |_| {
            (
                (0..shards)
                    .map(|_| Output::<Event, 16>::new(combine_out, fast))
                    .collect::<Vec<_>>(),
                (Vec::new(), Vec::new()),
            )
        },
        |(winners, (scratch, table)), part| {
            if direct {
                reduce_direct(&part, winners, table, start);
            } else {
                reduce(part, winners, scratch, start, bits, reuse);
            }
        },
        |_, lane| {
            if combine_out == 2 && fast {
                unsafe { std::arch::x86_64::_mm_sfence() };
            }
            (lane.0, lane.1.0)
        },
    );
    release.join().unwrap();
    stage("compact_dedup_route");
    let (winner_lanes, recycled): (Vec<_>, Vec<_>) = reducers.into_iter().unzip();
    let work = transpose(winner_lanes, shards)
        .into_iter()
        .zip(transpose(click_lanes, shards))
        .collect();
    let recycled: Vec<_> = recycled
        .into_iter()
        .map(|buf| Mutex::new(Some(buf)))
        .collect();
    let arenas: Vec<_> = arenas
        .into_iter()
        .map(|region| {
            let region = if arena == 2 {
                region.map(|owned| {
                    Arc::try_unwrap(owned)
                        .ok()
                        .expect("all parse parts were dropped")
                })
            } else {
                None
            };
            Mutex::new(region)
        })
        .collect();
    let count = people.count().div_ceil(shards);
    let partial = input::jobs_finish(
        work,
        threads,
        |thread| {
            let backing = recycled
                .get(thread)
                .and_then(|v| v.lock().unwrap().take())
                .unwrap_or_default();
            (
                Totals::new(catalog.categories.len(), catalog.channels.len(), start, end),
                crate::aggregate::DenseScratch::new(
                    backing,
                    reuse,
                    arenas.get(thread).and_then(|v| v.lock().unwrap().take()),
                ),
            )
        },
        |(totals, scratch), (events, clicks)| {
            totals.partition_dense(&keys, &people, events, clicks, shards, count, scratch);
        },
        |i, (totals, _)| {
            output::users(
                &out.join(format!("user_daily/part-{i}.parquet")),
                &totals.users,
                &keys,
                &catalog.categories,
            );
            totals.days()
        },
    );
    stage("dense_person_user_parquet");
    finish(out, &catalog, partial, stage)
}
