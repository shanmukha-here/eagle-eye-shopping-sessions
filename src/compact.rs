//! Event keys have their own stream. After copy reduction only the dense
//! person, time and packed purchase fields remain.
use crate::{
    ARRIVAL_BIAS, Candidate, Catalog, Detail, EventSink, Keys, SIDE, Totals, events, finish,
    input::{self, Dates, Input, jobs},
    keys::{self, Aliases, IntMap, Key},
    output, raw, read_people, transpose,
};
use std::{
    path::Path,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
};

#[derive(Default)]
struct Overflow {
    ids: IntMap<u32>,
    names: Vec<Key>,
}

pub(crate) struct DensePeople {
    // A new non-alias owns its slot. Linked keys know the exact root slot.
    table: Vec<AtomicU64>,
    roots: Vec<u32>,
    overflow: Mutex<Overflow>,
}
impl DensePeople {
    fn new(original: &Aliases, bytes: usize, divisor: usize) -> Self {
        let entries = original.entries().count();
        let n = (bytes / divisor + entries * 4 + 16)
            .next_power_of_two()
            .min(1 << 25);
        let mut people = Self {
            table: (0..n).map(|_| AtomicU64::new(0)).collect(),
            roots: vec![0; n],
            overflow: Mutex::default(),
        };
        for (_, root) in original.entries() {
            people.index(root);
        }
        for (alias, root) in original.entries() {
            let id = people.index(alias) as usize;
            let root = people.index(root);
            if let Some(target) = people.roots.get_mut(id) {
                *target = root + 1;
            }
        }
        people
    }

    #[inline]
    fn index(&self, key: Key) -> u32 {
        debug_assert_ne!(key, 0);
        let mask = self.table.len() - 1;
        let mut idx = key as usize & mask;
        for _ in 0..128 {
            let entry = &self.table[idx];
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
        let idx = (self.table.len() + spill.names.len()) as u32;
        spill.ids.insert(key, idx);
        spill.names.push(key);
        idx
    }

    #[inline]
    fn person(&self, key: Key, aliases: &Aliases) -> u32 {
        let id = self.index(key);
        if let Some(&root) = self.roots.get(id as usize) {
            return if root == 0 { id } else { root - 1 };
        }
        // Rare clustered identifiers can overflow the fixed slot array.
        self.index(aliases.canonical(key))
    }

    pub(crate) fn key(&self, person: u32) -> Key {
        let i = person as usize;
        let mixed = if let Some(k) = self.table.get(i) {
            k.load(Relaxed)
        } else {
            self.overflow.lock().unwrap().names[i - self.table.len()]
        };
        keys::unmix(mixed)
    }

    fn count(&self) -> usize {
        self.table.len() + self.overflow.lock().unwrap().names.len()
    }
}

#[derive(Clone, Copy)]
pub(crate) struct Event {
    pub person: u32,
    pub ts: u32,
    pub purchase: u32,
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

struct Part {
    ids: Vec<Key>,
    rows: Vec<Payload>,
    extra: Vec<Detail>,
    frames: Option<Box<Frames>>,
}
impl Part {
    fn new(cap: usize, combine: usize, fast: bool) -> Self {
        let direct = if combine == 0 { cap } else { 0 };
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
        }
    }

    #[inline(always)]
    fn push(&mut self, id: Key, row: Payload) {
        if let Some(frames) = &mut self.frames {
            frames.push(id, row);
        } else {
            self.ids.push(id);
            self.rows.push(row);
        }
    }

    fn len(&self) -> usize {
        if let Some(frames) = &self.frames {
            frames.output.len() * 16 + frames.count
        } else {
            self.ids.len()
        }
    }

    fn keys_and_rows(&self) -> impl Iterator<Item = (Key, Payload)> + '_ {
        self.ids
            .iter()
            .copied()
            .zip(self.rows.iter().copied())
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
        self.ids
            .iter()
            .copied()
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
            let slot = key as usize & (self.people.table.len() - 1);
            unsafe {
                let base = self.people.table.as_ptr().add(slot);
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
fn send(row: Payload, part: &Part, out: &mut [Vec<Event>]) {
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

fn reduce(parts: Vec<Part>, out: &mut [Vec<Event>], origin: i64, bits_per_row: usize) {
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
        input::jobs_finish(
            tasks,
            threads,
            |_| {
                (
                    Emitter {
                        bins: (0..partitions)
                            .map(|_| Part::new(cap, combine, fast))
                            .collect(),
                        people: &people,
                        aliases: &aliases,
                        names: LocalNames::new(cache),
                        prefetch,
                    },
                    Dates::new(),
                    input::ReadBuffer::new(),
                    super::columns(shards),
                    Vec::new(),
                )
            },
            |(emit, dates, buffer, clicks, temporary), task| match task {
                Scan::Events(chunk) => {
                    let bytes = chunk.read(buffer);
                    if fast {
                        raw::events(bytes, dates, &keys, &catalog, &clock, start, end, emit, 7);
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
                        fast,
                    );
                    for mut click in temporary.drain(..) {
                        let person = emit.names.get(click.u, &people, &aliases);
                        click.u = u64::from(person);
                        clicks[person as usize & (shards - 1)].push(click);
                    }
                }
            },
            |_, lane| {
                if let Some(frame) = lane.0.bins.first().and_then(|part| part.frames.as_ref()) {
                    frame.fence();
                }
                lane
            },
        )
    });
    let (mut parts, mut click_lanes) = (super::columns(partitions), Vec::new());
    for (emit, _, _, clicks, _) in lanes {
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
    let winner_lanes = jobs(
        parts,
        threads,
        |_| super::columns(shards),
        |out, part| reduce(part, out, start, bits),
    );
    release.join().unwrap();
    stage("compact_dedup_route");
    let work = transpose(winner_lanes, shards)
        .into_iter()
        .zip(transpose(click_lanes, shards))
        .enumerate()
        .collect();
    let count = people.count().div_ceil(shards);
    let partial = input::jobs_finish(
        work,
        threads,
        |_| Totals::new(catalog.categories.len(), catalog.channels.len(), start, end),
        |totals, (_, (events, clicks))| {
            totals.partition_dense(&keys, &people, events, clicks, shards, count);
        },
        |i, totals| {
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
