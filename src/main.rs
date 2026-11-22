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
        let mut names = vec![b"unknown".to_vec()];
        let mut pairs = Vec::new();
        for chunk in Input::open(&data.join("products.csv")).chunks() {
            rows::<2>(chunk.records(), |v| {
                names.push(v[1].to_vec());
                pairs.push((v[0].to_vec(), v[1].to_vec()));
            });
        }
        names.sort_unstable();
        names.dedup();
        let unknown = names
            .binary_search_by(|s| s.as_slice().cmp(b"unknown"))
            .unwrap() as u32;
        let mut products = Numbered::new(b"p", pairs.len());
        for (id, category) in pairs {
            products.insert(keys, &id, names.binary_search(&category).unwrap() as u32);
        }
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
}

struct EventStore<'a, T> {
    bins: &'a mut [Bucket<T>],
    mask: usize,
    source: Option<input::FieldSource<'a>>,
}

impl<'a, T> EventStore<'a, T> {
    fn new(bins: &'a mut [Bucket<T>], source: Option<input::FieldSource<'a>>) -> Self {
        assert!(bins.len().is_power_of_two());
        Self { mask: bins.len() - 1, bins, source }
    }

    #[inline(always)]
    fn bucket(&mut self, id: Key) -> &mut Bucket<T> {
        // new() establishes the power-of-two length, which cannot change.
        unsafe { self.bins.get_unchecked_mut(id as usize & self.mask) }
    }
}

impl<T: EventTime> EventSink<T> for EventStore<'_, T> {
    #[inline(always)]
    fn push(&mut self, mut row: Candidate<T>, detail: Option<Detail>) {
        let bin = self.bucket(row.id);
        if let Some(detail) = detail {
            row.arrival = SIDE | bin.extra.len() as u32;
            bin.extra.push(detail);
        }
        bin.rows.push(row);
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

    fn remove_retired<E>(&mut self, out: &mut [Vec<E>]) {
        if self.retired.is_empty() {
            return;
        }
        self.retired.sort_unstable();
        for handle in self.retired.drain(..).rev() {
            let (shard, row) = Self::index(handle);
            out[shard].swap_remove(row);
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
    out: &mut [Vec<E>],
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
    let shard_mask = out.len() - 1;
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
                        out[u as usize & shard_mask].push(route(u, t));
                    }
                    continue;
                }
                if let Some(span) = detail.opaque {
                    use hashbrown::hash_table::Entry;
                    let id = span.bytes(bin, source);
                    let candidate = OpaqueWinner { part, row: block_number * group + within_block };
                    match routes.opaque.entry(
                        row.id,
                        |winner| winner.id(&parts, source) == id,
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
                        let previous = *out[shard][at].user();
                        if previous == row.user { previous } else { name() }
                    } else {
                        name()
                    };
                    let shard = u as usize & shard_mask;
                    let (previous, at) = if old == 0 { (usize::MAX, 0) } else { Routes::index(old) };
                    if previous == shard {
                        out[shard][at] = route(u, t);
                        old
                    } else {
                        if old != 0 {
                            routes.retired.push(old);
                        }
                        let dest = &mut out[shard];
                        let handle = Routes::handle(shard, dest.len());
                        dest.push(route(u, t));
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
            out[u as usize & shard_mask].push(E::encode(
                Event { u, t, cents: detail.cents, cat: detail.category }, start,
            ));
        }
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
                    raw::links(chunk.records(), keys, fast, |a, b| {
                        if graph.join(keys, a, b).is_none() {
                            *ok = false;
                        }
                    });
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
    let event_bytes = fs::read_dir(data.join("events")).unwrap().map(|entry| {
        let entry = entry.unwrap();
        if entry.path().extension().is_some_and(|extension| extension == "csv") {
            entry.metadata().unwrap().len() as usize
        } else {
            0
        }
    }).sum();
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
            pipeline::<u32, SmallEvent>(&data, &out, threads, start, end, keys, catalog, stage);
        } else {
            pipeline::<u32, Event>(&data, &out, threads, start, end, keys, catalog, stage);
        }
    } else {
        pipeline::<i64, Event>(&data, &out, threads, start, end, keys, catalog, stage);
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

fn pipeline<T: EventTime, E: Stored>(
    data: &Path,
    out: &Path,
    threads: usize,
    start: i64,
    end: i64,
    keys: Keys,
    catalog: Catalog,
    mut stage: impl FnMut(&str),
) {
    let raw = Input::open_for_scan(&data.join("events"));
    let borrow_events = env::var_os("SHOPPING_BORROW_EVENT_IDS").is_some() && raw.prepare_revisit();
    // Each valid event consumes at least six bytes for separators.
    let narrow = raw.bytes() / 6 <= i32::MAX as usize;
    let n = ((raw.bytes() / (6 << 20)).max(1))
        .next_power_of_two()
        .min(4096);
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
    let person_shards = n.min(512);
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
        let expected = raw.bytes() / 64 / threads / n;
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
                (
                    Dates::new(),
                    (0..n)
                        .map(|_| Bucket {
                            rows: Vec::with_capacity(expected),
                            extra: Vec::new(),
                            ids: Vec::new(),
                            wide: Vec::new(),
                        })
                        .collect::<Vec<_>>(),
                    input::ReadBuffer::new(),
                    Vec::<Vec<Click>>::new(),
                    [0usize; 2],
                    columns::<Click>(person_shards),
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
                    let mut sink = EventStore::new(&mut s.1, source);
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
                    let mut pending = Vec::new();
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
    let mut copies = parts.into_iter();
    let mut chunks = pending.into_iter();
    let paired_bits = env::var_os("SHOPPING_PAIRED_BITS").is_some();
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
        move |_| (columns(person_shards), columns(person_shards), Routes::default()),
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
            (events, clicks)
        },
    );
    let (events, mut clicks): (Vec<_>, Vec<_>) = lanes.into_iter().unzip();
    clicks.extend(pre_routed);
    let event_parts = transpose(events, person_shards);
    let click_parts = transpose(clicks, person_shards);
    release_inputs.join().unwrap();
    if borrow_events {
        pool_scope.spawn(move || raw.release_pages());
    }
    stage("event dedup and click routing");
    let mut work: Vec<_> = event_parts.into_iter().zip(click_parts).collect();
    if env::var_os("SHOPPING_HEAVIEST_FIRST").is_some() {
        work.sort_unstable_by_key(|(events, clicks)| std::cmp::Reverse(
            events.iter().map(Vec::len).sum::<usize>() + clicks.iter().map(Vec::len).sum::<usize>(),
        ));
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
        move |local, (es, cs)| {
            local.partition(keys, es, cs);
        },
        |_, mut totals| {
            totals.writer.take().unwrap().close();
            totals.days()
        },
    );
    stage("person sorts, aggregation and user parquet");
    finish(out, catalog, partial, stage)
    })
}

fn finish(
    out: &Path,
    catalog: &Catalog,
    partial: Vec<BTreeMap<i32, Vec<Stats>>>,
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
    // Both Parquet footers have been closed and all output workers joined.
    unsafe extern "C" {
        fn _exit(status: std::ffi::c_int) -> !;
    }
    unsafe { _exit(0) }
}
