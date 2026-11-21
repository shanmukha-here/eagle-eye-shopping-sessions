#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;
mod aggregate;
mod compact;
mod input;
mod keys;
mod output;
mod pages;
mod persistence;
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
#[derive(Clone, Copy)]
struct Detail {
    arrival: i64,
    cents: u32,
    category: u32,
}
struct Bucket<T> {
    rows: Vec<Candidate<T>>,
    extra: Vec<Detail>,
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
}

impl<T: EventTime> EventSink<T> for [Bucket<T>] {
    #[inline(always)]
    fn push(&mut self, mut row: Candidate<T>, detail: Option<Detail>) {
        let bucket = row.id as usize & (self.len() - 1);
        let bin = &mut self[bucket];
        if let Some(detail) = detail {
            row.arrival = SIDE | bin.extra.len() as u32;
            bin.extra.push(detail);
        }
        bin.rows.push(row);
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
            let id = mix(keys.key(f[0]));
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
            let detail = (cents != 0 || (offset as u64) >= u64::from(SIDE)).then_some(Detail {
                arrival,
                cents,
                category,
            });
            partitions.push(candidate, detail);
        },
    );
}

fn winners<T: EventTime, E: Stored, const PAIRED_BITS: bool>(
    parts: Vec<Bucket<T>>,
    people: &Aliases,
    out: &mut [Vec<E>],
    start: i64,
    group: usize,
) {
    let count = parts.iter().map(|bin| bin.rows.len()).sum::<usize>();
    if count == 0 {
        return;
    }
    // First find unique hash positions in a cache-resident bitmap. Only IDs
    // at repeated positions require a full-key minimum-ingest reduction.
    let bits = (count * 16).max(64).next_power_of_two();
    let mut seen = vec![0u64; if PAIRED_BITS { 0 } else { bits / 64 }];
    let mut collisions = vec![0u64; bits / if PAIRED_BITS { 32 } else { 64 }];
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
    let mut emit = |e: Candidate<T>, d: Detail| {
        if let Some(t) = e.t.unpack(start) {
            let u = people.canonical(e.user);
            let shard = u as usize & (out.len() - 1);
            out[shard].push(E::encode(
                Event {
                    u,
                    t,
                    cents: d.cents,
                    cat: d.category,
                },
                start,
            ));
        }
    };
    let mut repeated = IntMap::<(Candidate<T>, Detail)>::default();
    repeated.reserve(count / 8);
    for bin in parts {
        for block in bin.rows.chunks(group) {
            for e in block {
                people.prefetch(e.user);
            }
            for &e in block {
                let d = if e.arrival & SIDE == 0 {
                    Detail {
                        arrival: i64::from(e.arrival) + start / 1000 - ARRIVAL_BIAS,
                        cents: 0,
                        category: 0,
                    }
                } else {
                    bin.extra[(e.arrival ^ SIDE) as usize]
                };
                let i = index(e.id);
                let duplicated = if PAIRED_BITS {
                    collisions[i / 32] & (2u64 << (2 * (i % 32))) != 0
                } else {
                    collisions[i / 64] & (1u64 << (i % 64)) != 0
                };
                if !duplicated {
                    emit(e, d);
                } else {
                    repeated
                        .entry(e.id)
                        .and_modify(|old| {
                            if d.arrival < old.1.arrival {
                                *old = (e, d)
                            }
                        })
                        .or_insert((e, d));
                }
            }
        }
    }
    for (e, d) in repeated.into_values() {
        emit(e, d);
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
                id: keys.key(f[0]),
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

fn main() {
    let data = PathBuf::from(env::var_os("DATA_DIR").unwrap_or_else(|| "/data".into()));
    match persistence::check(&data) {
        Ok(Some(code)) => std::process::exit(code),
        Ok(None) => {}
        Err(err) => {
            eprintln!("persistence probe setup failed: {err}");
            std::process::exit(74);
        }
    }
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
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(data.join("manifest.json")).unwrap()).unwrap();
    let mut dates = Dates::new();
    let mut bound = |key: &str| {
        dates
            .stamp(manifest[key].as_str().unwrap().as_bytes())
            .unwrap()
    };
    let (start, end) = (bound("window_start"), bound("window_end"));
    fs::create_dir_all(out.join("user_daily")).unwrap();
    fs::create_dir_all(out.join("campaign_daily")).unwrap();
    let keys = Keys::default();
    let catalog = Catalog::read(&data, &keys);
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
    let n = ((raw.bytes() / (6 << 20)).max(1))
        .next_power_of_two()
        .min(4096);
    let people = std::sync::OnceLock::new();
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
    tasks.extend(raw.chunks().into_iter().map(Scan::Events));
    let click_input = Input::open_for_scan(&data.join("clicks"));
    if !probe {
        tasks.extend(click_input.chunks().into_iter().map(Scan::Clicks));
    }
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
        jobs(
            tasks,
            threads,
            |_| {
                let expected = raw.bytes() / 64 / threads / n;
                (
                    Dates::new(),
                    (0..n)
                        .map(|_| Bucket {
                            rows: Vec::with_capacity(expected),
                            extra: Vec::new(),
                        })
                        .collect::<Vec<_>>(),
                    input::ReadBuffer::new(),
                    Vec::<Vec<Click>>::new(),
                    [0usize; 2],
                )
            },
            |s, task| match task {
                Scan::Events(chunk) => {
                    let data = chunk.read(&mut s.2);
                    if fast {
                        let seen = raw::events(
                            data,
                            &mut s.0,
                            &keys,
                            &catalog,
                            &clock,
                            start,
                            end,
                            &mut s.1[..],
                            level,
                        );
                        s.4[0] += seen[0];
                        s.4[1] += seen[1];
                    } else {
                        events(data, &mut s.0, &keys, &catalog, start, end, &mut s.1[..]);
                    }
                }
                Scan::Clicks(chunk) => {
                    let mut pending = Vec::new();
                    raw::clicks(
                        chunk.read(&mut s.2),
                        &mut s.0,
                        &keys,
                        &catalog,
                        &clock,
                        start,
                        end,
                        &mut pending,
                        raw_aux,
                    );
                    s.3.push(pending);
                }
                Scan::Links => {
                    people.get_or_init(|| read_people(data, &keys, threads, raw_aux));
                }
            },
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
    let people = people.into_inner().unwrap();
    let mut parts = columns(n);
    let mut pending = Vec::new();
    for (_, lane, _, clicks, _) in lanes {
        pending.extend(clicks);
        for (part, bucket) in parts.iter_mut().zip(lane) {
            part.push(bucket);
        }
    }
    // Every parser has joined. Release the input page tables while workers
    // reduce events, so the main thread need not unmap gigabytes at a barrier.
    let release_inputs = std::thread::spawn(move || {
        raw.release();
        click_input.release();
    });
    stage("event and click parsing, identity graph");
    let person_shards = n.min(512);
    enum Work<T> {
        Copies(Vec<Bucket<T>>),
        Clicks(Vec<Click>),
    }
    let mut copies = parts.into_iter();
    let mut chunks = pending.into_iter();
    let paired_bits = env::var_os("SHOPPING_PAIRED_BITS").is_some();
    let group = env::var("SHOPPING_PREFETCH_GROUP").ok()
        .and_then(|value| value.parse::<usize>().ok()).unwrap_or(32).clamp(1, 128);
    let mut tasks = Vec::with_capacity(copies.len() + chunks.len());
    while copies.len() != 0 || chunks.len() != 0 {
        if let Some(part) = copies.next() {
            tasks.push(Work::Copies(part));
        }
        if let Some(chunk) = chunks.next() {
            tasks.push(Work::Clicks(chunk));
        }
    }
    let lanes = jobs(
        tasks,
        threads,
        |_| (columns(person_shards), columns(person_shards)),
        |s, task| match task {
            Work::Copies(part) => {
                if paired_bits {
                    winners::<T, E, true>(part, &people, &mut s.0, start, group);
                } else {
                    winners::<T, E, false>(part, &people, &mut s.0, start, group);
                }
            }
            Work::Clicks(chunk) => {
                for block in chunk.chunks(group) {
                    for c in block {
                        people.prefetch(c.u);
                    }
                    for c in block {
                        let u = people.canonical(c.u);
                        let shard = u as usize & (person_shards - 1);
                        s.1[shard].push(Click { u, ..*c });
                    }
                }
            }
        },
    );
    let (events, clicks): (Vec<_>, Vec<_>) = lanes.into_iter().unzip();
    let event_parts = transpose(events, person_shards);
    let click_parts = transpose(clicks, person_shards);
    release_inputs.join().unwrap();
    drop(people);
    stage("event dedup and click routing");
    let mut work: Vec<_> = event_parts.into_iter().zip(click_parts).collect();
    if env::var_os("SHOPPING_HEAVIEST_FIRST").is_some() {
        work.sort_unstable_by_key(|(events, clicks)| std::cmp::Reverse(
            events.iter().map(Vec::len).sum::<usize>() + clicks.iter().map(Vec::len).sum::<usize>(),
        ));
    }
    let partial = input::jobs_finish(
        work,
        threads,
        |_| Totals::new(catalog.categories.len(), catalog.channels.len(), start, end),
        |local, (es, cs)| {
            local.partition(&keys, es, cs);
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
    stage("person sorts, aggregation and user parquet");
    finish(out, &catalog, partial, stage)
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
    stage("parquet");
    // Both Parquet footers have been closed and all output workers joined.
    unsafe extern "C" {
        fn _exit(status: std::ffi::c_int) -> !;
    }
    unsafe { _exit(0) }
}
