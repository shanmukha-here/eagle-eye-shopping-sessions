#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;
mod aggregate;
mod input;
mod keys;
mod output;

use aggregate::{Click, Event, Stats, Totals};
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

fn transpose<T>(lanes: Vec<Vec<Vec<T>>>, n: usize) -> Vec<Vec<Vec<T>>> {
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

// All ingest dates are four-digit Gregorian years. Their Unix seconds
// plus this offset fit in 39 bits, leaving 24 bits for a valid purchase.
const INGEST_BIAS: i64 = 70_000_000_000;
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
    ingest_price: u64,
    t: T,
    cat: u32,
}

fn events<T: EventTime>(
    chunk: &[u8],
    dates: &mut Dates,
    keys: &Keys,
    catalog: &Catalog,
    start: i64,
    end: i64,
    partitions: &mut [Vec<Candidate<T>>],
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
            let mut candidate = Candidate {
                id,
                user: 0,
                ingest_price: ((ingest / 1000 + INGEST_BIAS) as u64) << 24,
                t: T::pack(INVALID, start),
                cat: 0,
            };
            // Even a semantically invalid earliest copy must win deduplication.
            if let Some(t) = instant.filter(|&t| t >= start && t < end && t >= ingest - DAY) {
                if !f[1].is_empty() && !f[5].is_empty() {
                    let ok = match f[4] {
                        b"view" | b"cart" => true,
                        b"purchase" => match input::decimal(f[6], 10_000_000).filter(|&p| p != 0) {
                            Some(price) => {
                                candidate.ingest_price |= price;
                                candidate.cat =
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
            let bucket = (id as usize) & (partitions.len() - 1);
            partitions[bucket].push(candidate);
        },
    );
}

fn winners<T: EventTime>(
    parts: Vec<Vec<Candidate<T>>>,
    people: &Aliases,
    out: &mut [Vec<Event>],
    start: i64,
) {
    let count = parts.iter().map(Vec::len).sum::<usize>();
    if count == 0 {
        return;
    }
    // First find unique hash positions in a cache-resident bitmap. Only IDs
    // at repeated positions require a full-key minimum-ingest reduction.
    let bits = (count * 8).max(64).next_power_of_two();
    let mut seen = vec![0u64; bits / 64];
    let mut collisions = vec![0u64; bits / 64];
    let index = |key: u64| (key >> 20) as usize & (bits - 1);
    for e in parts.iter().flatten() {
        let i = index(e.id);
        let mask = 1u64 << (i % 64);
        collisions[i / 64] |= seen[i / 64] & mask;
        seen[i / 64] |= mask;
    }
    let mut emit = |e: Candidate<T>| {
        if let Some(t) = e.t.unpack(start) {
            let u = people.canonical(e.user);
            let shard = u as usize & (out.len() - 1);
            out[shard].push(Event {
                u,
                t,
                cents: (e.ingest_price & 0xffffff) as u32,
                cat: e.cat,
            });
        }
    };
    let mut repeated = IntMap::<Candidate<T>>::default();
    repeated.reserve(count / 8);
    for copies in parts {
        for block in copies.chunks(32) {
            for e in block {
                people.prefetch(e.user);
            }
            for &e in block {
                let i = index(e.id);
                if collisions[i / 64] & (1u64 << (i % 64)) == 0 {
                    emit(e);
                } else {
                    repeated
                        .entry(e.id)
                        .and_modify(|old| {
                            if e.ingest_price < old.ingest_price {
                                *old = e
                            }
                        })
                        .or_insert(e);
                }
            }
        }
    }
    for e in repeated.into_values() {
        emit(e);
    }
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
    let stage = |name: &str| {
        eprintln!("{name} {:.3}s", mark.elapsed().as_secs_f64());
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
    if end.saturating_sub(start) <= u32::MAX as i64 {
        pipeline::<u32>(&data, &out, threads, start, end, stage);
    } else {
        pipeline::<i64>(&data, &out, threads, start, end, stage);
    }
}

fn pipeline<T: EventTime>(
    data: &Path,
    out: &Path,
    threads: usize,
    start: i64,
    end: i64,
    mut stage: impl FnMut(&str),
) {
    let keys = Keys::default();
    let catalog = Catalog::read(data, &keys);
    let raw = Input::open(&data.join("events"));
    let n = ((raw.bytes() / (6 << 20)).max(1))
        .next_power_of_two()
        .min(4096);
    let people = std::sync::OnceLock::new();
    let mut tasks = vec![None];
    tasks.extend(raw.chunks().into_iter().map(Some));
    let click_input = Input::open(&data.join("clicks"));
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
                        .map(|_| Vec::with_capacity(expected))
                        .collect::<Vec<_>>(),
                )
            },
            |s, task| {
                if let Some(chunk) = task {
                    events(
                        chunk.records(),
                        &mut s.0,
                        &keys,
                        &catalog,
                        start,
                        end,
                        &mut s.1,
                    );
                } else {
                    people.get_or_init(|| {
                        let links = Input::open(&data.join("links"));
                        let mut people = People::default();
                        people.reserve(links.bytes() / 22);
                        for chunk in links.chunks() {
                            rows::<2>(chunk.records(), |f| {
                                if !f[0].is_empty() && !f[1].is_empty() {
                                    people.join(&keys, keys.key(f[0]), keys.key(f[1]));
                                }
                            });
                        }
                        drop(links);
                        people.freeze()
                    });
                }
            },
        )
    });
    let people = people.into_inner().unwrap();
    let parts = transpose(lanes.into_iter().map(|s| s.1).collect(), n);
    drop(raw);
    stage("event parse and identity graph");
    let person_shards = n.min(512);
    enum Work<'a, T> {
        Copies(Vec<Vec<Candidate<T>>>),
        Clicks(input::Chunk<'a>),
    }
    let mut copies = parts.into_iter();
    let mut chunks = click_input.chunks().into_iter();
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
        |_| (Dates::new(), columns(person_shards), columns(person_shards)),
        |s, task| match task {
            Work::Copies(part) => winners::<T>(part, &people, &mut s.1, start),
            Work::Clicks(chunk) => {
                rows::<4>(
                    chunk.records(),
                    #[inline(always)]
                    |f| {
                        if f[0].is_empty() || f[1].is_empty() {
                            return;
                        }
                        let Some(t) = s.0.stamp(f[3]).filter(|&t| t >= start - WEEK && t < end)
                        else {
                            return;
                        };
                        let Some(campaign) = catalog.campaigns.get(&keys, f[2]) else {
                            return;
                        };
                        let u = people.canonical(mix(keys.key(f[1])));
                        let bucket = u as usize & (person_shards - 1);
                        // Identical click copies have the same winner key and expire
                        // together. The active set counts their campaign exactly once.
                        s.2[bucket].push(Click {
                            u,
                            t,
                            id: keys.key(f[0]),
                            campaign,
                        });
                    },
                );
            }
        },
    );
    let (events, clicks): (Vec<_>, Vec<_>) = lanes.into_iter().map(|(_, e, c)| (e, c)).unzip();
    let event_parts = transpose(events, person_shards);
    let click_parts = transpose(clicks, person_shards);
    drop(click_input);
    drop(people);
    stage("event dedup and click parsing");
    let work = event_parts.into_iter().zip(click_parts).collect();
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
