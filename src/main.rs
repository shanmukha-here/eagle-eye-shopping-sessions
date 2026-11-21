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

struct Catalog {
    products: IntMap<u32>,
    categories: Vec<ByteArray>,
    unknown: u32,
    campaigns: IntMap<u32>,
    channels: Vec<(ByteArray, ByteArray)>,
}
impl Catalog {
    fn read(data: &Path, keys: &Keys) -> Self {
        let mut names = vec![b"unknown".to_vec()];
        let mut pairs = Vec::new();
        for chunk in Input::open(&data.join("products.csv")).chunks() {
            rows::<2>(chunk.records(), |v| {
                names.push(v[1].to_vec());
                pairs.push((keys.key(v[0]), v[1].to_vec()));
            });
        }
        names.sort_unstable();
        names.dedup();
        let unknown = names
            .binary_search_by(|s| s.as_slice().cmp(b"unknown"))
            .unwrap() as u32;
        let products = pairs
            .into_iter()
            .map(|(k, s)| (k, names.binary_search(&s).unwrap() as u32))
            .collect();
        let mut campaigns = IntMap::default();
        let mut channels = vec![(ByteArray::from("organic"), ByteArray::from("organic"))];
        for chunk in Input::open(&data.join("campaigns.csv")).chunks() {
            rows::<2>(chunk.records(), |v| {
                campaigns.insert(keys.key(v[0]), channels.len() as u32);
                channels.push((ByteArray::from(v[0]), ByteArray::from(v[1])));
            });
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

#[derive(Clone, Copy)]
struct Candidate {
    id: Key,
    user: Key,
    ingest: i64,
    t: i64,
    cents: u32,
    cat: u32,
}

fn events(
    chunk: &[u8],
    dates: &mut Dates,
    keys: &Keys,
    catalog: &Catalog,
    start: i64,
    end: i64,
    partitions: &mut [Vec<Candidate>],
) {
    rows::<7>(chunk, |f| {
        if f[0].is_empty() {
            return;
        }
        let id = keys.key(f[0]);
        let ingest = dates.ingest(f[3]);
        let mut candidate = Candidate {
            id,
            user: 0,
            ingest,
            t: INVALID,
            cents: 0,
            cat: 0,
        };
        // Even a semantically invalid earliest copy must win deduplication.
        if let Some(t) = dates
            .stamp(f[2])
            .filter(|&t| t >= start && t < end && t >= ingest - DAY)
        {
            if !f[1].is_empty() && !f[5].is_empty() {
                let ok = match f[4] {
                    b"view" | b"cart" => true,
                    b"purchase" => match input::decimal(f[6], 10_000_000).filter(|&p| p != 0) {
                        Some(price) => {
                            candidate.cents = price as u32;
                            candidate.cat = *catalog
                                .products
                                .get(&keys.key(f[5]))
                                .unwrap_or(&catalog.unknown);
                            true
                        }
                        None => false,
                    },
                    _ => false,
                };
                if ok {
                    candidate.t = t;
                    candidate.user = keys.key(f[1]);
                }
            }
        }
        let bucket = (mix(id) as usize) & (partitions.len() - 1);
        partitions[bucket].push(candidate);
    });
}

fn winners(parts: Vec<Vec<Candidate>>, people: &Aliases, out: &mut [Vec<Event>]) {
    let count = parts.iter().map(Vec::len).sum::<usize>();
    if count == 0 {
        return;
    }
    // First find unique hash positions in a cache-resident bitmap. Only IDs
    // at repeated positions require a full-key minimum-ingest reduction.
    let bits = (count * 8).max(64).next_power_of_two();
    let mut seen = vec![0u64; bits / 64];
    let mut collisions = vec![0u64; bits / 64];
    let index = |key| (mix(key) >> 20) as usize & (bits - 1);
    for e in parts.iter().flatten() {
        let i = index(e.id);
        let mask = 1u64 << (i % 64);
        collisions[i / 64] |= seen[i / 64] & mask;
        seen[i / 64] |= mask;
    }
    let mut emit = |e: Candidate| {
        if e.t != INVALID {
            let u = people.canonical(e.user);
            let shard = mix(u) as usize & (out.len() - 1);
            out[shard].push(Event {
                u,
                t: e.t,
                cents: e.cents,
                cat: e.cat,
            });
        }
    };
    let mut repeated = IntMap::<Candidate>::default();
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
                            if e.ingest < old.ingest {
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
    let mut stage = |name: &str| {
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
    let keys = Keys::default();
    let catalog = Catalog::read(&data, &keys);
    let raw = Input::open(&data.join("events"));
    let n = ((raw.bytes() / (6 << 20)).max(1))
        .next_power_of_two()
        .min(4096);
    let people = std::sync::OnceLock::new();
    let mut tasks = vec![None];
    tasks.extend(raw.chunks().into_iter().map(Some));
    let click_input = Input::open(&data.join("clicks"));
    let lanes = std::thread::scope(|scope| {
        scope.spawn(|| {
            raw.prefetch();
            click_input.prefetch();
        });
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
    let grouped = jobs(
        parts,
        threads,
        |_| columns(person_shards),
        |local, copies| {
            winners(copies, &people, local);
        },
    );
    let event_parts = transpose(grouped, person_shards);
    stage("event dedup and linking");

    let raw = click_input;
    let lanes = jobs(
        raw.chunks(),
        threads,
        |_| (Dates::new(), columns(person_shards)),
        |s, chunk| {
            rows::<4>(chunk.records(), |f| {
                if f[0].is_empty() || f[1].is_empty() {
                    return;
                }
                let Some(t) = s.0.stamp(f[3]).filter(|&t| t >= start - WEEK && t < end) else {
                    return;
                };
                let Some(&campaign) = catalog.campaigns.get(&keys.key(f[2])) else {
                    return;
                };
                let u = people.canonical(keys.key(f[1]));
                let bucket = mix(u) as usize & (person_shards - 1);
                // Identical click copies have the same winner key and expire
                // together. The active set counts their campaign exactly once.
                s.1[bucket].push(Click {
                    u,
                    t,
                    id: keys.key(f[0]),
                    campaign,
                });
            });
        },
    );
    let click_parts = transpose(lanes.into_iter().map(|s| s.1).collect(), person_shards);
    drop(raw);
    drop(people);
    stage("click parse and linking");
    let work = event_parts.into_iter().zip(click_parts).collect();
    let daily = jobs(
        work,
        threads,
        |_| Totals::new(catalog.categories.len(), catalog.channels.len()),
        |local, (es, cs)| {
            local.partition(&keys, es, cs);
        },
    );
    stage("person sorts and aggregation");
    let partial = jobs(
        daily.into_iter().enumerate().collect(),
        threads,
        |_| BTreeMap::<i32, Vec<Stats>>::new(),
        |days, (i, totals)| {
            output::users(
                &out.join(format!("user_daily/part-{i}.parquet")),
                &totals.users,
                &keys,
                &catalog.categories,
            );
            for (day, stats) in totals.campaigns {
                let acc = days
                    .entry(day)
                    .or_insert_with(|| vec![Stats::default(); catalog.channels.len()]);
                for (a, b) in acc.iter_mut().zip(stats) {
                    a.add(b);
                }
            }
        },
    );
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
