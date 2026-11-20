mod aggregate;
mod input;
mod keys;
mod output;

use aggregate::{Click, Event, Stats, Totals};
use input::{DAY, Dates, INVALID, Input, WEEK, jobs, rows};
use keys::{IntMap, Key, Keys, People, mix};
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
            rows::<2>(chunk, |v| {
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
            rows::<2>(chunk, |v| {
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
        let ingest = dates.stamp(f[3]).expect("invalid ingest time");
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

fn winners(parts: Vec<Vec<Candidate>>, people: &People, out: &mut [Vec<Event>]) {
    let count = parts.iter().map(Vec::len).sum::<usize>();
    if count == 0 {
        return;
    }
    let capacity = (count + count / 2 + 1).next_power_of_two();
    let mut slots = vec![0u32; capacity];
    let mut records: Vec<Candidate> = Vec::with_capacity(count);
    for copies in parts {
        for copy in copies {
            // Use different hash bits from the partition selector.
            let mut slot = (mix(copy.id) >> 20) as usize & (capacity - 1);
            loop {
                let pos = slots[slot];
                if pos == 0 {
                    records.push(copy);
                    slots[slot] = records.len() as u32;
                    break;
                }
                let winner = &mut records[pos as usize - 1];
                if winner.id == copy.id {
                    if copy.ingest < winner.ingest {
                        *winner = copy;
                    }
                    break;
                }
                slot = (slot + 1) & (capacity - 1);
            }
        }
    }
    for e in records {
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
    let links = Input::open(&data.join("links"));
    let edges = jobs(
        links.chunks(),
        threads,
        |_| Vec::new(),
        |local, chunk| {
            rows::<2>(chunk, |f| {
                if !f[0].is_empty() && !f[1].is_empty() {
                    local.push((keys.key(f[0]), keys.key(f[1])));
                }
            });
        },
    );
    let mut people = People::default();
    for lane in edges {
        for (a, b) in lane {
            people.join(&keys, a, b);
        }
    }
    people.freeze();
    drop(links);
    stage("catalogs and links");

    let raw = Input::open(&data.join("events"));
    let n = ((raw.bytes() / (2 << 20)).max(1))
        .next_power_of_two()
        .min(4096);
    let lanes = jobs(
        raw.chunks(),
        threads,
        |_| (Dates::new(), columns(n)),
        |s, chunk| {
            events(chunk, &mut s.0, &keys, &catalog, start, end, &mut s.1);
        },
    );
    let parts = transpose(lanes.into_iter().map(|s| s.1).collect(), n);
    drop(raw);
    stage("event parse");
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

    let raw = Input::open(&data.join("clicks"));
    let lanes = jobs(
        raw.chunks(),
        threads,
        |_| (Dates::new(), columns(person_shards)),
        |s, chunk| {
            rows::<4>(chunk, |f| {
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
            local.partition(
                &keys,
                es.into_iter().flatten().collect(),
                cs.into_iter().flatten().collect(),
            );
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
}
