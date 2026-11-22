use crate::{
    input::{DAY, WEEK},
    keys::{Key, Keys},
};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Default)]
pub struct Event {
    pub u: Key,
    pub t: i64,
    pub cents: u32,
    pub cat: u32,
}
pub trait Stored: Copy + Send {
    fn encode(event: Event, start: i64) -> Self;
    fn expand(self, start: i64) -> Event;
    fn user(&mut self) -> &mut Key;
}
impl Stored for Event {
    fn encode(event: Event, _: i64) -> Self {
        event
    }
    fn expand(self, _: i64) -> Event {
        self
    }
    fn user(&mut self) -> &mut Key {
        &mut self.u
    }
}
#[derive(Clone, Copy)]
pub struct SmallEvent {
    user: Key,
    data: u64,
}
impl Stored for SmallEvent {
    fn encode(e: Event, start: i64) -> Self {
        Self {
            user: e.u,
            data: ((e.t - start) as u64) << 32 | (u64::from(e.cat) << 24) | u64::from(e.cents),
        }
    }
    fn expand(self, start: i64) -> Event {
        Event {
            u: self.user,
            t: start + (self.data >> 32) as i64,
            cents: self.data as u32 & 0xffffff,
            cat: (self.data as u32) >> 24,
        }
    }
    fn user(&mut self) -> &mut Key {
        &mut self.user
    }
}
#[derive(Clone, Copy, Default)]
pub struct Click {
    pub u: Key,
    pub t: i64,
    pub id: Key,
    pub campaign: u32,
}

#[derive(Default, Clone, Copy)]
pub struct Stats {
    pub sessions: i64,
    pub buying: i64,
    pub revenue: i64,
    pub assists: i64,
}
impl Stats {
    pub fn add(&mut self, other: Self) {
        self.sessions += other.sessions;
        self.buying += other.buying;
        self.revenue += other.revenue;
        self.assists += other.assists;
    }
}
#[derive(Clone, Copy, Default)]
pub struct UserRow {
    pub u: Key,
    pub day: i32,
    pub sessions: i64,
    pub events: i64,
    pub revenue: i64,
    pub top: Option<u32>,
}
pub struct Totals {
    pub users: Vec<UserRow>,
    pub(crate) writer: Option<crate::output::UserWriter>,
    origin: i64,
    packed: bool,
    base_day: i32,
    dense_days: Vec<Vec<Stats>>,
    campaigns: BTreeMap<i32, Vec<Stats>>,
    category_cents: Vec<i64>,
    touched: Vec<u32>,
    counts: Vec<u32>,
    active: Vec<u32>,
    positions: Vec<usize>,
    bins: SessionBins,
    coarse_min: usize,
    coarse_bins: SessionBins,
    single_max: usize,
}
#[derive(Clone, Copy, Default)]
struct IndexSlot {
    key: Key,
    index: usize,
}

fn position(table: &[IndexSlot], key: Key) -> usize {
    let mask = table.len() - 1;
    let mut i = (key >> 16) as usize & mask;
    while table[i].key != 0 && table[i].key != key {
        i = (i + 1) & mask;
    }
    i
}
#[derive(Default, Clone, Copy)]
struct Moment {
    t: i64,
    cents: u32,
    cat: u32,
}
// A short reporting window and a small category catalog fit a complete event
// in one sortable word. The generic record covers every other manifest.
trait Observation: Copy + Default {
    fn new(event: Event, origin: i64) -> Self;
    fn time(self, origin: i64) -> i64;
    fn cents(self) -> u32;
    fn category(self) -> u32;
    fn order(values: &mut [Self]);
}
impl Observation for u64 {
    #[inline(always)]
    fn new(e: Event, origin: i64) -> Self {
        ((e.t - origin) as u64) << 32 | ((e.cat as u64) << 24) | e.cents as u64
    }
    #[inline(always)]
    fn time(self, origin: i64) -> i64 {
        origin + (self >> 32) as i64
    }
    #[inline(always)]
    fn cents(self) -> u32 {
        (self as u32) & 0xffffff
    }
    #[inline(always)]
    fn category(self) -> u32 {
        (self as u32) >> 24
    }
    fn order(values: &mut [Self]) {
        values.sort_unstable();
    }
}
impl Observation for Moment {
    #[inline(always)]
    fn new(e: Event, _: i64) -> Self {
        Self {
            t: e.t,
            cents: e.cents,
            cat: e.cat,
        }
    }
    #[inline(always)]
    fn time(self, _: i64) -> i64 {
        self.t
    }
    #[inline(always)]
    fn cents(self) -> u32 {
        self.cents
    }
    #[inline(always)]
    fn category(self) -> u32 {
        self.cat
    }
    fn order(values: &mut [Self]) {
        values.sort_unstable_by_key(|e| e.t);
    }
}

#[derive(Default, Clone, Copy)]
struct Touch {
    t: i64,
    id: Key,
    campaign: u32,
}

pub(crate) struct DenseScratch {
    index: Vec<u32>,
    es: Vec<u64>,
    cs: Vec<Touch>,
    reuse: bool,
    arena: Option<crate::compact::ParseArena>,
}
impl DenseScratch {
    pub(crate) fn new(
        es: Vec<u64>,
        reuse: bool,
        arena: Option<crate::compact::ParseArena>,
    ) -> Self {
        Self {
            index: Vec::new(),
            es,
            cs: Vec::new(),
            reuse,
            arena,
        }
    }
}

const GAP: u32 = 1_800_000;
const BIN_BITS: u32 = 20;
const BIN_THRESHOLD: usize = 256;
const N_BINS: usize = 1 << (32 - BIN_BITS);
const COARSE_WIDTH: u32 = 7_200_000;
const COARSE_COUNT: usize = (u32::MAX / COARSE_WIDTH) as usize + 1;

#[derive(Clone, Copy)]
struct SessionBin {
    first: u32,
    last: u32,
    count: u32,
    purchase: u32,
}

impl Default for SessionBin {
    fn default() -> Self {
        Self {
            first: u32::MAX,
            last: 0,
            count: 0,
            purchase: 0,
        }
    }
}

#[derive(Default)]
struct SessionBins {
    bins: Vec<SessionBin>,
    purchased: Vec<(u32, u32, u32)>,
}

#[derive(Default)]
struct Window {
    left: usize,
    right: usize,
    last_time: i64,
    winner: u32,
    winner_id: Key,
}

impl Totals {
    pub fn new(cats: usize, camps: usize, start: i64, end: i64) -> Self {
        let base_day = start.div_euclid(DAY) as i32;
        let len =
            (end.saturating_sub(1).div_euclid(DAY) - base_day as i64 + 1).clamp(0, i32::MAX as i64);
        let dense_days = if len <= 128 {
            vec![Vec::new(); len as usize]
        } else {
            Vec::new()
        };
        Self {
            users: Vec::new(),
            writer: None,
            origin: start,
            packed: end.saturating_sub(start) <= u32::MAX as i64 && cats <= 256,
            base_day,
            dense_days,
            campaigns: BTreeMap::new(),
            category_cents: vec![0; cats],
            touched: Vec::new(),
            counts: vec![0; camps],
            active: Vec::new(),
            positions: vec![0; camps],
            bins: SessionBins::default(),
            coarse_bins: SessionBins::default(),
            single_max: std::env::var("SHOPPING_CERT_SESSION_MAX").ok()
                .and_then(|n| n.parse().ok()).unwrap_or(0).min(64),
            // Every group of two or more events first tries the sort-free
            // two-hour interval proof; groups it cannot prove fall back.
            coarse_min: std::env::var("SHOPPING_COARSE_MIN").ok()
                .and_then(|n| n.parse().ok()).unwrap_or(2).max(2),
        }
    }
    pub fn days(self) -> BTreeMap<i32, Vec<Stats>> {
        let mut days = self.campaigns;
        for (i, stats) in self.dense_days.into_iter().enumerate() {
            if !stats.is_empty() {
                days.insert(self.base_day + i as i32, stats);
            }
        }
        days
    }
    fn finish_day(&mut self, mut row: UserRow, keys: &Keys) {
        let mut best = 0;
        for cat in self.touched.drain(..) {
            let amount = std::mem::take(&mut self.category_cents[cat as usize]);
            if amount > best || (amount == best && Some(cat) < row.top) {
                row.top = Some(cat);
                best = amount;
            }
        }
        if let Some(writer) = &mut self.writer {
            writer.row(row, keys);
        } else {
            self.users.push(row);
        }
    }
    fn add_click(&mut self, c: u32) {
        let i = c as usize;
        if self.counts[i] == 0 {
            self.positions[i] = self.active.len();
            self.active.push(c);
        }
        self.counts[i] += 1;
    }
    fn expire(&mut self, c: u32) {
        let i = c as usize;
        self.counts[i] -= 1;
        if self.counts[i] == 0 {
            let slot = self.positions[i];
            self.active.swap_remove(slot);
            if slot < self.active.len() {
                self.positions[self.active[slot] as usize] = slot;
            }
        }
    }
    fn person<M: Observation>(&mut self, keys: &Keys, u: Key, events: &[M], clicks: &[Touch]) {
        let mut row = UserRow {
            u,
            day: events[0].time(self.origin).div_euclid(DAY) as i32,
            ..Default::default()
        };
        let mut at = 0;
        let mut window = Window {
            last_time: i64::MIN,
            ..Window::default()
        };
        while at < events.len() {
            let start = events[at].time(self.origin);
            let day = start.div_euclid(DAY) as i32;
            if row.day != day {
                self.finish_day(row, keys);
                row = UserRow {
                    u,
                    day,
                    ..Default::default()
                };
            }
            let first = at;
            let mut revenue = 0;
            loop {
                let e = events[at];
                if e.cents() > 0 {
                    let value = &mut self.category_cents[e.category() as usize];
                    if *value == 0 {
                        self.touched.push(e.category());
                    }
                    *value += e.cents() as i64;
                    revenue += e.cents() as i64;
                }
                at += 1;
                if at == events.len()
                    || events[at].time(self.origin) - e.time(self.origin) > 1_800_000
                {
                    break;
                }
            }
            row.sessions += 1;
            row.events += (at - first) as i64;
            row.revenue += revenue;
            self.credit(keys, clicks, start, day, revenue, &mut window);
        }
        self.finish_day(row, keys);
        for c in self.active.drain(..) {
            self.counts[c as usize] = 0;
        }
    }
    #[inline(always)]
    fn credit(
        &mut self,
        keys: &Keys,
        clicks: &[Touch],
        start: i64,
        day: i32,
        revenue: i64,
        window: &mut Window,
    ) {
        while window.right < clicks.len() && clicks[window.right].t <= start {
            let c = clicks[window.right];
            self.add_click(c.campaign);
            if c.t != window.last_time || keys.cmp(c.id, window.winner_id).is_lt() {
                window.last_time = c.t;
                window.winner_id = c.id;
                window.winner = c.campaign;
            }
            window.right += 1;
        }
        while window.left < window.right && clicks[window.left].t < start - WEEK {
            self.expire(clicks[window.left].campaign);
            window.left += 1;
        }
        let stats = if !self.dense_days.is_empty() {
            let stats = &mut self.dense_days[(day - self.base_day) as usize];
            if stats.is_empty() {
                stats.resize(self.counts.len(), Stats::default());
            }
            stats
        } else {
            self.campaigns
                .entry(day)
                .or_insert_with(|| vec![Stats::default(); self.counts.len()])
        };
        let credit = if self.active.is_empty() {
            0
        } else {
            window.winner as usize
        };
        stats[credit].sessions += 1;
        stats[credit].buying += i64::from(revenue > 0);
        stats[credit].revenue += revenue;
        for &c in &self.active {
            stats[c as usize].assists += 1;
        }
    }
    fn single_session<M: Observation>(&mut self, keys: &Keys, user: Key, events: &[M], clicks: &[Touch]) -> bool {
        let mut first = events[0].time(self.origin);
        let mut last = first;
        for &event in &events[1..] {
            let time = event.time(self.origin);
            first = first.min(time);
            last = last.max(time);
            if last - first > i64::from(GAP) {
                return false;
            }
        }
        // All observed times are within one session gap, so sorting cannot
        // reveal an internal break and the minimum gives the exact start.
        let mut revenue = 0;
        for &event in events {
            let cents = i64::from(event.cents());
            if cents != 0 {
                let category = event.category();
                let sum = &mut self.category_cents[category as usize];
                if *sum == 0 {
                    self.touched.push(category);
                }
                *sum += cents;
                revenue += cents;
            }
        }
        let day = first.div_euclid(DAY) as i32;
        let mut best = None::<Touch>;
        for &click in clicks {
            if click.t < first - WEEK || click.t > first {
                continue;
            }
            let seen = &mut self.counts[click.campaign as usize];
            if *seen == 0 {
                *seen = 1;
                self.active.push(click.campaign);
            }
            if best.is_none_or(|previous|
                click.t > previous.t || (click.t == previous.t && keys.cmp(click.id, previous.id).is_lt())
            ) {
                best = Some(click);
            }
        }
        let stats = if !self.dense_days.is_empty() {
            let stats = &mut self.dense_days[(day - self.base_day) as usize];
            if stats.is_empty() {
                stats.resize(self.counts.len(), Stats::default());
            }
            stats
        } else {
            self.campaigns.entry(day).or_insert_with(|| vec![Stats::default(); self.counts.len()])
        };
        let credit = best.map_or(0, |click| click.campaign as usize);
        stats[credit].sessions += 1;
        stats[credit].buying += i64::from(revenue > 0);
        stats[credit].revenue += revenue;
        for &c in &self.active {
            stats[c as usize].assists += 1;
        }
        self.finish_day(UserRow {
            u: user, day, sessions: 1, events: events.len() as i64, revenue, top: None,
        }, keys);
        for channel in self.active.drain(..) {
            self.counts[channel as usize] = 0;
        }
        true
    }

    fn person_binned<M: Observation, const COARSE: bool>(
        &mut self,
        keys: &Keys,
        u: Key,
        events: &[M],
        clicks: &[Touch],
    ) -> bool {
        let mut scratch = std::mem::take(if COARSE { &mut self.coarse_bins } else { &mut self.bins });
        let count = if COARSE { COARSE_COUNT } else { N_BINS };
        scratch.bins.resize(count, SessionBin::default());
        scratch.purchased.clear();
        // Each interval is shorter than the session gap, so all of its
        // events are connected without sorting. Adjacent occupied intervals
        // connect exactly when their nearest observed times are within GAP.
        let mut bit_storage = [0u64; N_BINS / 64];
        let present = &mut bit_storage[..count.div_ceil(64)];
        let origin = self.origin;
        for &event in events {
            let time = (event.time(origin) - origin) as u32;
            let index = if COARSE { time / COARSE_WIDTH } else { time >> BIN_BITS } as usize;
            let bin = &mut scratch.bins[index];
            bin.first = bin.first.min(time);
            bin.last = bin.last.max(time);
            bin.count += 1;
            present[index >> 6] |= 1 << (index & 63);
            if COARSE && bin.last - bin.first > GAP {
                // A range wider than GAP has no connectivity proof. Reset
                // this person's scratch and use the existing exact path.
                for (word, &bits) in present.iter().enumerate() {
                    let mut live = bits;
                    while live != 0 {
                        let at = word * 64 + live.trailing_zeros() as usize;
                        scratch.bins[at] = SessionBin::default();
                        live &= live - 1;
                    }
                }
                self.coarse_bins = scratch;
                return false;
            }
            if event.cents() != 0 {
                scratch
                    .purchased
                    .push((event.category(), event.cents(), bin.purchase));
                bin.purchase = scratch.purchased.len() as u32;
            }
        }

        let mut row = UserRow {
            u,
            day: i32::MIN,
            ..UserRow::default()
        };
        // The latest left and earliest right timestamps give the actual
        // gap across intervals, including at reporting-day boundaries.
        let mut previous = 0u32;
        let mut session_start = 0i64;
        let mut session_revenue = 0;
        let mut window = Window {
            last_time: i64::MIN,
            ..Window::default()
        };
        for (upper, &bits) in present.iter().enumerate() {
            let mut bits = bits;
            while bits != 0 {
                let index = upper * 64 + bits.trailing_zeros() as usize;
                bits &= bits - 1;
                let bin = std::mem::take(&mut scratch.bins[index]);
                if row.sessions == 0 || bin.first - previous > GAP {
                    if row.sessions != 0 {
                        self.credit(
                            keys,
                            clicks,
                            session_start,
                            row.day,
                            session_revenue,
                            &mut window,
                        );
                    }
                    session_start = origin + i64::from(bin.first);
                    let day = session_start.div_euclid(DAY) as i32;
                    if row.day != day {
                        if row.sessions != 0 {
                            self.finish_day(row, keys);
                        }
                        row = UserRow {
                            u,
                            day,
                            ..UserRow::default()
                        };
                    }
                    row.sessions += 1;
                    session_revenue = 0;
                }
                previous = bin.last;
                row.events += i64::from(bin.count);
                let mut purchase = bin.purchase;
                while purchase != 0 {
                    let (category, cents, next) = scratch.purchased[(purchase - 1) as usize];
                    let value = &mut self.category_cents[category as usize];
                    if *value == 0 {
                        self.touched.push(category);
                    }
                    *value += i64::from(cents);
                    session_revenue += i64::from(cents);
                    row.revenue += i64::from(cents);
                    purchase = next;
                }
            }
        }
        self.credit(
            keys,
            clicks,
            session_start,
            row.day,
            session_revenue,
            &mut window,
        );
        self.finish_day(row, keys);
        for c in self.active.drain(..) {
            self.counts[c as usize] = 0;
        }
        if COARSE { self.coarse_bins = scratch; } else { self.bins = scratch; }
        true
    }

    pub(crate) fn partition_dense(
        &mut self,
        keys: &Keys,
        people: &crate::compact::DensePeople,
        mut events: Vec<crate::compact::Output<crate::compact::Event, 16>>,
        mut clicks: Vec<crate::compact::Output<crate::compact::Click, 8>>,
        shards: usize,
        max_people: usize,
        scratch: &mut DenseScratch,
    ) {
        let n = events.iter().map(|v| v.len()).sum();
        if n == 0 {
            return;
        }
        let mut index = if scratch.reuse {
            let mut index = std::mem::take(&mut scratch.index);
            index.resize(max_people, 0);
            index
        } else {
            vec![0u32; max_people]
        };
        let mut groups = Vec::<(u32, usize, usize)>::new();
        let shift = shards.trailing_zeros();
        for rows in &mut events {
            rows.walk(|event| {
                let slot = &mut index[(event.person >> shift) as usize];
                if *slot == 0 {
                    groups.push((event.person, 0, 0));
                    *slot = groups.len() as u32;
                }
                event.person = *slot - 1;
                groups[event.person as usize].1 += 1;
            });
        }
        for rows in &mut clicks {
            rows.walk(|click| {
                let slot = index[(click.person >> shift) as usize];
                if slot != 0 {
                    groups[slot as usize - 1].2 += 1;
                }
                click.person = slot;
            });
        }
        let (mut eoff, mut coff) = (0, 0);
        for (_, e, c) in &mut groups {
            let (ne, nc) = (*e, *c);
            *e = eoff;
            *c = coff;
            eoff += ne;
            coff += nc;
        }
        let mut es = if scratch.reuse {
            std::mem::take(&mut scratch.es)
        } else {
            Vec::new()
        };
        let mut cs = if scratch.reuse {
            std::mem::take(&mut scratch.cs)
        } else {
            Vec::new()
        };
        es.clear();
        cs.clear();
        let mut mapped = scratch.arena.as_mut().and_then(|arena| arena.words(n));
        let event_base = if let Some(words) = &mut mapped {
            words.as_mut_ptr()
        } else {
            es.reserve(n);
            if !scratch.reuse {
                es.resize(n, 0);
            }
            es.as_mut_ptr()
        };
        cs.reserve(coff);
        if !scratch.reuse {
            cs.resize(coff, Touch::default());
        }
        let click_base = cs.as_mut_ptr();
        for rows in &mut events {
            rows.walk(|event| {
                let at = &mut groups[event.person as usize].1;
                let value = u64::from(event.ts) << 32 | u64::from(event.purchase);
                // Group offsets are disjoint prefixes with exactly n events.
                unsafe { event_base.add(*at).write(value) };
                *at += 1;
            });
        }
        for rows in &mut clicks {
            rows.walk(|click| {
                if click.person != 0 {
                    let at = &mut groups[click.person as usize - 1].2;
                    // Click prefixes count only events with a non-empty group.
                    unsafe {
                        click_base.add(*at).write(Touch {
                            t: click.t,
                            id: click.id,
                            campaign: click.campaign,
                        })
                    };
                    *at += 1;
                }
            });
        }
        // Each prefix slot was initialized exactly once by the scatter above.
        unsafe {
            if mapped.is_none() {
                es.set_len(n);
            }
            cs.set_len(coff);
        }
        let events = if let Some(words) = mapped {
            words
        } else {
            es.as_mut_slice()
        };
        let (mut e, mut c) = (0, 0);
        for (person, end_e, end_c) in groups {
            let events = &mut events[e..end_e];
            let clicks = &mut cs[c..end_c];
            events.sort_unstable();
            clicks.sort_unstable_by_key(|x| x.t);
            self.person(keys, people.key(person), events, clicks);
            if scratch.reuse {
                index[(person >> shift) as usize] = 0;
            }
            e = end_e;
            c = end_c;
        }
        if scratch.reuse {
            scratch.index = index;
            scratch.es = es;
            scratch.cs = cs;
        }
    }

    pub fn partition<E: Stored>(
        &mut self,
        keys: &Keys,
        events: Vec<&mut [E]>,
        clicks: Vec<Vec<Click>>,
    ) {
        if self.packed {
            self.partition_inner::<u64, E>(keys, events, clicks)
        } else {
            self.partition_inner::<Moment, E>(keys, events, clicks)
        }
    }
    fn partition_inner<M: Observation, E: Stored>(
        &mut self,
        keys: &Keys,
        mut events: Vec<&mut [E]>,
        mut clicks: Vec<Vec<Click>>,
    ) {
        let ne = events.iter().map(|s| s.len()).sum::<usize>();
        if ne == 0 {
            return;
        }
        let mut table = vec![IndexSlot::default(); (ne / 12 + 8).next_power_of_two()];
        let mut groups: Vec<(Key, usize, usize)> = Vec::new();
        for e in events.iter_mut().flat_map(|s| s.iter_mut()) {
            let u = e.user();
            let i = position(&table, *u);
            if table[i].key == 0 {
                table[i] = IndexSlot {
                    key: *u,
                    index: groups.len(),
                };
                groups.push((crate::keys::unmix(*u), 0, 0));
            }
            let index = table[i].index;
            groups[index].1 += 1;
            *u = index as u64;
            if groups.len() * 4 >= table.len() * 3 {
                let n = table.len() * 2;
                let old = std::mem::replace(&mut table, vec![IndexSlot::default(); n]);
                for s in old.into_iter().filter(|s| s.key != 0) {
                    let i = position(&table, s.key);
                    table[i] = s;
                }
            }
        }
        for c in clicks.iter_mut().flatten() {
            let s = table[position(&table, c.u)];
            if s.key != 0 {
                groups[s.index].2 += 1;
                c.u = s.index as u64;
            } else {
                c.u = u64::MAX;
            }
        }
        let (mut eoff, mut coff) = (0, 0);
        for (_, e, c) in &mut groups {
            let (ne, nc) = (*e, *c);
            *e = eoff;
            *c = coff;
            eoff += ne;
            coff += nc;
        }
        let mut es = Vec::<M>::with_capacity(ne);
        let mut cs = Vec::<Touch>::with_capacity(coff);
        let event_slots = es.spare_capacity_mut();
        let click_slots = cs.spare_capacity_mut();
        for &e in events.iter().flat_map(|s| s.iter()) {
            let e = e.expand(self.origin);
            let i = &mut groups[e.u as usize].1;
            event_slots[*i].write(M::new(e, self.origin));
            *i += 1;
        }
        for c in clicks.into_iter().flatten().filter(|c| c.u != u64::MAX) {
            let i = &mut groups[c.u as usize].2;
            click_slots[*i].write(Touch {
                t: c.t,
                id: c.id,
                campaign: c.campaign,
            });
            *i += 1;
        }
        // The prefix ranges count every event and every retained click exactly
        // once. The two scatter passes initialized every counted position.
        unsafe {
            es.set_len(ne);
            cs.set_len(coff);
        }
        let (mut e, mut c) = (0, 0);
        for (user, end_e, end_c) in groups {
            let events = &mut es[e..end_e];
            let clicks = &mut cs[c..end_c];
            let single = events.len() <= self.single_max && self.single_session(keys, user, events, clicks);
            if !single {
                clicks.sort_unstable_by_key(|click| click.t);
            }
            let coarse = !single && self.packed && (self.coarse_min..=u32::MAX as usize).contains(&events.len())
                && self.person_binned::<M, true>(keys, user, events, clicks);
            if single || coarse {
                // The range proof already emitted the complete group.
            } else if self.packed && (BIN_THRESHOLD..=u32::MAX as usize).contains(&events.len()) {
                self.person_binned::<M, false>(keys, user, events, clicks);
            } else {
                M::order(events);
                self.person(keys, user, events, clicks);
            }
            e = end_e;
            c = end_c;
        }
    }
}
