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
#[derive(Default)]
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
    fn finish_day(&mut self, mut row: UserRow) {
        let mut best = 0;
        for cat in self.touched.drain(..) {
            let amount = std::mem::take(&mut self.category_cents[cat as usize]);
            if amount > best || (amount == best && Some(cat) < row.top) {
                row.top = Some(cat);
                best = amount;
            }
        }
        self.users.push(row);
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
        let (mut at, mut left, mut right) = (0, 0, 0);
        let mut last_time = i64::MIN;
        let mut winner = 0;
        let mut winner_id = 0;
        while at < events.len() {
            let start = events[at].time(self.origin);
            let day = start.div_euclid(DAY) as i32;
            if row.day != day {
                self.finish_day(row);
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
            while right < clicks.len() && clicks[right].t <= start {
                let c = clicks[right];
                self.add_click(c.campaign);
                if c.t != last_time || keys.cmp(c.id, winner_id).is_lt() {
                    last_time = c.t;
                    winner_id = c.id;
                    winner = c.campaign;
                }
                right += 1;
            }
            while left < right && clicks[left].t < start - WEEK {
                self.expire(clicks[left].campaign);
                left += 1;
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
                winner as usize
            };
            stats[credit].sessions += 1;
            stats[credit].buying += i64::from(revenue > 0);
            stats[credit].revenue += revenue;
            for &c in &self.active {
                stats[c as usize].assists += 1;
            }
        }
        self.finish_day(row);
        for c in self.active.drain(..) {
            self.counts[c as usize] = 0;
        }
    }
    pub(crate) fn partition_dense(
        &mut self,
        keys: &Keys,
        people: &crate::compact::DensePeople,
        mut events: Vec<Vec<crate::compact::Event>>,
        mut clicks: Vec<Vec<Click>>,
        shards: usize,
        max_people: usize,
    ) {
        let n = events.iter().map(Vec::len).sum();
        if n == 0 {
            return;
        }
        let mut index = vec![0u32; max_people];
        let mut groups = Vec::<(u32, usize, usize)>::new();
        let shift = shards.trailing_zeros();
        for event in events.iter_mut().flatten() {
            let slot = &mut index[(event.person >> shift) as usize];
            if *slot == 0 {
                groups.push((event.person, 0, 0));
                *slot = groups.len() as u32;
            }
            event.person = *slot - 1;
            groups[event.person as usize].1 += 1;
        }
        for click in clicks.iter_mut().flatten() {
            let slot = index[(click.u >> shift) as usize];
            if slot != 0 {
                groups[slot as usize - 1].2 += 1;
            }
            click.u = u64::from(slot);
        }
        let (mut eoff, mut coff) = (0, 0);
        for (_, e, c) in &mut groups {
            let (ne, nc) = (*e, *c);
            *e = eoff;
            *c = coff;
            eoff += ne;
            coff += nc;
        }
        let mut es = vec![0u64; n];
        let mut cs = vec![Touch::default(); coff];
        for event in events.into_iter().flatten() {
            let at = &mut groups[event.person as usize].1;
            es[*at] = u64::from(event.ts) << 32 | u64::from(event.purchase);
            *at += 1;
        }
        for click in clicks.into_iter().flatten().filter(|c| c.u != 0) {
            let at = &mut groups[click.u as usize - 1].2;
            cs[*at] = Touch {
                t: click.t,
                id: click.id,
                campaign: click.campaign,
            };
            *at += 1;
        }
        let (mut e, mut c) = (0, 0);
        for (person, end_e, end_c) in groups {
            let events = &mut es[e..end_e];
            let clicks = &mut cs[c..end_c];
            events.sort_unstable();
            clicks.sort_unstable_by_key(|x| x.t);
            self.person(keys, people.key(person), events, clicks);
            e = end_e;
            c = end_c;
        }
    }

    pub fn partition<E: Stored>(
        &mut self,
        keys: &Keys,
        events: Vec<Vec<E>>,
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
        mut events: Vec<Vec<E>>,
        mut clicks: Vec<Vec<Click>>,
    ) {
        let ne = events.iter().map(Vec::len).sum::<usize>();
        if ne == 0 {
            return;
        }
        let mut table = vec![IndexSlot::default(); (ne / 12 + 8).next_power_of_two()];
        let mut groups: Vec<(Key, usize, usize)> = Vec::new();
        for e in events.iter_mut().flatten() {
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
        let mut es = vec![M::default(); ne];
        let mut cs = vec![Touch::default(); coff];
        for e in events.into_iter().flatten() {
            let e = e.expand(self.origin);
            let i = &mut groups[e.u as usize].1;
            es[*i] = M::new(e, self.origin);
            *i += 1;
        }
        for c in clicks.into_iter().flatten().filter(|c| c.u != u64::MAX) {
            let i = &mut groups[c.u as usize].2;
            cs[*i] = Touch {
                t: c.t,
                id: c.id,
                campaign: c.campaign,
            };
            *i += 1;
        }
        let (mut e, mut c) = (0, 0);
        for (user, end_e, end_c) in groups {
            let events = &mut es[e..end_e];
            let clicks = &mut cs[c..end_c];
            M::order(events);
            clicks.sort_unstable_by_key(|c| c.t);
            self.person(keys, user, events, clicks);
            e = end_e;
            c = end_c;
        }
    }
}
