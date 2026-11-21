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
    pub campaigns: BTreeMap<i32, Vec<Stats>>,
    category_cents: Vec<i64>,
    touched: Vec<u32>,
    counts: Vec<u32>,
    active: Vec<u32>,
    positions: Vec<usize>,
}
// Higher hash bits distribute users independently of the outer person shard.
// People in different bins cannot share sessions. Only the small residual
// bins need comparison sorting, and whales still fit into one timeline.
fn radix_groups<T: Copy + Default>(
    parts: Vec<Vec<T>>,
    user: impl Fn(&T) -> Key,
) -> (Vec<T>, [usize; 128]) {
    let mut ends = [0usize; 128];
    let bin = |r: &T| (crate::keys::mix(user(r)) >> 20) as usize & 127;
    for r in parts.iter().flatten() {
        ends[bin(r)] += 1;
    }
    let mut total = 0;
    for e in &mut ends {
        let n = *e;
        *e = total;
        total += n;
    }
    let mut grouped = vec![T::default(); total];
    for r in parts.into_iter().flatten() {
        let off = &mut ends[bin(&r)];
        grouped[*off] = r;
        *off += 1;
    }
    (grouped, ends)
}
impl Totals {
    pub fn new(cats: usize, camps: usize) -> Self {
        Self {
            users: Vec::new(),
            campaigns: BTreeMap::new(),
            category_cents: vec![0; cats],
            touched: Vec::new(),
            counts: vec![0; camps],
            active: Vec::new(),
            positions: vec![0; camps],
        }
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
    fn person(&mut self, keys: &Keys, events: &[Event], clicks: &[Click]) {
        let u = events[0].u;
        let mut row = UserRow {
            u,
            day: events[0].t.div_euclid(DAY) as i32,
            ..Default::default()
        };
        let (mut at, mut left, mut right) = (0, 0, 0);
        let mut last_time = i64::MIN;
        let mut winner = 0;
        let mut winner_id = 0;
        while at < events.len() {
            let start = events[at].t;
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
                if e.cents > 0 {
                    let value = &mut self.category_cents[e.cat as usize];
                    if *value == 0 {
                        self.touched.push(e.cat);
                    }
                    *value += e.cents as i64;
                    revenue += e.cents as i64;
                }
                at += 1;
                if at == events.len() || events[at].t - e.t > 1_800_000 {
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
            let stats = self
                .campaigns
                .entry(day)
                .or_insert_with(|| vec![Stats::default(); self.counts.len()]);
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
    pub fn partition(&mut self, keys: &Keys, events: Vec<Vec<Event>>, clicks: Vec<Vec<Click>>) {
        let (mut events, ee) = radix_groups(events, |r| r.u);
        let (mut clicks, ce) = radix_groups(clicks, |r| r.u);
        let (mut ep, mut cp) = (0, 0);
        for bin in 0..ee.len() {
            let es = &mut events[ep..ee[bin]];
            let cs = &mut clicks[cp..ce[bin]];
            es.sort_unstable_by_key(|e| (e.u, e.t));
            cs.sort_unstable_by_key(|c| (c.u, c.t));
            self.timelines(keys, es, cs);
            ep = ee[bin];
            cp = ce[bin];
        }
    }
    fn timelines(&mut self, keys: &Keys, events: &[Event], clicks: &[Click]) {
        let (mut i, mut j) = (0, 0);
        while i < events.len() {
            let start = i;
            let u = events[i].u;
            while i < events.len() && events[i].u == u {
                i += 1;
            }
            while j < clicks.len() && clicks[j].u < u {
                j += 1;
            }
            let click_start = j;
            while j < clicks.len() && clicks[j].u == u {
                j += 1;
            }
            self.person(keys, &events[start..i], &clicks[click_start..j]);
        }
    }
}
