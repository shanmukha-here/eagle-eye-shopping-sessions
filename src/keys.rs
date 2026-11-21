//! Reversible, order-preserving packing for common short ASCII identifiers.
//! All other byte strings use exact interning, including strings containing NUL.
use std::{
    cmp::Ordering,
    collections::HashMap,
    hash::{BuildHasherDefault, Hasher},
    sync::Mutex,
};
pub type Key = u64;
const EXTERNAL: u64 = 1 << 63;
const LOW: u64 = 0x7f7f7f7f7f7f7f7f;

// The ninth byte occupies the unused high bits of the last seven ASCII bytes.
const fn ninth_table() -> [u64; 128] {
    let mut out = [0; 128];
    let mut i = 0;
    while i < 128 {
        let mut b = 0;
        while b < 7 {
            out[i] |= (((i >> b) & 1) as u64) << (7 + b * 8);
            b += 1;
        }
        i += 1;
    }
    out
}
const NINTH: [u64; 128] = ninth_table();

#[inline]
pub fn mix(mut k: u64) -> u64 {
    k ^= k >> 32;
    k.wrapping_mul(0x9e3779b97f4a7c15).rotate_left(27)
}

#[inline(always)]
pub fn unmix(k: u64) -> u64 {
    let x = k.rotate_right(27).wrapping_mul(0xf1de83e19937733d);
    x ^ (x >> 32)
}
#[derive(Default)]
pub struct IntHash(u64);
impl Hasher for IntHash {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write_u64(&mut self, x: u64) {
        self.0 = mix(x);
    }
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = mix(self.0 ^ b as u64);
        }
    }
}
pub type IntMap<V> = HashMap<u64, V, BuildHasherDefault<IntHash>>;

#[derive(Default)]
struct Long {
    ids: HashMap<Vec<u8>, u64>,
    strings: Vec<Vec<u8>>,
}
const SHARD_BITS: u32 = 5;
const SHARDS: usize = 1 << SHARD_BITS;

#[derive(Default)]
pub struct Keys([Mutex<Long>; SHARDS]);
impl Keys {
    #[inline(always)]
    pub fn key(&self, s: &[u8]) -> Key {
        if (8..=9).contains(&s.len()) {
            let word = u64::from_be_bytes(s[..8].try_into().unwrap());
            let last = s.get(8).copied().unwrap_or(0);
            let zero = word.wrapping_sub(0x0101010101010101) & !word & !LOW;
            if word & !LOW == 0 && zero == 0 && last < 128 && (last != 0 || s.len() == 8) {
                return word | NINTH[last as usize];
            }
        } else if (4..8).contains(&s.len()) {
            let first = u32::from_be_bytes(s[..4].try_into().unwrap());
            let last = u32::from_be_bytes(s[s.len() - 4..].try_into().unwrap());
            let bytes = (u64::from(first) << 32) | u64::from(last);
            if (bytes & !LOW) == 0 && (bytes.wrapping_sub(0x0101010101010101) & !bytes & !LOW) == 0
            {
                return (u64::from(first) << 32)
                    | ((u64::from(last) << (8 * (8 - s.len()))) & 0xffffffff);
            }
        } else if s.len() < 8 && s.iter().all(|&b| b > 0 && b < 128) {
            let mut bytes = [0; 8];
            bytes[..s.len()].copy_from_slice(s);
            return u64::from_be_bytes(bytes);
        }
        self.intern(s)
    }
    #[cold]
    #[inline(never)]
    fn intern(&self, s: &[u8]) -> Key {
        // IDs with a common prefix often differ near the end. Collisions only
        // select the same lock; the map still compares the complete byte string.
        let tail = if s.len() >= 8 {
            u64::from_le_bytes(s[s.len() - 8..].try_into().unwrap())
        } else {
            let mut bytes = [0; 8];
            bytes[..s.len()].copy_from_slice(s);
            u64::from_le_bytes(bytes)
        };
        let shard = (mix(tail) >> (64 - SHARD_BITS)) as usize;
        let mut pool = self.0[shard].lock().unwrap();
        if let Some(id) = pool.ids.get(s) {
            return *id;
        }
        let id = EXTERNAL | ((pool.strings.len() as u64) << SHARD_BITS) | shard as u64;
        pool.strings.push(s.to_vec());
        pool.ids.insert(s.to_vec(), id);
        id
    }
    pub fn bytes(&self, k: Key) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(9);
        self.append_bytes(k, &mut bytes);
        bytes
    }
    pub fn append_bytes(&self, k: Key, out: &mut Vec<u8>) {
        if k & EXTERNAL != 0 {
            let shard = k as usize & (SHARDS - 1);
            let index = ((k ^ EXTERNAL) >> SHARD_BITS) as usize;
            out.extend_from_slice(&self.0[shard].lock().unwrap().strings[index]);
            return;
        }
        let packed = k & LOW;
        let len = 8 - packed.trailing_zeros() as usize / 8;
        out.extend_from_slice(&packed.to_be_bytes()[..len]);
        let mut last = 0;
        for i in 0..7 {
            last |= (((k >> (7 + 8 * i)) & 1) as u8) << i;
        }
        if last != 0 {
            out.push(last);
        }
    }
    #[inline]
    pub fn cmp(&self, a: Key, b: Key) -> Ordering {
        if (a | b) & EXTERNAL == 0 {
            (a & LOW)
                .cmp(&(b & LOW))
                .then_with(|| (a & !LOW).cmp(&(b & !LOW)))
        } else {
            self.bytes(a).cmp(&self.bytes(b))
        }
    }
}

#[derive(Default)]
pub struct People {
    index: IntMap<u32>,
    parents: Vec<u32>,
    size: Vec<u32>,
    names: Vec<Key>,
}
impl People {
    pub fn reserve(&mut self, edges: usize) {
        self.index.reserve(edges + edges / 2);
        self.parents.reserve(edges);
        self.names.reserve(edges);
        self.size.reserve(edges);
    }
    fn node(&mut self, k: Key) -> u32 {
        let n = self.parents.len() as u32;
        *self.index.entry(k).or_insert_with(|| {
            self.parents.push(n);
            self.size.push(1);
            self.names.push(k);
            n
        })
    }
    fn root(&mut self, mut a: u32) -> u32 {
        while self.parents[a as usize] != a {
            let b = self.parents[a as usize];
            self.parents[a as usize] = self.parents[b as usize];
            a = self.parents[a as usize];
        }
        a
    }
    pub fn join(&mut self, keys: &Keys, a: Key, b: Key) {
        let (a, b) = (self.node(a), self.node(b));
        let (mut a, mut b) = (self.root(a), self.root(b));
        if a == b {
            return;
        }
        if self.size[a as usize] < self.size[b as usize] {
            std::mem::swap(&mut a, &mut b);
        }
        self.parents[b as usize] = a;
        self.size[a as usize] += self.size[b as usize];
        if keys
            .cmp(self.names[a as usize], self.names[b as usize])
            .is_gt()
        {
            self.names[a as usize] = self.names[b as usize];
        }
    }
    pub fn freeze(mut self) -> Aliases {
        let mut count = 0;
        for i in 0..self.names.len() {
            let r = self.root(i as u32);
            self.names[i] = self.names[r as usize];
            count += usize::from(r as usize != i);
        }
        let cap = (count * 2 + 1).next_power_of_two();
        let table = Aliases::empty(cap);
        for (key, id) in self.index {
            let name = self.names[id as usize];
            if key == name {
                continue;
            }
            let key = mix(key);
            let name = mix(name);
            let mut i = key as usize & (cap - 1);
            while table.table[i].0.load(Relaxed) != 0 {
                i = (i + 1) & (cap - 1);
            }
            table.table[i].0.store(key, Relaxed);
            table.table[i].1.store(name, Relaxed);
        }
        table
    }
}
// Nonempty keys use zero as the empty slot sentinel. The parent stores a
// slot index plus one, with zero identifying a root. Entries never move.
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering::Relaxed};
#[derive(Default)]
struct Vertex {
    key: AtomicU64,
    parent: AtomicU32,
}
pub struct Graph(Vec<Vertex>);
impl Graph {
    pub fn with_capacity(slots: usize) -> Self {
        Self(
            (0..slots.max(16).next_power_of_two())
                .map(|_| Vertex::default())
                .collect(),
        )
    }
    fn node(&self, key: Key) -> Option<usize> {
        let mask = self.0.len() - 1;
        let mut i = key as usize & mask;
        // Highly clustered or unexpectedly dense input can use the growable
        // serial graph. A failed insertion invalidates the whole parallel run.
        for _ in 0..128 {
            let cell = &self.0[i].key;
            let seen = cell.load(Relaxed);
            if seen == key {
                return Some(i);
            }
            if seen == 0 {
                match cell.compare_exchange(0, key, Relaxed, Relaxed) {
                    Ok(_) => return Some(i),
                    Err(other) if other == key => return Some(i),
                    _ => {}
                }
            }
            i = (i + 1) & mask;
        }
        None
    }
    fn root(&self, mut i: usize) -> usize {
        loop {
            let parent = &self.0[i].parent;
            let p = parent.load(Relaxed);
            if p == 0 {
                return i;
            }
            i = (p - 1) as usize;
            let next = self.0[i].parent.load(Relaxed);
            if next != 0 {
                let _ = parent.compare_exchange(p, next, Relaxed, Relaxed);
                i = (next - 1) as usize;
            }
        }
    }
    pub fn join(&self, keys: &Keys, a: Key, b: Key) -> Option<()> {
        let (mut a, mut b) = (self.node(mix(a))?, self.node(mix(b))?);
        loop {
            a = self.root(a);
            b = self.root(b);
            if a == b {
                return Some(());
            }
            // Every successful hook goes to a lexically smaller key, even if
            // another worker concurrently attaches that target to its own root.
            if keys
                .cmp(
                    unmix(self.0[a].key.load(Relaxed)),
                    unmix(self.0[b].key.load(Relaxed)),
                )
                .is_lt()
            {
                std::mem::swap(&mut a, &mut b);
            }
            if self.0[a]
                .parent
                .compare_exchange(0, (b + 1) as u32, Relaxed, Relaxed)
                .is_ok()
            {
                return Some(());
            }
        }
    }
    pub fn freeze(&self, threads: usize) -> Aliases {
        let block = self.0.len().div_ceil(threads.max(1));
        let count = std::thread::scope(|scope| {
            self.0
                .chunks(block)
                .map(|part| {
                    scope.spawn(move || part.iter().filter(|v| v.parent.load(Relaxed) != 0).count())
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|t| t.join().unwrap())
                .sum::<usize>()
        });
        let cap = (2 * count + 1).next_power_of_two();
        let aliases = Aliases::empty(cap);
        std::thread::scope(|scope| {
            for (part, vertices) in self.0.chunks(block).enumerate() {
                let table = &aliases.table;
                scope.spawn(move || {
                    for (off, v) in vertices.iter().enumerate() {
                        if v.parent.load(Relaxed) == 0 {
                            continue;
                        }
                        let key = v.key.load(Relaxed);
                        let name = self.0[self.root(part * block + off)].key.load(Relaxed);
                        let mut j = key as usize & (cap - 1);
                        while table[j]
                            .0
                            .compare_exchange(0, key, Relaxed, Relaxed)
                            .is_err()
                        {
                            j = (j + 1) & (cap - 1);
                        }
                        table[j].1.store(name, Relaxed);
                    }
                });
            }
        });
        aliases
    }
}
pub struct Aliases {
    table: Vec<(AtomicU64, AtomicU64)>,
}
impl Aliases {
    fn empty(n: usize) -> Self {
        Self {
            table: (0..n)
                .map(|_| (AtomicU64::new(0), AtomicU64::new(0)))
                .collect(),
        }
    }
    #[inline]
    pub fn prefetch(&self, k: Key) {
        let i = k as usize & (self.table.len() - 1);
        unsafe {
            std::arch::x86_64::_mm_prefetch(
                self.table.as_ptr().add(i).cast(),
                std::arch::x86_64::_MM_HINT_T0,
            );
        }
    }
    #[inline]
    pub fn canonical(&self, k: Key) -> Key {
        let mask = self.table.len() - 1;
        let mut i = k as usize & mask;
        loop {
            let (slot, name) = &self.table[i];
            let key = slot.load(Relaxed);
            if key == k {
                return name.load(Relaxed);
            }
            if key == 0 {
                return k;
            }
            i = (i + 1) & mask;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Keys, mix, unmix};
    #[test]
    fn concurrent_components_choose_the_exact_smallest_name() {
        use super::{Graph, People};
        let keys = Keys::default();
        let mut names: Vec<_> = (0..512)
            .map(|i| keys.key(format!("user-{i:04}").as_bytes()))
            .collect();
        names.push(keys.key("北-long".as_bytes()));
        names.push(keys.key(b"user-0001-extra"));
        let edges: Vec<_> = (1..names.len())
            .filter(|i| i % 7 != 0)
            .map(|i| (names[i], names[i - 1]))
            .collect();
        let mut serial = People::default();
        serial.reserve(edges.len());
        for &(a, b) in &edges {
            serial.join(&keys, a, b);
        }
        let expected = serial.freeze();
        let graph = Graph::with_capacity(2048);
        std::thread::scope(|scope| {
            for k in 0..6 {
                let (graph, keys, edges) = (&graph, &keys, &edges);
                scope.spawn(move || {
                    for &(a, b) in edges.iter().skip(k).step_by(6).chain(edges.iter().rev()) {
                        graph.join(keys, a, b).unwrap();
                    }
                });
            }
        });
        let got = graph.freeze(4);
        for k in names {
            assert_eq!(got.canonical(mix(k)), expected.canonical(mix(k)));
        }
        let small = Graph::with_capacity(16);
        assert!(
            edges
                .iter()
                .any(|&(a, b)| small.join(&keys, a, b).is_none())
        );
    }

    #[test]
    fn hash_permutation_round_trips() {
        for n in 0u64..50_000 {
            let k = n.wrapping_mul(0x9d37aa75229103dd);
            assert_eq!(unmix(mix(k)), k);
        }
    }
    #[test]
    fn output_names_round_trip() {
        let keys = Keys::default();
        for n in 0..16 {
            for value in 0..=127 {
                let mut name = vec![b'a'; n];
                if let Some(tail) = name.last_mut() {
                    *tail = value;
                }
                let key = keys.key(&name);
                let mut buf = vec![b'/'];
                keys.append_bytes(key, &mut buf);
                assert_eq!(&buf[1..], name);
            }
        }
        assert_eq!(keys.bytes(keys.key("café".as_bytes())), "café".as_bytes());
    }
}
