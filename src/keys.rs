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
    k ^= k >> 30;
    k = k.wrapping_mul(0xbf58476d1ce4e5b9);
    k ^= k >> 27;
    k = k.wrapping_mul(0x94d049bb133111eb);
    k ^ (k >> 31)
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
#[derive(Default)]
pub struct Keys(Mutex<Long>);
impl Keys {
    #[inline]
    pub fn key(&self, s: &[u8]) -> Key {
        if (8..=9).contains(&s.len()) {
            let word = u64::from_be_bytes(s[..8].try_into().unwrap());
            let last = s.get(8).copied().unwrap_or(0);
            let zero = word.wrapping_sub(0x0101010101010101) & !word & !LOW;
            if word & !LOW == 0 && zero == 0 && last < 128 && (last != 0 || s.len() == 8) {
                return word | NINTH[last as usize];
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
        let mut pool = self.0.lock().unwrap();
        if let Some(id) = pool.ids.get(s) {
            return *id;
        }
        let id = EXTERNAL | pool.strings.len() as u64;
        pool.strings.push(s.to_vec());
        pool.ids.insert(s.to_vec(), id);
        id
    }
    pub fn bytes(&self, k: Key) -> Vec<u8> {
        if k & EXTERNAL != 0 {
            return self.0.lock().unwrap().strings[(k ^ EXTERNAL) as usize].clone();
        }
        let mut out = Vec::with_capacity(9);
        out.extend_from_slice(&(k & LOW).to_be_bytes());
        let mut last = 0;
        for i in 0..7 {
            last |= (((k >> (7 + 8 * i)) & 1) as u8) << i;
        }
        out.push(last);
        while out.last() == Some(&0) {
            out.pop();
        }
        out
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
        let mut table = vec![(0, 0); cap];
        for (key, id) in self.index {
            let name = self.names[id as usize];
            if key == name {
                continue;
            }
            let mut i = mix(key) as usize & (cap - 1);
            while table[i].0 != 0 {
                i = (i + 1) & (cap - 1);
            }
            table[i] = (key, name);
        }
        Aliases { table }
    }
}
pub struct Aliases {
    table: Vec<(Key, Key)>,
}
impl Aliases {
    #[inline]
    pub fn prefetch(&self, k: Key) {
        let i = mix(k) as usize & (self.table.len() - 1);
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
        let mut i = mix(k) as usize & mask;
        loop {
            let (key, name) = self.table[i];
            if key == k {
                return name;
            }
            if key == 0 {
                return k;
            }
            i = (i + 1) & mask;
        }
    }
}
