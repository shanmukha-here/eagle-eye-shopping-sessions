//! Reversible, order-preserving packing for common short ASCII identifiers.
//! All other byte strings use exact interning, including strings containing NUL.
use std::{
    arch::x86_64::*,
    cmp::Ordering,
    collections::HashMap,
    hash::{BuildHasherDefault, Hasher},
    sync::Mutex,
};
pub type Key = u64;
const EXTERNAL: u64 = 1 << 63;
const LOW: u64 = 0x7f7f7f7f7f7f7f7f;

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
        if s.len() <= 9 {
            let mut bytes = [0u8; 8];
            let len = s.len().min(8);
            bytes[..len].copy_from_slice(&s[..len]);
            let v = u64::from_be_bytes(bytes);
            let last = s.get(8).copied().unwrap_or(0);
            if v & !LOW == 0 && last < 128 && !s.contains(&0) {
                return (unsafe { _pext_u64(v, LOW) } << 7) | last as u64;
            }
        }
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
        let first = unsafe { _pdep_u64(k >> 7, LOW) }.to_be_bytes();
        let mut out = first.to_vec();
        out.push((k & 127) as u8);
        while out.last() == Some(&0) {
            out.pop();
        }
        out
    }
    #[inline]
    pub fn cmp(&self, a: Key, b: Key) -> Ordering {
        if (a | b) & EXTERNAL == 0 {
            a.cmp(&b)
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
    pub fn freeze(&mut self) {
        for i in 0..self.names.len() {
            let r = self.root(i as u32);
            self.names[i] = self.names[r as usize];
        }
        self.parents = Vec::new();
        self.size = Vec::new();
    }
    #[inline]
    pub fn canonical(&self, k: Key) -> Key {
        self.index.get(&k).map_or(k, |&i| self.names[i as usize])
    }
}
