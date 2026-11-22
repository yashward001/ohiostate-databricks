// Shopping Sessions pipeline: fast native Rust implementation of rules.md.
//
// Phases (all parallel):
//  1. links: parse, intern by 64-bit hash, union-find, person name = byte-wise min ID
//  2. events: SIMD CSV scan, validate, emit compact records partitioned by event_id hash
//  3. dedup per event_id shard (smallest ingest_ts wins), re-partition kept events by person
//  4. clicks: scan, validate, partition by person
//  5. per person shard: sort, sessionize, attribute, aggregate
//  6. write Parquet
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use memmap2::Mmap;
use rayon::prelude::*;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

type FHash = foldhash::quality::FixedState;
#[inline]
fn hb(b: &[u8]) -> u64 {
    use std::hash::BuildHasher;
    const H: FHash = FHash::with_seed(0x243f6a8885a308d3);
    H.hash_one(b) | 1
}

const NS: usize = 256; // event-id shards
const NP: usize = 256; // person shards
#[inline]
fn eshard(h: u64) -> usize {
    (h >> 56) as usize & (NS - 1)
}
#[inline]
fn pshard(h: u64) -> usize {
    (h >> 48) as usize & (NP - 1)
}

const DAY_MS: i64 = 86_400_000;
const WEEK_MS: i64 = 7 * DAY_MS;
const GAP_MS: u32 = 1_800_000;

// ---------------------------------------------------------------- timestamps

#[inline(always)]
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

#[inline(always)]
fn d2(b: &[u8], i: usize) -> Option<i64> {
    let a = b[i].wrapping_sub(b'0');
    let c = b[i + 1].wrapping_sub(b'0');
    if a > 9 || c > 9 {
        return None;
    }
    Some((a as i64) * 10 + c as i64)
}

/// Parse a ts field (unquoted). None = bad format. Huge F3 values -> Some(i64::MAX).
#[inline]
fn parse_ts(b: &[u8]) -> Option<i64> {
    let n = b.len();
    if n == 0 {
        return None;
    }
    if n == 20 || n == 25 {
        if b[4] == b'-' && b[7] == b'-' && b[10] == b'T' && b[13] == b':' && b[16] == b':' {
            let y = d2(b, 0)? * 100 + d2(b, 2)?;
            let mo = d2(b, 5)?;
            let d = d2(b, 8)?;
            let h = d2(b, 11)?;
            let mi = d2(b, 14)?;
            let s = d2(b, 17)?;
            if !(1..=12).contains(&mo) || h > 23 || mi > 59 || s > 59 || d < 1 {
                return None;
            }
            let dim = match mo {
                1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
                4 | 6 | 9 | 11 => 30,
                _ => {
                    if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 {
                        29
                    } else {
                        28
                    }
                }
            };
            if d > dim {
                return None;
            }
            let mut t = (days_from_civil(y, mo, d) * 86400 + h * 3600 + mi * 60 + s) * 1000;
            if n == 20 {
                if b[19] != b'Z' {
                    return None;
                }
            } else {
                let sign = b[19];
                if (sign != b'+' && sign != b'-') || b[22] != b':' {
                    return None;
                }
                let oh = d2(b, 20)?;
                let om = d2(b, 23)?;
                if oh > 23 || om > 59 {
                    return None;
                }
                let off = (oh * 60 + om) * 60_000;
                if sign == b'+' {
                    t -= off
                } else {
                    t += off
                }
            }
            return Some(t);
        }
    }
    let mut v: u64 = 0;
    let mut big = false;
    for &c in b {
        let x = c.wrapping_sub(b'0');
        if x > 9 {
            return None;
        }
        if !big {
            match v.checked_mul(10).and_then(|w| w.checked_add(x as u64)) {
                Some(w) => v = w,
                None => big = true,
            }
        }
    }
    if big || v > i64::MAX as u64 {
        return Some(i64::MAX);
    }
    Some(v as i64)
}

#[inline]
fn parse_price(b: &[u8]) -> Option<u32> {
    if b.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in b {
        let x = c.wrapping_sub(b'0');
        if x > 9 {
            return None;
        }
        v = v * 10 + x as u64;
        if v > 10_000_000 {
            return None;
        }
    }
    if v < 1 {
        return None;
    }
    Some(v as u32)
}

// ---------------------------------------------------------------- CSV scanner

#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn has_avx2() -> bool {
    #[cfg(target_feature = "avx2")]
    {
        true
    }
    #[cfg(not(target_feature = "avx2"))]
    {
        std::is_x86_feature_detected!("avx2")
    }
}

/// Bitmask of positions of ',', '"', '\n' in buf[o..o+64].
#[inline(always)]
fn block_mask(buf: &[u8], o: usize) -> u64 {
    #[cfg(target_arch = "x86_64")]
    {
        if has_avx2() {
            unsafe { return block_mask_avx2(buf.as_ptr().add(o)) }
        }
    }
    let mut m = 0u64;
    for i in 0..64 {
        let c = buf[o + i];
        if c == b',' || c == b'"' || c == b'\n' {
            m |= 1 << i;
        }
    }
    m
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn block_mask_avx2(p: *const u8) -> u64 {
    use std::arch::x86_64::*;
    unsafe {
        let a = _mm256_loadu_si256(p as *const __m256i);
        let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
        let c1 = _mm256_set1_epi8(b',' as i8);
        let c2 = _mm256_set1_epi8(b'"' as i8);
        let c3 = _mm256_set1_epi8(b'\n' as i8);
        let ma = _mm256_or_si256(_mm256_or_si256(_mm256_cmpeq_epi8(a, c1), _mm256_cmpeq_epi8(a, c2)), _mm256_cmpeq_epi8(a, c3));
        let mb = _mm256_or_si256(_mm256_or_si256(_mm256_cmpeq_epi8(b, c1), _mm256_cmpeq_epi8(b, c2)), _mm256_cmpeq_epi8(b, c3));
        let lo = _mm256_movemask_epi8(ma) as u32 as u64;
        let hi = _mm256_movemask_epi8(mb) as u32 as u64;
        lo | (hi << 32)
    }
}

/// Scan CSV lines with exactly N fields under the rules' dialect; calls f with unquoted fields.
#[inline(always)]
fn scan<'a, const N: usize>(buf: &'a [u8], mut f: impl FnMut(&[&'a [u8]; N])) {
    let len = buf.len();
    let mut fs: [(usize, usize); N] = [(0, 0); N];
    let mut fi = 0usize;
    let mut fstart = 0usize;
    let mut nq = 0u32;
    let mut q1 = 0usize;
    let mut q2 = 0usize;
    let mut bad = false;
    macro_rules! close {
        ($pos:expr, $nl:expr) => {{
            let pos = $pos;
            if !bad {
                if fi >= N {
                    bad = true;
                } else if nq == 0 {
                    fs[fi] = (fstart, pos);
                } else if nq == 2 && q1 == fstart && q2 + 1 == pos {
                    fs[fi] = (fstart + 1, pos - 1);
                } else {
                    bad = true;
                }
            }
            fi += 1;
            nq = 0;
            fstart = pos + 1;
            if $nl {
                if !bad && fi == N {
                    let arr: [&'a [u8]; N] = std::array::from_fn(|k| unsafe { buf.get_unchecked(fs[k].0..fs[k].1) });
                    f(&arr);
                }
                fi = 0;
                bad = false;
            }
        }};
    }
    let mut o = 0usize;
    while o + 64 <= len {
        let mut m = block_mask(buf, o);
        while m != 0 {
            let pos = o + m.trailing_zeros() as usize;
            m &= m - 1;
            let c = unsafe { *buf.get_unchecked(pos) };
            if c == b'"' {
                nq += 1;
                if nq == 1 {
                    q1 = pos
                } else if nq == 2 {
                    q2 = pos
                }
            } else if c == b',' {
                close!(pos, false);
            } else {
                close!(pos, true);
            }
        }
        o += 64;
    }
    while o < len {
        let c = buf[o];
        if c == b'"' {
            nq += 1;
            if nq == 1 {
                q1 = o
            } else if nq == 2 {
                q2 = o
            }
        } else if c == b',' {
            close!(o, false);
        } else if c == b'\n' {
            close!(o, true);
        }
        o += 1;
    }
    if fstart < len || fi > 0 {
        close!(len, true);
    }
}

// ---------------------------------------------------------------- files

fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|x| x == "csv").unwrap_or(false) && p.is_file())
            .collect(),
        Err(_) => vec![],
    };
    v.sort();
    v
}

fn open_all(paths: &[PathBuf]) -> Vec<Mmap> {
    paths
        .par_iter()
        .filter_map(|p| {
            let f = fs::File::open(p).ok()?;
            let len = f.metadata().ok()?.len();
            if len == 0 {
                return None;
            }
            unsafe { memmap2::MmapOptions::new().populate().map(&f).ok() }
        })
        .collect()
}

/// Newline-aligned chunks of all files, excluding each file's header line.
fn chunks(maps: &[Mmap], target: usize) -> Vec<&[u8]> {
    let mut out = Vec::new();
    for m in maps {
        let b: &[u8] = &m[..];
        let start = match memchr::memchr(b'\n', b) {
            Some(p) => p + 1,
            None => continue,
        };
        let mut s = start;
        while s < b.len() {
            let mut e = (s + target).min(b.len());
            if e < b.len() {
                e = match memchr::memchr(b'\n', &b[e..]) {
                    Some(p) => e + p + 1,
                    None => b.len(),
                };
            }
            out.push(&b[s..e]);
            s = e;
        }
    }
    out
}

fn read_small(path: &Path) -> Vec<u8> {
    fs::read(path).unwrap_or_default()
}

// ---------------------------------------------------------------- records

/// Event after parsing (needed for dedup).
#[derive(Clone, Copy)]
#[repr(C)]
struct Ev {
    eid: u64,
    person: u64,
    payload: u64, // ts_rel:31 | typ:2 | cat:7 | price:24 ; typ 3 = invalid
    name: usize,  // ptr
    ingest: i32,  // seconds rel to ws (saturating)
    nlen: u16,
}

/// Kept event for sessionization.
#[derive(Clone, Copy)]
#[repr(C)]
struct Ke {
    person: u64,
    payload: u64,
    name: u64, // ptr << 16 | len
}

#[derive(Clone, Copy)]
#[repr(C)]
struct Ck {
    person: u64,
    ts: u32, // rel to ws - 7d
    camp: u16,
    clen: u16,
    cid: usize,
}

#[inline]
fn pl_ts(p: u64) -> u32 {
    (p & 0x7fff_ffff) as u32
}
#[inline]
fn pl_typ(p: u64) -> u64 {
    (p >> 31) & 3
}
#[inline]
fn pl_cat(p: u64) -> usize {
    ((p >> 33) & 0x7f) as usize
}
#[inline]
fn pl_price(p: u64) -> u64 {
    p >> 40
}

/// Open-addressing table: user hash -> person hash (hash of the byte-wise min ID).
struct LinkTable {
    mask: usize,
    slots: Vec<(u64, u64)>, // (key, person); key 0 = empty
    names: Vec<(usize, u16)>,
}
impl LinkTable {
    #[inline(always)]
    fn slot(&self, k: u64) -> usize {
        (k as usize) & self.mask
    }
    #[inline(always)]
    fn prefetch(&self, k: u64) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::*;
            _mm_prefetch(self.slots.as_ptr().add(self.slot(k)) as *const i8, _MM_HINT_T0);
        }
    }
    #[inline(always)]
    fn find(&self, k: u64) -> Option<usize> {
        let mut h = self.slot(k);
        loop {
            let kk = unsafe { self.slots.get_unchecked(h).0 };
            if kk == k {
                return Some(h);
            }
            if kk == 0 {
                return None;
            }
            h = (h + 1) & self.mask;
        }
    }
    #[inline(always)]
    fn person(&self, k: u64) -> u64 {
        match self.find(k) {
            Some(h) => self.slots[h].1,
            None => k,
        }
    }
}

fn manifest_ts(s: &str, key: &str) -> i64 {
    let i = s.find(key).expect("manifest key");
    let rest = &s[i + key.len()..];
    let q1 = rest.find('"').unwrap();
    let rest = &rest[q1 + 1..];
    let q2 = rest.find('"').unwrap();
    parse_ts(rest[..q2].as_bytes()).expect("manifest ts")
}

fn find(p: &mut [u32], mut x: u32) -> u32 {
    while p[x as usize] != x {
        let gp = p[p[x as usize] as usize];
        p[x as usize] = gp;
        x = gp;
    }
    x
}

struct NoHash(u64);
impl std::hash::Hasher for NoHash {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, _: &[u8]) {
        unreachable!()
    }
    fn write_u64(&mut self, x: u64) {
        self.0 = x;
    }
}
#[derive(Clone, Copy, Default)]
struct BuildNoHash;
impl std::hash::BuildHasher for BuildNoHash {
    type Hasher = NoHash;
    fn build_hasher(&self) -> NoHash {
        NoHash(0)
    }
}

fn new_parts<T>(n: usize, cap: usize) -> Vec<Vec<T>> {
    (0..n).map(|_| Vec::with_capacity(cap)).collect()
}

#[inline]
unsafe fn sl<'a>(p: usize, n: u16) -> &'a [u8] {
    unsafe { std::slice::from_raw_parts(p as *const u8, n as usize) }
}

type URow = (usize, u16, i32, u32, u32, u64, u8);

fn main() {
    let t0 = Instant::now();
    let args: Vec<String> = std::env::args().collect();
    let data = std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into());
    let out = std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into());
    let (data, out) = if args.len() >= 3 { (args[1].clone(), args[2].clone()) } else { (data, out) };
    let data = PathBuf::from(data);
    let out = PathBuf::from(out);
    let ncpu: usize = std::env::var("NCPU").ok().and_then(|s| s.parse().ok()).unwrap_or(0);
    if ncpu > 0 {
        let _ = rayon::ThreadPoolBuilder::new().num_threads(ncpu).build_global();
    }
    let verbose = std::env::var("SS_VERBOSE").is_ok();
    macro_rules! log {
        ($($t:tt)*) => { if verbose { eprintln!("{:>8.1?} {}", t0.elapsed(), format!($($t)*)); } };
    }

    let man = String::from_utf8(read_small(&data.join("manifest.json"))).unwrap();
    let ws = manifest_ts(&man, "\"window_start\"");
    let we = manifest_ts(&man, "\"window_end\"");
    let cws = ws - WEEK_MS;
    assert!(we - ws < (1i64 << 31) - WEEK_MS);

    let ev_paths = list_csv(&data.join("events"));
    let ck_paths = list_csv(&data.join("clicks"));
    let lk_paths = list_csv(&data.join("links"));
    let (ev_maps, (ck_maps, lk_maps)) = rayon::join(|| open_all(&ev_paths), || rayon::join(|| open_all(&ck_paths), || open_all(&lk_paths)));
    log!("mmap");

    // ---- products / campaigns
    let prod_raw = read_small(&data.join("products.csv"));
    let camp_raw = read_small(&data.join("campaigns.csv"));
    let mut cat_names: Vec<Vec<u8>> = Vec::new();
    let mut prod_pairs: Vec<(&[u8], &[u8])> = Vec::new();
    {
        let body = match memchr::memchr(b'\n', &prod_raw) {
            Some(p) => &prod_raw[p + 1..],
            None => &[][..],
        };
        scan::<2>(body, |f| {
            prod_pairs.push((f[0], f[1]));
            cat_names.push(f[1].to_vec());
        });
    }
    cat_names.push(b"unknown".to_vec());
    cat_names.sort();
    cat_names.dedup();
    assert!(cat_names.len() <= 127);
    let cat_idx: HashMap<&[u8], u8> = cat_names.iter().enumerate().map(|(i, c)| (c.as_slice(), i as u8)).collect();
    let unknown_cat = cat_idx[&b"unknown"[..]];
    let prod_map: HashMap<u64, u8, BuildNoHash> = {
        let mut m = HashMap::with_capacity_and_hasher(prod_pairs.len(), BuildNoHash);
        for (p, c) in &prod_pairs {
            m.insert(hb(p), cat_idx[c]);
        }
        m
    };
    let mut camp_ids: Vec<Vec<u8>> = Vec::new();
    let mut camp_chan: Vec<Vec<u8>> = Vec::new();
    {
        let body = match memchr::memchr(b'\n', &camp_raw) {
            Some(p) => &camp_raw[p + 1..],
            None => &[][..],
        };
        scan::<2>(body, |f| {
            camp_ids.push(f[0].to_vec());
            camp_chan.push(f[1].to_vec());
        });
    }
    let camp_map: HashMap<u64, u16, BuildNoHash> = {
        let mut m = HashMap::with_capacity_and_hasher(camp_ids.len(), BuildNoHash);
        for (i, c) in camp_ids.iter().enumerate() {
            m.insert(hb(c), i as u16);
        }
        m
    };
    let ncamp = camp_ids.len();
    let organic = ncamp;

    // ---- links (runs concurrently with event parsing)
    let lk_chunks = chunks(&lk_maps, 1 << 20);
    let lk_parse = || -> Vec<Vec<(u64, u64, usize, u16, usize, u16)>> {
    lk_chunks
        .par_iter()
        .map(|buf| {
            let mut v = Vec::with_capacity(buf.len() / 20);
            scan::<2>(buf, |f| {
                let (a, b) = (f[0], f[1]);
                if !a.is_empty() && !b.is_empty() {
                    v.push((hb(a), hb(b), a.as_ptr() as usize, a.len() as u16, b.as_ptr() as usize, b.len() as u16));
                }
            });
            v
        })
        .collect()
    };
    let links_job = || -> LinkTable {
        let lk_pairs = lk_parse();
        let n: usize = lk_pairs.iter().map(|v| v.len()).sum();
        let mut idx: HashMap<u64, u32, BuildNoHash> = HashMap::with_capacity_and_hasher(n, BuildNoHash);
        let mut names: Vec<(usize, u16)> = Vec::with_capacity(n);
        let mut hashes: Vec<u64> = Vec::with_capacity(n);
        let mut edges: Vec<(u32, u32)> = Vec::with_capacity(n);
        for v in &lk_pairs {
            for &(ha, hbb, pa, la, pb, lb) in v {
                let ia = *idx.entry(ha).or_insert_with(|| {
                    names.push((pa, la));
                    hashes.push(ha);
                    (names.len() - 1) as u32
                });
                let ib = *idx.entry(hbb).or_insert_with(|| {
                    names.push((pb, lb));
                    hashes.push(hbb);
                    (names.len() - 1) as u32
                });
                edges.push((ia, ib));
            }
        }
        let nu = names.len();
        let mut parent: Vec<u32> = (0..nu as u32).collect();
        let nm = |i: u32| unsafe { sl(names[i as usize].0, names[i as usize].1) };
        for &(a, b) in &edges {
            let ra = find(&mut parent, a);
            let rb = find(&mut parent, b);
            if ra != rb {
                if nm(ra) < nm(rb) {
                    parent[rb as usize] = ra;
                } else {
                    parent[ra as usize] = rb;
                }
            }
        }
        let cap = (nu * 2).next_power_of_two().max(16);
        let mut t = LinkTable { mask: cap - 1, slots: vec![(0, 0); cap], names: vec![(0, 0); cap] };
        for u in 0..nu as u32 {
            let r = find(&mut parent, u);
            let k = hashes[u as usize];
            let mut h = t.slot(k);
            while t.slots[h].0 != 0 {
                h = (h + 1) & t.mask;
            }
            t.slots[h] = (k, hashes[r as usize]);
            t.names[h] = names[u as usize];
        }
        t
    };
    log!("links done");

    // ---- events: parse
    let ev_chunks = chunks(&ev_maps, 8 << 20);
    let ev_job = || -> Vec<Vec<Vec<Ev>>> { ev_chunks
        .par_iter()
        .map(|buf| {
            let mut parts: Vec<Vec<Ev>> = new_parts(NS, buf.len() / 85 / NS + 16);
            scan::<7>(buf, |f| {
                let eid = f[0];
                if eid.is_empty() {
                    return;
                }
                let ingest_ms = parse_ts(f[3]).unwrap_or(i64::MAX);
                let ingest = ((ingest_ms.saturating_sub(ws)) / 1000).clamp(i32::MIN as i64, i32::MAX as i64) as i32;
                let eh = hb(eid);
                let mut e = Ev { eid: eh, person: 0, payload: 3 << 31, name: 0, ingest, nlen: 0 };
                let user = f[1];
                let typ = match f[4] {
                    b"view" => 0u64,
                    b"cart" => 1,
                    b"purchase" => 2,
                    _ => 9,
                };
                if typ != 9 && !user.is_empty() && !f[5].is_empty() {
                    if let Some(t) = parse_ts(f[2]) {
                        if t >= ws && t < we && ingest_ms.saturating_sub(t) <= DAY_MS {
                            let mut ok = true;
                            let mut extra = 0u64;
                            if typ == 2 {
                                match parse_price(f[6]) {
                                    Some(p) => {
                                        let cat = prod_map.get(&hb(f[5])).copied().unwrap_or(unknown_cat);
                                        extra = ((cat as u64) << 33) | ((p as u64) << 40);
                                    }
                                    None => ok = false,
                                }
                            }
                            if ok {
                                e.person = hb(user);
                                e.name = user.as_ptr() as usize;
                                e.nlen = user.len() as u16;
                                e.payload = ((t - ws) as u64) | (typ << 31) | extra;
                            }
                        }
                    }
                }
                parts[eshard(eh)].push(e);
            });
            parts
        })
        .collect() };
    let (link_tab, ev_parts) = rayon::join(links_job, ev_job);
    log!("links + parse events: {} chunks", ev_chunks.len());

    // ---- dedup per event shard, emit kept events partitioned by person
    let kept_parts: Vec<Vec<Vec<Ke>>> = (0..NS)
        .into_par_iter()
        .map(|s| {
            let n: usize = ev_parts.iter().map(|c| c[s].len()).sum();
            let mut loc: Vec<Ev> = Vec::with_capacity(n);
            for c in ev_parts.iter() {
                loc.extend_from_slice(&c[s]);
            }
            let cap = (n + n / 2 + 16).next_power_of_two();
            let mask = cap - 1;
            let mut tab = vec![u32::MAX; cap];
            for i in 0..n {
                let e = unsafe { loc.get_unchecked(i) };
                let mut h = (e.eid as usize) & mask;
                loop {
                    let j = unsafe { *tab.get_unchecked(h) };
                    if j == u32::MAX {
                        tab[h] = i as u32;
                        break;
                    }
                    let b = unsafe { loc.get_unchecked(j as usize) };
                    if b.eid == e.eid {
                        if e.ingest < b.ingest {
                            tab[h] = i as u32;
                        }
                        break;
                    }
                    h = (h + 1) & mask;
                }
            }
            let mut kept: Vec<Ke> = Vec::with_capacity(n);
            for &j in &tab {
                if j != u32::MAX {
                    let e = unsafe { loc.get_unchecked(j as usize) };
                    if pl_typ(e.payload) != 3 {
                        kept.push(Ke { person: e.person, payload: e.payload, name: ((e.name as u64) << 16) | e.nlen as u64 });
                    }
                }
            }
            drop(tab);
            drop(loc);
            const PF: usize = 16;
            for i in 0..kept.len().min(PF) {
                link_tab.prefetch(kept[i].person);
            }
            let mut outp: Vec<Vec<Ke>> = new_parts(NP, n / NP + 8);
            for i in 0..kept.len() {
                if i + PF < kept.len() {
                    link_tab.prefetch(kept[i + PF].person);
                }
                let mut k = kept[i];
                k.person = link_tab.person(k.person);
                outp[pshard(k.person)].push(k);
            }
            outp
        })
        .collect();
    drop(ev_parts);
    log!("dedup");

    // ---- clicks
    let ck_chunks = chunks(&ck_maps, 4 << 20);
    let ck_parts: Vec<Vec<Vec<Ck>>> = ck_chunks
        .par_iter()
        .map(|buf| {
            let mut parts: Vec<Vec<Ck>> = new_parts(NP, buf.len() / 50 / NP + 16);
            let mut raw: Vec<Ck> = Vec::with_capacity(buf.len() / 45);
            scan::<4>(buf, |f| {
                let (cid, user, camp, ts) = (f[0], f[1], f[2], f[3]);
                if cid.is_empty() || user.is_empty() {
                    return;
                }
                let Some(t) = parse_ts(ts) else { return };
                if t < cws || t >= we {
                    return;
                }
                let Some(&ci) = camp_map.get(&hb(camp)) else { return };
                raw.push(Ck { person: hb(user), ts: (t - cws) as u32, camp: ci, clen: cid.len() as u16, cid: cid.as_ptr() as usize });
            });
            const PF: usize = 16;
            for i in 0..raw.len().min(PF) {
                link_tab.prefetch(raw[i].person);
            }
            for i in 0..raw.len() {
                if i + PF < raw.len() {
                    link_tab.prefetch(raw[i + PF].person);
                }
                let mut c = raw[i];
                c.person = link_tab.person(c.person);
                parts[pshard(c.person)].push(c);
            }
            parts
        })
        .collect();
    log!("clicks");

    // ---- per person shard: sort, sessionize, attribute
    let ws_day = ws.div_euclid(DAY_MS);
    let ndays = ((we - 1).div_euclid(DAY_MS) - ws_day + 1) as usize;
    let ncat = cat_names.len();
    let nthreads = rayon::current_num_threads();
    let camp_accs: Vec<Mutex<Vec<[u64; 4]>>> = (0..nthreads + 1).map(|_| Mutex::new(Vec::new())).collect();

    let user_rows: Vec<Vec<URow>> = (0..NP)
        .into_par_iter()
        .map(|p| {
            let n: usize = kept_parts.iter().map(|c| c[p].len()).sum();
            let mut ev: Vec<Ke> = Vec::with_capacity(n);
            for c in &kept_parts {
                ev.extend_from_slice(&c[p]);
            }
            ev.sort_unstable_by_key(|e| (e.person, pl_ts(e.payload)));
            let nc: usize = ck_parts.iter().map(|c| c[p].len()).sum();
            let mut cl: Vec<Ck> = Vec::with_capacity(nc);
            for c in &ck_parts {
                cl.extend_from_slice(&c[p]);
            }
            cl.sort_unstable_by_key(|c| (c.person, c.ts));

            let tid = rayon::current_thread_index().unwrap_or(nthreads);
            let mut accg = camp_accs[tid].lock().unwrap();
            if accg.is_empty() {
                accg.resize((ncamp + 1) * ndays, [0; 4]);
            }
            let acc = &mut *accg;
            let mut rows: Vec<URow> = Vec::with_capacity(n / 12 + 16);
            let mut catsum = vec![0u64; ncat];
            let mut touched: Vec<u8> = Vec::with_capacity(ncat);
            let mut ccount = vec![0u32; ncamp];
            let mut present: Vec<u16> = Vec::new();
            let mut ppos = vec![0u32; ncamp];
            let mut cp = 0usize;
            let mut i = 0usize;
            while i < ev.len() {
                let person = ev[i].person;
                let (name, nlen) = match link_tab.find(person) {
                    Some(h) => link_tab.names[h],
                    None => ((ev[i].name >> 16) as usize, ev[i].name as u16),
                };
                let mut j = i + 1;
                while j < ev.len() && ev[j].person == person {
                    j += 1;
                }
                while cp < cl.len() && cl[cp].person < person {
                    cp += 1;
                }
                let cs = cp;
                while cp < cl.len() && cl[cp].person == person {
                    cp += 1;
                }
                let clk = &cl[cs..cp];
                let (mut lo, mut hi) = (0usize, 0usize);
                let mut cur_day = i64::MIN;
                let (mut d_sess, mut d_ev, mut d_rev) = (0u32, 0u32, 0u64);
                macro_rules! flush {
                    () => {{
                        let mut best = u8::MAX;
                        let mut bv = 0u64;
                        touched.sort_unstable();
                        for &c in touched.iter() {
                            let v = catsum[c as usize];
                            if v > bv {
                                bv = v;
                                best = c;
                            }
                            catsum[c as usize] = 0;
                        }
                        touched.clear();
                        rows.push((name, nlen, cur_day as i32, d_sess, d_ev, d_rev, best));
                    }};
                }
                let mut k = i;
                while k < j {
                    let start = pl_ts(ev[k].payload);
                    let mut last = start;
                    let mut cnt = 0u32;
                    let mut rev = 0u64;
                    let mut purch = false;
                    let sday = (ws + start as i64).div_euclid(DAY_MS);
                    if sday != cur_day {
                        if cur_day != i64::MIN {
                            flush!();
                        }
                        cur_day = sday;
                        d_sess = 0;
                        d_ev = 0;
                        d_rev = 0;
                    }
                    while k < j {
                        let pld = ev[k].payload;
                        let t = pl_ts(pld);
                        if t - last > GAP_MS {
                            break;
                        }
                        last = t;
                        cnt += 1;
                        if pl_typ(pld) == 2 {
                            let price = pl_price(pld);
                            let cat = pl_cat(pld);
                            rev += price;
                            purch = true;
                            if catsum[cat] == 0 {
                                touched.push(cat as u8);
                            }
                            catsum[cat] += price;
                        }
                        k += 1;
                    }
                    d_sess += 1;
                    d_ev += cnt;
                    d_rev += rev;
                    let s_abs = start as u64 + WEEK_MS as u64;
                    while hi < clk.len() && (clk[hi].ts as u64) <= s_abs {
                        let c = clk[hi].camp as usize;
                        if ccount[c] == 0 {
                            ppos[c] = present.len() as u32;
                            present.push(c as u16);
                        }
                        ccount[c] += 1;
                        hi += 1;
                    }
                    while lo < hi && (clk[lo].ts as u64) < s_abs - WEEK_MS as u64 {
                        let c = clk[lo].camp as usize;
                        ccount[c] -= 1;
                        if ccount[c] == 0 {
                            let pos = ppos[c] as usize;
                            let lastc = *present.last().unwrap();
                            present[pos] = lastc;
                            ppos[lastc as usize] = pos as u32;
                            present.pop();
                        }
                        lo += 1;
                    }
                    let dayi = (sday - ws_day) as usize;
                    let credit = if lo < hi {
                        let tl = clk[hi - 1].ts;
                        let mut b = hi - 1;
                        let mut q = hi - 1;
                        while q > lo && clk[q - 1].ts == tl {
                            q -= 1;
                            if unsafe { sl(clk[q].cid, clk[q].clen) < sl(clk[b].cid, clk[b].clen) } {
                                b = q;
                            }
                        }
                        for &c in &present {
                            acc[c as usize * ndays + dayi][3] += 1;
                        }
                        clk[b].camp as usize
                    } else {
                        organic
                    };
                    let a = &mut acc[credit * ndays + dayi];
                    a[0] += 1;
                    a[1] += purch as u64;
                    a[2] += rev;
                }
                if cur_day != i64::MIN {
                    flush!();
                }
                for &c in &present {
                    ccount[c as usize] = 0;
                }
                present.clear();
                i = j;
            }
            rows
        })
        .collect();
    log!("sessions");

    let mut camp = vec![[0u64; 4]; (ncamp + 1) * ndays];
    for m in &camp_accs {
        let g = m.lock().unwrap();
        for (a, b) in camp.iter_mut().zip(g.iter()) {
            for x in 0..4 {
                a[x] += b[x];
            }
        }
    }
    write_outputs(&out, &user_rows, &cat_names, &camp, &camp_ids, &camp_chan, ndays, ws_day);
    log!("done");
    std::process::exit(0);
}

#[allow(clippy::too_many_arguments)]
fn write_outputs(out: &Path, user_rows: &[Vec<URow>], cat_names: &[Vec<u8>], camp: &[[u64; 4]], camp_ids: &[Vec<u8>], camp_chan: &[Vec<u8>], ndays: usize, ws_day: i64) {
    use arrow_array::builder::{Date32Builder, Int64Builder, StringBuilder};
    use arrow_array::{ArrayRef, RecordBatch};
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;

    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    fs::create_dir_all(&ud).unwrap();
    fs::create_dir_all(&cd).unwrap();
    let props = || {
        WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_statistics_enabled(parquet::file::properties::EnabledStatistics::None)
            .build()
    };

    let ng = 16usize.min(user_rows.len().max(1));
    let per = user_rows.len().div_ceil(ng).max(1);
    let groups: Vec<&[Vec<URow>]> = user_rows.chunks(per).collect();
    let cat_strs: Vec<&str> = cat_names.iter().map(|c| unsafe { std::str::from_utf8_unchecked(c) }).collect();
    let camp_job = || {
        let mut cid = StringBuilder::new();
        let mut ch = StringBuilder::new();
        let mut day = Date32Builder::new();
        let mut ses = Int64Builder::new();
        let mut pur = Int64Builder::new();
        let mut rev = Int64Builder::new();
        let mut ast = Int64Builder::new();
        let ncamp = camp_ids.len();
        for c in 0..=ncamp {
            for d in 0..ndays {
                let a = camp[c * ndays + d];
                if a[0] == 0 && a[3] == 0 {
                    continue;
                }
                let (id, chan): (&[u8], &[u8]) = if c == ncamp { (b"organic", b"organic") } else { (&camp_ids[c], &camp_chan[c]) };
                cid.append_value(unsafe { std::str::from_utf8_unchecked(id) });
                ch.append_value(unsafe { std::str::from_utf8_unchecked(chan) });
                day.append_value((ws_day + d as i64) as i32);
                ses.append_value(a[0] as i64);
                pur.append_value(a[1] as i64);
                rev.append_value(a[2] as i64);
                ast.append_value(a[3] as i64);
            }
        }
        let cols: Vec<(&str, ArrayRef)> = vec![
            ("campaign_id", Arc::new(cid.finish())),
            ("channel", Arc::new(ch.finish())),
            ("day", Arc::new(day.finish())),
            ("sessions", Arc::new(ses.finish())),
            ("purchasing_sessions", Arc::new(pur.finish())),
            ("revenue_cents", Arc::new(rev.finish())),
            ("assisted_sessions", Arc::new(ast.finish())),
        ];
        let batch = RecordBatch::try_from_iter(cols).unwrap();
        let f = fs::File::create(cd.join("part-0.parquet")).unwrap();
        let mut w = ArrowWriter::try_new(f, batch.schema(), Some(props())).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    };
    rayon::join(camp_job, || {
        groups.par_iter().enumerate().for_each(|(gi, g)| {
            let n: usize = g.iter().map(|p| p.len()).sum();
            if n == 0 && gi != 0 {
                return;
            }
            let mut uid = StringBuilder::with_capacity(n, n * 16);
            let mut day = Date32Builder::with_capacity(n);
            let mut ses = Int64Builder::with_capacity(n);
            let mut evs = Int64Builder::with_capacity(n);
            let mut rev = Int64Builder::with_capacity(n);
            let mut top = StringBuilder::with_capacity(n, n * 8);
            for p in g.iter() {
                for r in p.iter() {
                    uid.append_value(unsafe { std::str::from_utf8_unchecked(sl(r.0, r.1)) });
                    day.append_value(r.2);
                    ses.append_value(r.3 as i64);
                    evs.append_value(r.4 as i64);
                    rev.append_value(r.5 as i64);
                    if r.6 == u8::MAX {
                        top.append_null();
                    } else {
                        top.append_value(cat_strs[r.6 as usize]);
                    }
                }
            }
            let cols: Vec<(&str, ArrayRef)> = vec![
                ("user_id", Arc::new(uid.finish())),
                ("day", Arc::new(day.finish())),
                ("sessions", Arc::new(ses.finish())),
                ("events", Arc::new(evs.finish())),
                ("revenue_cents", Arc::new(rev.finish())),
                ("top_category", Arc::new(top.finish())),
            ];
            let batch = RecordBatch::try_from_iter(cols).unwrap();
            let f = fs::File::create(ud.join(format!("part-{gi}.parquet"))).unwrap();
            let mut w = ArrowWriter::try_new(f, batch.schema(), Some(props())).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
        });
    });
}
