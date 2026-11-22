// Shopping Sessions pipeline: native Rust implementation of rules.md.
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use memmap2::Mmap;
use rayon::prelude::*;

type FHash = foldhash::fast::FixedState;
fn hasher() -> FHash {
    FHash::with_seed(0x9e3779b97f4a7c15)
}
#[inline]
fn hash_bytes(h: &FHash, b: &[u8]) -> u64 {
    use std::hash::BuildHasher;
    h.hash_one(b)
}
#[inline]
fn shard_of(h: u64) -> usize {
    ((h >> 40) & (NSHARD as u64 - 1)) as usize
}
const NSHARD: usize = 256;

const DAY_MS: i64 = 86_400_000;
const WEEK_MS: i64 = 7 * DAY_MS;

#[derive(Clone, Copy)]
struct SyncPtr<T>(*mut T);
unsafe impl<T> Send for SyncPtr<T> {}
unsafe impl<T> Sync for SyncPtr<T> {}

// ---------------------------------------------------------------- timestamps

#[inline]
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

#[inline]
fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

#[inline]
fn d2(b: &[u8], i: usize) -> Option<i64> {
    let a = b[i].wrapping_sub(b'0');
    let c = b[i + 1].wrapping_sub(b'0');
    if a > 9 || c > 9 {
        return None;
    }
    Some((a as i64) * 10 + c as i64)
}

/// Parse a ts field (already unquoted). Returns epoch ms or None if bad (format).
/// Out-of-range F3 values return Some(i64::MAX).
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
                    if is_leap(y) {
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
    // F3: digits only
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

// ---------------------------------------------------------------- CSV

#[inline]
fn unquote(f: &[u8]) -> Option<&[u8]> {
    if let Some(&b'"') = f.first() {
        let n = f.len();
        if n < 2 || f[n - 1] != b'"' {
            return None;
        }
        let inner = &f[1..n - 1];
        if memchr::memchr(b'"', inner).is_some() {
            return None;
        }
        Some(inner)
    } else {
        if memchr::memchr(b'"', f).is_some() {
            return None;
        }
        Some(f)
    }
}

#[inline]
fn split_fields<'a, const N: usize>(line: &'a [u8]) -> Option<[&'a [u8]; N]> {
    if line.is_empty() {
        return None;
    }
    let mut out: [&[u8]; N] = [&[]; N];
    let mut start = 0usize;
    let mut k = 0usize;
    for pos in memchr::memchr_iter(b',', line) {
        if k >= N - 1 {
            return None;
        }
        out[k] = unquote(&line[start..pos])?;
        k += 1;
        start = pos + 1;
    }
    if k != N - 1 {
        return None;
    }
    out[k] = unquote(&line[start..])?;
    Some(out)
}

/// Iterate lines of a byte range.
#[inline]
fn for_each_line<'a>(buf: &'a [u8], mut f: impl FnMut(&'a [u8])) {
    let mut start = 0usize;
    for pos in memchr::memchr_iter(b'\n', buf) {
        f(&buf[start..pos]);
        start = pos + 1;
    }
    if start < buf.len() {
        f(&buf[start..]);
    }
}

// ---------------------------------------------------------------- files

struct Files {
    maps: Vec<Mmap>,
}

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

fn open_all(paths: &[PathBuf]) -> Files {
    let maps = paths
        .par_iter()
        .filter_map(|p| {
            let f = fs::File::open(p).ok()?;
            let len = f.metadata().ok()?.len();
            if len == 0 {
                return None;
            }
            let m = unsafe { memmap2::MmapOptions::new().populate().map(&f).ok()? };
            Some(m)
        })
        .collect();
    Files { maps }
}

/// Split all files into newline-aligned chunks, excluding each file's header line.
fn chunks<'a>(files: &'a Files, target: usize) -> Vec<&'a [u8]> {
    let mut out = Vec::new();
    for m in &files.maps {
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

// ---------------------------------------------------------------- partition

fn partition<T: Copy + Send + Sync>(chunks: &[Vec<T>], key: impl Fn(&T) -> usize + Sync) -> (Vec<T>, Vec<usize>) {
    let counts: Vec<Vec<usize>> = chunks
        .par_iter()
        .map(|c| {
            let mut h = vec![0usize; NSHARD];
            for x in c {
                h[key(x)] += 1;
            }
            h
        })
        .collect();
    let mut offs = vec![0usize; NSHARD + 1];
    for s in 0..NSHARD {
        offs[s + 1] = offs[s] + counts.iter().map(|c| c[s]).sum::<usize>();
    }
    let total = offs[NSHARD];
    // per chunk starting positions
    let mut starts: Vec<Vec<usize>> = Vec::with_capacity(chunks.len());
    let mut cur = offs.clone();
    for c in &counts {
        starts.push(cur[..NSHARD].to_vec());
        for s in 0..NSHARD {
            cur[s] += c[s];
        }
    }
    let mut out: Vec<T> = Vec::with_capacity(total);
    let ptr = SyncPtr(out.as_mut_ptr());
    chunks.par_iter().zip(starts.into_par_iter()).for_each(|(c, mut st)| {
        let p = ptr;
        for x in c {
            let s = key(x);
            unsafe { p.0.add(st[s]).write(*x) };
            st[s] += 1;
        }
    });
    unsafe { out.set_len(total) };
    (out, offs)
}

// ---------------------------------------------------------------- main

#[derive(Clone, Copy)]
struct Ev<'a> {
    h: u64,
    ingest: i64,
    eid: &'a [u8],
    user: &'a [u8],
    prod: &'a [u8],
    ts: i64,
    price: u32,
    typ: u8, // 0 view 1 cart 2 purchase
    valid: bool,
}

#[derive(Clone, Copy)]
struct UItem<'a> {
    h: u64,
    s: &'a [u8],
    idx: u32, // index into event list or link side
}

fn find(p: &mut [u32], mut x: u32) -> u32 {
    while p[x as usize] != x {
        let gp = p[p[x as usize] as usize];
        p[x as usize] = gp;
        x = gp;
    }
    x
}

fn manifest_ts(s: &str, key: &str) -> i64 {
    let i = s.find(key).expect("manifest key");
    let rest = &s[i + key.len()..];
    let q1 = rest.find('"').unwrap();
    let rest = &rest[q1 + 1..];
    let q2 = rest.find('"').unwrap();
    parse_ts(rest[..q2].as_bytes()).expect("manifest ts")
}

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
    let hs_owned = hasher();
    let hs = &hs_owned;

    let man = String::from_utf8(read_small(&data.join("manifest.json"))).unwrap();
    let ws = manifest_ts(&man, "\"window_start\"");
    let we = manifest_ts(&man, "\"window_end\"");
    let cws = ws - WEEK_MS;

    // products and categories
    let prod_raw = read_small(&data.join("products.csv"));
    let camp_raw = read_small(&data.join("campaigns.csv"));
    let mut cat_names: Vec<Vec<u8>> = Vec::new();
    let mut prod_pairs: Vec<(&[u8], &[u8])> = Vec::new();
    {
        let mut first = true;
        for_each_line(&prod_raw, |l| {
            if first {
                first = false;
                return;
            }
            if let Some([p, c]) = split_fields::<2>(l) {
                prod_pairs.push((p, c));
                cat_names.push(c.to_vec());
            }
        });
    }
    cat_names.push(b"unknown".to_vec());
    cat_names.sort();
    cat_names.dedup();
    let cat_idx: HashMap<&[u8], u16> = cat_names.iter().enumerate().map(|(i, c)| (c.as_slice(), i as u16)).collect();
    let unknown_cat = cat_idx[&b"unknown"[..]];
    let prod_map: HashMap<&[u8], u16, FHash> = {
        let mut m = HashMap::with_hasher(hs.clone());
        for (p, c) in &prod_pairs {
            m.insert(*p, cat_idx[c]);
        }
        m
    };
    let mut camp_ids: Vec<Vec<u8>> = Vec::new();
    let mut camp_chan: Vec<Vec<u8>> = Vec::new();
    {
        let mut first = true;
        for_each_line(&camp_raw, |l| {
            if first {
                first = false;
                return;
            }
            if let Some([c, ch]) = split_fields::<2>(l) {
                camp_ids.push(c.to_vec());
                camp_chan.push(ch.to_vec());
            }
        });
    }
    let camp_map: HashMap<&[u8], u16, FHash> = {
        let mut m = HashMap::with_hasher(hs.clone());
        for (i, c) in camp_ids.iter().enumerate() {
            m.insert(c.as_slice(), i as u16);
        }
        m
    };
    let ncamp = camp_ids.len();
    let organic = ncamp; // index for organic

    let ev_files = open_all(&list_csv(&data.join("events")));
    let ck_files = open_all(&list_csv(&data.join("clicks")));
    let lk_files = open_all(&list_csv(&data.join("links")));
    eprintln!("mmap {:?}", t0.elapsed());

    // ---- parse events
    let ev_chunks = chunks(&ev_files, 4 << 20);
    let ev_parsed: Vec<Vec<Ev>> = ev_chunks
        .par_iter()
        .map(|buf| {
            let mut v = Vec::with_capacity(buf.len() / 90);
            for_each_line(buf, |l| {
                let Some([eid, user, ts, ing, typ, prod, price]) = split_fields::<7>(l) else { return };
                if eid.is_empty() {
                    return;
                }
                let ingest = parse_ts(ing).unwrap_or(i64::MAX);
                let mut e = Ev { h: hash_bytes(hs, eid), ingest, eid, user, prod: &[], ts: 0, price: 0, typ: 0, valid: false };
                let typ_c = match typ {
                    b"view" => 0u8,
                    b"cart" => 1,
                    b"purchase" => 2,
                    _ => 9,
                };
                if typ_c != 9 && !user.is_empty() && !prod.is_empty() {
                    if let Some(t) = parse_ts(ts) {
                        if t >= ws && t < we && ingest.saturating_sub(t) <= DAY_MS {
                            let mut ok = true;
                            if typ_c == 2 {
                                match parse_price(price) {
                                    Some(p) => {
                                        e.price = p;
                                        e.prod = prod;
                                    }
                                    None => ok = false,
                                }
                            }
                            if ok {
                                e.valid = true;
                                e.ts = t;
                                e.typ = typ_c;
                            }
                        }
                    }
                }
                v.push(e);
            });
            v
        })
        .collect();
    eprintln!("parse events {:?}", t0.elapsed());

    // ---- dedup events
    let (ev_all, ev_offs) = partition(&ev_parsed, |e| shard_of(e.h));
    drop(ev_parsed);
    let kept: Vec<Vec<Ev>> = (0..NSHARD)
        .into_par_iter()
        .map(|s| {
            let sl = &ev_all[ev_offs[s]..ev_offs[s + 1]];
            let mut m: HashMap<&[u8], u32, FHash> = HashMap::with_capacity_and_hasher(sl.len(), hs.clone());
            for (i, e) in sl.iter().enumerate() {
                m.entry(e.eid)
                    .and_modify(|j| {
                        if e.ingest < sl[*j as usize].ingest {
                            *j = i as u32
                        }
                    })
                    .or_insert(i as u32);
            }
            m.values().map(|&j| sl[j as usize]).filter(|e| e.valid).collect()
        })
        .collect();
    drop(ev_all);
    let kept: Vec<Ev> = kept.into_iter().flatten().collect();
    eprintln!("dedup events {:?} kept {}", t0.elapsed(), kept.len());

    // ---- parse links
    let lk_chunks = chunks(&lk_files, 1 << 20);
    let links: Vec<Vec<(&[u8], &[u8])>> = lk_chunks
        .par_iter()
        .map(|buf| {
            let mut v = Vec::new();
            for_each_line(buf, |l| {
                if let Some([a, b]) = split_fields::<2>(l) {
                    if !a.is_empty() && !b.is_empty() {
                        v.push((a, b));
                    }
                }
            });
            v
        })
        .collect();
    let links: Vec<(&[u8], &[u8])> = links.into_iter().flatten().collect();
    let nlinks = links.len();

    // ---- intern users: link sides [0, 2*nlinks), then events
    let nev = kept.len();
    let items: Vec<Vec<UItem>> = {
        let mut v: Vec<Vec<UItem>> = links
            .par_chunks(1 << 16)
            .enumerate()
            .map(|(ci, c)| {
                let base = (ci << 16) as u32;
                let mut out = Vec::with_capacity(c.len() * 2);
                for (k, (a, b)) in c.iter().enumerate() {
                    let i = (base + k as u32) * 2;
                    out.push(UItem { h: hash_bytes(hs, a), s: a, idx: i });
                    out.push(UItem { h: hash_bytes(hs, b), s: b, idx: i + 1 });
                }
                out
            })
            .collect();
        let off = (2 * nlinks) as u32;
        let ve: Vec<Vec<UItem>> = kept
            .par_chunks(1 << 16)
            .enumerate()
            .map(|(ci, c)| {
                let base = off + (ci << 16) as u32;
                c.iter().enumerate().map(|(k, e)| UItem { h: hash_bytes(hs, e.user), s: e.user, idx: base + k as u32 }).collect()
            })
            .collect();
        v.extend(ve);
        v
    };
    let (uitems, u_offs) = partition(&items, |u| shard_of(u.h));
    drop(items);
    // per shard maps
    let shard_maps: Vec<HashMap<&[u8], u32, FHash>> = (0..NSHARD)
        .into_par_iter()
        .map(|s| {
            let sl = &uitems[u_offs[s]..u_offs[s + 1]];
            let mut m: HashMap<&[u8], u32, FHash> = HashMap::with_hasher(hs.clone());
            for u in sl {
                let n = m.len() as u32;
                m.entry(u.s).or_insert(n);
            }
            m
        })
        .collect();
    let mut shard_base = vec![0u32; NSHARD + 1];
    for s in 0..NSHARD {
        shard_base[s + 1] = shard_base[s] + shard_maps[s].len() as u32;
    }
    let nusers = shard_base[NSHARD] as usize;
    let mut uname: Vec<&[u8]> = vec![&[]; nusers];
    {
        let p = SyncPtr(uname.as_mut_ptr());
        shard_maps.par_iter().enumerate().for_each(|(s, m)| {
            let p = p;
            for (k, &v) in m.iter() {
                unsafe { p.0.add((shard_base[s] + v) as usize).write(*k) };
            }
        });
    }
    // uid for each item index
    let mut item_uid = vec![0u32; 2 * nlinks + nev];
    {
        let p = SyncPtr(item_uid.as_mut_ptr());
        (0..NSHARD).into_par_iter().for_each(|s| {
            let p = p;
            let m = &shard_maps[s];
            for u in &uitems[u_offs[s]..u_offs[s + 1]] {
                let v = m[u.s] + shard_base[s];
                unsafe { p.0.add(u.idx as usize).write(v) };
            }
        });
    }
    drop(uitems);
    eprintln!("intern users {:?} users {}", t0.elapsed(), nusers);

    // ---- union-find
    let mut parent: Vec<u32> = (0..nusers as u32).collect();
    for i in 0..nlinks {
        let a = find(&mut parent, item_uid[2 * i]);
        let b = find(&mut parent, item_uid[2 * i + 1]);
        if a != b {
            // union by name: keep root with smaller name so root name = min
            if uname[a as usize] < uname[b as usize] {
                parent[b as usize] = a;
            } else {
                parent[a as usize] = b;
            }
        }
    }
    // root has minimal name in component (since union keeps smaller-named root)
    // dense person ids
    let mut pid_of_root = vec![u32::MAX; nusers];
    let mut pname: Vec<&[u8]> = Vec::new();
    let mut upid = vec![0u32; nusers];
    for u in 0..nusers as u32 {
        let r = find(&mut parent, u);
        if pid_of_root[r as usize] == u32::MAX {
            pid_of_root[r as usize] = pname.len() as u32;
            pname.push(uname[r as usize]);
        }
        upid[u as usize] = pid_of_root[r as usize];
    }
    eprintln!("union-find {:?} persons {}", t0.elapsed(), pname.len());

    // ---- event sort keys
    let evoff = 2 * nlinks;
    let mut ekeys: Vec<(u64, u64)> = kept
        .par_iter()
        .enumerate()
        .map(|(i, e)| {
            let pid = upid[item_uid[evoff + i] as usize] as u64;
            let rel = (e.ts - ws) as u64;
            let cat = if e.typ == 2 { prod_map.get(e.prod).copied().unwrap_or(unknown_cat) as u64 } else { 0 };
            ((pid << 32) | rel, (e.typ as u64) | (cat << 2) | ((e.price as u64) << 32))
        })
        .collect();
    drop(kept);
    ekeys.par_sort_unstable_by_key(|x| x.0);
    eprintln!("sort events {:?}", t0.elapsed());

    // ---- parse clicks
    let ck_chunks = chunks(&ck_files, 4 << 20);
    let mut ckeys: Vec<(u64, u16, &[u8])> = ck_chunks
        .par_iter()
        .map(|buf| {
            let mut v = Vec::new();
            for_each_line(buf, |l| {
                let Some([cid, user, camp, ts]) = split_fields::<4>(l) else { return };
                if cid.is_empty() || user.is_empty() {
                    return;
                }
                let Some(t) = parse_ts(ts) else { return };
                if t < cws || t >= we {
                    return;
                }
                let Some(&ci) = camp_map.get(camp) else { return };
                let h = hash_bytes(hs, user);
                let s = shard_of(h);
                let Some(&lu) = shard_maps[s].get(user) else { return };
                let pid = upid[(shard_base[s] + lu) as usize] as u64;
                v.push(((pid << 32) | (t - cws) as u64, ci, cid));
            });
            v
        })
        .flatten()
        .collect();
    ckeys.par_sort_unstable_by_key(|x| x.0);
    eprintln!("clicks {:?} n {}", t0.elapsed(), ckeys.len());

    // ---- sessionize + attribution, parallel over person ranges
    let ws_day = ws.div_euclid(DAY_MS);
    let ndays = ((we - 1).div_euclid(DAY_MS) - ws_day + 1) as usize;
    let ncat = cat_names.len();
    // split ekeys into ~ 64*threads ranges at person boundaries
    let nparts = 256usize;
    let mut bounds = vec![0usize];
    for k in 1..nparts {
        let mut b = ekeys.len() * k / nparts;
        if b <= *bounds.last().unwrap() {
            continue;
        }
        let pid = ekeys[b].0 >> 32;
        while b < ekeys.len() && ekeys[b].0 >> 32 == pid {
            b += 1;
        }
        if b > *bounds.last().unwrap() && b < ekeys.len() {
            bounds.push(b);
        }
    }
    bounds.push(ekeys.len());
    bounds.dedup();

    struct PartOut {
        user_rows: Vec<(u32, i32, u32, u32, u64, u16)>, // pid, day, sessions, events, revenue, topcat (u16::MAX none)
        camp: Vec<[u64; 4]>,                             // (ncamp+1)*ndays: sessions, purchasing, revenue, assisted
    }
    let ckeys_ref = &ckeys;
    let parts: Vec<PartOut> = bounds
        .par_windows(2)
        .map(|w| {
            let ev = &ekeys[w[0]..w[1]];
            let mut po = PartOut { user_rows: Vec::new(), camp: vec![[0u64; 4]; (ncamp + 1) * ndays] };
            let mut catsum = vec![0u64; ncat];
            let mut touched: Vec<u16> = Vec::new();
            let mut ccount = vec![0u32; ncamp];
            let mut present: Vec<u16> = Vec::new();
            let mut ppos = vec![u32::MAX; ncamp];
            // find click start for first pid
            let first_pid = ev[0].0 >> 32;
            let mut cp = ckeys_ref.partition_point(|c| (c.0 >> 32) < first_pid);
            let mut i = 0;
            while i < ev.len() {
                let pid = ev[i].0 >> 32;
                let mut j = i;
                while j < ev.len() && ev[j].0 >> 32 == pid {
                    j += 1;
                }
                // clicks of pid
                while cp < ckeys_ref.len() && (ckeys_ref[cp].0 >> 32) < pid {
                    cp += 1;
                }
                let cstart = cp;
                while cp < ckeys_ref.len() && (ckeys_ref[cp].0 >> 32) == pid {
                    cp += 1;
                }
                let cl = &ckeys_ref[cstart..cp];
                let mut lo = 0usize;
                let mut hi = 0usize;
                // per-day accumulators
                let mut cur_day: i64 = i64::MIN;
                let mut d_sess = 0u32;
                let mut d_ev = 0u32;
                let mut d_rev = 0u64;
                let flush = |po: &mut PartOut, day: i64, s: u32, e: u32, r: u64, catsum: &mut Vec<u64>, touched: &mut Vec<u16>| {
                    let mut best = u16::MAX;
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
                    po.user_rows.push((pid as u32, day as i32, s, e, r, best));
                };
                let mut k = i;
                while k < j {
                    // session from k
                    let start_rel = (ev[k].0 & 0xffff_ffff) as i64;
                    let mut last = start_rel;
                    let mut n = 0u32;
                    let mut rev = 0u64;
                    let mut purch = false;
                    let sday = (ws + start_rel).div_euclid(DAY_MS);
                    if sday != cur_day {
                        if cur_day != i64::MIN {
                            flush(&mut po, cur_day, d_sess, d_ev, d_rev, &mut catsum, &mut touched);
                        }
                        cur_day = sday;
                        d_sess = 0;
                        d_ev = 0;
                        d_rev = 0;
                    }
                    while k < j {
                        let t = (ev[k].0 & 0xffff_ffff) as i64;
                        if t - last > 1_800_000 {
                            break;
                        }
                        last = t;
                        n += 1;
                        let p = ev[k].1;
                        if p & 3 == 2 {
                            let price = p >> 32;
                            let cat = ((p >> 2) & 0xffff) as u16;
                            rev += price;
                            purch = true;
                            if catsum[cat as usize] == 0 {
                                touched.push(cat);
                            }
                            catsum[cat as usize] += price;
                        }
                        k += 1;
                    }
                    d_sess += 1;
                    d_ev += n;
                    d_rev += rev;
                    // attribution
                    let s_abs = start_rel + WEEK_MS; // relative to cws
                    while hi < cl.len() && ((cl[hi].0 & 0xffff_ffff) as i64) <= s_abs {
                        let c = cl[hi].1 as usize;
                        if ccount[c] == 0 {
                            ppos[c] = present.len() as u32;
                            present.push(c as u16);
                        }
                        ccount[c] += 1;
                        hi += 1;
                    }
                    while lo < hi && ((cl[lo].0 & 0xffff_ffff) as i64) < s_abs - WEEK_MS {
                        let c = cl[lo].1 as usize;
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
                        let tlast = cl[hi - 1].0;
                        let mut b = hi - 1;
                        let mut q = hi - 1;
                        while q > lo && cl[q - 1].0 == tlast {
                            q -= 1;
                            if cl[q].2 < cl[b].2 {
                                b = q;
                            }
                        }
                        for &c in &present {
                            po.camp[c as usize * ndays + dayi][3] += 1;
                        }
                        cl[b].1 as usize
                    } else {
                        organic
                    };
                    let a = &mut po.camp[credit * ndays + dayi];
                    a[0] += 1;
                    if purch {
                        a[1] += 1;
                    }
                    a[2] += rev;
                }
                if cur_day != i64::MIN {
                    flush(&mut po, cur_day, d_sess, d_ev, d_rev, &mut catsum, &mut touched);
                }
                // reset window state
                for &c in &present {
                    ccount[c as usize] = 0;
                }
                present.clear();
                i = j;
            }
            po
        })
        .collect();
    eprintln!("sessions {:?}", t0.elapsed());

    // ---- outputs
    let mut camp = vec![[0u64; 4]; (ncamp + 1) * ndays];
    for p in &parts {
        for (a, b) in camp.iter_mut().zip(p.camp.iter()) {
            for x in 0..4 {
                a[x] += b[x];
            }
        }
    }
    write_outputs(&out, &parts.iter().map(|p| &p.user_rows[..]).collect::<Vec<_>>(), &pname, &cat_names, &camp, &camp_ids, &camp_chan, ndays, ws_day);
    eprintln!("done {:?}", t0.elapsed());
    let _ = Arc::new(());
}

fn write_outputs(
    out: &Path,
    user_parts: &[&[(u32, i32, u32, u32, u64, u16)]],
    pname: &[&[u8]],
    cat_names: &[Vec<u8>],
    camp: &[[u64; 4]],
    camp_ids: &[Vec<u8>],
    camp_chan: &[Vec<u8>],
    ndays: usize,
    ws_day: i64,
) {
    use arrow_array::builder::{Date32Builder, Int64Builder, StringBuilder};
    use arrow_array::{ArrayRef, RecordBatch};
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;

    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    fs::create_dir_all(&ud).unwrap();
    fs::create_dir_all(&cd).unwrap();
    let props = || WriterProperties::builder().set_compression(Compression::UNCOMPRESSED).set_dictionary_enabled(false).build();

    // user_daily, one file per group of parts in parallel
    let groups: Vec<Vec<&[(u32, i32, u32, u32, u64, u16)]>> = {
        let ng = 8usize;
        let mut g: Vec<Vec<_>> = (0..ng).map(|_| Vec::new()).collect();
        for (i, p) in user_parts.iter().enumerate() {
            g[i * ng / user_parts.len().max(1)].push(*p);
        }
        g
    };
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
        for p in g {
            for r in p.iter() {
                uid.append_value(unsafe { std::str::from_utf8_unchecked(pname[r.0 as usize]) });
                day.append_value(r.1);
                ses.append_value(r.2 as i64);
                evs.append_value(r.3 as i64);
                rev.append_value(r.4 as i64);
                if r.5 == u16::MAX {
                    top.append_null();
                } else {
                    top.append_value(unsafe { std::str::from_utf8_unchecked(&cat_names[r.5 as usize]) });
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

    // campaign_daily
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
}
