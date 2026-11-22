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

static PT: [std::sync::atomic::AtomicU64; 4] = [const { std::sync::atomic::AtomicU64::new(0) }; 4];
const NS: usize = 1024; // event-id shards
const NP: usize = 256; // person shards
#[inline]
fn eshard(h: u64) -> usize {
    (h >> 54) as usize & (NS - 1)
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

const Y0: i64 = 1600;
const NY: usize = 1000;
const fn build_ydays() -> [i32; NY] {
    let mut t = [0i32; NY];
    let mut i = 0;
    while i < NY {
        let y = Y0 + i as i64 - 1;
        // days from 1970-01-01 to Jan 1 of year (Y0 + i)
        let yy = y + 1;
        let yb = yy - 1;
        let d = (yb * 365 + yb / 4 - yb / 100 + yb / 400) - (1969 * 365 + 1969 / 4 - 1969 / 100 + 1969 / 400);
        t[i] = d as i32;
        let _ = y;
        i += 1;
    }
    t
}
static YDAYS: [i32; NY] = build_ydays();
static MCUM: [[i32; 13]; 2] = [
    [0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334],
    [0, 0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335],
];
static MDAYS: [[u8; 13]; 2] = [[0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31], [0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]];

/// Parse a ts field (unquoted). None = bad format. Huge F3 values -> Some(i64::MAX).
#[inline(always)]
fn parse_ts(b: &[u8]) -> Option<i64> {
    let n = b.len();
    if n == 20 || n == 25 {
        if b[4] == b'-' && b[7] == b'-' && b[10] == b'T' && b[13] == b':' && b[16] == b':' {
            let g = |i: usize| b[i].wrapping_sub(b'0');
            let (y0, y1, y2, y3) = (g(0), g(1), g(2), g(3));
            let (m0, m1, d0, d1) = (g(5), g(6), g(8), g(9));
            let (h0, h1, i0, i1, s0, s1) = (g(11), g(12), g(14), g(15), g(17), g(18));
            if (y0 > 9) | (y1 > 9) | (y2 > 9) | (y3 > 9) | (m0 > 9) | (m1 > 9) | (d0 > 9) | (d1 > 9) | (h0 > 9) | (h1 > 9) | (i0 > 9) | (i1 > 9) | (s0 > 9) | (s1 > 9) {
                return None;
            }
            let y = y0 as i64 * 1000 + y1 as i64 * 100 + y2 as i64 * 10 + y3 as i64;
            let mo = (m0 * 10 + m1) as usize;
            let d = (d0 * 10 + d1) as i64;
            let h = (h0 * 10 + h1) as i64;
            let mi = (i0 * 10 + i1) as i64;
            let sc = (s0 * 10 + s1) as i64;
            if !(1..=12).contains(&mo) || h > 23 || mi > 59 || sc > 59 || d < 1 {
                return None;
            }
            let leap = ((y % 4 == 0 && y % 100 != 0) || y % 400 == 0) as usize;
            if d > MDAYS[leap][mo] as i64 {
                return None;
            }
            let yd = if y >= Y0 && y < Y0 + NY as i64 { YDAYS[(y - Y0) as usize] as i64 } else { days_from_civil(y, 1, 1) };
            let days = yd + MCUM[leap][mo] as i64 + d - 1;
            let mut t = (days * 86400 + h * 3600 + mi * 60 + sc) * 1000;
            if n == 20 {
                if b[19] != b'Z' {
                    return None;
                }
            } else {
                let sign = b[19];
                let (a0, a1, c0, c1) = (g(20), g(21), g(23), g(24));
                if (sign != b'+' && sign != b'-') || b[22] != b':' || (a0 > 9) | (a1 > 9) | (c0 > 9) | (c1 > 9) {
                    return None;
                }
                let oh = (a0 * 10 + a1) as i64;
                let om = (c0 * 10 + c1) as i64;
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
    if n == 0 {
        return None;
    }
    if n <= 18 {
        let mut v: u64 = 0;
        let mut bad = false;
        for &c in b {
            let x = c.wrapping_sub(b'0');
            bad |= x > 9;
            v = v * 10 + x as u64;
        }
        return if bad { None } else { Some(v as i64) };
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

#[cfg(target_arch = "x86_64")]
#[inline(always)]
unsafe fn masks128(p: *const u8) -> (u128, u128, u128) {
    use std::arch::x86_64::*;
    unsafe {
        let c1 = _mm256_set1_epi8(b',' as i8);
        let c2 = _mm256_set1_epi8(b'"' as i8);
        let c3 = _mm256_set1_epi8(b'\n' as i8);
        let mut cm = 0u128;
        let mut qm = 0u128;
        let mut nm = 0u128;
        for i in 0..4 {
            let v = _mm256_loadu_si256(p.add(32 * i) as *const __m256i);
            cm |= (_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, c1)) as u32 as u128) << (32 * i);
            qm |= (_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, c2)) as u32 as u128) << (32 * i);
            nm |= (_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, c3)) as u32 as u128) << (32 * i);
        }
        (cm, qm, nm)
    }
}

/// Scratch for `scan_idx`.
#[derive(Default)]
struct ScanScr {
    sp: Vec<u32>,
    qm: Vec<u64>,
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
unsafe fn masks64(p: *const u8) -> (u64, u64, u64) {
    use std::arch::x86_64::*;
    unsafe {
        let a = _mm256_loadu_si256(p as *const __m256i);
        let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
        let c1 = _mm256_set1_epi8(b',' as i8);
        let c2 = _mm256_set1_epi8(b'"' as i8);
        let c3 = _mm256_set1_epi8(b'\n' as i8);
        let m = |v: __m256i, c: __m256i| _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, c)) as u32 as u64;
        (m(a, c1) | (m(b, c1) << 32), m(a, c2) | (m(b, c2) << 32), m(a, c3) | (m(b, c3) << 32))
    }
}

#[inline(always)]
fn qcount(qm: &[u64], s: usize, e: usize) -> u32 {
    // quotes in [s, e)
    if e <= s {
        return 0;
    }
    let (b0, b1) = (s >> 6, (e - 1) >> 6);
    let mut c = 0u32;
    for b in b0..=b1 {
        let mut m = unsafe { *qm.get_unchecked(b) };
        if b == b0 {
            m &= !0u64 << (s & 63);
        }
        if b == b1 {
            let hi = ((e - 1) & 63) as u32;
            m &= u64::MAX >> (63 - hi);
        }
        c += m.count_ones();
    }
    c
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(never)]
fn stage1(buf: &[u8], scr: &mut ScanScr) -> usize {
    const NL: u32 = 1 << 31;
    let len = buf.len();
    let nb = len.div_ceil(64);
    scr.sp.clear();
    scr.sp.reserve(len + 80);
    scr.qm.clear();
    scr.qm.reserve(nb + 1);
    let base = buf.as_ptr();
    let mut cnt = 0usize;
    unsafe {
        let sp = scr.sp.as_mut_ptr();
        let qm = scr.qm.as_mut_ptr();
        for b in 0..nb {
            let o = b * 64;
            let (c, q, n) = if o + 64 <= len {
                masks64(base.add(o))
            } else {
                let mut tmp = [0u8; 64];
                std::ptr::copy_nonoverlapping(base.add(o), tmp.as_mut_ptr(), len - o);
                masks64(tmp.as_ptr())
            };
            *qm.add(b) = q;
            let mut sep = c | n;
            let k = sep.count_ones() as usize;
            let mut w = sp.add(cnt);
            while sep != 0 {
                for _ in 0..8 {
                    let tz = sep.trailing_zeros();
                    *w = (o as u32 + tz) | (((n.wrapping_shr(tz)) as u32 & 1) << 31);
                    w = w.add(1);
                    sep &= sep.wrapping_sub(1);
                }
            }
            cnt += k;
        }
        if *base.add(len - 1) != b'\n' {
            *sp.add(cnt) = len as u32 | NL;
            cnt += 1;
        }
        // padding so 8-lane loads past the end read defined memory
        for i in 0..8 {
            *sp.add(cnt + i) = 0;
        }
        scr.sp.set_len(cnt + 8);
        scr.qm.set_len(nb);
    }
    cnt
}

/// Same contract as `scan`: two-stage (structural index, then per line) scanner.
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
fn scan_idx<'a, const N: usize>(buf: &'a [u8], scr: &mut ScanScr, mut f: impl FnMut(&[&'a [u8]; N])) {
    use std::arch::x86_64::*;
    const NL: u32 = 1 << 31;
    let len = buf.len();
    if len == 0 {
        return;
    }
    let cnt = stage1(buf, scr);
    let sp = &scr.sp[..];
    let qm = &scr.qm[..];
    let want: i32 = 1 << (N - 1);
    let lane: i32 = (1 << N) - 1;
    let mut j = 0usize;
    let mut start = 0usize;
    while j < cnt {
        let bits = unsafe { _mm256_movemask_ps(_mm256_castsi256_ps(_mm256_loadu_si256(sp.as_ptr().add(j) as *const __m256i))) } & lane;
        if bits == want && j + N <= cnt {
            let mut fs: [(usize, usize); N] = [(0, 0); N];
            let mut a = start;
            for k in 0..N {
                let e = (sp[j + k] & !NL) as usize;
                fs[k] = (a, e);
                a = e + 1;
            }
            let end = fs[N - 1].1;
            let tq = qcount(qm, start, end);
            let mut ok = true;
            if tq != 0 {
                let mut qb = 0u32;
                for k in 0..N {
                    let (a, e) = fs[k];
                    let sq = a < e && buf[a] == b'"';
                    let eq = e > a + 1 && buf[e - 1] == b'"';
                    qb += sq as u32 + eq as u32;
                    if sq != eq {
                        ok = false;
                    } else if sq {
                        fs[k] = (a + 1, e - 1);
                    }
                }
                if qb != tq {
                    ok = false;
                }
            }
            if ok {
                let arr: [&'a [u8]; N] = std::array::from_fn(|k| unsafe { buf.get_unchecked(fs[k].0..fs[k].1) });
                f(&arr);
            }
            j += N;
            start = end + 1;
        } else {
            // wrong field count: skip to the end of this line
            let mut k = j;
            while sp[k] & NL == 0 {
                k += 1;
            }
            start = (sp[k] & !NL) as usize + 1;
            j = k + 1;
        }
    }
}

/// Same contract as `scan`, with a per-line SIMD fast path (lines < 128 bytes).
#[inline(always)]
fn scan_fast<'a, const N: usize>(buf: &'a [u8], mut f: impl FnMut(&[&'a [u8]; N])) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        let len = buf.len();
        let base = buf.as_ptr();
        let mut s = 0usize;
        let mut fs: [(u32, u32); N] = [(0, 0); N];
        while s + 128 <= len {
            let (cm, qm, nm) = unsafe { masks128(base.add(s)) };
            if nm == 0 {
                // long line: hand it to the generic scanner
                let e = match memchr::memchr(b'\n', &buf[s..]) {
                    Some(p) => s + p + 1,
                    None => len,
                };
                scan::<N>(&buf[s..e], &mut f);
                s = e;
                continue;
            }
            let e = nm.trailing_zeros();
            let lm = (1u128 << e) - 1;
            let mut cms = cm & lm;
            if cms.count_ones() as usize == N - 1 {
                let mut a = 0u32;
                for k in 0..N - 1 {
                    let c = cms.trailing_zeros();
                    fs[k] = (a, c);
                    a = c + 1;
                    cms &= cms - 1;
                }
                fs[N - 1] = (a, e);
                let qms = qm & lm;
                let mut ok = true;
                if qms != 0 {
                    for k in 0..N {
                        let (a, b) = fs[k];
                        let rm = ((1u128 << b) - 1) & !((1u128 << a) - 1);
                        let fq = qms & rm;
                        if fq != 0 {
                            if b >= a + 2 && fq == ((1u128 << a) | (1u128 << (b - 1))) {
                                fs[k] = (a + 1, b - 1);
                            } else {
                                ok = false;
                            }
                        }
                    }
                }
                if ok {
                    let arr: [&'a [u8]; N] = std::array::from_fn(|k| unsafe { std::slice::from_raw_parts(base.add(s + fs[k].0 as usize), (fs[k].1 - fs[k].0) as usize) });
                    f(&arr);
                }
            }
            s += e as usize + 1;
        }
        if s < len {
            scan::<N>(&buf[s..], &mut f);
        }
        return;
    }
    #[allow(unreachable_code)]
    scan::<N>(buf, f)
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
            // Lazy mapping: page-ins happen in parallel inside the scan instead of serially here.
            if std::env::var("SS_POP").is_ok() {
                return unsafe { memmap2::MmapOptions::new().populate().map(&f).ok() };
            }
            unsafe { memmap2::MmapOptions::new().map(&f).ok() }
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

/// One contiguous buffer split into shards by offsets.
struct Parts<T, const M: usize> {
    ptr: *mut T,
    off: [u32; M],
}
unsafe impl<T, const M: usize> Send for Parts<T, M> {}
unsafe impl<T, const M: usize> Sync for Parts<T, M> {}
impl<T, const M: usize> std::ops::Index<usize> for Parts<T, M> {
    type Output = [T];
    #[inline(always)]
    fn index(&self, i: usize) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.ptr.add(self.off[i] as usize), (self.off[i + 1] - self.off[i]) as usize) }
    }
}

/// Bump arena over one anonymous huge-page mapping (no per-buffer rounding waste); never freed.
struct Arena {
    base: usize,
    cap: usize,
    off: std::sync::atomic::AtomicUsize,
}
impl Arena {
    fn new(cap: usize) -> Arena {
        let cap = (cap + (2 << 20) - 1) & !((2 << 20) - 1);
        let base = match memmap2::MmapMut::map_anon(cap + (2 << 20)) {
            Ok(m) => {
                // 4 KB pages measured faster here: in a VM, fresh 2 MB pages are often memory the host has to
                // supply again, while small pages are more often reused ones. (On warm runs, and on the scorer, 2 MB pages won.)
                if huge_pages() {
                    let _ = m.advise(memmap2::Advice::HugePage);
                }
                let p = m.as_ptr() as usize;
                std::mem::forget(m);
                (p + (2 << 20) - 1) & !((2 << 20) - 1)
            }
            Err(_) => 0,
        };
        Arena { base, cap: if base == 0 { 0 } else { cap }, off: std::sync::atomic::AtomicUsize::new(0) }
    }
    fn alloc<T>(&self, n: usize) -> *mut T {
        let bytes = (n * std::mem::size_of::<T>() + 63) & !63;
        let o = self.off.fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
        if o + bytes <= self.cap {
            (self.base + o) as *mut T
        } else {
            let mut v = std::mem::ManuallyDrop::new(Vec::<T>::with_capacity(n));
            v.as_mut_ptr()
        }
    }
    fn reset(&self) {
        self.off.store(0, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Dedup key, 12 bytes: a = eid hash bits 0..54 | ig bits 0..10 << 54; b = ig bits 10..21 | loc << 11.
/// The hash's top 10 bits pick the shard, so all 64 are compared. ig = ingest seconds + 2 days relative to
/// the window start; 0 and IG_HI mean out of that range, and the exact value is in the chunk's side list.
/// loc = index of the kept candidate in its chunk (DD_INV = never kept).
#[derive(Clone, Copy)]
#[repr(C, packed)]
struct Dd {
    a: u64,
    b: u32,
}
const DD_INV: u32 = (1 << 21) - 1;
const M54: u64 = (1 << 54) - 1;
const IG_OFF: i64 = 2 * 86_400;
const IG_HI: u32 = (1 << 21) - 1;
impl Dd {
    #[inline(always)]
    fn new(eid: u64, ig: u32, loc: u32) -> Dd {
        Dd { a: (eid & M54) | (((ig & 1023) as u64) << 54), b: (ig >> 10) | (loc << 11) }
    }
    #[inline(always)]
    fn key(&self) -> u64 {
        self.a & M54
    }
    #[inline(always)]
    fn ig(&self) -> u32 {
        ((self.a >> 54) as u32) | ((self.b & 2047) << 10)
    }
    #[inline(always)]
    fn loc(&self) -> u32 {
        self.b >> 11
    }
}
#[inline(always)]
fn ig_pack(ingest: i32, narrow: bool) -> u32 {
    let r = ingest as i64 + IG_OFF;
    if narrow || r >= IG_HI as i64 {
        IG_HI
    } else if r < 1 {
        0
    } else {
        r as u32
    }
}

/// Kept event: pidx << 32 | ts_rel << 1 | is_purchase (ts_rel all ones = dropped by dedup).
/// Purchases also have price:24 | cat << 24 in a parallel list, in the same order.
type Ke = u64;
const KE_DEAD: u64 = 0x7fff_ffff << 1;
#[inline(always)]
fn ke_dead(k: u64) -> bool {
    k & KE_DEAD == KE_DEAD
}
#[inline(always)]
fn ke_ts(k: u64) -> u32 {
    ((k >> 1) & 0x7fff_ffff) as u32
}

/// Kept event in session scratch: key as Ke, pv = purchase price:24 | cat << 24.
#[derive(Clone, Copy)]
#[repr(C)]
struct KeS {
    key: u64,
    pv: u32,
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

#[derive(Clone, Copy, Default)]
#[repr(C, align(16))]
struct LSlot {
    key: u64,
    person: u64, // high 32 bits: 0 = the ID is its own person, else the person's slot + 1 (low 32: link-build scratch)
}

/// Open-addressing table over every user ID seen (linked IDs first, then event IDs as they come).
/// A person is numbered by one slot (pidx): its smallest ID's slot, so the slot's name is the person's name.
struct LinkTable {
    cap: usize,
    slots: &'static mut [LSlot], // key 0 = empty
    name: &'static mut [u64],    // per slot: the ID, ptr << 16 | len
    sp: usize,         // slots.as_mut_ptr(), for concurrent inserts
    np: usize,         // name.as_mut_ptr()
    used: std::sync::atomic::AtomicUsize,
    lim: usize,
    full: std::sync::atomic::AtomicBool,
}
impl LinkTable {
    #[inline(always)]
    fn pidx_at(&self, h: usize) -> u32 {
        let p = unsafe { (*(self.sp as *const LSlot).add(h)).person } >> 32;
        if p == 0 { h as u32 } else { (p - 1) as u32 }
    }
    /// Person number of the ID with hash k, adding the ID as its own person if new. Past the load limit it
    /// returns 0 and sets `full`; the caller then redoes the parse with a bigger table.
    #[inline(always)]
    fn resolve(&self, k: u64, ptr: usize, len: u16) -> u32 {
        use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
        let mut h = self.slot(k);
        loop {
            let a = unsafe { AtomicU64::from_ptr(&raw mut (*(self.sp as *mut LSlot).add(h)).key) };
            let cur = a.load(Relaxed);
            if cur == k {
                return self.pidx_at(h);
            }
            if cur == 0 {
                if self.used.fetch_add(1, Relaxed) >= self.lim {
                    self.full.store(true, Relaxed);
                    return 0;
                }
                match a.compare_exchange(0, k, Relaxed, Relaxed) {
                    Ok(_) => {
                        unsafe { *(self.np as *mut u64).add(h) = ((ptr as u64) << 16) | len as u64 };
                        return h as u32;
                    }
                    Err(x) if x == k => return self.pidx_at(h),
                    Err(_) => {}
                }
            }
            h += 1;
            if h == self.cap {
                h = 0;
            }
        }
    }
    #[inline(always)]
    fn name_of(&self, pidx: u64) -> (usize, u16) {
        let r = self.name[pidx as usize];
        ((r >> 16) as usize, r as u16)
    }
    #[inline(always)]
    fn prefetch_name(&self, pidx: u64) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::*;
            _mm_prefetch(self.name.as_ptr().add(pidx as usize) as *const i8, _MM_HINT_T0);
        }
    }
    #[inline(always)]
    fn slot(&self, k: u64) -> usize {
        ((k as u128 * self.cap as u128) >> 64) as usize
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
            let kk = unsafe { self.slots.get_unchecked(h).key };
            if kk == k {
                return Some(h);
            }
            if kk == 0 {
                return None;
            }
            h += 1;
            if h == self.cap {
                h = 0;
            }
        }
    }
    #[inline(always)]
    fn prefetch2(&self, k: u64) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::*;
            let h = self.slot(k);
            _mm_prefetch(self.slots.as_ptr().add(h) as *const i8, _MM_HINT_T0);
        }
    }
}
unsafe impl Sync for LinkTable {}

/// 2 MB pages for arenas and the link table (fewer TLB misses on warm machines); SS_SMALL=1 uses 4 KB pages.
fn huge_pages() -> bool {
    std::env::var_os("SS_SMALL").is_none()
}

/// A zeroed, never-freed buffer in its own anonymous mapping.
fn anon_slice<T>(n: usize) -> &'static mut [T] {
    let bytes = (n * std::mem::size_of::<T>()).max(1);
    let m = memmap2::MmapMut::map_anon(bytes).expect("map_anon");
    if huge_pages() {
        let _ = m.advise(memmap2::Advice::HugePage);
    }
    let p = m.as_ptr() as *mut T;
    std::mem::forget(m);
    unsafe { std::slice::from_raw_parts_mut(p, n) }
}

/// Touch every page of a fresh zeroed buffer, 2 MB per parallel task.
fn prefault(p: *mut u8, len: usize) {
    let p = p as usize;
    (0..len.div_ceil(1 << 21)).into_par_iter().for_each(|i| {
        let s = i << 21;
        let e = (s + (1 << 21)).min(len);
        let mut o = s;
        while o < e {
            unsafe { std::ptr::write_volatile((p + o) as *mut u8, 0) };
            o += 4096;
        }
    });
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

fn rss_anon_mb() -> u64 {
    let st = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    for l in st.lines() {
        if let Some(r) = l.strip_prefix("RssAnon:") {
            return r.trim().trim_end_matches("kB").trim().parse::<u64>().unwrap_or(0) / 1024;
        }
    }
    0
}

fn proc_cpu() -> f64 {
    let st = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    let rest = st.rsplit(')').next().unwrap_or("");
    let v: Vec<&str> = rest.split_whitespace().collect();
    let ut: f64 = v.get(11).and_then(|x| x.parse().ok()).unwrap_or(0.0);
    let stt: f64 = v.get(12).and_then(|x| x.parse().ok()).unwrap_or(0.0);
    (ut + stt) / 100.0
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
    let verbose = std::env::var("SS_VERBOSE").is_ok();
    macro_rules! log {
        ($($t:tt)*) => { if verbose { eprintln!("{:>8.1}ms cpu {:>6.2}s anon {:>5}M {}", t0.elapsed().as_secs_f64() * 1000.0, proc_cpu(), rss_anon_mb(), format!($($t)*)); } };
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
    // Optional: start read-ahead of all inputs in the background (measured slower in Docker, so off).
    if std::env::var("SS_WILLNEED").is_ok() {
        let regs: Vec<(usize, usize)> = ev_maps.iter().chain(ck_maps.iter()).chain(lk_maps.iter()).map(|m| (m.as_ptr() as usize, m.len())).collect();
        std::thread::spawn(move || {
            let t = Instant::now();
            for (p, l) in regs {
                unsafe { madvise(p as *mut std::ffi::c_void, l, 3 /* MADV_WILLNEED */) };
            }
            if std::env::var("SS_VERBOSE").is_ok() {
                eprintln!("willneed took {:?}", t.elapsed());
            }
        });
    }

    // ---- products / campaigns
    let prod_raw = read_small(&data.join("products.csv"));
    let camp_raw = read_small(&data.join("campaigns.csv"));
    // Categories are few: find each line's in a short list instead of allocating per product
    // (fresh memory is costly when the machine is cold).
    let mut cats: Vec<&[u8]> = Vec::new();
    let mut prod_tmp: Vec<(u64, u8)> = Vec::new();
    {
        let body = match memchr::memchr(b'\n', &prod_raw) {
            Some(p) => &prod_raw[p + 1..],
            None => &[][..],
        };
        prod_tmp.reserve(memchr::memchr_iter(b'\n', body).count() + 1);
        scan::<2>(body, |f| {
            let c = match cats.iter().position(|x| *x == f[1]) {
                Some(i) => i,
                None => {
                    cats.push(f[1]);
                    assert!(cats.len() <= 127);
                    cats.len() - 1
                }
            };
            prod_tmp.push((hb(f[0]), c as u8));
        });
    }
    let mut cat_names: Vec<Vec<u8>> = cats.iter().map(|c| c.to_vec()).collect();
    cat_names.push(b"unknown".to_vec());
    cat_names.sort();
    cat_names.dedup();
    assert!(cat_names.len() <= 127);
    let gidx = |c: &[u8]| cat_names.binary_search_by(|x| x.as_slice().cmp(c)).unwrap() as u8;
    let remap: Vec<u8> = cats.iter().map(|c| gidx(c)).collect();
    let unknown_cat = gidx(b"unknown");
    let prod_map: HashMap<u64, u8, BuildNoHash> = {
        let mut m = HashMap::with_capacity_and_hasher(prod_tmp.len(), BuildNoHash);
        for &(h, c) in &prod_tmp {
            m.insert(h, remap[c as usize]);
        }
        m
    };
    drop(prod_tmp);
    log!("products");
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
    let links_job = |extra: usize| -> LinkTable {
        // Upper bound on link lines (every line is newline-terminated or ends a chunk).
        let n: usize = lk_chunks.par_iter().map(|b| memchr::memchr_iter(b'\n', b).count() + 1).sum();
        use std::sync::atomic::{AtomicU32, AtomicU64, Ordering::Relaxed};
        // > the most distinct IDs n lines can hold, plus room for IDs that appear only in events
        let cap = n * 2 + 64 + extra;
        // Own 4 KB-page mappings (huge pages are slow to fault in when the VM is cold).
        let slots: &'static mut [LSlot] = anon_slice(cap);
        let oname: &'static mut [u64] = anon_slice(cap);
        let onp = oname.as_mut_ptr() as usize;
        // Fault the table in parallel, one huge page per task: random inserts would otherwise pile up
        // on the same few page faults (each zeroes 2 MB) and run nearly serially.
        prefault(slots.as_mut_ptr() as *mut u8, cap * std::mem::size_of::<LSlot>());
        prefault(onp as *mut u8, cap * 8);
        // Union-find state lives in each slot's person field: low half = parent (0 = root, else parent slot + 1),
        // high half = a root's best (smallest-name) slot + 1, which becomes every member's person below.
        let sp = slots.as_mut_ptr() as usize;
        let sat = |h: usize| unsafe { (sp as *mut LSlot).add(h) };
        let key_at = |h: usize| unsafe { AtomicU64::from_ptr(&raw mut (*sat(h)).key) };
        let par_at = |h: usize| unsafe { AtomicU32::from_ptr((&raw mut (*sat(h)).person) as *mut u32) };
        let best_at = |h: usize| unsafe { AtomicU32::from_ptr(((&raw mut (*sat(h)).person) as *mut u32).add(1)) };
        let insert = |k: u64, ptr: usize, len: u16| -> u32 {
            let mut h = ((k as u128 * cap as u128) >> 64) as usize;
            loop {
                let a = key_at(h);
                let cur = a.load(Relaxed);
                if cur == k {
                    return h as u32;
                }
                if cur == 0 {
                    match a.compare_exchange(0, k, Relaxed, Relaxed) {
                        Ok(_) => {
                            unsafe { *(onp as *mut u64).add(h) = ((ptr as u64) << 16) | len as u64 };
                            return h as u32;
                        }
                        Err(x) if x == k => return h as u32,
                        Err(_) => {}
                    }
                }
                h += 1;
                if h == cap {
                    h = 0;
                }
            }
        };
        let edges: Vec<Vec<(u32, u32)>> = lk_chunks
            .par_iter()
            .map(|buf| {
                let mut v: Vec<(u32, u32)> = Vec::with_capacity(buf.len() / 4 + 2);
                let vb = v.as_mut_ptr();
                let mut vn = 0usize;
                scan::<2>(buf, |f| {
                    let (a, b) = (f[0], f[1]);
                    if !a.is_empty() && !b.is_empty() {
                        let e = (insert(hb(a), a.as_ptr() as usize, a.len() as u16), insert(hb(b), b.as_ptr() as usize, b.len() as u16));
                        unsafe { vb.add(vn).write(e) };
                        vn += 1;
                    }
                });
                unsafe { v.set_len(vn) };
                v
            })
            .collect();
        log!("  lk intern");
        let find = |mut x: u32| -> u32 {
            loop {
                let p = par_at(x as usize).load(Relaxed);
                if p == 0 {
                    return x;
                }
                x = p - 1;
            }
        };
        edges.par_iter().for_each(|v| {
            for &(a, b) in v {
                loop {
                    let (mut ra, mut rb) = (find(a), find(b));
                    if ra == rb {
                        break;
                    }
                    if ra < rb {
                        std::mem::swap(&mut ra, &mut rb);
                    }
                    // link root ra under rb (smaller index)
                    if par_at(ra as usize).compare_exchange(0, rb + 1, Relaxed, Relaxed).is_ok() {
                        break;
                    }
                }
            }
        });
        log!("  lk uf");
        let nm = |i: u32| unsafe {
            let o = *(onp as *const u64).add(i as usize);
            sl((o >> 16) as usize, o as u16)
        };
        let relax_best = |u: u32| {
            let r = find(u);
            let a = best_at(r as usize);
            let mut b = a.load(Relaxed);
            loop {
                if b != 0 && (b - 1 == u || nm(b - 1) <= nm(u)) {
                    return;
                }
                match a.compare_exchange(b, u + 1, Relaxed, Relaxed) {
                    Ok(_) => return,
                    Err(x) => b = x,
                }
            }
        };
        edges.par_iter().for_each(|v| {
            for &(a, b) in v {
                relax_best(a);
                relax_best(b);
            }
        });
        let set_person = |u: u32| {
            let r = find(u);
            let bs = best_at(r as usize).load(Relaxed) - 1;
            best_at(u as usize).store(bs + 1, Relaxed); // parents (low halves) stay intact for other finds
        };
        edges.par_iter().for_each(|v| {
            for &(a, b) in v {
                set_person(a);
                set_person(b);
            }
        });
        let used: usize = slots.par_chunks(1 << 16).map(|c| c.iter().filter(|s| s.key != 0).count()).sum();
        let sp = slots.as_mut_ptr() as usize;
        LinkTable { cap, slots, name: oname, sp, np: onp, used: used.into(), lim: cap / 100 * std::env::var("SS_LIMPCT").ok().and_then(|x| x.parse().ok()).unwrap_or(87), full: false.into() }
    };
    log!("links done");

    // ---- events: parse, resolve person, write kept-candidates by person shard and dedup keys by eid shard
    let ev_chunks = chunks(&ev_maps, 8 << 20);
    if let Ok(k) = std::env::var("SS_PB") {
        let k: u32 = k.parse().unwrap();
        let t = Instant::now();
        let mut acc = 0u64;
        let mut cnt = 0u64;
        let copies: Vec<Vec<u8>> = ev_chunks.iter().take(2).map(|b| b.to_vec()).collect();
        let use_copy = std::env::var("SS_PBC").is_ok();
        let mut pscr = ScanScr::default();
        let t = Instant::now();
        for (bi, mbuf) in ev_chunks.iter().take(2).cycle().take(40).enumerate() {
            let buf: &[u8] = if use_copy { &copies[bi % 2] } else { mbuf };
            if k == 0 {
                let mut o = 0;
                while o + 64 <= buf.len() {
                    acc += block_mask(buf, o).count_ones() as u64;
                    o += 64;
                }
                continue;
            }
            let cb = |f: &[&[u8]; 7]| {
                cnt += 1;
                if k % 10 >= 2 {
                    acc = acc.wrapping_add(parse_ts(f[3]).unwrap_or(0) as u64);
                    acc = acc.wrapping_add(parse_ts(f[2]).unwrap_or(0) as u64);
                }
                if k % 10 >= 3 {
                    acc ^= hb(f[0]) ^ hb(f[1]);
                }
                acc ^= (f[4].len() + f[5].len() + f[6].len()) as u64;
            };
            if k >= 30 { acc += stage1(buf, &mut pscr) as u64; continue; }
            if k >= 20 { scan_idx::<7>(buf, &mut pscr, cb) } else if k >= 10 { scan_fast::<7>(buf, cb) } else { scan::<7>(buf, cb) }
        }
        let bytes: usize = ev_chunks.iter().take(2).cycle().take(40).map(|b| b.len()).sum();
        eprintln!("k={} lines={} {:?} {:.1} ns/line {:.2} GB/s ({})", k, cnt, t.elapsed(), t.elapsed().as_nanos() as f64 / cnt.max(1) as f64, bytes as f64 / t.elapsed().as_secs_f64() / 1e9, acc);
        std::process::exit(0);
    }
    let ev_chunks = chunks(&ev_maps, (8 << 20) - (1 << 16));
    let nthr = rayon::current_num_threads() + 1;
    let ev_bytes: usize = ev_chunks.iter().map(|b| b.len()).sum();
    // Room for IDs seen only in events: about events / 20 (people are about events / 30, many linked).
    let mut extra = ev_bytes / 1600 + 4096;
    if let Some(x) = std::env::var("SS_EXTRA").ok().and_then(|x| x.parse().ok()) {
        extra = x; // testing the redo path
    }
    #[allow(clippy::type_complexity)]
    let (link_tab, ke_arena, dd_arena, parsed) = loop {
    let link_tab = links_job(extra);
    log!("links");
    let ke_arena = Arena::new(ev_bytes / 30 * (std::mem::size_of::<Ke>() + 1) + (8 << 20));
    let dd_arena = Arena::new(ev_bytes / 30 * 16 + (8 << 20)); // 16: clicks reuse it after dedup
    let narrow = std::env::var("SS_IGSIDE").is_ok(); // testing: every ingest via the side lists
    let st_scr: Vec<Mutex<Vec<Ev>>> = (0..nthr).map(|_| Mutex::new(Vec::new())).collect();
    #[allow(clippy::type_complexity)]
    let parsed: Vec<(Parts<Ke, { NP + 1 }>, Parts<u32, { NP + 1 }>, Parts<Dd, { NS + 1 }>, Vec<(u32, i32)>)> = ev_chunks
        .par_iter()
        .map(|buf| {
            let tid = rayon::current_thread_index().unwrap_or(nthr - 1);
            let mut stg = st_scr[tid].lock().unwrap();
            let stg = &mut *stg;
            stg.clear();
            stg.reserve(buf.len() / 32 + 64);
            let mut sbase = stg.as_mut_ptr();
            let mut scap = stg.capacity();
            let mut sn = 0usize;
            let tp0 = Instant::now();
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
                let tf = f[4];
                let typ = match tf.len() {
                    4 => {
                        let w = u32::from_le_bytes([tf[0], tf[1], tf[2], tf[3]]);
                        if w == u32::from_le_bytes(*b"view") {
                            0u64
                        } else if w == u32::from_le_bytes(*b"cart") {
                            1
                        } else {
                            9
                        }
                    }
                    8 => {
                        if u64::from_le_bytes(tf.try_into().unwrap()) == u64::from_le_bytes(*b"purchase") {
                            2
                        } else {
                            9
                        }
                    }
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
                if sn == scap {
                    unsafe { stg.set_len(sn) };
                    stg.reserve(sn);
                    sbase = stg.as_mut_ptr();
                    scap = stg.capacity();
                }
                unsafe { sbase.add(sn).write(e) };
                sn += 1;
            });
            unsafe { stg.set_len(sn) };
            let n = stg.len();
            let tp1 = Instant::now();
            const PF: usize = 32;
            for i in 0..n.min(PF) {
                link_tab.prefetch2(stg[i].person);
            }
            let mut kc = [0u32; NP + 1];
            let mut pc = [0u32; NP + 1];
            let mut dc = [0u32; NS + 1];
            for i in 0..n {
                if i + PF < n {
                    link_tab.prefetch2(stg[i + PF].person);
                }
                let e = &mut stg[i];
                let typ = pl_typ(e.payload);
                if typ != 3 {
                    let pi = link_tab.resolve(e.person, e.name, e.nlen) as u64;
                    e.person = pi;
                    kc[(pi as usize & (NP - 1)) + 1] += 1;
                    pc[(pi as usize & (NP - 1)) + 1] += (typ == 2) as u32;
                }
                dc[eshard(e.eid) + 1] += 1;
            }
            for i in 0..NP {
                kc[i + 1] += kc[i];
                pc[i + 1] += pc[i];
            }
            for i in 0..NS {
                dc[i + 1] += dc[i];
            }
            let (nk, nd) = (kc[NP] as usize, dc[NS] as usize);
            let koff = kc;
            let poff = pc;
            let doff = dc;
            let tp2 = Instant::now();
            let kq: *mut Ke = ke_arena.alloc(nk);
            let pq: *mut u32 = ke_arena.alloc(pc[NP] as usize);
            let dq: *mut Dd = dd_arena.alloc(nd);
            assert!(nk < DD_INV as usize);
            let mut side: Vec<(u32, i32)> = Vec::new();
            unsafe {
                for e in stg.iter() {
                    let mut loc = DD_INV;
                    let typ = pl_typ(e.payload);
                    if typ != 3 {
                        let ps = e.person as usize & (NP - 1);
                        loc = kc[ps];
                        kc[ps] += 1;
                        let pur = (typ == 2) as u64;
                        kq.add(loc as usize).write((e.person << 32) | ((pl_ts(e.payload) as u64) << 1) | pur);
                        if pur != 0 {
                            pq.add(pc[ps] as usize).write(pl_price(e.payload) as u32 | ((pl_cat(e.payload) as u32) << 24));
                            pc[ps] += 1;
                        }
                    }
                    let es = eshard(e.eid);
                    let ig = ig_pack(e.ingest, narrow);
                    if ig == 0 || ig == IG_HI {
                        side.push((dc[es], e.ingest));
                    }
                    dq.add(dc[es] as usize).write(Dd::new(e.eid, ig, loc));
                    dc[es] += 1;
                }
            }
            side.sort_unstable_by_key(|x| x.0);
            let tp3 = Instant::now();
            PT[0].fetch_add((tp1 - tp0).as_nanos() as u64, std::sync::atomic::Ordering::Relaxed);
            PT[1].fetch_add((tp2 - tp1).as_nanos() as u64, std::sync::atomic::Ordering::Relaxed);
            PT[2].fetch_add((tp3 - tp2).as_nanos() as u64, std::sync::atomic::Ordering::Relaxed);
            (Parts { ptr: kq, off: koff }, Parts { ptr: pq, off: poff }, Parts { ptr: dq, off: doff }, side)
        })
        .collect();
    log!("parse events: {} chunks  scan {:?} resolve {:?} write {:?}", ev_chunks.len(), std::time::Duration::from_nanos(PT[0].load(std::sync::atomic::Ordering::Relaxed)), std::time::Duration::from_nanos(PT[1].load(std::sync::atomic::Ordering::Relaxed)), std::time::Duration::from_nanos(PT[2].load(std::sync::atomic::Ordering::Relaxed)));
    log!("ids {} of {} slots, full {}", link_tab.used.load(std::sync::atomic::Ordering::Relaxed), link_tab.cap, link_tab.full.load(std::sync::atomic::Ordering::Relaxed));
    if !link_tab.full.load(std::sync::atomic::Ordering::Relaxed) {
        break (link_tab, ke_arena, dd_arena, parsed);
    }
    // More distinct user IDs than estimated (never on generator data): redo with a bigger table.
    extra = extra * 4 + (1 << 20);
    for x in PT.iter() {
        x.store(0, std::sync::atomic::Ordering::Relaxed);
    }
    };

    // ---- dedup per event shard: mark losing copies dead (typ 3) in place
    let mut kept_parts: Vec<Parts<Ke, { NP + 1 }>> = Vec::with_capacity(parsed.len());
    let mut pur_parts: Vec<Parts<u32, { NP + 1 }>> = Vec::with_capacity(parsed.len());
    let mut dd_parts: Vec<Parts<Dd, { NS + 1 }>> = Vec::with_capacity(parsed.len());
    let mut sides: Vec<Vec<(u32, i32)>> = Vec::with_capacity(parsed.len());
    for (k, q, d, sd) in parsed {
        kept_parts.push(k);
        pur_parts.push(q);
        dd_parts.push(d);
        sides.push(sd);
    }
    let kptr: Vec<usize> = kept_parts.iter().map(|c| c.ptr as usize).collect();
    {
        let dd_scr: Vec<Mutex<Vec<(u64, u32, u32)>>> = (0..nthr).map(|_| Mutex::new(Vec::new())).collect();
        let kptr = &kptr;
        let dd_parts = &dd_parts;
        let sides = &sides;
        // exact ingest of copy ii of shard s in chunk ci
        let exact = |ci: usize, s: usize, ii: usize, d: &Dd| -> i64 {
            let g = d.ig();
            if g != 0 && g != IG_HI {
                return g as i64 - IG_OFF;
            }
            let pos = dd_parts[ci].off[s] + ii as u32;
            let sd = &sides[ci];
            sd[sd.binary_search_by_key(&pos, |x| x.0).unwrap()].1 as i64
        };
        let kill = |ci: usize, d: &Dd| {
            let loc = d.loc();
            if loc != DD_INV {
                unsafe {
                    let k = (kptr[ci] as *mut Ke).add(loc as usize);
                    *k |= KE_DEAD;
                }
            }
        };
        (0..NS).into_par_iter().for_each(|s| {
            let n: usize = dd_parts.iter().map(|c| c[s].len()).sum();
            let cap = (n + n / 2 + 16).next_power_of_two();
            let mask = cap - 1;
            let tid = rayon::current_thread_index().unwrap_or(nthr - 1);
            let mut tab = dd_scr[tid].lock().unwrap();
            tab.clear();
            tab.resize(cap, (0, 0, 0));
            for (ci, c) in dd_parts.iter().enumerate() {
                for (ii, d) in c[s].iter().enumerate() {
                    let key = d.key();
                    let mut h = (key as usize) & mask;
                    loop {
                        let sl_ = unsafe { tab.get_unchecked_mut(h) };
                        if sl_.0 == 0 {
                            *sl_ = (key, ci as u32, ii as u32);
                            break;
                        }
                        if sl_.0 == key {
                            let bc = sl_.1 as usize;
                            let bi = sl_.2 as usize;
                            let b = &dd_parts[bc][s][bi];
                            let (gd, gb) = (d.ig(), b.ig());
                            let less = if gd != 0 && gd != IG_HI && gb != 0 && gb != IG_HI { gd < gb } else { exact(ci, s, ii, d) < exact(bc, s, bi, b) };
                            if less {
                                kill(bc, b);
                                sl_.1 = ci as u32;
                                sl_.2 = ii as u32;
                            } else {
                                kill(ci, d);
                            }
                            break;
                        }
                        h = (h + 1) & mask;
                    }
                }
            }
        });
    }
    drop(dd_parts);
    let dd_hw = dd_arena.off.load(std::sync::atomic::Ordering::Relaxed).min(dd_arena.cap); // touched extent
    dd_arena.reset(); // reuse the already-touched dedup memory for clicks
    log!("dedup");

    // ---- clicks
    let ck_chunks = chunks(&ck_maps, 3 << 20);
    let ck_scr: Vec<Mutex<Vec<Ck>>> = (0..nthr).map(|_| Mutex::new(Vec::new())).collect();
    let ck_parts: Vec<Parts<Ck, { NP + 1 }>> = ck_chunks
        .par_iter()
        .map(|buf| {
            let tid = rayon::current_thread_index().unwrap_or(nthr - 1);
            let mut rawg = ck_scr[tid].lock().unwrap();
            let raw = &mut *rawg;
            raw.clear();
            raw.reserve(buf.len() / 24 + 64);
            let mut rbase = raw.as_mut_ptr();
            let mut rcap = raw.capacity();
            let mut rn = 0usize;
            let tc0 = Instant::now();
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
                if rn == rcap {
                    unsafe { raw.set_len(rn) };
                    raw.reserve(rn);
                    rbase = raw.as_mut_ptr();
                    rcap = raw.capacity();
                }
                unsafe { rbase.add(rn).write(Ck { person: hb(user), ts: (t - cws) as u32, camp: ci, clen: cid.len() as u16, cid: cid.as_ptr() as usize }) };
                rn += 1;
            });
            unsafe { raw.set_len(rn) };
            PT[3].fetch_add(tc0.elapsed().as_nanos() as u64, std::sync::atomic::Ordering::Relaxed);
            const PF: usize = 32;
            for i in 0..raw.len().min(PF) {
                link_tab.prefetch(raw[i].person);
            }
            let mut kc = [0u32; NP + 1];
            for i in 0..raw.len() {
                if i + PF < raw.len() {
                    link_tab.prefetch(raw[i + PF].person);
                }
                let c = &mut raw[i];
                // An ID in no link and no kept event has no sessions to credit.
                c.person = match link_tab.find(c.person) {
                    Some(h) => link_tab.pidx_at(h) as u64,
                    None => u64::MAX,
                };
                if c.person != u64::MAX {
                    kc[(c.person as usize & (NP - 1)) + 1] += 1;
                }
            }
            for i in 0..NP {
                kc[i + 1] += kc[i];
            }
            let off = kc;
            let n = kc[NP] as usize;
            let q: *mut Ck = dd_arena.alloc(n);
            unsafe {
                for c in raw.iter() {
                    if c.person == u64::MAX {
                        continue;
                    }
                    let ps = c.person as usize & (NP - 1);
                    q.add(kc[ps] as usize).write(*c);
                    kc[ps] += 1;
                }
            }
            Parts { ptr: q, off }
        })
        .collect();
    log!("clicks: scan {:?}", std::time::Duration::from_nanos(PT[3].load(std::sync::atomic::Ordering::Relaxed)));

    // ---- per person shard: sort, sessionize, attribute
    let ws_day = ws.div_euclid(DAY_MS);
    let ndays = ((we - 1).div_euclid(DAY_MS) - ws_day + 1) as usize;
    let ncat = cat_names.len();
    let nthreads = rayon::current_num_threads();
    let camp_accs: Vec<Mutex<Vec<[u64; 4]>>> = (0..nthreads + 1).map(|_| Mutex::new(Vec::new())).collect();

    for x in PT.iter() {
        x.store(0, std::sync::atomic::Ordering::Relaxed);
    }
    let ss_scr: Vec<Mutex<(Vec<u32>, Vec<KeS>, Vec<Ck>)>> = (0..nthreads + 1).map(|_| Mutex::new((Vec::new(), Vec::new(), Vec::new()))).collect();
    let rows_scr: Vec<Mutex<Vec<URow>>> = (0..nthreads + 1).map(|_| Mutex::new(Vec::new())).collect();
    // Per-thread sort scratch carved from the dedup memory that is already touched (fresh pages are costly);
    // each region fits the largest shard if there is room, else shards that do not fit use the Vec scratch.
    let scr_need = |p: usize| -> usize {
        let n: usize = kept_parts.iter().map(|c| c[p].len()).sum();
        let nc: usize = ck_parts.iter().map(|c| c[p].len()).sum();
        n * std::mem::size_of::<KeS>() + nc * std::mem::size_of::<Ck>() + 64
    };
    let max_need = (0..NP).into_par_iter().map(scr_need).max().unwrap_or(0);
    let warm_left = dd_hw.saturating_sub(dd_arena.off.load(std::sync::atomic::Ordering::Relaxed));
    let region = max_need.min(warm_left / 2 / (nthreads + 1)) & !63;
    let regions: Vec<std::sync::atomic::AtomicUsize> = (0..nthreads + 1).map(|_| std::sync::atomic::AtomicUsize::new(0)).collect();
    let user_rows: Vec<&[URow]> = (0..NP)
        .into_par_iter()
        .map(|p| {
            let tid = rayon::current_thread_index().unwrap_or(nthreads);
            let ts0 = Instant::now();
            let mut scr = ss_scr[tid].lock().unwrap();
            let (cnt, ev, cl) = &mut *scr;
            // counting sort by 16 person-hash bits, then small sorts per bucket
            const BB: usize = 1 << 14;
            #[inline(always)]
            fn bkt(pi: u64) -> usize {
                ((pi >> 8) as usize) & (BB - 1)
            }
            cnt.clear();
            cnt.resize(BB + 1, 0);
            let nc: usize = ck_parts.iter().map(|c| c[p].len()).sum();
            let mut n = 0usize;
            for c in &kept_parts {
                for &k in &c[p] {
                    let live = !ke_dead(k) as u32;
                    cnt[bkt(k >> 32) + 1] += live;
                    n += live as usize;
                }
            }
            for b in 0..BB {
                cnt[b + 1] += cnt[b];
            }
            let bstart = cnt.clone();
            let need = n * std::mem::size_of::<KeS>() + nc * std::mem::size_of::<Ck>() + 64;
            let (evp, clp): (*mut KeS, *mut Ck) = if region > 0 && need <= region {
                let mut r = regions[tid].load(std::sync::atomic::Ordering::Relaxed);
                if r == 0 {
                    r = dd_arena.alloc::<u8>(region) as usize;
                    regions[tid].store(r, std::sync::atomic::Ordering::Relaxed);
                }
                (r as *mut KeS, ((r + n * std::mem::size_of::<KeS>() + 63) & !63) as *mut Ck)
            } else {
                ev.clear();
                ev.reserve(n);
                cl.clear();
                cl.reserve(nc);
                (ev.as_mut_ptr(), cl.as_mut_ptr())
            };
            {
                let ptr = evp;
                for (ci, c) in kept_parts.iter().enumerate() {
                    let pp = &pur_parts[ci][p];
                    let mut pi = 0usize;
                    for &k in &c[p] {
                        let pv = if k & 1 != 0 {
                            pi += 1;
                            pp[pi - 1]
                        } else {
                            0
                        };
                        if ke_dead(k) {
                            continue;
                        }
                        let b = bkt(k >> 32);
                        unsafe { ptr.add(cnt[b] as usize).write(KeS { key: k, pv }) };
                        cnt[b] += 1;
                    }
                }
            }
            let ev: &mut [KeS] = unsafe { std::slice::from_raw_parts_mut(evp, n) };
            let ts1 = Instant::now();
            PT[0].fetch_add((ts1 - ts0).as_nanos() as u64, std::sync::atomic::Ordering::Relaxed);
            for b in 0..BB {
                let (s0, s1) = (bstart[b] as usize, bstart[b + 1] as usize);
                if s1 - s0 > 1 {
                    let v = &mut ev[s0..s1];
                    if v.len() <= 32 {
                        // insertion sort by (person, ts)
                        for i in 1..v.len() {
                            let x = v[i];
                            let mut j = i;
                            while j > 0 && v[j - 1].key > x.key {
                                v[j] = v[j - 1];
                                j -= 1;
                            }
                            v[j] = x;
                        }
                    } else {
                        v.sort_unstable_by_key(|e| e.key);
                    }
                }
            }
            let ts2 = Instant::now();
            let cl: &mut [Ck] = unsafe {
                let mut o = 0usize;
                for c in &ck_parts {
                    let x = &c[p];
                    std::ptr::copy_nonoverlapping(x.as_ptr(), clp.add(o), x.len());
                    o += x.len();
                }
                std::slice::from_raw_parts_mut(clp, nc)
            };
            cl.sort_unstable_by_key(|c| ((bkt(c.person) as u128) << 96) | ((c.person as u128) << 32) | c.ts as u128);
            let pk = |h: u64| ((bkt(h) as u128) << 64) | h as u128;
            let ts3 = Instant::now();
            PT[1].fetch_add((ts2 - ts1).as_nanos() as u64, std::sync::atomic::Ordering::Relaxed);
            PT[2].fetch_add((ts3 - ts2).as_nanos() as u64, std::sync::atomic::Ordering::Relaxed);

            let mut accg = camp_accs[tid].lock().unwrap();
            if accg.is_empty() {
                accg.resize((ncamp + 1) * ndays, [0; 4]);
            }
            let acc = &mut *accg;
            let mut rowsg = rows_scr[tid].lock().unwrap();
            let rows = &mut *rowsg;
            rows.clear();
            let mut catsum = vec![0u64; ncat];
            let mut touched: Vec<u8> = Vec::with_capacity(ncat);
            let mut ccount = vec![0u32; ncamp];
            let mut present: Vec<u16> = Vec::new();
            let mut ppos = vec![0u32; ncamp];
            let mut cp = 0usize;
            let mut i = 0usize;
            while i < ev.len() {
                let person = ev[i].key >> 32;
                let mut j = i + 1;
                while j < ev.len() && ev[j].key >> 32 == person {
                    j += 1;
                }
                if j + 24 < ev.len() {
                    link_tab.prefetch_name(ev[j + 24].key >> 32);
                }
                let (name, nlen) = link_tab.name_of(person);
                while cp < cl.len() && pk(cl[cp].person) < pk(person) {
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
                    let start = ke_ts(ev[k].key);
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
                        let pld = ev[k].key;
                        let t = ke_ts(pld);
                        if t - last > GAP_MS {
                            break;
                        }
                        last = t;
                        cnt += 1;
                        if pld & 1 != 0 {
                            let price = (ev[k].pv & 0xff_ffff) as u64;
                            let cat = (ev[k].pv >> 24) as usize;
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
            let q: *mut URow = dd_arena.alloc(rows.len());
            unsafe {
                std::ptr::copy_nonoverlapping(rows.as_ptr(), q, rows.len());
                std::slice::from_raw_parts(q as *const URow, rows.len())
            }
        })
        .collect();
    log!("sessions: gather {:?} sort {:?} clicks {:?}", std::time::Duration::from_nanos(PT[0].load(std::sync::atomic::Ordering::Relaxed)), std::time::Duration::from_nanos(PT[1].load(std::sync::atomic::Ordering::Relaxed)), std::time::Duration::from_nanos(PT[2].load(std::sync::atomic::Ordering::Relaxed)));

    let mut camp = vec![[0u64; 4]; (ncamp + 1) * ndays];
    for m in &camp_accs {
        let g = m.lock().unwrap();
        for (a, b) in camp.iter_mut().zip(g.iter()) {
            for x in 0..4 {
                a[x] += b[x];
            }
        }
    }
    log!("camp merge");
    if let Some(k) = std::env::var("SS_WREP").ok().and_then(|x| x.parse::<usize>().ok()) {
        for i in 0..k {
            let t = Instant::now();
            let c0 = proc_cpu();
            write_outputs(&out.join(format!("rep{i}")), &user_rows, &cat_names, &camp, &camp_ids, &camp_chan, ndays, ws_day);
            eprintln!("write rep {i}: {:.1} ms wall {:.3} cpu", t.elapsed().as_secs_f64() * 1e3, proc_cpu() - c0);
        }
    }
    write_outputs(&out, &user_rows, &cat_names, &camp, &camp_ids, &camp_chan, ndays, ws_day);
    log!("done");
    if verbose {
        let st = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
        for l in st.lines() {
            if l.starts_with("VmHWM") || l.starts_with("RssAnon") || l.starts_with("RssFile") {
                eprintln!("{}", l);
            }
        }
    }
    // Outputs are closed. Release the big mappings in parallel (madvise holds the mmap lock only for
    // reading) so the exit-time teardown, which runs on one core, has little left to do.
    if std::env::var("SS_NOREL").is_err() {
        let mut regs: Vec<(usize, usize)> = Vec::new();
        for m in ev_maps.iter().chain(ck_maps.iter()).chain(lk_maps.iter()) {
            regs.push((m.as_ptr() as usize, m.len()));
        }
        for a in [&ke_arena, &dd_arena] {
            if a.base != 0 {
                regs.push((a.base, a.off.load(std::sync::atomic::Ordering::Relaxed).min(a.cap)));
            }
        }
        regs.push((link_tab.slots.as_ptr() as usize, link_tab.slots.len() * std::mem::size_of::<LSlot>()));
        regs.push((link_tab.name.as_ptr() as usize, link_tab.name.len() * 8));
        let mut pieces: Vec<(usize, usize)> = Vec::new();
        for (p, l) in regs {
            let (s0, e0) = ((p + 4095) & !4095, (p + l) & !4095);
            let mut o = s0;
            while o < e0 {
                let e = (o + (16 << 20)).min(e0);
                pieces.push((o, e - o));
                o = e;
            }
        }
        pieces.par_iter().for_each(|&(p, l)| unsafe {
            madvise(p as *mut std::ffi::c_void, l, 4 /* MADV_DONTNEED */);
        });
        log!("released");
    }
    std::process::exit(0);
}

unsafe extern "C" {
    fn madvise(addr: *mut std::ffi::c_void, len: usize, advice: i32) -> i32;
}

#[allow(clippy::too_many_arguments)]
fn write_outputs(out: &Path, user_rows: &[&[URow]], cat_names: &[Vec<u8>], camp: &[[u64; 4]], camp_ids: &[Vec<u8>], camp_chan: &[Vec<u8>], ndays: usize, ws_day: i64) {
    use arrow_array::builder::{Date32Builder, Int32Builder, Int64Builder, StringBuilder};
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
    let groups: Vec<&[&[URow]]> = user_rows.chunks(per).collect();
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
            // Small row groups, written as they fill: the builders and the writer's page
            // buffers stay a few MB instead of holding the whole file in fresh memory.
            const RG: usize = 1 << 14;
            let f = fs::File::create(ud.join(format!("part-{gi}.parquet"))).unwrap();
            let mut w: Option<ArrowWriter<fs::File>> = None;
            let mut left = n;
            let mut it = g.iter().flat_map(|p| p.iter());
            let mut f = Some(f);
            loop {
                let m = left.min(RG);
                let mut uid = StringBuilder::with_capacity(m, m * 16);
                let mut day = Date32Builder::with_capacity(m);
                let mut ses = Int32Builder::with_capacity(m);
                let mut evs = Int32Builder::with_capacity(m);
                let mut rev = Int64Builder::with_capacity(m);
                let mut top = StringBuilder::with_capacity(m, m * 8);
                for r in it.by_ref().take(m) {
                    uid.append_value(unsafe { std::str::from_utf8_unchecked(sl(r.0, r.1)) });
                    day.append_value(r.2);
                    ses.append_value(r.3 as i32);
                    evs.append_value(r.4 as i32);
                    rev.append_value(r.5 as i64);
                    if r.6 == u8::MAX {
                        top.append_null();
                    } else {
                        top.append_value(cat_strs[r.6 as usize]);
                    }
                }
                left -= m;
                let cols: Vec<(&str, ArrayRef)> = vec![
                    ("user_id", Arc::new(uid.finish())),
                    ("day", Arc::new(day.finish())),
                    ("sessions", Arc::new(ses.finish())),
                    ("events", Arc::new(evs.finish())),
                    ("revenue_cents", Arc::new(rev.finish())),
                    ("top_category", Arc::new(top.finish())),
                ];
                let batch = RecordBatch::try_from_iter(cols).unwrap();
                if w.is_none() {
                    let pr = WriterProperties::builder()
                        .set_compression(Compression::UNCOMPRESSED)
                        .set_dictionary_enabled(false)
                        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::None)
                        .set_max_row_group_size(RG)
                        // Compact encodings: rows of one person sit together (shared prefixes) and the
                        // integers are small, so the file (fresh page-cache memory) is far smaller.
                        ;
                    let enc: u32 = std::env::var("SS_ENC").ok().and_then(|x| x.parse().ok()).unwrap_or(0); // plain measured fastest
                    let pr = if enc >= 1 {
                        pr.set_column_encoding("day".into(), parquet::basic::Encoding::DELTA_BINARY_PACKED)
                            .set_column_encoding("sessions".into(), parquet::basic::Encoding::DELTA_BINARY_PACKED)
                            .set_column_encoding("events".into(), parquet::basic::Encoding::DELTA_BINARY_PACKED)
                            .set_column_encoding("revenue_cents".into(), parquet::basic::Encoding::DELTA_BINARY_PACKED)
                    } else {
                        pr
                    };
                    let pr = if enc >= 2 { pr.set_column_encoding("user_id".into(), parquet::basic::Encoding::DELTA_BYTE_ARRAY) } else { pr };
                    let pr = pr.build();
                    w = Some(ArrowWriter::try_new(f.take().unwrap(), batch.schema(), Some(pr)).unwrap());
                }
                let wr = w.as_mut().unwrap();
                wr.write(&batch).unwrap();
                wr.flush().unwrap();
                if left == 0 {
                    break;
                }
            }
            w.unwrap().close().unwrap();
        });
    });
}
