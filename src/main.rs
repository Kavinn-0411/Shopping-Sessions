//! Shopping Sessions pipeline, v2. See rules.md for the spec and plan.md for the roadmap.
//!
//! Data flow:
//!   links  -> parallel sort-intern -> union-find -> sharded map id-key -> person-key
//!   events -> parse (parallel chunks) -> DRec into eid buckets (dedup keys)
//!                                     -> PRec into person buckets (valid events only)
//!   clicks -> parse (parallel chunks) -> CRec into person buckets
//!   dedup per eid bucket (L2-sized open addressing) -> winner bitmap
//!   per person bucket: filter winners, group, sessionize, attribute
//!   parquet written in parallel
//!
//! IDs of at most 15 bytes are packed exactly into a u128 (bytes big-endian, length in the
//! low byte), so key order equals byte-wise string order. Longer IDs get a 120-bit hash with
//! low byte 0xFF and their bytes are kept in side tables.

use memchr::{memchr, memchr_iter};
use memmap2::Mmap;
use rayon::prelude::*;
use rustc_hash::FxHashMap;
use std::error::Error;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

const DAY_MS: i64 = 86_400_000;
const GAP_MS: i64 = 1_800_000;
const WEEK_MS: i64 = 7 * DAY_MS;
const LATE_MS: i64 = DAY_MS;
const EBITS: u32 = 10;
const NE: usize = 1 << EBITS; // eid buckets
const PBITS: u32 = 8;
const NP: usize = 1 << PBITS; // person buckets
const SBITS: u32 = 8;
const NS: usize = 1 << SBITS; // link map shards
const USER_FILES: usize = 16;

type B = &'static [u8];
type Res<T> = Result<T, Box<dyn Error + Send + Sync>>;

// ---------------------------------------------------------------- keys & hashing

#[inline]
fn fmix(mut h: u64) -> u64 {
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^= h >> 33;
    h
}

#[inline]
fn mix(k: u128) -> u64 {
    fmix((k as u64) ^ ((k >> 64) as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15))
}

fn long_key(s: &[u8]) -> u128 {
    let mut h1: u64 = 0x243F_6A88_85A3_08D3 ^ s.len() as u64;
    let mut h2: u64 = 0x1319_8A2E_0370_7344 ^ (s.len() as u64).rotate_left(17);
    for c in s.chunks(8) {
        let mut w = [0u8; 8];
        w[..c.len()].copy_from_slice(c);
        let w = u64::from_le_bytes(w);
        h1 = (h1 ^ w).wrapping_mul(0x9E37_79B9_7F4A_7C15).rotate_left(23);
        h2 = (h2 ^ w).wrapping_mul(0xC2B2_AE3D_27D4_EB4F).rotate_left(37);
    }
    let k = ((fmix(h1) as u128) << 64) | fmix(h2 ^ h1.rotate_left(7)) as u128;
    k | 0xFF
}

#[inline]
fn key_of(s: &[u8]) -> u128 {
    let n = s.len();
    if n <= 15 {
        let mut buf = [0u8; 16];
        buf[..n].copy_from_slice(s);
        buf[15] = n as u8;
        u128::from_be_bytes(buf)
    } else {
        long_key(s)
    }
}

#[inline]
fn is_long(k: u128) -> bool {
    (k as u8) == 0xFF
}

fn key_bytes(k: u128, long: &FxHashMap<u128, B>) -> Vec<u8> {
    if is_long(k) {
        long.get(&k).map(|s| s.to_vec()).unwrap_or_default()
    } else {
        let b = k.to_be_bytes();
        b[..b[15] as usize].to_vec()
    }
}

/// Exact lookup table keyed by byte strings, via packed keys (long keys verified by bytes).
struct KeyMap<V: Copy> {
    short: FxHashMap<u128, V>,
    long: FxHashMap<B, V>,
}

impl<V: Copy> KeyMap<V> {
    fn new() -> Self {
        KeyMap { short: FxHashMap::default(), long: FxHashMap::default() }
    }
    fn insert_if_absent(&mut self, s: B, v: V) {
        if s.len() <= 15 {
            self.short.entry(key_of(s)).or_insert(v);
        } else {
            self.long.entry(s).or_insert(v);
        }
    }
    #[inline]
    fn get(&self, s: &[u8]) -> Option<V> {
        if s.len() <= 15 {
            self.short.get(&key_of(s)).copied()
        } else {
            self.long.get(s).copied()
        }
    }
}

// ---------------------------------------------------------------- files & lines

fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut v = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let name = e.file_name();
            let Some(name) = name.to_str() else { continue };
            if !name.ends_with(".csv") || name.starts_with('.') {
                continue;
            }
            let p = e.path();
            if p.is_file() {
                v.push(p);
            }
        }
    }
    v.sort();
    v
}

fn map_file(p: &Path) -> Res<B> {
    let f = File::open(p)?;
    if f.metadata()?.len() == 0 {
        return Ok(&[]);
    }
    let m = unsafe { Mmap::map(&f)? };
    let m: &'static Mmap = Box::leak(Box::new(m));
    Ok(&m[..])
}

fn map_dir(dir: &Path) -> Res<Vec<B>> {
    let paths = list_csv(dir);
    paths.par_iter().map(|p| map_file(p)).collect()
}

#[derive(Clone, Copy)]
struct Chunk {
    data: B,
    skip_first: bool,
    /// First record id of this chunk (events only).
    base: u32,
}

fn make_chunks(files: &[B], target: usize) -> Vec<Chunk> {
    let mut out = Vec::new();
    for &f in files {
        let n = f.len();
        let mut start = 0;
        let mut first = true;
        while start < n {
            let mut end = (start + target).min(n);
            if end < n {
                end = match memchr(b'\n', &f[end..]) {
                    Some(i) => end + i + 1,
                    None => n,
                };
            }
            out.push(Chunk { data: &f[start..end], skip_first: first, base: 0 });
            first = false;
            start = end;
        }
    }
    out
}

fn chunk_target(files: &[B], ncpu: usize) -> usize {
    let total: usize = files.iter().map(|f| f.len()).sum();
    (total / (ncpu * 6).max(1)).clamp(1 << 20, 64 << 20)
}

#[inline]
fn for_each_line(c: &Chunk, mut f: impl FnMut(B)) {
    let d = c.data;
    let mut pos = 0;
    if c.skip_first {
        match memchr(b'\n', d) {
            Some(i) => pos = i + 1,
            None => return,
        }
    }
    while pos < d.len() {
        let end = match memchr(b'\n', &d[pos..]) {
            Some(i) => pos + i,
            None => d.len(),
        };
        f(&d[pos..end]);
        pos = end + 1;
    }
}

#[inline]
fn unquote(f: B) -> Option<B> {
    if memchr(b'"', f).is_none() {
        return Some(f);
    }
    let n = f.len();
    if n >= 2 && f[0] == b'"' && f[n - 1] == b'"' && memchr(b'"', &f[1..n - 1]).is_none() {
        Some(&f[1..n - 1])
    } else {
        None
    }
}

/// Split a line into exactly N fields, unquoting each; false if malformed.
#[inline]
fn split<const N: usize>(line: B, out: &mut [B; N]) -> bool {
    let mut n = 0;
    let mut start = 0;
    for i in memchr_iter(b',', line) {
        if n == N - 1 {
            return false;
        }
        match unquote(&line[start..i]) {
            Some(x) => out[n] = x,
            None => return false,
        }
        n += 1;
        start = i + 1;
    }
    if n != N - 1 {
        return false;
    }
    match unquote(&line[start..]) {
        Some(x) => out[n] = x,
        None => return false,
    }
    true
}

// ---------------------------------------------------------------- time

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[inline]
fn dig(c: u8) -> Option<i64> {
    if c.is_ascii_digit() {
        Some((c - b'0') as i64)
    } else {
        None
    }
}

#[inline]
fn d2(b: &[u8], i: usize) -> Option<i64> {
    Some(dig(b[i])? * 10 + dig(b[i + 1])?)
}

/// Parse ts in F1, F2 or F3 to epoch ms. None if bad (F3 overflow included).
#[inline]
fn parse_ts(b: &[u8]) -> Option<i64> {
    let n = b.len();
    if n == 0 {
        return None;
    }
    if n != 20 && n != 25 || b[4] != b'-' {
        if !b.iter().all(|c| c.is_ascii_digit()) {
            return None;
        }
        let mut v: u64 = 0;
        for &c in b {
            v = v.checked_mul(10)?.checked_add((c - b'0') as u64)?;
        }
        if v > i64::MAX as u64 {
            return None;
        }
        return Some(v as i64);
    }
    if b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let y = d2(b, 0)? * 100 + d2(b, 2)?;
    let mo = d2(b, 5)?;
    let d = d2(b, 8)?;
    let h = d2(b, 11)?;
    let mi = d2(b, 14)?;
    let s = d2(b, 17)?;
    if !(1..=12).contains(&mo) || h > 23 || mi > 59 || s > 59 || d < 1 {
        return None;
    }
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let dim = match mo {
        2 => {
            if leap {
                29
            } else {
                28
            }
        }
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if d > dim {
        return None;
    }
    let mut secs = days_from_civil(y, mo, d) * 86_400 + h * 3600 + mi * 60 + s;
    if n == 20 {
        if b[19] != b'Z' {
            return None;
        }
    } else {
        let sign = match b[19] {
            b'+' => 1,
            b'-' => -1,
            _ => return None,
        };
        let oh = d2(b, 20)?;
        if b[22] != b':' {
            return None;
        }
        let om = d2(b, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        secs -= sign * (oh * 3600 + om * 60);
    }
    Some(secs * 1000)
}

#[inline]
fn parse_price(b: &[u8]) -> Option<u32> {
    if b.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in b {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v * 10 + (c - b'0') as u64;
        if v > 10_000_000 {
            return None;
        }
    }
    if v == 0 {
        None
    } else {
        Some(v as u32)
    }
}

fn manifest_value(text: &str, key: &str) -> Option<i64> {
    let at = text.find(&format!("\"{key}\""))? + key.len() + 2;
    let rest = &text[at..];
    let colon = rest.find(':')?;
    let rest = &rest[colon + 1..];
    let q1 = rest.find('"')?;
    let rest = &rest[q1 + 1..];
    let q2 = rest.find('"')?;
    parse_ts(rest[..q2].as_bytes())
}

// ---------------------------------------------------------------- records

/// Dedup entry: one per parsed event line with a non-empty event_id.
#[derive(Clone, Copy)]
struct DRec {
    key: u128,
    ingest: i64,
    rec: u32,
}

/// A valid (validated, not late) event, routed by person.
#[derive(Clone, Copy)]
struct PRec {
    pkey: u128,
    ts: u32, // ms since window start
    price: u32,
    rec: u32,
    cat: u16,
    kind: u8,
}

/// A valid click, routed by person.
#[derive(Clone, Copy)]
struct CRec {
    pkey: u128,
    cid: B,
    ts: i64,
    camp: u32,
}

struct URow {
    pkey: u128,
    day: i32,
    sessions: u64,
    events: u64,
    revenue: u64,
    top: Option<u16>,
}

struct LinkMap {
    shards: Vec<FxHashMap<u128, u128>>,
}

impl LinkMap {
    #[inline]
    fn person(&self, k: u128) -> u128 {
        let s = (mix(k) >> (64 - SBITS)) as usize;
        self.shards[s].get(&k).copied().unwrap_or(k)
    }
}

struct Ctx {
    ws: i64,
    we: i64,
    product: KeyMap<u16>,
    unknown: u16,
    ncat: usize,
    cat_rank: Vec<u16>,
    camp: KeyMap<u32>,
    ncamp: usize,
    link: LinkMap,
    day0: i64,
    nd: usize,
}

fn load_lookup(path: &Path) -> Res<Vec<(B, B)>> {
    let data = map_file(path)?;
    let mut v = Vec::new();
    let c = Chunk { data, skip_first: true, base: 0 };
    let mut f: [B; 2] = [&[]; 2];
    for_each_line(&c, |line| {
        if split(line, &mut f) {
            v.push((f[0], f[1]));
        }
    });
    Ok(v)
}

#[inline]
fn find(parent: &mut [u32], mut x: u32) -> u32 {
    while parent[x as usize] != x {
        let p = parent[x as usize];
        parent[x as usize] = parent[p as usize];
        x = parent[x as usize];
    }
    x
}

/// Returns the id-key -> person-key map and long person names.
fn build_links(data: &Path, ncpu: usize) -> Res<(LinkMap, Vec<(u128, B)>)> {
    let files = map_dir(&data.join("links"))?;
    let chunks = make_chunks(&files, chunk_target(&files, ncpu).min(4 << 20));
    let pairs: Vec<Vec<(B, B)>> = chunks
        .par_iter()
        .map(|c| {
            let mut v = Vec::new();
            let mut f: [B; 2] = [&[]; 2];
            for_each_line(c, |line| {
                if split(line, &mut f) && !f[0].is_empty() && !f[1].is_empty() {
                    v.push((f[0], f[1]));
                }
            });
            v
        })
        .collect();
    let edges: Vec<(B, B)> = pairs.into_iter().flatten().collect();
    let ne = edges.len();
    // Intern by sorting (key, bytes, slot) in byte order; ids come out in byte order.
    let mut items: Vec<(u128, B, u32)> = edges
        .par_iter()
        .enumerate()
        .flat_map_iter(|(e, &(a, b))| {
            [(key_of(a), a, 2 * e as u32), (key_of(b), b, 2 * e as u32 + 1)]
        })
        .collect();
    let cmp = |x: &(u128, B, u32), y: &(u128, B, u32)| {
        if !is_long(x.0) && !is_long(y.0) {
            x.0.cmp(&y.0)
        } else {
            x.1.cmp(y.1)
        }
    };
    items.par_sort_unstable_by(cmp);
    let mut slot_id = vec![0u32; 2 * ne];
    let mut names: Vec<(u128, B)> = Vec::new();
    for (i, it) in items.iter().enumerate() {
        let new = i == 0 || {
            let p = &items[i - 1];
            if !is_long(p.0) && !is_long(it.0) {
                p.0 != it.0
            } else {
                p.1 != it.1
            }
        };
        if new {
            names.push((it.0, it.1));
        }
        slot_id[it.2 as usize] = (names.len() - 1) as u32;
    }
    drop(items);
    let n = names.len();
    let mut parent: Vec<u32> = (0..n as u32).collect();
    for e in 0..ne {
        let ra = find(&mut parent, slot_id[2 * e]);
        let rb = find(&mut parent, slot_id[2 * e + 1]);
        if ra != rb {
            // The smaller id is the byte-wise smaller name: it becomes the root.
            parent[ra.max(rb) as usize] = ra.min(rb);
        }
    }
    let mut by_shard: Vec<Vec<(u128, u128)>> = (0..NS).map(|_| Vec::new()).collect();
    let mut long_names = Vec::new();
    for i in 0..n as u32 {
        let r = find(&mut parent, i);
        let k = names[i as usize].0;
        if r != i {
            by_shard[(mix(k) >> (64 - SBITS)) as usize].push((k, names[r as usize].0));
        } else if is_long(k) {
            long_names.push((k, names[i as usize].1));
        }
    }
    let shards: Vec<FxHashMap<u128, u128>> = by_shard
        .into_par_iter()
        .map(|v| {
            let mut m = FxHashMap::default();
            m.reserve(v.len());
            m.extend(v);
            m
        })
        .collect();
    Ok((LinkMap { shards }, long_names))
}

// ---------------------------------------------------------------- main

fn main() {
    if let Err(e) = run() {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
    std::process::exit(0);
}

fn run() -> Res<()> {
    let t0 = Instant::now();
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));
    let ncpu: usize = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(8, |n| n.get()));
    rayon::ThreadPoolBuilder::new().num_threads(ncpu).build_global()?;

    let manifest = std::fs::read_to_string(data.join("manifest.json"))?;
    let ws = manifest_value(&manifest, "window_start").ok_or("bad manifest")?;
    let we = manifest_value(&manifest, "window_end").ok_or("bad manifest")?;
    if we - ws > u32::MAX as i64 || we < ws {
        return Err("window longer than 49 days is not supported".into());
    }

    // Map all inputs and build lookups concurrently with the links.
    let (links_res, (lookups, files)) = rayon::join(
        || build_links(&data, ncpu),
        || {
            rayon::join(
                || -> Res<_> {
                    Ok((
                        load_lookup(&data.join("products.csv"))?,
                        load_lookup(&data.join("campaigns.csv"))?,
                    ))
                },
                || -> Res<_> { Ok((map_dir(&data.join("events"))?, map_dir(&data.join("clicks"))?)) },
            )
        },
    );
    let (link, long_link) = links_res?;
    let (products, campaigns) = lookups?;
    let (ev_files, ck_files) = files?;
    eprintln!("[{:?}] links + lookups", t0.elapsed());

    // products / categories
    let mut cat_names: Vec<B> = Vec::new();
    let mut cat_idx: FxHashMap<B, u16> = FxHashMap::default();
    let mut product: KeyMap<u16> = KeyMap::new();
    for (p, c) in products {
        let ci = *cat_idx.entry(c).or_insert_with(|| {
            cat_names.push(c);
            (cat_names.len() - 1) as u16
        });
        product.insert_if_absent(p, ci);
    }
    let unknown = cat_names.len() as u16;
    cat_names.push(b"unknown");
    let ncat = cat_names.len();
    let mut order: Vec<usize> = (0..ncat).collect();
    order.sort_by(|&a, &b| cat_names[a].cmp(cat_names[b]));
    let mut cat_rank = vec![0u16; ncat];
    for (r, &i) in order.iter().enumerate() {
        cat_rank[i] = r as u16;
    }

    // campaigns
    let mut camp_ids: Vec<B> = Vec::new();
    let mut camp_ch: Vec<B> = Vec::new();
    let mut camp: KeyMap<u32> = KeyMap::new();
    for (id, ch) in campaigns {
        if camp.get(id).is_none() {
            camp.insert_if_absent(id, camp_ids.len() as u32);
            camp_ids.push(id);
            camp_ch.push(ch);
        }
    }
    let ncamp = camp_ids.len();

    let day0 = ws.div_euclid(DAY_MS);
    let nd = ((we - 1).div_euclid(DAY_MS) - day0 + 1).max(1) as usize;
    let ctx = Arc::new(Ctx {
        ws,
        we,
        product,
        unknown,
        ncat,
        cat_rank,
        camp,
        ncamp,
        link,
        day0,
        nd,
    });

    // ---- chunking; each event chunk gets a disjoint record-id range
    let mut ev_chunks = make_chunks(&ev_files, chunk_target(&ev_files, ncpu));
    let mut next: u64 = 0;
    for c in ev_chunks.iter_mut() {
        c.base = next as u32;
        // a record line holds at least 27 bytes plus its newline
        next += (c.data.len() / 20 + 2) as u64;
    }
    if next > u32::MAX as u64 {
        return Err("too many records".into());
    }
    let nrec = next as usize;
    let ck_chunks = make_chunks(&ck_files, chunk_target(&ck_files, ncpu));

    // ---- parse events and clicks
    let (parsed, clicks) = rayon::join(
        || ev_chunks.par_iter().map(|c| parse_events(c, &ctx)).collect::<Vec<_>>(),
        || ck_chunks.par_iter().map(|c| parse_clicks(c, &ctx)).collect::<Vec<_>>(),
    );
    eprintln!("[{:?}] parsed", t0.elapsed());

    // ---- dedup per eid bucket -> winner bitmap
    let winners: Vec<AtomicU64> = (0..nrec / 64 + 1).map(|_| AtomicU64::new(0)).collect();
    (0..NE).into_par_iter().for_each(|b| dedup_bucket(b, &parsed, &winners));
    eprintln!("[{:?}] dedup", t0.elapsed());

    // long names: from links and from unlinked long user ids
    let mut long: FxHashMap<u128, B> = FxHashMap::default();
    long.extend(long_link);
    for p in &parsed {
        long.extend(p.long.iter().copied());
    }

    // ---- sessionize + attribute per person bucket
    let results: Vec<(Vec<URow>, Vec<[u64; 4]>)> = (0..NP)
        .into_par_iter()
        .map(|b| person_bucket(b, &parsed, &clicks, &winners, &ctx))
        .collect();
    eprintln!("[{:?}] sessions", t0.elapsed());

    let mut camp_agg = vec![[0u64; 4]; (ncamp + 1) * nd];
    let mut user_parts: Vec<Vec<URow>> = Vec::with_capacity(NP);
    for (u, c) in results {
        user_parts.push(u);
        for (a, x) in camp_agg.iter_mut().zip(c.iter()) {
            for k in 0..4 {
                a[k] += x[k];
            }
        }
    }

    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    std::fs::create_dir_all(&ud)?;
    std::fs::create_dir_all(&cd)?;
    let per = NP / USER_FILES;
    let nuser: usize = user_parts.iter().map(|u| u.len()).sum();
    let (r1, r2) = rayon::join(
        || {
            (0..USER_FILES).into_par_iter().try_for_each(|f| {
                write_users(
                    &ud.join(format!("part-{f:03}.parquet")),
                    &user_parts[f * per..(f + 1) * per],
                    &cat_names,
                    &long,
                )
            })
        },
        || write_campaigns(&cd.join("part-0.parquet"), &camp_agg, &camp_ids, &camp_ch, day0, nd),
    );
    r1?;
    r2?;
    let hwm = std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("VmHWM")).map(|l| l.to_string()))
        .unwrap_or_default();
    eprintln!("[{:?}] done: {} user rows; {}", t0.elapsed(), nuser, hwm);
    Ok(())
}

struct EvOut {
    dedup: Vec<Vec<DRec>>,
    person: Vec<Vec<PRec>>,
    long: Vec<(u128, B)>,
}

fn parse_events(c: &Chunk, ctx: &Ctx) -> EvOut {
    let est = c.data.len() / 70 + 16;
    let mut dedup: Vec<Vec<DRec>> = (0..NE).map(|_| Vec::with_capacity(est / NE + est / NE / 4 + 4)).collect();
    let mut person: Vec<Vec<PRec>> = (0..NP).map(|_| Vec::with_capacity(est / NP + est / NP / 4 + 4)).collect();
    let mut long = Vec::new();
    let mut f: [B; 7] = [&[]; 7];
    let mut rec = c.base;
    for_each_line(c, |line| {
        if !split(line, &mut f) {
            return;
        }
        let eid = f[0];
        if eid.is_empty() {
            return;
        }
        let ingest = parse_ts(f[3]).unwrap_or(i64::MAX);
        let key = key_of(eid);
        dedup[(mix(key) >> (64 - EBITS)) as usize].push(DRec { key, ingest, rec });
        let r = rec;
        rec += 1;
        // validation
        let user = f[1];
        if user.is_empty() {
            return;
        }
        let Some(t) = parse_ts(f[2]) else { return };
        if t < ctx.ws || t >= ctx.we {
            return;
        }
        let kind = match f[4] {
            b"view" => 1,
            b"cart" => 2,
            b"purchase" => 3,
            _ => return,
        };
        if f[5].is_empty() {
            return;
        }
        let mut price = 0;
        if kind == 3 {
            match parse_price(f[6]) {
                Some(p) => price = p,
                None => return,
            }
        }
        if ingest.saturating_sub(t) > LATE_MS {
            return;
        }
        let cat = ctx.product.get(f[5]).unwrap_or(ctx.unknown);
        let ukey = key_of(user);
        let pkey = ctx.link.person(ukey);
        if pkey == ukey && is_long(ukey) {
            long.push((ukey, user));
        }
        person[(mix(pkey) >> (64 - PBITS)) as usize].push(PRec {
            pkey,
            ts: (t - ctx.ws) as u32,
            price,
            rec: r,
            cat,
            kind,
        });
    });
    EvOut { dedup, person, long }
}

fn parse_clicks(c: &Chunk, ctx: &Ctx) -> Vec<Vec<CRec>> {
    let est = c.data.len() / 40 + 16;
    let mut out: Vec<Vec<CRec>> = (0..NP).map(|_| Vec::with_capacity(est / NP + est / NP / 4 + 4)).collect();
    let mut f: [B; 4] = [&[]; 4];
    let lo = ctx.ws - WEEK_MS;
    for_each_line(c, |line| {
        if !split(line, &mut f) {
            return;
        }
        let (cid, user) = (f[0], f[1]);
        if cid.is_empty() || user.is_empty() {
            return;
        }
        let Some(ts) = parse_ts(f[3]) else { return };
        if ts < lo || ts >= ctx.we {
            return;
        }
        let Some(camp) = ctx.camp.get(f[2]) else { return };
        let pkey = ctx.link.person(key_of(user));
        out[(mix(pkey) >> (64 - PBITS)) as usize].push(CRec { pkey, cid, ts, camp });
    });
    out
}

fn dedup_bucket(b: usize, parsed: &[EvOut], winners: &[AtomicU64]) {
    let total: usize = parsed.iter().map(|p| p.dedup[b].len()).sum();
    if total == 0 {
        return;
    }
    let cap = (total * 2).next_power_of_two();
    let mask = cap - 1;
    let mut table: Vec<DRec> = vec![DRec { key: 0, ingest: 0, rec: 0 }; cap];
    for p in parsed {
        for d in &p.dedup[b] {
            let mut i = (mix(d.key) as usize) & mask;
            loop {
                let t = &mut table[i];
                if t.key == 0 {
                    *t = *d;
                    break;
                }
                if t.key == d.key {
                    if d.ingest < t.ingest {
                        t.ingest = d.ingest;
                        t.rec = d.rec;
                    }
                    break;
                }
                i = (i + 1) & mask;
            }
        }
    }
    for t in &table {
        if t.key != 0 {
            winners[(t.rec >> 6) as usize].fetch_or(1u64 << (t.rec & 63), Ordering::Relaxed);
        }
    }
}

#[inline]
fn is_winner(w: &[AtomicU64], rec: u32) -> bool {
    w[(rec >> 6) as usize].load(Ordering::Relaxed) & (1u64 << (rec & 63)) != 0
}

fn person_bucket(
    b: usize,
    parsed: &[EvOut],
    clicks: &[Vec<Vec<CRec>>],
    winners: &[AtomicU64],
    ctx: &Ctx,
) -> (Vec<URow>, Vec<[u64; 4]>) {
    let nd = ctx.nd;
    let organic = ctx.ncamp;
    let mut camp_agg = vec![[0u64; 4]; (ctx.ncamp + 1) * nd];
    let mut users: Vec<URow> = Vec::new();

    // gather surviving events, assign local person ids
    let total: usize = parsed.iter().map(|p| p.person[b].len()).sum();
    let mut pid_of: FxHashMap<u128, u32> = FxHashMap::default();
    let mut pkeys: Vec<u128> = Vec::new();
    let mut tmp: Vec<(u32, PRec)> = Vec::with_capacity(total);
    for p in parsed {
        for e in &p.person[b] {
            if !is_winner(winners, e.rec) {
                continue;
            }
            let id = *pid_of.entry(e.pkey).or_insert_with(|| {
                pkeys.push(e.pkey);
                (pkeys.len() - 1) as u32
            });
            tmp.push((id, *e));
        }
    }
    let np = pkeys.len();
    // counting sort by person
    let mut start = vec![0u32; np + 1];
    for (id, _) in &tmp {
        start[*id as usize + 1] += 1;
    }
    for i in 0..np {
        start[i + 1] += start[i];
    }
    let mut fill = start.clone();
    let mut evs: Vec<(u32, u32, u32, u16, u8)> = vec![(0, 0, 0, 0, 0); tmp.len()]; // ts, price, -, cat, kind
    for (id, e) in &tmp {
        let at = &mut fill[*id as usize];
        evs[*at as usize] = (e.ts, e.price, 0, e.cat, e.kind);
        *at += 1;
    }
    drop(tmp);

    // clicks of known persons, grouped the same way
    let mut ctmp: Vec<(u32, i64, B, u32)> = Vec::new();
    for c in clicks {
        for k in &c[b] {
            if let Some(&id) = pid_of.get(&k.pkey) {
                ctmp.push((id, k.ts, k.cid, k.camp));
            }
        }
    }
    let mut cstart = vec![0u32; np + 1];
    for c in &ctmp {
        cstart[c.0 as usize + 1] += 1;
    }
    for i in 0..np {
        cstart[i + 1] += cstart[i];
    }
    let mut cfill = cstart.clone();
    let mut cks: Vec<(i64, B, u32)> = vec![(0, &[], 0); ctmp.len()];
    for c in &ctmp {
        let at = &mut cfill[c.0 as usize];
        cks[*at as usize] = (c.1, c.2, c.3);
        *at += 1;
    }
    drop(ctmp);

    let mut cnt = vec![0u32; ctx.ncamp];
    let mut apos = vec![0u32; ctx.ncamp];
    let mut active: Vec<u32> = Vec::new();
    let mut catsum = vec![0u64; ctx.ncat];
    let mut touched: Vec<u16> = Vec::new();

    let add = |c: u32, cnt: &mut Vec<u32>, apos: &mut Vec<u32>, active: &mut Vec<u32>| {
        let c = c as usize;
        if cnt[c] == 0 {
            apos[c] = active.len() as u32;
            active.push(c as u32);
        }
        cnt[c] += 1;
    };
    let remove = |c: u32, cnt: &mut Vec<u32>, apos: &mut Vec<u32>, active: &mut Vec<u32>| {
        let c = c as usize;
        cnt[c] -= 1;
        if cnt[c] == 0 {
            let i = apos[c] as usize;
            active.swap_remove(i);
            if i < active.len() {
                apos[active[i] as usize] = i as u32;
            }
        }
    };

    for p in 0..np {
        let (i, j) = (start[p] as usize, start[p + 1] as usize);
        let ev = &mut evs[i..j];
        ev.sort_unstable_by_key(|e| e.0);
        let (ci, cj) = (cstart[p] as usize, cstart[p + 1] as usize);
        let ck = &mut cks[ci..cj];
        ck.sort_unstable_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));
        let pkey = pkeys[p];
        let mut lo = 0usize;
        let mut hi = 0usize;

        let mut cur_day: i64 = i64::MIN;
        let (mut us, mut ue, mut ur) = (0u64, 0u64, 0u64);
        let flush = |day: i64,
                     us: u64,
                     ue: u64,
                     ur: u64,
                     touched: &mut Vec<u16>,
                     catsum: &mut Vec<u64>,
                     users: &mut Vec<URow>| {
            let mut top: Option<u16> = None;
            for &c in touched.iter() {
                top = match top {
                    None => Some(c),
                    Some(t) => {
                        let (sc, st) = (catsum[c as usize], catsum[t as usize]);
                        if sc > st || (sc == st && ctx.cat_rank[c as usize] < ctx.cat_rank[t as usize]) {
                            Some(c)
                        } else {
                            Some(t)
                        }
                    }
                };
            }
            for &c in touched.iter() {
                catsum[c as usize] = 0;
            }
            touched.clear();
            users.push(URow { pkey, day: day as i32, sessions: us, events: ue, revenue: ur, top });
        };

        let n = ev.len();
        let mut k = 0;
        while k < n {
            let s_off = ev[k].0 as i64;
            let s = ctx.ws + s_off;
            let day = s.div_euclid(DAY_MS);
            if day != cur_day {
                if cur_day != i64::MIN {
                    flush(cur_day, us, ue, ur, &mut touched, &mut catsum, &mut users);
                }
                cur_day = day;
                us = 0;
                ue = 0;
                ur = 0;
            }
            let mut last = s_off;
            let (mut nev, mut rev, mut purch) = (0u64, 0u64, false);
            while k < n && ev[k].0 as i64 - last <= GAP_MS {
                let e = ev[k];
                last = e.0 as i64;
                nev += 1;
                if e.4 == 3 {
                    purch = true;
                    rev += e.1 as u64;
                    let c = e.3 as usize;
                    if catsum[c] == 0 {
                        touched.push(e.3);
                    }
                    catsum[c] += e.1 as u64;
                }
                k += 1;
            }
            us += 1;
            ue += nev;
            ur += rev;

            while hi < ck.len() && ck[hi].0 <= s {
                add(ck[hi].2, &mut cnt, &mut apos, &mut active);
                hi += 1;
            }
            while lo < hi && ck[lo].0 < s - WEEK_MS {
                remove(ck[lo].2, &mut cnt, &mut apos, &mut active);
                lo += 1;
            }
            let d = (day - ctx.day0) as usize;
            let credited = if lo < hi {
                let t = ck[hi - 1].0;
                let mut f = hi - 1;
                while f > lo && ck[f - 1].0 == t {
                    f -= 1;
                }
                ck[f].2 as usize
            } else {
                organic
            };
            let a = &mut camp_agg[credited * nd + d];
            a[0] += 1;
            if purch {
                a[1] += 1;
            }
            a[2] += rev;
            for &c in &active {
                camp_agg[c as usize * nd + d][3] += 1;
            }
        }
        if cur_day != i64::MIN {
            flush(cur_day, us, ue, ur, &mut touched, &mut catsum, &mut users);
        }
        while lo < hi {
            remove(ck[lo].2, &mut cnt, &mut apos, &mut active);
            lo += 1;
        }
    }
    (users, camp_agg)
}

// ---------------------------------------------------------------- parquet

use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;

fn props() -> Arc<WriterProperties> {
    Arc::new(
        WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::None)
            .build(),
    )
}

fn write_users(path: &Path, parts: &[Vec<URow>], cat_names: &[B], long: &FxHashMap<u128, B>) -> Res<()> {
    let rows: Vec<&URow> = parts.iter().flatten().collect();
    let schema = Arc::new(parse_message_type(
        "message user_daily {
            REQUIRED BYTE_ARRAY user_id (UTF8);
            REQUIRED INT32 day (DATE);
            REQUIRED INT64 sessions;
            REQUIRED INT64 events;
            REQUIRED INT64 revenue_cents;
            OPTIONAL BYTE_ARRAY top_category (UTF8);
        }",
    )?);
    let mut w = SerializedFileWriter::new(File::create(path)?, schema, props())?;
    let mut rg = w.next_row_group()?;
    let mut idx = 0;
    while let Some(mut col) = rg.next_column()? {
        match idx {
            0 => {
                let v: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(key_bytes(r.pkey, long))).collect();
                col.typed::<ByteArrayType>().write_batch(&v, None, None)?;
            }
            1 => {
                let v: Vec<i32> = rows.iter().map(|r| r.day).collect();
                col.typed::<Int32Type>().write_batch(&v, None, None)?;
            }
            2 | 3 | 4 => {
                let v: Vec<i64> = rows
                    .iter()
                    .map(|r| match idx {
                        2 => r.sessions,
                        3 => r.events,
                        _ => r.revenue,
                    } as i64)
                    .collect();
                col.typed::<Int64Type>().write_batch(&v, None, None)?;
            }
            _ => {
                let defs: Vec<i16> = rows.iter().map(|r| r.top.is_some() as i16).collect();
                let v: Vec<ByteArray> = rows
                    .iter()
                    .filter_map(|r| r.top.map(|c| ByteArray::from(cat_names[c as usize].to_vec())))
                    .collect();
                col.typed::<ByteArrayType>().write_batch(&v, Some(&defs), None)?;
            }
        }
        col.close()?;
        idx += 1;
    }
    rg.close()?;
    w.close()?;
    Ok(())
}

fn write_campaigns(
    path: &Path,
    agg: &[[u64; 4]],
    ids: &[B],
    ch: &[B],
    day0: i64,
    nd: usize,
) -> Res<()> {
    let mut rows: Vec<(B, B, i32, [u64; 4])> = Vec::new();
    for c in 0..=ids.len() {
        for d in 0..nd {
            let a = agg[c * nd + d];
            if a[0] > 0 || a[3] > 0 {
                let (id, chn): (B, B) = if c == ids.len() {
                    (b"organic", b"organic")
                } else {
                    (ids[c], ch[c])
                };
                let assisted = if c == ids.len() { 0 } else { a[3] };
                rows.push((id, chn, (day0 + d as i64) as i32, [a[0], a[1], a[2], assisted]));
            }
        }
    }
    let schema = Arc::new(parse_message_type(
        "message campaign_daily {
            REQUIRED BYTE_ARRAY campaign_id (UTF8);
            REQUIRED BYTE_ARRAY channel (UTF8);
            REQUIRED INT32 day (DATE);
            REQUIRED INT64 sessions;
            REQUIRED INT64 purchasing_sessions;
            REQUIRED INT64 revenue_cents;
            REQUIRED INT64 assisted_sessions;
        }",
    )?);
    let mut w = SerializedFileWriter::new(File::create(path)?, schema, props())?;
    let mut rg = w.next_row_group()?;
    let mut idx = 0;
    while let Some(mut col) = rg.next_column()? {
        match idx {
            0 | 1 => {
                let v: Vec<ByteArray> = rows
                    .iter()
                    .map(|r| ByteArray::from(if idx == 0 { r.0 } else { r.1 }.to_vec()))
                    .collect();
                col.typed::<ByteArrayType>().write_batch(&v, None, None)?;
            }
            2 => {
                let v: Vec<i32> = rows.iter().map(|r| r.2).collect();
                col.typed::<Int32Type>().write_batch(&v, None, None)?;
            }
            _ => {
                let k = idx - 3;
                let v: Vec<i64> = rows.iter().map(|r| r.3[k] as i64).collect();
                col.typed::<Int64Type>().write_batch(&v, None, None)?;
            }
        }
        col.close()?;
        idx += 1;
    }
    rg.close()?;
    w.close()?;
    Ok(())
}
