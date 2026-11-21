//! Shopping Sessions pipeline, v1: straightforward, parallel, exact.
//! See rules.md for the spec; plan.md for the roadmap.

use memchr::{memchr, memchr_iter};
use memmap2::Mmap;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHasher};
use std::error::Error;
use std::fs::File;
use std::hash::Hasher;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

const DAY_MS: i64 = 86_400_000;
const GAP_MS: i64 = 1_800_000;
const WEEK_MS: i64 = 7 * DAY_MS;
const LATE_MS: i64 = DAY_MS;
const NB: usize = 128;
const CHUNK: usize = 8 << 20;

type B = &'static [u8];
type Res<T> = Result<T, Box<dyn Error + Send + Sync>>;

// ---------------------------------------------------------------- helpers

#[inline]
fn hash_bytes(b: &[u8]) -> u64 {
    let mut h = FxHasher::default();
    h.write(b);
    h.finish()
}

#[inline]
fn bucket(h: u64) -> usize {
    ((h >> 40) as usize) & (NB - 1)
}

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

#[derive(Clone, Copy)]
struct Chunk {
    data: B,
    skip_first: bool,
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
            out.push(Chunk { data: &f[start..end], skip_first: first });
            first = false;
            start = end;
        }
    }
    out
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
fn parse_ts(b: &[u8]) -> Option<i64> {
    let n = b.len();
    if n == 0 {
        return None;
    }
    if b.iter().all(|c| c.is_ascii_digit()) {
        let mut v: u64 = 0;
        for &c in b {
            v = v.checked_mul(10)?.checked_add((c - b'0') as u64)?;
        }
        if v > i64::MAX as u64 {
            return None;
        }
        return Some(v as i64);
    }
    if n != 20 && n != 25 {
        return None;
    }
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
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
            // keep scanning for non-digits is unnecessary: invalid either way
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

#[derive(Clone, Copy)]
struct Ev {
    eid: B,
    user: B,
    ingest: i64,
    ts: i64,
    price: u32,
    cat: u16,
    kind: u8, // 0 invalid, 1 view, 2 cart, 3 purchase
}

#[derive(Clone, Copy)]
struct PEv {
    name: B,
    ts: i64,
    price: u32,
    pid: u32,
    cat: u16,
    kind: u8,
}

#[derive(Clone, Copy)]
struct Ck {
    name: B,
    cid: B,
    ts: i64,
    camp: u32,
    pid: u32,
}

struct URow {
    name: B,
    day: i32,
    sessions: u64,
    events: u64,
    revenue: u64,
    top: Option<u16>,
}

struct Ctx {
    ws: i64,
    we: i64,
    product: FxHashMap<B, u16>,
    unknown: u16,
    ncat: usize,
    cat_rank: Vec<u16>,
    camp: FxHashMap<B, u32>,
    ncamp: usize,
    link: FxHashMap<B, B>,
    day0: i64,
    nd: usize,
}

fn load_lookup(path: &Path) -> Res<Vec<(B, B)>> {
    let data = map_file(path)?;
    let mut v = Vec::new();
    let c = Chunk { data, skip_first: true };
    let mut f: [B; 2] = [&[]; 2];
    for_each_line(&c, |line| {
        if split(line, &mut f) {
            v.push((f[0], f[1]));
        }
    });
    Ok(v)
}

fn find(parent: &mut [u32], mut x: u32) -> u32 {
    while parent[x as usize] != x {
        let p = parent[x as usize];
        parent[x as usize] = parent[p as usize];
        x = parent[x as usize];
    }
    x
}

fn build_links(data: &Path) -> Res<FxHashMap<B, B>> {
    let files: Vec<B> = list_csv(&data.join("links"))
        .iter()
        .map(|p| map_file(p))
        .collect::<Res<_>>()?;
    let chunks = make_chunks(&files, CHUNK);
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
    let mut ids: FxHashMap<B, u32> = FxHashMap::default();
    let mut names: Vec<B> = Vec::new();
    let mut parent: Vec<u32> = Vec::new();
    let mut intern = |s: B, names: &mut Vec<B>, parent: &mut Vec<u32>| -> u32 {
        *ids.entry(s).or_insert_with(|| {
            let i = names.len() as u32;
            names.push(s);
            parent.push(i);
            i
        })
    };
    for v in &pairs {
        for &(a, b) in v {
            let ia = intern(a, &mut names, &mut parent);
            let ib = intern(b, &mut names, &mut parent);
            let ra = find(&mut parent, ia);
            let rb = find(&mut parent, ib);
            if ra != rb {
                parent[ra.max(rb) as usize] = ra.min(rb);
            }
        }
    }
    let n = names.len();
    let mut best: Vec<B> = vec![&[]; n];
    let mut has: Vec<bool> = vec![false; n];
    for i in 0..n as u32 {
        let r = find(&mut parent, i) as usize;
        if !has[r] || names[i as usize] < best[r] {
            best[r] = names[i as usize];
            has[r] = true;
        }
    }
    let mut link: FxHashMap<B, B> = FxHashMap::default();
    link.reserve(n);
    for i in 0..n as u32 {
        let r = find(&mut parent, i) as usize;
        let name = best[r];
        if name != names[i as usize] {
            link.insert(names[i as usize], name);
        }
    }
    Ok(link)
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

    // products / categories
    let mut cat_names: Vec<B> = Vec::new();
    let mut cat_idx: FxHashMap<B, u16> = FxHashMap::default();
    let mut product: FxHashMap<B, u16> = FxHashMap::default();
    for (p, c) in load_lookup(&data.join("products.csv"))? {
        let ci = *cat_idx.entry(c).or_insert_with(|| {
            cat_names.push(c);
            (cat_names.len() - 1) as u16
        });
        product.entry(p).or_insert(ci);
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
    let mut camp: FxHashMap<B, u32> = FxHashMap::default();
    for (id, ch) in load_lookup(&data.join("campaigns.csv"))? {
        if !camp.contains_key(id) {
            camp.insert(id, camp_ids.len() as u32);
            camp_ids.push(id);
            camp_ch.push(ch);
        }
    }
    let ncamp = camp_ids.len();

    let link = build_links(&data)?;
    eprintln!("[{:?}] links: {} mapped ids", t0.elapsed(), link.len());

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

    // ---- events: parse into eid buckets
    let ev_files: Vec<B> = list_csv(&data.join("events"))
        .iter()
        .map(|p| map_file(p))
        .collect::<Res<_>>()?;
    let ev_chunks = make_chunks(&ev_files, CHUNK);
    let parsed: Vec<Vec<Vec<Ev>>> = ev_chunks
        .par_iter()
        .map(|c| parse_events(c, &ctx))
        .collect();
    eprintln!("[{:?}] events parsed", t0.elapsed());

    // ---- clicks: parse into person buckets
    let ck_files: Vec<B> = list_csv(&data.join("clicks"))
        .iter()
        .map(|p| map_file(p))
        .collect::<Res<_>>()?;
    let ck_chunks = make_chunks(&ck_files, CHUNK);
    let clicks: Vec<Vec<Vec<Ck>>> = ck_chunks
        .par_iter()
        .map(|c| parse_clicks(c, &ctx))
        .collect();
    eprintln!("[{:?}] clicks parsed", t0.elapsed());

    // ---- dedup per eid bucket, then route to person buckets
    let routed: Vec<Vec<Vec<PEv>>> = (0..NB)
        .into_par_iter()
        .map(|b| dedup_bucket(b, &parsed, &ctx))
        .collect();
    drop(parsed);
    eprintln!("[{:?}] dedup done", t0.elapsed());

    // ---- sessionize + attribute per person bucket
    let results: Vec<(Vec<URow>, Vec<[u64; 4]>)> = (0..NB)
        .into_par_iter()
        .map(|b| person_bucket(b, &routed, &clicks, &ctx))
        .collect();
    eprintln!("[{:?}] sessions done", t0.elapsed());

    let mut users: Vec<URow> = Vec::new();
    let mut camp_agg = vec![[0u64; 4]; (ncamp + 1) * nd];
    for (u, c) in results {
        users.extend(u);
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
    write_users(&ud.join("part-0.parquet"), &users, &cat_names)?;
    write_campaigns(&cd.join("part-0.parquet"), &camp_agg, &camp_ids, &camp_ch, day0, nd)?;
    let hwm = std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("VmHWM")).map(|l| l.to_string()))
        .unwrap_or_default();
    eprintln!("[{:?}] done: {} user rows; {}", t0.elapsed(), users.len(), hwm);
    Ok(())
}

fn parse_events(c: &Chunk, ctx: &Ctx) -> Vec<Vec<Ev>> {
    let mut out: Vec<Vec<Ev>> = (0..NB).map(|_| Vec::new()).collect();
    let mut f: [B; 7] = [&[]; 7];
    for_each_line(c, |line| {
        if !split(line, &mut f) {
            return;
        }
        let eid = f[0];
        if eid.is_empty() {
            return;
        }
        let ingest = parse_ts(f[3]).unwrap_or(i64::MAX);
        let mut kind = 0u8;
        let mut ts = 0i64;
        let mut price = 0u32;
        let mut cat = 0u16;
        'v: {
            if f[1].is_empty() {
                break 'v;
            }
            let Some(t) = parse_ts(f[2]) else { break 'v };
            if t < ctx.ws || t >= ctx.we {
                break 'v;
            }
            let k = match f[4] {
                b"view" => 1,
                b"cart" => 2,
                b"purchase" => 3,
                _ => break 'v,
            };
            if f[5].is_empty() {
                break 'v;
            }
            if k == 3 {
                match parse_price(f[6]) {
                    Some(p) => price = p,
                    None => break 'v,
                }
            }
            if ingest.saturating_sub(t) > LATE_MS {
                break 'v;
            }
            cat = ctx.product.get(f[5]).copied().unwrap_or(ctx.unknown);
            ts = t;
            kind = k;
        }
        out[bucket(hash_bytes(eid))].push(Ev { eid, user: f[1], ingest, ts, price, cat, kind });
    });
    out
}

fn parse_clicks(c: &Chunk, ctx: &Ctx) -> Vec<Vec<Ck>> {
    let mut out: Vec<Vec<Ck>> = (0..NB).map(|_| Vec::new()).collect();
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
        let Some(&camp) = ctx.camp.get(f[2]) else { return };
        let name = ctx.link.get(user).copied().unwrap_or(user);
        out[bucket(hash_bytes(name))].push(Ck { name, cid, ts, camp, pid: 0 });
    });
    out
}

fn dedup_bucket(b: usize, parsed: &[Vec<Vec<Ev>>], ctx: &Ctx) -> Vec<Vec<PEv>> {
    let total: usize = parsed.iter().map(|c| c[b].len()).sum();
    let mut m: FxHashMap<B, Ev> = FxHashMap::default();
    m.reserve(total);
    for c in parsed {
        for e in &c[b] {
            match m.entry(e.eid) {
                std::collections::hash_map::Entry::Vacant(v) => {
                    v.insert(*e);
                }
                std::collections::hash_map::Entry::Occupied(mut o) => {
                    if e.ingest < o.get().ingest {
                        *o.get_mut() = *e;
                    }
                }
            }
        }
    }
    let mut out: Vec<Vec<PEv>> = (0..NB).map(|_| Vec::new()).collect();
    for e in m.values() {
        if e.kind == 0 {
            continue;
        }
        let name = ctx.link.get(e.user).copied().unwrap_or(e.user);
        out[bucket(hash_bytes(name))].push(PEv {
            name,
            ts: e.ts,
            price: e.price,
            pid: 0,
            cat: e.cat,
            kind: e.kind,
        });
    }
    out
}

fn person_bucket(
    b: usize,
    routed: &[Vec<Vec<PEv>>],
    clicks: &[Vec<Vec<Ck>>],
    ctx: &Ctx,
) -> (Vec<URow>, Vec<[u64; 4]>) {
    let nd = ctx.nd;
    let organic = ctx.ncamp;
    let mut camp_agg = vec![[0u64; 4]; (ctx.ncamp + 1) * nd];
    let mut users: Vec<URow> = Vec::new();

    let total: usize = routed.iter().map(|r| r[b].len()).sum();
    let mut evs: Vec<PEv> = Vec::with_capacity(total);
    for r in routed {
        evs.extend_from_slice(&r[b]);
    }
    let mut pid: FxHashMap<B, u32> = FxHashMap::default();
    let mut pnames: Vec<B> = Vec::new();
    for e in evs.iter_mut() {
        let id = *pid.entry(e.name).or_insert_with(|| {
            pnames.push(e.name);
            (pnames.len() - 1) as u32
        });
        e.pid = id;
    }
    evs.sort_unstable_by_key(|e| (e.pid, e.ts));

    let mut cks: Vec<Ck> = Vec::new();
    for c in clicks {
        for k in &c[b] {
            if let Some(&p) = pid.get(k.name) {
                let mut k = *k;
                k.pid = p;
                cks.push(k);
            }
        }
    }
    cks.sort_unstable_by(|a, b| (a.pid, a.ts, a.cid).cmp(&(b.pid, b.ts, b.cid)));

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

    let mut i = 0;
    let mut ci = 0;
    while i < evs.len() {
        let p = evs[i].pid;
        let mut j = i;
        while j < evs.len() && evs[j].pid == p {
            j += 1;
        }
        while ci < cks.len() && cks[ci].pid < p {
            ci += 1;
        }
        let mut cj = ci;
        while cj < cks.len() && cks[cj].pid == p {
            cj += 1;
        }
        let name = pnames[p as usize];
        let mut lo = ci;
        let mut hi = ci;

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
            users.push(URow { name, day: day as i32, sessions: us, events: ue, revenue: ur, top });
        };

        let mut k = i;
        while k < j {
            let s = evs[k].ts;
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
            let mut last = s;
            let (mut nev, mut rev, mut purch) = (0u64, 0u64, false);
            while k < j && evs[k].ts - last <= GAP_MS {
                let e = &evs[k];
                last = e.ts;
                nev += 1;
                if e.kind == 3 {
                    purch = true;
                    rev += e.price as u64;
                    let c = e.cat as usize;
                    if catsum[c] == 0 {
                        touched.push(e.cat);
                    }
                    catsum[c] += e.price as u64;
                }
                k += 1;
            }
            us += 1;
            ue += nev;
            ur += rev;

            while hi < cj && cks[hi].ts <= s {
                add(cks[hi].camp, &mut cnt, &mut apos, &mut active);
                hi += 1;
            }
            while lo < hi && cks[lo].ts < s - WEEK_MS {
                remove(cks[lo].camp, &mut cnt, &mut apos, &mut active);
                lo += 1;
            }
            let d = (day - ctx.day0) as usize;
            let credited = if lo < hi {
                let t = cks[hi - 1].ts;
                let mut f = hi - 1;
                while f > lo && cks[f - 1].ts == t {
                    f -= 1;
                }
                cks[f].camp as usize
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
            remove(cks[lo].camp, &mut cnt, &mut apos, &mut active);
            lo += 1;
        }
        i = j;
        ci = cj;
    }
    (users, camp_agg)
}

// ---------------------------------------------------------------- parquet

use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;

fn write_users(path: &Path, rows: &[URow], cat_names: &[B]) -> Res<()> {
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
    let props = Arc::new(WriterProperties::builder().build());
    let mut w = SerializedFileWriter::new(File::create(path)?, schema, props)?;
    let mut rg = w.next_row_group()?;
    let mut idx = 0;
    while let Some(mut col) = rg.next_column()? {
        match idx {
            0 => {
                let v: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.name.to_vec())).collect();
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
    let props = Arc::new(WriterProperties::builder().build());
    let mut w = SerializedFileWriter::new(File::create(path)?, schema, props)?;
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
