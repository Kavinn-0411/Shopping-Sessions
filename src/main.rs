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
//! IDs of 1 to 10 characters from [0-9A-Za-z] (every generated ID) are packed exactly into a
//! u64 whose order equals byte-wise string order; any other ID gets a 63-bit hash with the top
//! bit set, and its bytes are kept in side tables.

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

// ---------------------------------------------------------------- allocator

/// Large allocations come straight from mmap, 2 MB aligned and marked MADV_HUGEPAGE, so the big
/// random-access tables sit on transparent huge pages where the kernel allows them (fewer TLB
/// misses and page faults). Medium ones (the many bucket vectors) are bump-allocated from one
/// reserved huge-page region and never freed: the process is short-lived and they live almost to
/// the end anyway. Small ones go to the system allocator.
mod halloc {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::sync::atomic::{AtomicUsize, Ordering};

    const MED: usize = 16 << 10;
    const BIG: usize = 1 << 20;
    const HUGE: usize = 2 << 20;
    const PAGE: usize = 4096;
    const RESERVE: usize = 64 << 30;

    pub struct Alloc;

    static ARENA: AtomicUsize = AtomicUsize::new(0); // base; 1 = unavailable
    static NEXT: AtomicUsize = AtomicUsize::new(0);

    #[inline]
    fn big(l: &Layout) -> bool {
        l.size() >= BIG && l.align() <= PAGE
    }
    #[inline]
    fn med(l: &Layout) -> bool {
        l.size() >= MED && l.size() < BIG && l.align() <= 64
    }
    #[inline]
    fn round(n: usize) -> usize {
        (n + PAGE - 1) & !(PAGE - 1)
    }
    fn off() -> bool {
        static OFF: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        *OFF.get_or_init(|| std::env::var_os("NOHUGE").is_some())
    }
    fn arena_off() -> bool {
        static OFF: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        *OFF.get_or_init(|| std::env::var_os("NOARENA").is_some())
    }
    #[inline]
    fn in_arena(p: *mut u8) -> bool {
        let b = ARENA.load(Ordering::Relaxed);
        b > 1 && (p as usize) >= b && (p as usize) < b + RESERVE
    }

    unsafe fn arena_base() -> usize {
        let b = ARENA.load(Ordering::Acquire);
        if b != 0 {
            return b;
        }
        let p = libc::mmap(
            std::ptr::null_mut(),
            RESERVE + HUGE,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
            -1,
            0,
        );
        let nb = if p == libc::MAP_FAILED {
            1
        } else {
            let a = (p as usize + HUGE - 1) & !(HUGE - 1);
            if !off() {
                libc::madvise(a as *mut libc::c_void, RESERVE, libc::MADV_HUGEPAGE);
            }
            a
        };
        match ARENA.compare_exchange(0, nb, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => nb,
            Err(cur) => {
                if nb > 1 {
                    libc::munmap(p, RESERVE + HUGE);
                }
                cur
            }
        }
    }

    unsafe fn arena_alloc(size: usize) -> *mut u8 {
        if arena_off() {
            return std::ptr::null_mut();
        }
        let b = arena_base();
        if b <= 1 {
            return std::ptr::null_mut();
        }
        let n = (size + 63) & !63;
        let o = NEXT.fetch_add(n, Ordering::Relaxed);
        if o + n > RESERVE {
            return std::ptr::null_mut();
        }
        (b + o) as *mut u8
    }

    unsafe fn map(size: usize) -> *mut u8 {
        let len = round(size);
        let prot = libc::PROT_READ | libc::PROT_WRITE;
        let flags = libc::MAP_PRIVATE | libc::MAP_ANONYMOUS;
        if len < HUGE {
            let p = libc::mmap(std::ptr::null_mut(), len, prot, flags, -1, 0);
            return if p == libc::MAP_FAILED { std::ptr::null_mut() } else { p as *mut u8 };
        }
        let p = libc::mmap(std::ptr::null_mut(), len + HUGE, prot, flags, -1, 0);
        if p == libc::MAP_FAILED {
            return std::ptr::null_mut();
        }
        let a = p as usize;
        let start = (a + HUGE - 1) & !(HUGE - 1);
        if start > a {
            libc::munmap(p, start - a);
        }
        let tail = a + len + HUGE - (start + len);
        if tail > 0 {
            libc::munmap((start + len) as *mut libc::c_void, tail);
        }
        if !off() {
            libc::madvise(start as *mut libc::c_void, len, libc::MADV_HUGEPAGE);
        }
        start as *mut u8
    }

    unsafe impl GlobalAlloc for Alloc {
        unsafe fn alloc(&self, l: Layout) -> *mut u8 {
            if big(&l) {
                map(l.size())
            } else if med(&l) {
                let p = arena_alloc(l.size());
                if p.is_null() {
                    System.alloc(l)
                } else {
                    p
                }
            } else {
                System.alloc(l)
            }
        }
        unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
            if big(&l) {
                map(l.size())
            } else if med(&l) {
                // arena memory is fresh (never reused), hence zero
                let p = arena_alloc(l.size());
                if p.is_null() {
                    System.alloc_zeroed(l)
                } else {
                    p
                }
            } else {
                System.alloc_zeroed(l)
            }
        }
        unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
            if in_arena(p) {
                return;
            }
            if big(&l) {
                libc::munmap(p as *mut libc::c_void, round(l.size()));
            } else {
                System.dealloc(p, l)
            }
        }
        unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
            let nl = Layout::from_size_align_unchecked(new, l.align());
            if !in_arena(p) && !big(&l) && !big(&nl) && !med(&nl) {
                return System.realloc(p, l, new);
            }
            if big(&l) && big(&nl) {
                let (o, n) = (round(l.size()), round(new));
                if o == n {
                    return p;
                }
                let q = libc::mremap(p as *mut libc::c_void, o, n, libc::MREMAP_MAYMOVE);
                if q == libc::MAP_FAILED {
                    return std::ptr::null_mut();
                }
                if n >= HUGE && !off() {
                    libc::madvise(q, n, libc::MADV_HUGEPAGE);
                }
                return q as *mut u8;
            }
            let q = self.alloc(nl);
            if !q.is_null() {
                std::ptr::copy_nonoverlapping(p, q, l.size().min(new));
                self.dealloc(p, l);
            }
            q
        }
    }
}

#[global_allocator]
static GLOBAL: halloc::Alloc = halloc::Alloc;

const DAY_MS: i64 = 86_400_000;
const GAP_MS: i64 = 1_800_000;
const WEEK_MS: i64 = 7 * DAY_MS;
const LATE_MS: i64 = DAY_MS;
const EBITS: u32 = 8;
const NE: usize = 1 << EBITS; // eid buckets
const PBITS: u32 = 8;
const NP: usize = 1 << PBITS; // person buckets
const SBITS: u32 = 8;
const NS: usize = 1 << SBITS; // link map shards
/// Threads of the separate pool that builds the links while the main pool parses.
const LINK_THREADS: usize = 2;

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
fn mix(k: u64) -> u64 {
    fmix(k ^ 0x9E37_79B9_7F4A_7C15)
}

/// 6-bit codes for [0-9A-Za-z] in ASCII order (0 = not in the alphabet).
const CODE: [u8; 256] = {
    let mut t = [0u8; 256];
    let mut i = 0;
    while i < 10 {
        t[b'0' as usize + i] = 1 + i as u8;
        i += 1;
    }
    let mut i = 0;
    while i < 26 {
        t[b'A' as usize + i] = 11 + i as u8;
        t[b'a' as usize + i] = 37 + i as u8;
        i += 1;
    }
    t
};
const DECODE: [u8; 64] = {
    let mut t = [0u8; 64];
    let mut i = 0;
    while i < 256 {
        if CODE[i] != 0 {
            t[CODE[i] as usize] = i as u8;
        }
        i += 1;
    }
    t
};
const MAXPACK: usize = 10;
const HASHED: u64 = 1 << 63;

#[cold]
fn hashed_key(s: &[u8]) -> u64 {
    let mut h: u64 = 0x243F_6A88_85A3_08D3 ^ s.len() as u64;
    for c in s.chunks(8) {
        let mut w = [0u8; 8];
        w[..c.len()].copy_from_slice(c);
        h = (h ^ u64::from_le_bytes(w)).wrapping_mul(0x9E37_79B9_7F4A_7C15).rotate_left(29);
    }
    fmix(h) | HASHED
}

/// Key of an ID. Strings of 1 to 10 characters from [0-9A-Za-z] are packed exactly, 6 bits a
/// character, first character highest, so key order is byte order. Anything else is hashed
/// (top bit set) and its bytes are kept in side tables where order or bytes matter.
#[inline(always)]
fn key_of(s: &[u8]) -> u64 {
    let n = s.len();
    if n == 0 {
        return 0;
    }
    if n <= MAXPACK {
        let mut v: u64 = 0;
        let mut ok = true;
        for &c in s {
            let code = CODE[c as usize];
            ok &= code != 0;
            v = (v << 6) | code as u64;
        }
        if ok {
            return v << (6 * (MAXPACK - n));
        }
    }
    hashed_key(s)
}

#[inline]
fn is_long(k: u64) -> bool {
    k & HASHED != 0
}

fn key_bytes(k: u64, long: &FxHashMap<u64, B>) -> Vec<u8> {
    if is_long(k) {
        long.get(&k).map(|s| s.to_vec()).unwrap_or_default()
    } else {
        let mut v = Vec::with_capacity(MAXPACK);
        for i in 0..MAXPACK {
            let code = (k >> (6 * (MAXPACK - 1 - i))) & 63;
            if code == 0 {
                break;
            }
            v.push(DECODE[code as usize]);
        }
        v
    }
}

#[inline(always)]
fn key_fast(s: &[u8], _limit: usize) -> u64 {
    key_of(s)
}

/// Lookup for IDs like `p123` / `c_45`: a direct array for `<prefix><digits, no leading zero>`,
/// exact KeyMap fallback for every other string.
struct IdLookup<V: Copy> {
    prefix: &'static [u8],
    arr: Vec<Option<V>>,
    map: KeyMap<V>,
}

const ID_ARRAY_MAX: u64 = 1 << 22;

impl<V: Copy> IdLookup<V> {
    fn new(prefix: &'static [u8]) -> Self {
        IdLookup { prefix, arr: Vec::new(), map: KeyMap::new() }
    }
    #[inline]
    fn num(&self, s: &[u8]) -> Option<u64> {
        let p = self.prefix.len();
        if s.len() <= p || s.len() > p + 7 || &s[..p] != self.prefix || s[p] == b'0' {
            return None;
        }
        let mut v = 0u64;
        for &c in &s[p..] {
            if !c.is_ascii_digit() {
                return None;
            }
            v = v * 10 + (c - b'0') as u64;
        }
        if v < ID_ARRAY_MAX {
            Some(v)
        } else {
            None
        }
    }
    fn insert_if_absent(&mut self, s: B, v: V) {
        if let Some(n) = self.num(s) {
            let n = n as usize;
            if self.arr.len() <= n {
                self.arr.resize(n + 1, None);
            }
            if self.arr[n].is_none() {
                self.arr[n] = Some(v);
            }
        } else {
            self.map.insert_if_absent(s, v);
        }
    }
    #[inline]
    fn get(&self, s: &[u8]) -> Option<V> {
        match self.num(s) {
            Some(n) => self.arr.get(n as usize).copied().flatten(),
            None => self.map.get(s),
        }
    }
}

/// Exact lookup table keyed by byte strings, via packed keys (long keys verified by bytes).
struct KeyMap<V: Copy> {
    short: FxHashMap<u64, V>,
    long: FxHashMap<B, V>,
}

impl<V: Copy> KeyMap<V> {
    fn new() -> Self {
        KeyMap { short: FxHashMap::default(), long: FxHashMap::default() }
    }
    fn insert_if_absent(&mut self, s: B, v: V) {
        let k = key_of(s);
        if !is_long(k) {
            self.short.entry(k).or_insert(v);
        } else {
            self.long.entry(s).or_insert(v);
        }
    }
    #[inline]
    fn get(&self, s: &[u8]) -> Option<V> {
        let k = key_of(s);
        if !is_long(k) {
            self.short.get(&k).copied()
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
    let m = if std::env::var_os("NOPOP").is_none() {
        unsafe { memmap2::MmapOptions::new().populate().map(&f)? }
    } else {
        unsafe { Mmap::map(&f)? }
    };
    let m: &'static Mmap = Box::leak(Box::new(m));
    Ok(&m[..])
}

fn map_dir(dir: &Path) -> Res<Vec<B>> {
    let paths = list_csv(dir);
    paths.par_iter().map(|p| map_file(p)).collect()
}

/// Map the chunk's pages up front (MADV_POPULATE_READ, Linux 5.14+); errors are ignored.
fn populate(d: &[u8]) {
    #[cfg(target_os = "linux")]
    unsafe {
        if std::env::var_os("NOMADV").is_some() || d.is_empty() {
            return;
        }
        let page = 4096usize;
        let start = (d.as_ptr() as usize) & !(page - 1);
        let end = d.as_ptr() as usize + d.len();
        libc::madvise(start as *mut libc::c_void, end - start, 22 /* MADV_POPULATE_READ */);
    }
}

/// A CSV file read with pread (no mapping: the bytes are copied piece by piece into a small
/// per-thread buffer that stays in cache, and nothing large has to be faulted in or torn down).
struct SrcFile {
    path: PathBuf,
    len: u64,
}

fn list_src(dir: &Path) -> Res<Vec<SrcFile>> {
    let mut v = Vec::new();
    for path in list_csv(dir) {
        let len = std::fs::metadata(&path)?.len();
        v.push(SrcFile { path, len });
    }
    Ok(v)
}

/// The lines of one file that start in [a, b), the file's first line (header) excluded.
#[derive(Clone, Copy)]
struct Task {
    file: usize,
    a: u64,
    b: u64,
    /// First record id of this task (events only).
    base: u32,
}

fn make_tasks(files: &[SrcFile], target: u64) -> Vec<Task> {
    let mut out = Vec::new();
    for (i, f) in files.iter().enumerate() {
        let mut a = 0;
        while a < f.len {
            let b = (a + target).min(f.len);
            out.push(Task { file: i, a, b, base: 0 });
            a = b;
        }
    }
    out
}

fn task_target(files: &[SrcFile], ncpu: usize) -> u64 {
    let total: u64 = files.iter().map(|f| f.len).sum();
    (total / (ncpu as u64 * 6).max(1)).clamp(1 << 20, 64 << 20)
}

const PIECE: usize = 4 << 20;

thread_local! {
    static RBUF: std::cell::RefCell<Vec<u8>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// pread until `buf` is full or the file ends; returns the bytes read.
fn read_full(f: &File, buf: &mut [u8], mut off: u64) -> std::io::Result<usize> {
    use std::os::unix::fs::FileExt;
    let mut n = 0;
    while n < buf.len() {
        match f.read_at(&mut buf[n..], off) {
            Ok(0) => break,
            Ok(k) => {
                n += k;
                off += k as u64;
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(n)
}

/// Call `f` with runs of whole lines covering exactly the task's lines. The slices are only
/// valid during the call.
fn read_task(files: &[SrcFile], t: &Task, mut f: impl FnMut(&[u8])) -> Res<()> {
    let sf = &files[t.file];
    let file = File::open(&sf.path)?;
    #[cfg(target_os = "linux")]
    unsafe {
        use std::os::unix::io::AsRawFd;
        libc::posix_fadvise(file.as_raw_fd(), t.a as i64, (t.b - t.a) as i64 + (1 << 20), libc::POSIX_FADV_SEQUENTIAL);
    }
    RBUF.with(|cell| -> Res<()> {
        let mut buf = cell.borrow_mut();
        // Lines start at 0 and after each newline; the line at 0 is the header. The first line
        // we own is the first one starting at or after max(a, 1): skip through the first
        // newline at or after a - 1.
        let mut off = t.a.saturating_sub(1);
        let mut skipping = true;
        let mut carry = 0usize;
        loop {
            if buf.len() < carry + PIECE {
                buf.resize(carry + PIECE, 0);
            }
            let n = read_full(&file, &mut buf[carry..carry + PIECE], off)?;
            let base = off - carry as u64; // file offset of buf[0]
            off += n as u64;
            let have = carry + n;
            let eof = n < PIECE || off >= sf.len;
            let mut s = 0usize;
            if skipping {
                match memchr(b'\n', &buf[..have]) {
                    Some(i) => {
                        s = i + 1;
                        skipping = false;
                    }
                    None => {
                        if eof {
                            return Ok(());
                        }
                        carry = 0;
                        continue;
                    }
                }
            }
            // Our lines start before b; `cut` is b in buffer coordinates.
            let cut = t.b.saturating_sub(base);
            if cut <= have as u64 {
                let cut = cut as usize;
                let end = if s >= cut {
                    Some(s)
                } else {
                    match memchr(b'\n', &buf[cut - 1..have]) {
                        Some(i) => Some(cut + i),
                        None if eof => Some(have),
                        None => None,
                    }
                };
                if let Some(end) = end {
                    if end > s {
                        f(&buf[s..end]);
                    }
                    return Ok(());
                }
            }
            let lim = if eof {
                have
            } else {
                match memchr::memrchr(b'\n', &buf[s..have]) {
                    Some(i) => s + i + 1,
                    None => s,
                }
            };
            if lim > s {
                f(&buf[s..lim]);
            }
            if eof {
                return Ok(());
            }
            buf.copy_within(lim..have, 0);
            carry = have - lim;
        }
    })
}

/// Copy bytes that must outlive the read buffer.
fn keep(b: &[u8]) -> B {
    Box::leak(b.to_vec().into_boxed_slice())
}

#[derive(Clone, Copy)]
struct Chunk<'a> {
    data: &'a [u8],
    skip_first: bool,
    /// First record id of this chunk (events only).
    base: u32,
}

fn make_chunks(files: &[B], target: usize) -> Vec<Chunk<'static>> {
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
fn for_each_line<'a>(c: &Chunk<'a>, mut f: impl FnMut(&'a [u8])) {
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

/// Bitmasks of ',', '\n' and '"' in 64 bytes at `p`.
#[inline(always)]
fn masks64(p: *const u8) -> (u64, u64, u64) {
    #[cfg(target_feature = "avx2")]
    unsafe {
        use std::arch::x86_64::*;
        let a = _mm256_loadu_si256(p as *const __m256i);
        let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
        let m = |x: __m256i, c: u8| -> u64 { _mm256_movemask_epi8(_mm256_cmpeq_epi8(x, _mm256_set1_epi8(c as i8))) as u32 as u64 };
        (
            m(a, b',') | (m(b, b',') << 32),
            m(a, b'\n') | (m(b, b'\n') << 32),
            m(a, b'"') | (m(b, b'"') << 32),
        )
    }
    #[cfg(not(target_feature = "avx2"))]
    {
        let (mut c, mut n, mut q) = (0u64, 0u64, 0u64);
        for i in 0..64 {
            let x = unsafe { *p.add(i) };
            c |= ((x == b',') as u64) << i;
            n |= ((x == b'\n') as u64) << i;
            q |= ((x == b'"') as u64) << i;
        }
        (c, n, q)
    }
}

/// Scan a chunk, calling `on_line` with the N unquoted fields of every well-formed line.
///
/// Delimiters come from 64-byte bitmasks. Quotes are checked once per line: a line is well
/// formed when every field either has no quote or starts and ends with one (length >= 2), and
/// the line holds no other quotes (its quote count is twice the number of quoted fields).
#[inline(always)]
fn scan<'a, const N: usize>(c: &Chunk<'a>, mut on_line: impl FnMut(&[&'a [u8]; N])) {
    debug_assert!(N >= 1 && N <= 8);
    let d = c.data;
    let end = d.len();
    let mut pos = 0;
    if c.skip_first {
        match memchr(b'\n', d) {
            Some(i) => pos = i + 1,
            None => return,
        }
    }
    if pos >= end {
        return;
    }
    let base = d.as_ptr();
    // byte at i (callers guarantee i < end)
    let at = |i: usize| unsafe { *base.add(i) };
    let mut dl = [0usize; 8]; // comma positions of the current line (slot 7 absorbs extras)
    let mut nf = 0usize; // commas seen in the current line
    let mut ls = pos; // current line start
    let mut q_ls: u64 = 0; // quotes before the current line start (counted from `pos`)
    let mut qb: u64 = 0; // quotes before the current block
    let mut blk = pos;
    let mut buf = [0u8; 64];
    macro_rules! line {
        ($p:expr, $q:expr) => {{
            let p: usize = $p;
            if nf == N - 1 {
                let lq: u64 = $q - q_ls;
                let mut fields: [&'a [u8]; N] = [&[]; N];
                let mut s = ls;
                let mut bad = false;
                let mut nq = 0u64;
                for i in 0..N {
                    let e = if i + 1 < N { dl[i] } else { p };
                    let ne = e > s;
                    let qs = ne & (at(s.min(end - 1)) == b'"');
                    let qe = ne & (at(e.max(1) - 1) == b'"');
                    bad |= qs != qe;
                    bad |= qs & (e - s < 2);
                    nq += 2 * qs as u64;
                    let q = qs as usize;
                    fields[i] = unsafe { std::slice::from_raw_parts(base.add(s + q), (e - s).saturating_sub(2 * q)) };
                    s = e + 1;
                }
                if !bad && nq == lq {
                    on_line(&fields);
                }
            }
            nf = 0;
            ls = p + 1;
        }};
    }
    while blk < end {
        let (cm, nl, qt) = if blk + 64 <= end {
            masks64(unsafe { base.add(blk) })
        } else {
            buf = [0u8; 64];
            buf[..end - blk].copy_from_slice(&d[blk..]);
            masks64(buf.as_ptr())
        };
        let mut st = cm | nl;
        while st != 0 {
            let bit = st.trailing_zeros();
            let p = blk + bit as usize;
            if (nl >> bit) & 1 != 0 {
                let q = qb + (qt & ((1u64 << bit) - 1)).count_ones() as u64;
                line!(p, q);
                q_ls = q;
            } else {
                dl[nf.min(7)] = p;
                nf += 1;
            }
            st &= st - 1;
        }
        qb += qt.count_ones() as u64;
        blk += 64;
    }
    if ls < end {
        line!(end, qb);
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

/// Days from 1970-01-01 to the first of each month, 1970..2099 (index (y-1970)*12 + m-1).
static MONTH_DAYS: std::sync::LazyLock<Vec<i64>> = std::sync::LazyLock::new(|| {
    let mut v = Vec::with_capacity(130 * 12);
    for y in 1970..2100 {
        for m in 1..=12 {
            v.push(days_from_civil_slow(y, m, 1));
        }
    }
    v
});

#[inline(always)]
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let i = (y - 1970) * 12 + (m - 1);
    if (0..130 * 12).contains(&i) && (1..=12).contains(&m) {
        MONTH_DAYS[i as usize] + d - 1
    } else {
        days_from_civil_slow(y, m, d)
    }
}

fn days_from_civil_slow(y: i64, m: i64, d: i64) -> i64 {
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
    key: u64,
    ingest: u32, // epoch seconds (ingest_ts is whole-second F1)
    rec: u32,
}

const DEMPTY: DRec = DRec { key: 0, ingest: 0, rec: 0 };

/// A valid (validated, not late) event, routed by person.
#[derive(Clone, Copy)]
struct PRec {
    pkey: u64,
    ts: u32, // ms since window start
    rk: u32, // record id << 2 | kind (1 view, 2 cart)
}

/// A valid purchase, routed by person (only purchases need price and category).
#[derive(Clone, Copy)]
struct PurRec {
    pkey: u64,
    ts: u32,
    rec: u32,
    price: u32,
    cat: u16,
}

/// A valid click, routed by person.
#[derive(Clone, Copy)]
struct CRec {
    pkey: u64,
    cid: u64,
    ts: i64,
    camp: u32,
}

struct URow {
    pkey: u64,
    day: i32,
    sessions: u64,
    events: u64,
    revenue: u64,
    top: Option<u16>,
}

/// id-key -> person-key, open addressing in NS independent regions (built in parallel).
struct LinkMap {
    slots: Vec<(u64, u64)>,
    rbits: u32,
}

impl LinkMap {
    fn empty() -> Self {
        LinkMap { slots: vec![(0, 0); NS], rbits: 0 }
    }
    #[inline(always)]
    fn home(&self, k: u64) -> (usize, usize, usize) {
        let h = mix(k);
        let base = ((h >> (64 - SBITS)) as usize) << self.rbits;
        let m = (1usize << self.rbits) - 1;
        (base, (h as usize) & m, m)
    }
    #[inline(always)]
    fn prefetch(&self, k: u64) {
        let (base, i, _) = self.home(k);
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::*;
            _mm_prefetch(self.slots.as_ptr().add(base + i) as *const i8, _MM_HINT_T0);
        }
    }
    #[inline(always)]
    fn person(&self, k: u64) -> u64 {
        let (base, mut i, m) = self.home(k);
        loop {
            let (kk, v) = unsafe { *self.slots.get_unchecked(base + i) };
            if kk == k {
                return v;
            }
            if kk == 0 {
                return k;
            }
            i = (i + 1) & m;
        }
    }
}

struct Ctx {
    ws: i64,
    we: i64,
    product: IdLookup<u16>,
    unknown: u16,
    ncat: usize,
    cat_rank: Vec<u16>,
    camp: IdLookup<u32>,
    ncamp: usize,
    /// Set once the links are built; parsing starts before that and holds records back.
    link: std::sync::OnceLock<LinkMap>,
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

/// Byte-wise order of two ID keys; hashed keys are compared through their bytes.
#[inline]
fn key_lt(a: u64, b: u64, long: &FxHashMap<u64, B>) -> bool {
    if !is_long(a) && !is_long(b) {
        a < b
    } else {
        key_bytes(a, long) < key_bytes(b, long)
    }
}

#[inline]
fn shard_of(k: u64) -> usize {
    (mix(k) >> (64 - SBITS)) as usize
}

/// Returns the id-key -> person-key map and the bytes of long person names.
fn build_links(data: &Path, ncpu: usize) -> Res<(LinkMap, Vec<(u64, B)>)> {
    let tl = Instant::now();
    let files = list_src(&data.join("links"))?;
    let tasks = make_tasks(&files, task_target(&files, ncpu).min(4 << 20));
    // parse straight into key pairs; long ids keep their bytes on the side
    let parsed: Vec<(Vec<(u64, u64)>, Vec<(u64, B)>)> = tasks
        .par_iter()
        .map(|t| -> Res<_> {
            let mut edges = Vec::with_capacity(((t.b - t.a) / 20) as usize);
            let mut long = Vec::new();
            let mut on_line = |f: &[&[u8]; 2]| {
                if f[0].is_empty() || f[1].is_empty() {
                    return;
                }
                let a = key_of(f[0]);
                let b = key_of(f[1]);
                if is_long(a) {
                    long.push((a, keep(f[0])));
                }
                if is_long(b) {
                    long.push((b, keep(f[1])));
                }
                edges.push((a, b));
            };
            read_task(&files, t, |piece| scan::<2>(&Chunk { data: piece, skip_first: false, base: 0 }, &mut on_line))?;
            Ok((edges, long))
        })
        .collect::<Res<Vec<_>>>()?;
    let mut long: FxHashMap<u64, B> = FxHashMap::default();
    for (_, l) in &parsed {
        long.extend(l.iter().copied());
    }
    eprintln!("  links: parse {:?}", tl.elapsed());

    // global edge numbering
    let mut base = Vec::with_capacity(parsed.len());
    let mut ne = 0usize;
    for (e, _) in &parsed {
        base.push(ne);
        ne += e.len();
    }
    // scatter endpoints (key, slot) by shard
    let scattered: Vec<Vec<Vec<(u64, u32)>>> = parsed
        .par_iter()
        .zip(base.par_iter())
        .map(|((edges, _), &b0)| {
            let mut out: Vec<Vec<(u64, u32)>> = (0..NS).map(|_| Vec::with_capacity(edges.len() * 2 / NS + 8)).collect();
            for (i, &(a, b)) in edges.iter().enumerate() {
                let slot = 2 * (b0 + i) as u32;
                out[shard_of(a)].push((a, slot));
                out[shard_of(b)].push((b, slot + 1));
            }
            out
        })
        .collect();
    // intern per shard: local ids and the shard's key list
    let interned: Vec<(Vec<u64>, Vec<(u32, u32)>)> = (0..NS)
        .into_par_iter()
        .map(|s| {
            let total: usize = scattered.iter().map(|v| v[s].len()).sum();
            let mut m: FxHashMap<u64, u32> = FxHashMap::default();
            m.reserve(total);
            let mut keys = Vec::new();
            let mut slots = Vec::with_capacity(total);
            for v in &scattered {
                for &(k, slot) in &v[s] {
                    let id = *m.entry(k).or_insert_with(|| {
                        keys.push(k);
                        (keys.len() - 1) as u32
                    });
                    slots.push((slot, id));
                }
            }
            (keys, slots)
        })
        .collect();
    drop(scattered);
    let mut sbase = vec![0u32; NS + 1];
    for s in 0..NS {
        sbase[s + 1] = sbase[s] + interned[s].0.len() as u32;
    }
    let n = sbase[NS] as usize;
    let mut names: Vec<u64> = Vec::with_capacity(n);
    for (keys, _) in &interned {
        names.extend_from_slice(keys);
    }
    let mut slot_id = vec![0u32; 2 * ne];
    for (s, (_, slots)) in interned.iter().enumerate() {
        for &(slot, id) in slots {
            slot_id[slot as usize] = sbase[s] + id;
        }
    }
    eprintln!("  links: intern {} edges, {} ids {:?}", ne, n, tl.elapsed());

    // union-find; a root is always the byte-wise smallest name of its set
    let mut parent: Vec<u32> = (0..n as u32).collect();
    for e in 0..ne {
        let ra = find(&mut parent, slot_id[2 * e]);
        let rb = find(&mut parent, slot_id[2 * e + 1]);
        if ra != rb {
            if key_lt(names[rb as usize], names[ra as usize], &long) {
                parent[ra as usize] = rb;
            } else {
                parent[rb as usize] = ra;
            }
        }
    }
    let mut root = vec![0u32; n];
    for i in 0..n as u32 {
        root[i as usize] = find(&mut parent, i);
    }
    eprintln!("  links: union-find {:?}", tl.elapsed());

    // long person names
    let mut long_names = Vec::new();
    for i in 0..n {
        if root[i] as usize == i && is_long(names[i]) {
            long_names.push((names[i], long[&names[i]]));
        }
    }
    // each shard's ids fill that shard's region of the table
    let maxs = (0..NS).map(|s| (sbase[s + 1] - sbase[s]) as usize).max().unwrap_or(0);
    let rbits = ((maxs + maxs / 2).max(2)).next_power_of_two().trailing_zeros();
    let rsize = 1usize << rbits;
    let mut slots: Vec<(u64, u64)> = vec![(0, 0); rsize * NS];
    slots.par_chunks_mut(rsize).enumerate().for_each(|(s, region)| {
        let m = rsize - 1;
        for i in sbase[s] as usize..sbase[s + 1] as usize {
            let r = root[i] as usize;
            if r == i {
                continue;
            }
            let k = names[i];
            let mut j = (mix(k) as usize) & m;
            while region[j].0 != 0 {
                j = (j + 1) & m;
            }
            region[j] = (k, names[r]);
        }
    });
    eprintln!("  links: table {:?}", tl.elapsed());
    Ok((LinkMap { slots, rbits }, long_names))
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

    // Ask the kernel to start reading every input now (all files at full queue depth) in case
    // they are not in the page cache; a no-op walk when they are.
    if std::env::var_os("NOPREFETCH").is_none() {
        let data = data.clone();
        std::thread::spawn(move || {
            use std::os::unix::io::AsRawFd;
            for dir in ["links", "clicks", "events"] {
                let Ok(rd) = std::fs::read_dir(data.join(dir)) else { continue };
                let mut paths: Vec<PathBuf> = rd.filter_map(|e| e.ok().map(|e| e.path())).collect();
                paths.sort();
                for p in paths {
                    if let Ok(f) = File::open(&p) {
                        unsafe {
                            libc::posix_fadvise(f.as_raw_fd(), 0, 0, libc::POSIX_FADV_WILLNEED);
                        }
                    }
                }
            }
        });
    }

    let manifest = std::fs::read_to_string(data.join("manifest.json"))?;
    let ws = manifest_value(&manifest, "window_start").ok_or("bad manifest")?;
    let we = manifest_value(&manifest, "window_end").ok_or("bad manifest")?;
    if we - ws > u32::MAX as i64 || we < ws {
        return Err("window longer than 49 days is not supported".into());
    }

    let products = load_lookup(&data.join("products.csv"))?;
    let campaigns = load_lookup(&data.join("campaigns.csv"))?;
    let ev_files = list_src(&data.join("events"))?;
    let ck_files = list_src(&data.join("clicks"))?;
    eprintln!("[{:?}] lookups", t0.elapsed());

    // products / categories
    let mut cat_names: Vec<B> = Vec::new();
    let mut cat_idx: FxHashMap<B, u16> = FxHashMap::default();
    let mut product: IdLookup<u16> = IdLookup::new(b"p");
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
    let mut camp: IdLookup<u32> = IdLookup::new(b"c_");
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
        link: std::sync::OnceLock::new(),
        day0,
        nd,
    });

    // ---- tasks; each event task gets a disjoint record-id range
    let mut ev_tasks = make_tasks(&ev_files, task_target(&ev_files, ncpu));
    let mut next: u64 = 0;
    for t in ev_tasks.iter_mut() {
        t.base = next as u32;
        // a record line holds at least 27 bytes plus its newline
        next += (t.b - t.a) / 20 + 2;
    }
    if next >= 1 << 30 {
        return Err("too many records".into());
    }
    let nrec = next as usize;
    let ck_tasks = make_tasks(&ck_files, task_target(&ck_files, ncpu));

    // ---- build the links while parsing events and clicks
    // The links are built from their own thread (its parallel parts run in the pool), so no
    // pool worker ever waits for the links inside a frame the links need.
    // On few physical cores (the scorer has 4 cores / 8 hyperthreads) building the links first
    // with every thread beats overlapping them with parsing; OVERLAP=1 selects the overlap.
    let overlap = std::env::var_os("OVERLAP").is_some();
    let (links_res, (parsed, clicks)) = if !overlap {
        let r = match build_links(&data, ncpu) {
            Ok((l, long_link)) => {
                let _ = ctx.link.set(l);
                eprintln!("[{:?}] links ready", t0.elapsed());
                Ok(long_link)
            }
            Err(e) => Err(e.to_string()),
        };
        if r.is_err() {
            return Err(r.unwrap_err().into());
        }
        let pc = rayon::join(
            || ev_tasks.par_iter().map(|t| parse_events(&ev_files, t, &ctx)).collect::<Res<Vec<_>>>(),
            || ck_tasks.par_iter().map(|t| parse_clicks(&ck_files, t, &ctx)).collect::<Res<Vec<_>>>(),
        );
        (r, pc)
    } else { std::thread::scope(|sc| {
        let (data, ctx) = (&data, &ctx);
        let h = sc.spawn(move || {
            let r: Res<_> = match rayon::ThreadPoolBuilder::new().num_threads(LINK_THREADS).build() {
                Ok(lp) => lp.install(|| build_links(data, LINK_THREADS)),
                Err(e) => Err(e.into()),
            };
            match r {
            Ok((l, long_link)) => {
                let _ = ctx.link.set(l);
                eprintln!("[{:?}] links ready", t0.elapsed());
                Ok(long_link)
            }
            Err(e) => {
                // let waiting parse tasks finish; the error is returned below
                let _ = ctx.link.set(LinkMap::empty());
                Err(e.to_string())
            }
            }
        });
        let pc = rayon::join(
            || ev_tasks.par_iter().map(|t| parse_events(&ev_files, t, &ctx)).collect::<Res<Vec<_>>>(),
            || ck_tasks.par_iter().map(|t| parse_clicks(&ck_files, t, &ctx)).collect::<Res<Vec<_>>>(),
        );
        (h.join().unwrap(), pc)
    }) };
    let long_link = links_res.map_err(|e| -> Box<dyn Error + Send + Sync> { e.into() })?;
    let (parsed, clicks) = (parsed?, clicks?);
    let mut lcid: FxHashMap<u64, B> = FxHashMap::default();
    let clicks: Vec<Vec<Vec<CRec>>> = clicks
        .into_iter()
        .map(|(c, l)| {
            lcid.extend(l);
            c
        })
        .collect();
    eprintln!("[{:?}] parsed", t0.elapsed());

    // ---- dedup per eid bucket -> winner bitmap
    let winners: Vec<AtomicU64> = (0..nrec / 64 + 1).map(|_| AtomicU64::new(0)).collect();
    (0..NE).into_par_iter().for_each(|b| dedup_bucket(b, &parsed, &winners));
    eprintln!("[{:?}] dedup", t0.elapsed());

    // long names: from links and from unlinked long user ids (copied out of the event files,
    // which are unmapped below)
    let mut long: FxHashMap<u64, B> = FxHashMap::default();
    long.extend(long_link);
    for p in &parsed {
        for &(k, s) in &p.long {
            long.entry(k).or_insert(s);
        }
    }
    // Free what later stages no longer need in the background, so less is left to tear down
    // at exit. Nothing below refers to event-file bytes or dedup records.
    let mut parsed = parsed;
    if std::env::var_os("NOBG").is_none() {
        let dedup_bufs: Vec<Vec<Vec<DRec>>> = parsed.iter_mut().map(|p| std::mem::take(&mut p.dedup)).collect();
        std::thread::spawn(move || drop(dedup_bufs));
    }
    let parsed = parsed;

    // ---- sessionize + attribute per person bucket
    let nthreads = rayon::current_num_threads();
    let slots: Vec<std::sync::Mutex<Option<PState>>> = (0..nthreads).map(|_| std::sync::Mutex::new(None)).collect();
    (0..NP).into_par_iter().with_max_len(1).for_each(|b| {
        let t = rayon::current_thread_index().unwrap_or(0) % nthreads;
        let mut g = slots[t].lock().unwrap();
        let st = g.get_or_insert_with(|| PState::new(&ctx));
        person_bucket(b, &parsed, &clicks, &winners, &ctx, &lcid, st);
    });
    let states: Vec<PState> = slots.into_iter().filter_map(|m| m.into_inner().unwrap()).collect();
    eprintln!("[{:?}] sessions ({} states)", t0.elapsed(), states.len());

    let mut camp_agg = vec![[0u64; 4]; (ncamp + 1) * nd];
    let mut user_parts: Vec<Vec<URow>> = Vec::with_capacity(states.len());
    for st in states {
        for (a, x) in camp_agg.iter_mut().zip(st.camp_agg.iter()) {
            for k in 0..4 {
                a[k] += x[k];
            }
        }
        user_parts.push(st.users);
    }

    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    std::fs::create_dir_all(&ud)?;
    std::fs::create_dir_all(&cd)?;
    let nuser: usize = user_parts.iter().map(|u| u.len()).sum();
    let (r1, r2) = rayon::join(
        || {
            user_parts.par_iter().enumerate().try_for_each(|(f, part)| {
                write_users(
                    &ud.join(format!("part-{f:03}.parquet")),
                    std::slice::from_ref(part),
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
    if std::env::var_os("STATS").is_some() {
        let r = std::fs::read_to_string("/proc/self/smaps_rollup").unwrap_or_default();
        for l in r.lines().filter(|l| l.starts_with("AnonHuge") || l.starts_with("Anonymous") || l.starts_with("Rss")) {
            eprintln!("  {l}");
        }
    }
    Ok(())
}

struct EvOut {
    dedup: Vec<Vec<DRec>>,
    person: Vec<Vec<PRec>>,
    pur: Vec<Vec<PurRec>>,
    long: Vec<(u64, B)>,
}

/// ingest_ts is always valid F1; fall back to the full parser if it is not 20 bytes.
#[inline(always)]
fn parse_ingest(b: &[u8]) -> i64 {
    if b.len() == 20 {
        let n = |i: usize| (b[i].wrapping_sub(b'0')) as i64;
        let y = n(0) * 1000 + n(1) * 100 + n(2) * 10 + n(3);
        let mo = n(5) * 10 + n(6);
        let d = n(8) * 10 + n(9);
        let secs = days_from_civil(y, mo, d) * 86_400
            + (n(11) * 10 + n(12)) * 3600
            + (n(14) * 10 + n(15)) * 60
            + n(17) * 10
            + n(18);
        secs * 1000
    } else {
        parse_ts(b).unwrap_or(i64::MAX)
    }
}

const BATCH: usize = 16;

#[derive(Clone, Copy)]
struct Pend {
    ukey: u64,
    user: B,
    ts: u32,
    price: u32,
    rec: u32,
    cat: u16,
    kind: u8,
}

/// Write-combining scatter: W records per bucket are staged in a small cache-resident buffer
/// and appended to the bucket's vector together.
struct Wc<T: Copy, const W: usize> {
    buf: Vec<T>,
    lens: Vec<u8>,
    out: Vec<Vec<T>>,
}

impl<T: Copy, const W: usize> Wc<T, W> {
    fn new(nb: usize, cap: usize, fill: T) -> Self {
        Wc { buf: vec![fill; nb * W], lens: vec![0; nb], out: (0..nb).map(|_| Vec::with_capacity(cap)).collect() }
    }
    #[inline(always)]
    fn push(&mut self, b: usize, x: T) {
        unsafe {
            let l = *self.lens.get_unchecked(b) as usize;
            *self.buf.get_unchecked_mut(b * W + l) = x;
            if l + 1 == W {
                self.out.get_unchecked_mut(b).extend_from_slice(self.buf.get_unchecked(b * W..b * W + W));
                *self.lens.get_unchecked_mut(b) = 0;
            } else {
                *self.lens.get_unchecked_mut(b) = (l + 1) as u8;
            }
        }
    }
    fn finish(mut self) -> Vec<Vec<T>> {
        for b in 0..self.lens.len() {
            let l = self.lens[b] as usize;
            self.out[b].extend_from_slice(&self.buf[b * W..b * W + l]);
        }
        self.out
    }
}

const WC: usize = 8;
const PZERO: PRec = PRec { pkey: 0, ts: 0, rk: 0 };

#[inline(always)]
fn flush_events(pend: &[Pend], link: &LinkMap, person: &mut Wc<PRec, WC>, pur: &mut [Vec<PurRec>], long: &mut Vec<(u64, B)>) {
    for p in pend {
        let pkey = link.person(p.ukey);
        if pkey == p.ukey && is_long(pkey) {
            long.push((pkey, p.user));
        }
        let b = (mix(pkey) >> (64 - PBITS)) as usize;
        if p.kind == 3 {
            pur[b].push(PurRec { pkey, ts: p.ts, rec: p.rec, price: p.price, cat: p.cat });
        } else {
            person.push(b, PRec { pkey, ts: p.ts, rk: p.rec << 2 | p.kind as u32 });
        }
    }
}

/// Lines per byte near the start of a task (from a small sample), for sizing its buckets.
fn line_density(files: &[SrcFile], t: &Task) -> f64 {
    let n = ((t.b - t.a) as usize).min(64 << 10);
    let mut buf = vec![0u8; n];
    let got = File::open(&files[t.file].path).ok().and_then(|f| read_full(&f, &mut buf, t.a).ok()).unwrap_or(0);
    let lines = memchr_iter(b'\n', &buf[..got]).count();
    if got < 4096 || lines < 16 {
        1.0 / 40.0
    } else {
        lines as f64 / got as f64
    }
}

/// Capacity for a bucket expected to hold `m` records (mean plus a few standard deviations).
fn bucket_cap(m: f64) -> usize {
    (m + 4.0 * m.sqrt()) as usize + 16
}

/// The link map, waiting (and running other pending work) until it is built.
fn wait_link(ctx: &Ctx) -> &LinkMap {
    loop {
        if let Some(l) = ctx.link.get() {
            return l;
        }
        if rayon::yield_now() != Some(rayon::Yield::Executed) {
            std::thread::yield_now();
        }
    }
}

fn parse_events(files: &[SrcFile], t: &Task, ctx: &Ctx) -> Res<EvOut> {
    let est = (t.b - t.a) as f64 * line_density(files, t);
    let mut dedup: Wc<DRec, WC> = Wc::new(NE, bucket_cap(est / NE as f64), DEMPTY);
    let mut person: Wc<PRec, WC> = Wc::new(NP, bucket_cap(est * 0.85 / NP as f64), PZERO);
    let mut pur: Vec<Vec<PurRec>> = (0..NP).map(|_| Vec::with_capacity(bucket_cap(est * 0.035 / NP as f64))).collect();
    let mut long = Vec::new();
    let mut rec = t.base;
    let mut pend = [Pend { ukey: 0, user: &[], ts: 0, price: 0, rec: 0, cat: 0, kind: 0 }; BATCH];
    let mut np = 0usize;
    let mut link: Option<&LinkMap> = ctx.link.get();
    let mut backlog: Vec<Pend> = Vec::new();
    let mut on_line = |f: &[&[u8]; 7]| {
        let eid = f[0];
        if eid.is_empty() {
            return;
        }
        let ingest = parse_ingest(f[3]);
        let key = key_of(eid);
        dedup.push((mix(key) >> (64 - EBITS)) as usize, DRec {
            key,
            ingest: (ingest / 1000).clamp(0, u32::MAX as i64) as u32,
            rec,
        });
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
        let cat = if kind == 3 { ctx.product.get(f[5]).unwrap_or(ctx.unknown) } else { 0 };
        let ukey = key_of(user);
        if let Some(l) = link {
            l.prefetch(ukey);
        }
        // long ids keep their bytes (the read buffer is reused)
        let user: B = if is_long(ukey) { keep(user) } else { &[] };
        pend[np] = Pend { ukey, user, ts: (t - ctx.ws) as u32, price, rec: r, cat, kind };
        np += 1;
        if np == BATCH {
            if link.is_none() {
                link = ctx.link.get();
                if let Some(l) = link {
                    flush_events(&backlog, l, &mut person, &mut pur, &mut long);
                    backlog = Vec::new();
                }
            }
            match link {
                Some(l) => flush_events(&pend, l, &mut person, &mut pur, &mut long),
                None => backlog.extend_from_slice(&pend),
            }
            np = 0;
        }
    };
    read_task(files, t, |piece| scan::<7>(&Chunk { data: piece, skip_first: false, base: 0 }, &mut on_line))?;
    let l = wait_link(ctx);
    flush_events(&backlog, l, &mut person, &mut pur, &mut long);
    flush_events(&pend[..np], l, &mut person, &mut pur, &mut long);
    Ok(EvOut { dedup: dedup.finish(), person: person.finish(), pur, long })
}

fn parse_clicks(files: &[SrcFile], t: &Task, ctx: &Ctx) -> Res<(Vec<Vec<CRec>>, Vec<(u64, B)>)> {
    let est = (t.b - t.a) as f64 * line_density(files, t);
    let mut out: Vec<Vec<CRec>> = (0..NP).map(|_| Vec::with_capacity(bucket_cap(est * 0.9 / NP as f64))).collect();
    let mut long = Vec::new();
    let lo = ctx.ws - WEEK_MS;
    let mut pend: [(u64, u64, i64, u32); BATCH] = [(0, 0, 0, 0); BATCH];
    let mut np = 0usize;
    let mut link: Option<&LinkMap> = ctx.link.get();
    let mut backlog: Vec<(u64, u64, i64, u32)> = Vec::new();
    let flush = |pend: &[(u64, u64, i64, u32)], link: &LinkMap, out: &mut Vec<Vec<CRec>>| {
        for &(ukey, cid, ts, camp) in pend {
            let pkey = link.person(ukey);
            out[(mix(pkey) >> (64 - PBITS)) as usize].push(CRec { pkey, cid, ts, camp });
        }
    };
    let mut on_line = |f: &[&[u8]; 4]| {
        let (cid, user) = (f[0], f[1]);
        if cid.is_empty() || user.is_empty() {
            return;
        }
        let Some(ts) = parse_ts(f[3]) else { return };
        if ts < lo || ts >= ctx.we {
            return;
        }
        let Some(camp) = ctx.camp.get(f[2]) else { return };
        let ukey = key_of(user);
        let ck = key_of(cid);
        if is_long(ck) {
            long.push((ck, keep(cid)));
        }
        if let Some(l) = link {
            l.prefetch(ukey);
        }
        pend[np] = (ukey, ck, ts, camp);
        np += 1;
        if np == BATCH {
            if link.is_none() {
                link = ctx.link.get();
                if let Some(l) = link {
                    flush(&backlog, l, &mut out);
                    backlog = Vec::new();
                }
            }
            match link {
                Some(l) => flush(&pend, l, &mut out),
                None => backlog.extend_from_slice(&pend),
            }
            np = 0;
        }
    };
    read_task(files, t, |piece| scan::<4>(&Chunk { data: piece, skip_first: false, base: 0 }, &mut on_line))?;
    let l = wait_link(ctx);
    flush(&backlog, l, &mut out);
    flush(&pend[..np], l, &mut out);
    Ok((out, long))
}

thread_local! {
    static DTABLE: std::cell::RefCell<Vec<DRec>> = const { std::cell::RefCell::new(Vec::new()) };
}

fn dedup_bucket(b: usize, parsed: &[EvOut], winners: &[AtomicU64]) {
    let total: usize = parsed.iter().map(|p| p.dedup[b].len()).sum();
    if total == 0 {
        return;
    }
    let cap = (total + total / 2).next_power_of_two();
    let mask = cap - 1;
    DTABLE.with(|cell| {
        let mut tab = cell.borrow_mut();
        if tab.len() < cap {
            tab.resize(cap, DEMPTY);
        }
        // invariant: the table is all-empty between buckets
        let table = &mut tab[..cap];
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
        for t in table.iter_mut() {
            if t.key != 0 {
                winners[(t.rec >> 6) as usize].fetch_or(1u64 << (t.rec & 63), Ordering::Relaxed);
                *t = DEMPTY;
            }
        }
    });
}

#[inline]
fn is_winner(w: &[AtomicU64], rec: u32) -> bool {
    w[(rec >> 6) as usize].load(Ordering::Relaxed) & (1u64 << (rec & 63)) != 0
}

/// Per-thread state for the person stage; buffers are reused across buckets.
struct PState {
    camp_agg: Vec<[u64; 4]>,
    users: Vec<URow>,
    pid_of: FxHashMap<u64, u32>,
    pkeys: Vec<u64>,
    ids: Vec<u32>,
    start: Vec<u32>,
    fill: Vec<u32>,
    evs: Vec<(u32, u32, u16, u8)>, // ts, price, cat, kind
    cids: Vec<u32>,
    cstart: Vec<u32>,
    cks: Vec<(i64, u64, u32)>, // ts, click_id, campaign
    cnt: Vec<u32>,
    apos: Vec<u32>,
    active: Vec<u32>,
    catsum: Vec<u64>,
    touched: Vec<u16>,
}

impl PState {
    fn new(ctx: &Ctx) -> Self {
        PState {
            camp_agg: vec![[0u64; 4]; (ctx.ncamp + 1) * ctx.nd],
            users: Vec::new(),
            pid_of: FxHashMap::default(),
            pkeys: Vec::new(),
            ids: Vec::new(),
            start: Vec::new(),
            fill: Vec::new(),
            evs: Vec::new(),
            cids: Vec::new(),
            cstart: Vec::new(),
            cks: Vec::new(),
            cnt: vec![0; ctx.ncamp],
            apos: vec![0; ctx.ncamp],
            active: Vec::new(),
            catsum: vec![0; ctx.ncat],
            touched: Vec::new(),
        }
    }
}

const NONE: u32 = u32::MAX;

/// Counting sort setup: group starts from ids (NONE = skipped); `fill` gets per-group cursors.
fn group_starts(ids: &[u32], ngroups: usize, start: &mut Vec<u32>, fill: &mut Vec<u32>) -> usize {
    start.clear();
    start.resize(ngroups + 1, 0);
    for &id in ids {
        if id != NONE {
            start[id as usize + 1] += 1;
        }
    }
    for i in 0..ngroups {
        start[i + 1] += start[i];
    }
    fill.clear();
    fill.extend_from_slice(&start[..ngroups]);
    start[ngroups] as usize
}

fn person_bucket(
    b: usize,
    parsed: &[EvOut],
    clicks: &[Vec<Vec<CRec>>],
    winners: &[AtomicU64],
    ctx: &Ctx,
    lcid: &FxHashMap<u64, B>,
    st: &mut PState,
) {
    let nd = ctx.nd;
    let organic = ctx.ncamp;
    let PState {
        camp_agg,
        users,
        pid_of,
        pkeys,
        ids,
        start,
        fill,
        evs,
        cids,
        cstart,
        cks,
        cnt,
        apos,
        active,
        catsum,
        touched,
    } = st;

    // pass 1: surviving events -> local person ids
    pid_of.clear();
    pkeys.clear();
    ids.clear();
    for p in parsed {
        for e in &p.person[b] {
            if !is_winner(winners, e.rk >> 2) {
                ids.push(NONE);
                continue;
            }
            let id = *pid_of.entry(e.pkey).or_insert_with(|| {
                pkeys.push(e.pkey);
                (pkeys.len() - 1) as u32
            });
            ids.push(id);
        }
    }
    for p in parsed {
        for e in &p.pur[b] {
            if !is_winner(winners, e.rec) {
                ids.push(NONE);
                continue;
            }
            let id = *pid_of.entry(e.pkey).or_insert_with(|| {
                pkeys.push(e.pkey);
                (pkeys.len() - 1) as u32
            });
            ids.push(id);
        }
    }
    let np = pkeys.len();
    let n = group_starts(ids, np, start, fill);
    evs.clear();
    evs.resize(n, (0, 0, 0, 0));
    let mut x = 0;
    for p in parsed {
        for e in &p.person[b] {
            let id = ids[x];
            x += 1;
            if id != NONE {
                let at = &mut fill[id as usize];
                evs[*at as usize] = (e.ts, 0, 0, (e.rk & 3) as u8);
                *at += 1;
            }
        }
    }
    for p in parsed {
        for e in &p.pur[b] {
            let id = ids[x];
            x += 1;
            if id != NONE {
                let at = &mut fill[id as usize];
                evs[*at as usize] = (e.ts, e.price, e.cat, 3);
                *at += 1;
            }
        }
    }

    // clicks of known persons, grouped the same way
    cids.clear();
    for c in clicks {
        for k in &c[b] {
            cids.push(pid_of.get(&k.pkey).copied().unwrap_or(NONE));
        }
    }
    let cn = group_starts(cids, np, cstart, fill);
    cks.clear();
    cks.resize(cn, (0, 0, 0));
    let mut x = 0;
    for c in clicks {
        for k in &c[b] {
            let id = cids[x];
            x += 1;
            if id != NONE {
                let at = &mut fill[id as usize];
                cks[*at as usize] = (k.ts, k.cid, k.camp);
                *at += 1;
            }
        }
    }

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
        let ev = &mut evs[start[p] as usize..start[p + 1] as usize];
        ev.sort_unstable_by_key(|e| e.0);
        let ck = &mut cks[cstart[p] as usize..cstart[p + 1] as usize];
        if ck.len() > 1 {
            ck.sort_unstable_by(|a, b| {
                a.0.cmp(&b.0).then_with(|| {
                    if !is_long(a.1) && !is_long(b.1) {
                        a.1.cmp(&b.1)
                    } else {
                        key_bytes(a.1, lcid).cmp(&key_bytes(b.1, lcid))
                    }
                })
            });
        }
        let pkey = pkeys[p];
        let mut lo = 0usize;
        let mut hi = 0usize;

        let mut cur_day: i64 = i64::MIN;
        let (mut us, mut ue, mut ur) = (0u64, 0u64, 0u64);
        let flush = |day: i64, us: u64, ue: u64, ur: u64, touched: &mut Vec<u16>, catsum: &mut Vec<u64>, users: &mut Vec<URow>| {
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
                    flush(cur_day, us, ue, ur, touched, catsum, users);
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
                if e.3 == 3 {
                    purch = true;
                    rev += e.1 as u64;
                    let c = e.2 as usize;
                    if catsum[c] == 0 {
                        touched.push(e.2);
                    }
                    catsum[c] += e.1 as u64;
                }
                k += 1;
            }
            us += 1;
            ue += nev;
            ur += rev;

            while hi < ck.len() && ck[hi].0 <= s {
                add(ck[hi].2, cnt, apos, active);
                hi += 1;
            }
            while lo < hi && ck[lo].0 < s - WEEK_MS {
                remove(ck[lo].2, cnt, apos, active);
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
            for &c in active.iter() {
                camp_agg[c as usize * nd + d][3] += 1;
            }
        }
        if cur_day != i64::MIN {
            flush(cur_day, us, ue, ur, touched, catsum, users);
        }
        while lo < hi {
            remove(ck[lo].2, cnt, apos, active);
            lo += 1;
        }
    }
}

// ---------------------------------------------------------------- parquet

mod pq;
use pq::{Col, Logical, BYTE_ARRAY, INT32, INT64};

fn write_users(path: &Path, parts: &[Vec<URow>], cat_names: &[B], long: &FxHashMap<u64, B>) -> Res<()> {
    let n: usize = parts.iter().map(|p| p.len()).sum();
    let mut uid = Col::new("user_id", BYTE_ARRAY, Logical::String, false, n * 14);
    let mut day = Col::new("day", INT32, Logical::Date, false, n * 4);
    let mut ses = Col::new("sessions", INT64, Logical::None, false, n * 8);
    let mut evs = Col::new("events", INT64, Logical::None, false, n * 8);
    let mut rev = Col::new("revenue_cents", INT64, Logical::None, false, n * 8);
    let mut top = Col::new("top_category", BYTE_ARRAY, Logical::String, true, n * 8);
    let mut name = Vec::with_capacity(64);
    for r in parts.iter().flatten() {
        name.clear();
        if is_long(r.pkey) {
            name.extend_from_slice(long.get(&r.pkey).copied().unwrap_or(&[]));
        } else {
            for i in 0..MAXPACK {
                let code = (r.pkey >> (6 * (MAXPACK - 1 - i))) & 63;
                if code == 0 {
                    break;
                }
                name.push(DECODE[code as usize]);
            }
        }
        uid.put_bytes(&name);
        day.put_i32(r.day);
        ses.put_i64(r.sessions as i64);
        evs.put_i64(r.events as i64);
        rev.put_i64(r.revenue as i64);
        top.put_opt_bytes(r.top.map(|c| cat_names[c as usize]));
    }
    pq::write(path, &[uid, day, ses, evs, rev, top], n)?;
    Ok(())
}

fn write_campaigns(path: &Path, agg: &[[u64; 4]], ids: &[B], ch: &[B], day0: i64, nd: usize) -> Res<()> {
    let mut cid = Col::new("campaign_id", BYTE_ARRAY, Logical::String, false, 0);
    let mut chn = Col::new("channel", BYTE_ARRAY, Logical::String, false, 0);
    let mut day = Col::new("day", INT32, Logical::Date, false, 0);
    let mut ses = Col::new("sessions", INT64, Logical::None, false, 0);
    let mut pur = Col::new("purchasing_sessions", INT64, Logical::None, false, 0);
    let mut rev = Col::new("revenue_cents", INT64, Logical::None, false, 0);
    let mut ast = Col::new("assisted_sessions", INT64, Logical::None, false, 0);
    let mut n = 0;
    for c in 0..=ids.len() {
        for d in 0..nd {
            let a = agg[c * nd + d];
            if a[0] > 0 || a[3] > 0 {
                let organic = c == ids.len();
                cid.put_bytes(if organic { b"organic" } else { ids[c] });
                chn.put_bytes(if organic { b"organic" } else { ch[c] });
                day.put_i32((day0 + d as i64) as i32);
                ses.put_i64(a[0] as i64);
                pur.put_i64(a[1] as i64);
                rev.put_i64(a[2] as i64);
                ast.put_i64(if organic { 0 } else { a[3] as i64 });
                n += 1;
            }
        }
    }
    pq::write(path, &[cid, chn, day, ses, pur, rev, ast], n)?;
    Ok(())
}
