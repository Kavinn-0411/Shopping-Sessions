//! Minimal Parquet writer: one row group, flat schema of REQUIRED/OPTIONAL columns, uncompressed.
//! Each column is dictionary encoded (PLAIN dictionary page + RLE_DICTIONARY data page v1) when
//! that is smaller, else one PLAIN data page. Integer columns are written as INT32 when every
//! value fits. Thrift compact protocol written by hand.

use rustc_hash::FxHashMap;
use std::io::Write;

pub const INT32: i32 = 1;
pub const INT64: i32 = 2;
pub const BYTE_ARRAY: i32 = 6;

/// Logical annotation of a column.
#[derive(Clone, Copy)]
pub enum Logical {
    None,
    String,
    Date,
}

pub struct Col {
    pub name: &'static str,
    /// INT32 (kept as is), INT64 (narrowed to INT32 when every value fits) or BYTE_ARRAY.
    pub ptype: i32,
    pub logical: Logical,
    pub optional: bool,
    /// Integer values (non-null only).
    ints: Vec<i64>,
    /// Byte-array values (non-null only): concatenated bytes and end offsets.
    bytes: Vec<u8>,
    ends: Vec<u32>,
    /// For optional columns: one flag per row (true = present).
    present: Vec<bool>,
}

impl Col {
    pub fn new(name: &'static str, ptype: i32, logical: Logical, optional: bool, cap: usize) -> Self {
        let (ints, bytes, ends) = if ptype == BYTE_ARRAY {
            (Vec::new(), Vec::with_capacity(cap), Vec::with_capacity(cap / 8))
        } else {
            (Vec::with_capacity(cap / 4), Vec::new(), Vec::new())
        };
        Col { name, ptype, logical, optional, ints, bytes, ends, present: Vec::new() }
    }
    #[inline]
    pub fn put_i32(&mut self, v: i32) {
        self.ints.push(v as i64);
    }
    #[inline]
    pub fn put_i64(&mut self, v: i64) {
        self.ints.push(v);
    }
    #[inline]
    pub fn put_bytes(&mut self, b: &[u8]) {
        self.bytes.extend_from_slice(b);
        self.ends.push(self.bytes.len() as u32);
    }
    #[inline]
    pub fn put_opt_bytes(&mut self, b: Option<&[u8]>) {
        self.present.push(b.is_some());
        if let Some(b) = b {
            self.put_bytes(b);
        }
    }
    fn str_at(&self, i: usize) -> &[u8] {
        let a = if i == 0 { 0 } else { self.ends[i - 1] as usize };
        &self.bytes[a..self.ends[i] as usize]
    }
    fn nvals(&self) -> usize {
        if self.ptype == BYTE_ARRAY {
            self.ends.len()
        } else {
            self.ints.len()
        }
    }
}

struct Tw {
    buf: Vec<u8>,
    last: Vec<i16>,
}

const T_I32: u8 = 5;
const T_I64: u8 = 6;
const T_BINARY: u8 = 8;
const T_LIST: u8 = 9;
const T_STRUCT: u8 = 12;

fn varint(buf: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

impl Tw {
    fn new() -> Self {
        Tw { buf: Vec::new(), last: vec![0] }
    }
    fn varint(&mut self, v: u64) {
        varint(&mut self.buf, v);
    }
    fn zz(&mut self, v: i64) {
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn field(&mut self, id: i16, ty: u8) {
        let last = *self.last.last().unwrap();
        let d = id - last;
        if d > 0 && d <= 15 {
            self.buf.push(((d as u8) << 4) | ty);
        } else {
            self.buf.push(ty);
            let v = id as i64;
            self.varint(((v << 1) ^ (v >> 63)) as u64);
        }
        *self.last.last_mut().unwrap() = id;
    }
    fn i32(&mut self, id: i16, v: i32) {
        self.field(id, T_I32);
        self.zz(v as i64);
    }
    fn i64(&mut self, id: i16, v: i64) {
        self.field(id, T_I64);
        self.zz(v);
    }
    fn bin(&mut self, id: i16, b: &[u8]) {
        self.field(id, T_BINARY);
        self.varint(b.len() as u64);
        self.buf.extend_from_slice(b);
    }
    fn begin(&mut self, id: i16) {
        self.field(id, T_STRUCT);
        self.last.push(0);
    }
    /// A struct that is a list element (no field header).
    fn begin_elem(&mut self) {
        self.last.push(0);
    }
    fn end(&mut self) {
        self.buf.push(0);
        self.last.pop();
    }
    fn list(&mut self, id: i16, elem: u8, n: usize) {
        self.field(id, T_LIST);
        if n < 15 {
            self.buf.push(((n as u8) << 4) | elem);
        } else {
            self.buf.push(0xF0 | elem);
            self.varint(n as u64);
        }
    }
}

// ---------------------------------------------------------------- RLE / bit-packing hybrid

/// Bit-packed run of `v` (padded with zeros to a multiple of 8 values).
fn bitpack(v: &[u32], bw: u32, out: &mut Vec<u8>) {
    let groups = v.len().div_ceil(8);
    varint(out, ((groups as u64) << 1) | 1);
    let mut acc: u64 = 0;
    let mut nb: u32 = 0;
    for k in 0..groups * 8 {
        let x = if k < v.len() { v[k] as u64 } else { 0 };
        acc |= x << nb;
        nb += bw;
        while nb >= 8 {
            out.push(acc as u8);
            acc >>= 8;
            nb -= 8;
        }
    }
    debug_assert_eq!(nb, 0);
}

fn rle_run(v: u32, len: usize, vb: usize, out: &mut Vec<u8>) {
    varint(out, (len as u64) << 1);
    out.extend_from_slice(&v.to_le_bytes()[..vb]);
}

/// Greedy hybrid: at each group boundary take a repeated run as RLE when that is smaller,
/// otherwise bit-pack the next 8 values.
fn hybrid_greedy(v: &[u32], bw: u32, out: &mut Vec<u8>) {
    let vb = bw.div_ceil(8) as usize;
    let rle_bits = 8 * (2 + vb);
    let n = v.len();
    let (mut p, mut i) = (0usize, 0usize);
    while i < n {
        let x = v[i];
        let mut j = i + 1;
        while j < n && v[j] == x {
            j += 1;
        }
        let r = j - i;
        if r >= 8 || r * bw as usize > rle_bits {
            if i > p {
                bitpack(&v[p..i], bw, out);
            }
            rle_run(x, r, vb, out);
            i = j;
            p = j;
        } else {
            i = (i + 8).min(n);
            if i - p >= 63 * 8 {
                bitpack(&v[p..i], bw, out);
                p = i;
            }
        }
    }
    if n > p {
        bitpack(&v[p..n], bw, out);
    }
}

/// Every run as RLE (good for values that come in runs, like a sorted key).
fn hybrid_rle(v: &[u32], bw: u32, out: &mut Vec<u8>) {
    let vb = bw.div_ceil(8) as usize;
    let mut i = 0;
    while i < v.len() {
        let x = v[i];
        let mut j = i + 1;
        while j < v.len() && v[j] == x {
            j += 1;
        }
        rle_run(x, j - i, vb, out);
        i = j;
    }
}

fn hybrid(v: &[u32], bw: u32) -> Vec<u8> {
    let mut a = Vec::with_capacity(v.len() * bw as usize / 8 + 16);
    hybrid_greedy(v, bw, &mut a);
    let mut b = Vec::with_capacity(a.len());
    hybrid_rle(v, bw, &mut b);
    if b.len() < a.len() {
        b
    } else {
        a
    }
}

// ---------------------------------------------------------------- column encoding

struct Enc {
    ptype: i32,
    /// PLAIN dictionary values and their count, when dictionary encoded.
    dict: Option<(Vec<u8>, usize)>,
    /// Data page body after the definition levels: PLAIN values, or bit width + hybrid indices.
    data: Vec<u8>,
}

fn encode(c: &Col) -> Enc {
    let n = c.nvals();
    if c.ptype == BYTE_ARRAY {
        let plain_len = c.bytes.len() + 4 * n;
        let mut map: FxHashMap<&[u8], u32> = FxHashMap::default();
        let mut order: Vec<u32> = Vec::new();
        let mut idx: Vec<u32> = Vec::with_capacity(n);
        let mut dict_len = 0usize;
        for i in 0..n {
            let s = c.str_at(i);
            let next = order.len() as u32;
            let id = *map.entry(s).or_insert_with(|| {
                order.push(i as u32);
                dict_len += 4 + s.len();
                next
            });
            idx.push(id);
        }
        if dict_len < plain_len {
            let bw = bits(order.len());
            let ix = hybrid(&idx, bw);
            if dict_len + 1 + ix.len() < plain_len {
                let mut d = Vec::with_capacity(dict_len);
                for &i in &order {
                    let s = c.str_at(i as usize);
                    d.extend_from_slice(&(s.len() as u32).to_le_bytes());
                    d.extend_from_slice(s);
                }
                let mut data = Vec::with_capacity(ix.len() + 1);
                data.push(bw as u8);
                data.extend_from_slice(&ix);
                return Enc { ptype: BYTE_ARRAY, dict: Some((d, order.len())), data };
            }
        }
        let mut data = Vec::with_capacity(plain_len);
        for i in 0..n {
            let s = c.str_at(i);
            data.extend_from_slice(&(s.len() as u32).to_le_bytes());
            data.extend_from_slice(s);
        }
        return Enc { ptype: BYTE_ARRAY, dict: None, data };
    }
    let fits32 = c.ints.iter().all(|&x| x >= i32::MIN as i64 && x <= i32::MAX as i64);
    let ptype = if c.ptype == INT32 || fits32 { INT32 } else { INT64 };
    let w = if ptype == INT32 { 4 } else { 8 };
    let put = |d: &mut Vec<u8>, x: i64| {
        if w == 4 {
            d.extend_from_slice(&(x as i32).to_le_bytes());
        } else {
            d.extend_from_slice(&x.to_le_bytes());
        }
    };
    let plain_len = w * n;
    let mut map: FxHashMap<i64, u32> = FxHashMap::default();
    let mut order: Vec<i64> = Vec::new();
    let mut idx: Vec<u32> = Vec::with_capacity(n);
    for &x in &c.ints {
        let next = order.len() as u32;
        let id = *map.entry(x).or_insert_with(|| {
            order.push(x);
            next
        });
        idx.push(id);
    }
    if order.len() * w < plain_len {
        let bw = bits(order.len());
        let ix = hybrid(&idx, bw);
        if order.len() * w + 1 + ix.len() < plain_len {
            let mut d = Vec::with_capacity(order.len() * w);
            for &x in &order {
                put(&mut d, x);
            }
            let mut data = Vec::with_capacity(ix.len() + 1);
            data.push(bw as u8);
            data.extend_from_slice(&ix);
            return Enc { ptype, dict: Some((d, order.len())), data };
        }
    }
    let mut data = Vec::with_capacity(plain_len);
    for &x in &c.ints {
        put(&mut data, x);
    }
    Enc { ptype, dict: None, data }
}

/// Bit width for dictionary indices 0..n (at least 1).
fn bits(n: usize) -> u32 {
    let m = n.saturating_sub(1) as u64;
    (64 - m.leading_zeros()).max(1)
}

/// Write the columns as one Parquet file with `nrows` rows.
pub fn write(path: &std::path::Path, cols: &[Col], nrows: usize) -> std::io::Result<()> {
    let encs: Vec<Enc> = cols.iter().map(encode).collect();
    let mut file: Vec<u8> =
        Vec::with_capacity(encs.iter().map(|e| e.data.len() + e.dict.as_ref().map_or(0, |d| d.0.len()) + nrows / 8 + 128).sum::<usize>() + 4096);
    file.extend_from_slice(b"PAR1");
    // (chunk offset, total size, dictionary page offset, data page offset) of each column chunk
    let mut chunks: Vec<(i64, i64, Option<i64>, i64)> = Vec::with_capacity(cols.len());
    for (c, e) in cols.iter().zip(&encs) {
        let start = file.len() as i64;
        let mut dict_off = None;
        if let Some((d, nd)) = &e.dict {
            let mut h = Tw::new();
            h.i32(1, 2); // type = DICTIONARY_PAGE
            h.i32(2, d.len() as i32); // uncompressed_page_size
            h.i32(3, d.len() as i32); // compressed_page_size
            h.begin(7); // dictionary_page_header
            h.i32(1, *nd as i32); // num_values
            h.i32(2, 0); // encoding = PLAIN
            h.end();
            h.buf.push(0); // end PageHeader
            dict_off = Some(file.len() as i64);
            file.extend_from_slice(&h.buf);
            file.extend_from_slice(d);
        }
        let mut levels: Vec<u8> = Vec::new();
        if c.optional {
            // definition levels: RLE/bit-packed hybrid, one bit-packed run, bit width 1
            let mut lv = Vec::new();
            let groups = nrows.div_ceil(8);
            varint(&mut lv, ((groups as u64) << 1) | 1);
            let mut bitsv = vec![0u8; groups];
            for (i, &p) in c.present.iter().enumerate() {
                if p {
                    bitsv[i >> 3] |= 1 << (i & 7);
                }
            }
            lv.extend_from_slice(&bitsv);
            levels.extend_from_slice(&(lv.len() as u32).to_le_bytes());
            levels.extend_from_slice(&lv);
        }
        let page_len = levels.len() + e.data.len();
        let mut h = Tw::new();
        h.i32(1, 0); // type = DATA_PAGE
        h.i32(2, page_len as i32); // uncompressed_page_size
        h.i32(3, page_len as i32); // compressed_page_size
        h.begin(5); // data_page_header
        h.i32(1, nrows as i32); // num_values
        h.i32(2, if e.dict.is_some() { 8 } else { 0 }); // encoding = RLE_DICTIONARY / PLAIN
        h.i32(3, 3); // definition_level_encoding = RLE
        h.i32(4, 3); // repetition_level_encoding = RLE
        h.end();
        h.buf.push(0); // end PageHeader
        let data_off = file.len() as i64;
        file.extend_from_slice(&h.buf);
        file.extend_from_slice(&levels);
        file.extend_from_slice(&e.data);
        chunks.push((start, file.len() as i64 - start, dict_off, data_off));
    }
    // FileMetaData
    let mut m = Tw::new();
    m.i32(1, 1); // version
    m.list(2, T_STRUCT, cols.len() + 1); // schema
    m.begin_elem();
    m.bin(4, b"schema");
    m.i32(5, cols.len() as i32); // num_children
    m.end();
    for (c, e) in cols.iter().zip(&encs) {
        m.begin_elem();
        m.i32(1, e.ptype);
        m.i32(3, if c.optional { 1 } else { 0 }); // repetition_type
        m.bin(4, c.name.as_bytes());
        match c.logical {
            Logical::None => {}
            Logical::String => {
                m.i32(6, 0); // converted_type = UTF8
                m.begin(10); // logicalType
                m.begin(1); // STRING
                m.end();
                m.end();
            }
            Logical::Date => {
                m.i32(6, 6); // converted_type = DATE
                m.begin(10);
                m.begin(6); // DATE
                m.end();
                m.end();
            }
        }
        m.end();
    }
    m.i64(3, nrows as i64); // num_rows
    m.list(4, T_STRUCT, 1); // row_groups
    m.begin_elem();
    m.list(1, T_STRUCT, cols.len()); // columns
    let mut total = 0i64;
    for ((c, e), &(off, size, dict_off, data_off)) in cols.iter().zip(&encs).zip(&chunks) {
        total += size;
        m.begin_elem();
        m.i64(2, off); // file_offset
        m.begin(3); // meta_data
        m.i32(1, e.ptype);
        if e.dict.is_some() {
            m.list(2, T_I32, 3); // encodings
            m.zz(0); // PLAIN
            m.zz(3); // RLE
            m.zz(8); // RLE_DICTIONARY
        } else {
            m.list(2, T_I32, 2);
            m.zz(0); // PLAIN
            m.zz(3); // RLE
        }
        m.list(3, T_BINARY, 1); // path_in_schema
        m.varint(c.name.len() as u64);
        m.buf.extend_from_slice(c.name.as_bytes());
        m.i32(4, 0); // codec = UNCOMPRESSED
        m.i64(5, nrows as i64); // num_values
        m.i64(6, size); // total_uncompressed_size
        m.i64(7, size); // total_compressed_size
        m.i64(9, data_off); // data_page_offset
        if let Some(d) = dict_off {
            m.i64(11, d); // dictionary_page_offset
        }
        m.end();
        m.end();
    }
    m.i64(2, total); // total_byte_size
    m.i64(3, nrows as i64); // num_rows
    m.end();
    m.buf.push(0); // end FileMetaData
    file.extend_from_slice(&m.buf);
    file.extend_from_slice(&(m.buf.len() as u32).to_le_bytes());
    file.extend_from_slice(b"PAR1");
    std::fs::File::create(path)?.write_all(&file)
}
