//! Minimal Parquet writer: one row group, one uncompressed PLAIN data page (v1) per column,
//! flat schema of REQUIRED/OPTIONAL columns. Thrift compact protocol written by hand.

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
    pub ptype: i32,
    pub logical: Logical,
    pub optional: bool,
    /// Values PLAIN-encoded (non-null values only).
    pub values: Vec<u8>,
    /// For optional columns: one flag per row (true = present).
    pub present: Vec<bool>,
}

impl Col {
    pub fn new(name: &'static str, ptype: i32, logical: Logical, optional: bool, cap: usize) -> Self {
        Col { name, ptype, logical, optional, values: Vec::with_capacity(cap), present: Vec::new() }
    }
    #[inline]
    pub fn put_i32(&mut self, v: i32) {
        self.values.extend_from_slice(&v.to_le_bytes());
    }
    #[inline]
    pub fn put_i64(&mut self, v: i64) {
        self.values.extend_from_slice(&v.to_le_bytes());
    }
    #[inline]
    pub fn put_bytes(&mut self, b: &[u8]) {
        self.values.extend_from_slice(&(b.len() as u32).to_le_bytes());
        self.values.extend_from_slice(b);
    }
    #[inline]
    pub fn put_opt_bytes(&mut self, b: Option<&[u8]>) {
        self.present.push(b.is_some());
        if let Some(b) = b {
            self.put_bytes(b);
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

impl Tw {
    fn new() -> Self {
        Tw { buf: Vec::new(), last: vec![0] }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.buf.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.buf.push(v as u8);
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

/// Write the columns as one Parquet file with `nrows` rows.
pub fn write(path: &std::path::Path, cols: &[Col], nrows: usize) -> std::io::Result<()> {
    let mut file: Vec<u8> = Vec::with_capacity(cols.iter().map(|c| c.values.len() + c.present.len() / 8 + 64).sum::<usize>() + 4096);
    file.extend_from_slice(b"PAR1");
    // (offset, total size) of each column chunk
    let mut chunks: Vec<(i64, i64)> = Vec::with_capacity(cols.len());
    for c in cols {
        let mut page: Vec<u8> = Vec::new();
        if c.optional {
            // definition levels: RLE/bit-packed hybrid, one bit-packed run, bit width 1
            let mut lv = Tw::new();
            let groups = nrows.div_ceil(8);
            lv.varint(((groups as u64) << 1) | 1);
            let mut bits = vec![0u8; groups];
            for (i, &p) in c.present.iter().enumerate() {
                if p {
                    bits[i >> 3] |= 1 << (i & 7);
                }
            }
            lv.buf.extend_from_slice(&bits);
            page.extend_from_slice(&(lv.buf.len() as u32).to_le_bytes());
            page.extend_from_slice(&lv.buf);
        }
        let page_len = page.len() + c.values.len();
        let mut h = Tw::new();
        h.i32(1, 0); // type = DATA_PAGE
        h.i32(2, page_len as i32); // uncompressed_page_size
        h.i32(3, page_len as i32); // compressed_page_size
        h.begin(5); // data_page_header
        h.i32(1, nrows as i32); // num_values
        h.i32(2, 0); // encoding = PLAIN
        h.i32(3, 3); // definition_level_encoding = RLE
        h.i32(4, 3); // repetition_level_encoding = RLE
        h.end();
        h.buf.push(0); // end PageHeader
        let off = file.len() as i64;
        file.extend_from_slice(&h.buf);
        file.extend_from_slice(&page);
        file.extend_from_slice(&c.values);
        chunks.push((off, file.len() as i64 - off));
    }
    // FileMetaData
    let mut m = Tw::new();
    m.i32(1, 1); // version
    m.list(2, T_STRUCT, cols.len() + 1); // schema
    m.begin_elem();
    m.bin(4, b"schema");
    m.i32(5, cols.len() as i32); // num_children
    m.end();
    for c in cols {
        m.begin_elem();
        m.i32(1, c.ptype);
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
    for (c, &(off, size)) in cols.iter().zip(&chunks) {
        total += size;
        m.begin_elem();
        m.i64(2, off); // file_offset
        m.begin(3); // meta_data
        m.i32(1, c.ptype);
        m.list(2, T_I32, 2); // encodings
        m.zz(0); // PLAIN
        m.zz(3); // RLE
        m.list(3, T_BINARY, 1); // path_in_schema
        m.varint(c.name.len() as u64);
        m.buf.extend_from_slice(c.name.as_bytes());
        m.i32(4, 0); // codec = UNCOMPRESSED
        m.i64(5, nrows as i64); // num_values
        m.i64(6, size); // total_uncompressed_size
        m.i64(7, size); // total_compressed_size
        m.i64(9, off); // data_page_offset
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
