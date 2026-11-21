# Shopping Sessions: plan

Goal: the fastest **exactly correct** run. Practice is about 20M events (about 1.7 GB of CSV), the final is about 60M (about 5 GB), each final run is capped at 3 min, and a wrong table scores nothing.
Cutoff: **Sun 2026-11-22 11:00 AM EST**. Submissions: 2 per hour.

## Principles

1. **Correctness comes first.** Nothing ships until it matches the reference on the 36 fixtures, the dev answer, and every seed in our differential test set.
2. **The final counts, not the practice board.** The run has to stay inside 24 GB at 60M events and must never crash on new data.
3. **Clock time includes startup.** Use one static Rust binary as `CMD`, with no interpreter, shell wrapper or JVM.

## Architecture (Rust, single binary)

```
manifest/products/campaigns ─┐
links/*.csv ──► parse ──► union-find ──► id → person map (read-only)
events/*.csv ─► mmap, split into ~equal byte chunks at '\n' ─► parallel parse
                 ├─ CSV dialect check (field count, quotes, \r as data)
                 ├─ ts → i64 ms (F1 / F2 / F3, strict calendar)
                 └─ compact record {eid, ingest, ts, person, type, price, cat}
            ─► dedup on event_id (min ingest_ts, before validation)
            ─► validate + late filter
            ─► group by person, sort by ts ─► sessionize (gap > 30 min)
clicks/*.csv ─► parse ─► dedup click_id ─► validate ─► group by person, sort by ts
sessions × clicks ─► latest click within 7 days (ties: smallest click_id)
                  ─► assists (distinct campaigns in [start - 7 days, start])
aggregate ─► user_daily / campaign_daily ─► Parquet (day = DATE)
```

### Correctness traps (from rules.md and the fixtures)

- Skip only the **first line of each file**. A later line that repeats the header is data.
- Only `\n` ends a line. `\r` is data, so a trailing `\r` makes a price or ts bad, and `e3\r` is its own ID.
- A quote is allowed only around a whole field. `""` is null (empty). An empty line is malformed.
- Spaces are significant and nothing is normalized (no trimming, no case folding).
- Digits are ASCII only. F3 may have leading zeros. An F3 value too long for i64, or too short, is outside the window, not malformed.
- F1 and F2 must be valid on the calendar (leap years, no hour 24 or second 60). The offset is `±HH:MM` with HH from 00 to 23 and MM from 00 to 59, and `-00:00` is fine. No lowercase `t`/`z`, no fractions.
- Dedup happens **before** validation. The kept copy is the one with the smallest ingest_ts among copies that are not malformed (equal ingest_ts means identical copies).
- Late means `ingest - ts > 24h` (exactly 24h is kept; ingest before ts is kept).
- Equal timestamps share a session. Measure the gap between distinct timestamps.
- A session's day is the UTC date of its start.
- top_category: highest summed price, ties go to the byte-wise smallest name, `unknown` competes like any category, null with no purchases.
- Click window: `[ws - 7d, we)`. Credit: `0 <= start - click <= 7d`. Assists use the same set of clicks.
- A campaign that only assisted still gets a row with zeros in the credited columns. `organic` has assisted = 0.
- A person is named by the byte-wise smallest ID, which can be an ID that is linked but never used.

## Phases

### Phase 1: correct v1 (target: first submission)
- [x] Project skeleton: Cargo, multi-stage Dockerfile (`rust` builder → `scratch` runtime), fixtures, scripts
- [x] Build the generator in Docker; generate `dev/` (seed 1, 1M events and 250k clicks)
- [x] v1 pipeline: straightforward and parallel (one chunk of work per file, std hash maps, sort)
- [x] Minimal Parquet writer (or the `parquet` crate) with `day` as DATE
- [x] Pass all 36 fixtures and `check-dev`
- [ ] Commit, push to a public GitHub repo, submit

### Phase 2: safety net
- [ ] Generate seeds 2–10 at 1M, plus one at 20M and one at 60M
- [ ] Freeze v1 as the reference binary; write `scripts/diff_seeds.sh` (run reference and candidate, compare)
- [ ] Measure peak RSS at 60M (must stay well under 24 GB) and wall time

### Phase 3: speed (each step diffed against the reference before submitting)
- [ ] Split the work into ~equal byte chunks across all files (the largest file holds ~20% of the rows)
- [ ] Zero-copy parser over mmap; SIMD newline and separator scanning (AVX-512 / AVX2)
- [ ] Fast paths for timestamps; fast lookups for products and campaigns
- [ ] Dedup by radix-partitioning on an `event_id` hash into per-bucket tables
- [ ] Sessionize with a radix sort on packed `(person, ts_offset)` u64 keys
- [ ] Allocator (mimalloc), `MAP_POPULATE`/madvise, thread count, `target-cpu`
- [ ] Profile with `perf` and iterate

### Submission policy
- One early submission with the correct v1.
- After that, submit only builds that passed the differential tests. Keep a log of commit → practice time.
- The final commit is the fastest build that passed every check, with comfortable memory headroom at 60M.

## Verification set (every build)

36 fixtures, dev answer, seeds 1-6 at 1M, two weird-ID stress sets (`scripts/weird.py`: long/odd IDs that force the hashed-key path), 20M and 60M, all diffed against the frozen v1 outputs (`scripts/bench.sh` with `REF=v1`). Parquet also validated with pyarrow and DuckDB.

## Log

| Commit | Change | Local dev time | Practice time |
| --- | --- | --- | --- |
| v1 | Correct parallel baseline: 36/36 fixtures, dev MATCH | ~160 ms (1M, warm) | 4.5 s |
| v2d `f7422a3` | u128 keys, bucketed dedup + winner bitmap, AVX2 scanner, prefetched link table, memory reuse | 20M: ~1.8 s (slow state) | ? |
| v2h `0a4ff68` | exact 64-bit order-preserving ID keys, sharded parallel link interning, day table, 256 dedup buckets | 20M: ~0.75 s (quiet) | |
| v2m `2fbfd74` | hand-rolled Parquet writer, write-combining scatter, background unmapping | 20M: ~0.7 s (quiet); 60M: ~4.5 s, 10 GB peak | 1.68 s |
| v3f `75776cb` | per-line quote check in scanner; huge-page allocator (mmap'd big blocks, bump arena) | 20M: ~0.65-0.78 s | |
| v3g `a61d6db` | pread into reused per-thread buffers instead of mmap; click ids as packed keys | 20M: ~0.65-0.72 s; 60M: ~2.4 s, 5.8 GB peak | |
| v3h `9d4d511` | 16-byte event records (purchases on a side list), bucket sizes from sampled line density | 20M: ~0.65 s, 1.7 GB peak; 60M: 4.8 GB peak | |
| v3l `7078250` | links built on a separate 2-thread pool while parsing (records held back until ready) | ~5-7% under v3h | |

Notes from v3 work: v1 used 24 CPU-s and scored 4.5 s (local 4.24 s), so scorer cores are about as fast as local ones for compute; v2m's 1.68 s against ~0.7-1.0 s locally points at something environment-specific (file mapping / page faults / cold cache). Tried and dropped: resolving links per user-id bucket after parsing (v3d/e, slower), Bloom filter in front of the link table, non-temporal scatter stores (no gain).
