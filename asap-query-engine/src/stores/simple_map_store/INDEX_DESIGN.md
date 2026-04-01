# SimpleStore Index Design

## Overview

`SimpleMapStore` uses an **epoch-partitioned columnar store** with label interning. The design applies six optimizations targeting the two most expensive paths: ingestion and range scan.

| Opt | What | Where |
|-----|------|-------|
| 1 | Lazy `window_to_ids` index — built on first exact query, invalidated cheaply on insert | `MutableEpoch` |
| 2 | Offset-based index — stores `u32` column offsets, not `Arc` clones | `MutableEpoch::exact_query` |
| 3 | Monotonic ingest fast path — skip `HashSet` probe for consecutive same-window inserts | `MutableEpoch::insert` |
| 4 | Batch metadata hoisting — config lookup, label interning, timestamp update moved out of per-item loop | `global.rs`, `per_key.rs` |
| 5 | Columnar storage — three parallel arrays; range scan hot loop touches only `windows_col` | `MutableEpoch` |
| 6 | Pre-allocated epoch buffers — `with_capacity(prev_epoch.len())` on rotation | `maybe_rotate_epoch` |

---

## Data Structures

### Types (`common.rs`)

```rust
pub type MetricID        = u32;              // compact interned label ID
pub type EpochID         = u64;              // monotonically increasing epoch counter
pub type TimestampRange  = (u64, u64);       // (start_timestamp, end_timestamp)
pub type MetricBucketMap = HashMap<MetricID, Vec<(TimestampRange, Arc<dyn AggregateCore>)>>;
```

### `InternTable` (`common.rs`)

```
InternTable {
    label_to_id: HashMap<Option<KeyByLabelValues>, MetricID>
    id_to_label: Vec<Option<KeyByLabelValues>>
}
```

- `intern(label)` — O(1) amortized via `HashMap::entry`; no double-hashing
- `resolve(id)` — O(1) indexed Vec lookup
- All internal maps key on `MetricID` (u32), never on full label strings

### `MutableEpoch` (`common.rs`)

Active epoch: append-only insert, O(1) amortized.

```
MutableEpoch {
    // Columnar storage (Opt 5): three parallel arrays
    windows_col:     Vec<TimestampRange>
    metric_ids_col:  Vec<MetricID>
    aggregates_col:  Vec<Arc<dyn AggregateCore>>

    // Distinct-window count for epoch rotation threshold
    windows_set:     HashSet<TimestampRange>

    // Monotonic ingest fast path (Opt 3)
    last_window:     Option<TimestampRange>

    // Lazy offset index (Opt 1 + 2): built on first exact_query, None after any insert
    window_to_ids:   Option<HashMap<TimestampRange, Vec<u32>>>

    // Epoch bounds for O(1) skip check (updated incrementally on insert)
    min_start:       Option<u64>
    max_end:         Option<u64>
}
```

**Insert** (`O(1)` amortized):
- Opt 3: if incoming window == `last_window`, skip `windows_set.insert` entirely
- Three `Vec::push` calls — no secondary index maintenance
- `window_to_ids = None` — single pointer-width write to invalidate the index

**`seal()` → `SealedEpoch`** (`O(M log M)`, paid once at rotation):
- Zips the three columns into tuples, sorts by `(TimestampRange, MetricID)`, moves `Arc`s without cloning

**`exact_query(&mut self)`** (`O(M)` first call after a write, `O(m)` cached):
- Opt 1 + 2: if `window_to_ids` is `None`, build it from `windows_col` in one pass storing `u32` offsets
- Cache is valid until the next `insert`

**`range_query_into`** (`O(M)` mutable epoch):
- Opt 5: hot loop iterates only `windows_col`; aggregate pointer only chased on match

### `SealedEpoch` (`common.rs`)

Immutable epoch: flat sorted `Vec` for cache-friendly binary-search scans.

```
SealedEpoch {
    entries:   Vec<(TimestampRange, MetricID, Arc<dyn AggregateCore>)>  // sorted by (TR, MetricID)
    min_start: Option<u64>
    max_end:   Option<u64>
}
```

**`range_query_into`** (`O(log N + k)`): `partition_point` to find start, linear scan until `tr.0 > end`

**`exact_query`** (`O(log N + m)`): `partition_point` to find the window, linear scan while `tr == range`

### Per-Key Store (`per_key.rs`)

Each `aggregation_id` gets its own `StoreKeyData` behind a per-key `RwLock`:

```
DashMap<aggregation_id, Arc<RwLock<StoreKeyData>>>

StoreKeyData {
    intern:           InternTable
    current_epoch:    MutableEpoch          // always present, accepts inserts
    sealed_epochs:    BTreeMap<EpochID, SealedEpoch>
    current_epoch_id: EpochID
    epoch_capacity:   Option<usize>         // None = unlimited
    max_epochs:       usize                 // default 4
    read_counts:      Mutex<HashMap<TimestampRange, u64>>
}
```

`read_counts` is behind an inner `Mutex` so queries can hold the outer `RwLock::read` and still update counts.

### Global Store (`global.rs`)

Same per-key epoch structure, but all aggregation_ids share a single `Mutex<StoreData>`:

```
Mutex<StoreData>

StoreData {
    stores:      HashMap<aggregation_id, PerKeyState>
    read_counts: HashMap<aggregation_id, HashMap<TimestampRange, u64>>
    metrics:     HashSet<String>
}

PerKeyState {
    intern:           InternTable
    current_epoch:    MutableEpoch
    sealed_epochs:    BTreeMap<EpochID, SealedEpoch>
    current_epoch_id: EpochID
    epoch_capacity:   Option<usize>
    max_epochs:       usize
}
```

No inner `Mutex` for `read_counts` — the outer `Mutex` already serializes all access.

---

## Complexity

### Variables

| Symbol | Meaning |
|--------|---------|
| A | Distinct aggregation IDs |
| L | Distinct label combinations |
| N | Distinct time windows per epoch |
| E | Epochs retained (≤ `max_epochs`, default 4) |
| M | Total entries in an epoch (`windows_col.len()`) |
| k | Matched results or entries removed |
| m | Labels present in a specific time window |

### Time Complexity

| Operation | Time | Notes |
|-----------|------|-------|
| **Insert** | O(1) amortized | Three `Vec::push` + conditional `HashSet::insert` (skipped by Opt 3 on ordered ingest) |
| **Seal** | O(M log M) | Paid once at rotation; not on insert hot path |
| **Epoch rotation** | O(M log M + 1) | Seal current + drop oldest in O(1) |
| **Range query** (mutable epoch) | O(M) | Linear scan of `windows_col` only |
| **Range query** (sealed epoch) | O(log N + k) | Binary search + linear scan |
| **Range query** (full store) | O(M + E · (log N + k)) | One mutable scan + binary-search per sealed epoch |
| **Exact query** (first after write) | O(M) | Build `window_to_ids` from `windows_col` |
| **Exact query** (cached) | O(m) | HashMap lookup + `Arc::clone` per offset |
| **Exact query** (sealed epoch) | O(log N + m) | Binary search to window + linear scan |
| **ReadBased cleanup** | O(N + k · m) | Scan `read_counts` + targeted removal via `remove_windows` |
| **get_earliest_timestamp** | O(A) | DashMap iteration with AtomicU64 loads |

### Space

| Structure | Space |
|-----------|-------|
| `InternTable` | O(L) per agg_id |
| `MutableEpoch` columns | O(M) |
| `SealedEpoch` entries | O(M) per sealed epoch |
| `window_to_ids` (when built) | O(M) |
| `read_counts` | O(N) total |
| **Total** | **O(A · E · M)** where E ≤ `max_epochs` |

---

## Query Mechanics

### Range Query `[start, end]`

1. Acquire **read lock** on `StoreKeyData`
2. Scan `current_epoch.range_query_into(start, end)` — O(M), touches only `windows_col` in hot loop
3. For each sealed epoch (newest first):
   - Skip if `min_start > end || max_end < start` — O(1) bounds check
   - `sealed_epoch.range_query_into(start, end)` — O(log N + k) binary search + scan
4. Resolve MetricIDs → labels via `InternTable` in one pass
5. Briefly acquire inner `Mutex` to update `read_counts`

### Exact Query `(exact_start, exact_end)`

1. Acquire **write lock** (needed to potentially build the lazy `window_to_ids` index)
2. Try `current_epoch.exact_query(range)` — builds/uses cached `window_to_ids`
3. If not found, iterate `sealed_epochs.values().rev()` calling `SealedEpoch::exact_query`
4. Return owned `Vec<(MetricID, Arc<dyn AggregateCore>)>`, drop write lock
5. Re-acquire read lock to resolve MetricIDs → labels

---

## Cleanup Policies

### CircularBuffer

Epoch-based eviction — O(1) amortized per insert:

1. On first insert: set `epoch_capacity` from `num_aggregates_to_retain`
2. After each insert: call `maybe_rotate_epoch()`
   - If `current_epoch.window_count() >= epoch_capacity`: seal current epoch, open new one with `with_capacity(hint)` (Opt 6)
   - If `1 + sealed_epochs.len() > max_epochs`: pop oldest sealed epoch in O(1), purge its windows from `read_counts`

### ReadBased

Read-count triggered eviction:

1. Scan `read_counts` for windows with `count >= threshold`
2. For each such window, call `MutableEpoch::remove_windows` or `SealedEpoch::remove_windows`
3. Drop any epochs that become empty

### NoCleanup

No eviction — data accumulates indefinitely.

---

## Concurrency (Per-Key Store)

| Operation | Lock |
|-----------|------|
| **Insert** | `RwLock::write` for the batch duration |
| **Range query** | `RwLock::read` → brief `Mutex::lock` on `read_counts` |
| **Exact query** | `RwLock::write` (lazy index build) → drop → `RwLock::read` for label resolution |
| **Cleanup** | Under existing write lock; `Mutex::get_mut()` bypasses inner lock |

Multiple readers per `aggregation_id` run concurrently. Writers only block readers of the same `aggregation_id`.
