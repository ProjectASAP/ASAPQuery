# SimpleStore Index Design

## Overview

`SimpleMapStore` uses an **epoch-partitioned inverted index** to store precomputed aggregates. Three VictoriaMetrics-inspired optimizations are applied on top of the basic label-primary layout:

1. **Label Interning** — label combinations are mapped to compact `MetricID` (u32), reducing key size and hash cost.
2. **Epoch Partitioning** — data is split into fixed-capacity epoch slots; the oldest epoch is dropped O(1) when the cap is exceeded (CircularBuffer policy).
3. **Sorted Vec Posting Lists** — the reverse index (`window_to_ids`) stores `Vec<MetricID>` maintained in sorted order, enabling binary-search deduplication on insert and cache-friendly iteration on lookup.

---

## Data Structures

### Types (common.rs)

```rust
pub type MetricID = u32;          // compact interned label ID
pub type EpochID  = u64;          // monotonically increasing epoch counter
pub type TimestampRange = (u64, u64);  // (start_timestamp, end_timestamp)
pub type MetricBucketMap = HashMap<MetricID, Vec<(TimestampRange, Arc<dyn AggregateCore>)>>;
```

### InternTable (common.rs)

```
InternTable {
    label_to_id: HashMap<Option<KeyByLabelValues>, MetricID>
    id_to_label: Vec<Option<KeyByLabelValues>>
}
```

- `intern(label)` → O(1) amortized, no double-hashing (uses `HashMap::entry`)
- `resolve(id)` → O(1) indexed Vec lookup
- All internal index maps use `MetricID` (u32) as keys, not full label strings

### EpochData (common.rs)

One epoch holds up to `epoch_capacity` distinct time windows.

```
EpochData {
    label_map:     HashMap<MetricID, BTreeMap<TimestampRange, Vec<Arc<dyn AggregateCore>>>>
    window_to_ids: HashMap<TimestampRange, Vec<MetricID>>   // sorted (Optimization 3)
    time_ranges:   BTreeSet<TimestampRange>
}
```

- **`label_map`** (primary index): inverted index MetricID → time-sorted BTreeMap of aggregates. Enables O(log N + k) range queries per label.
- **`window_to_ids`** (reverse index): for each time window, sorted `Vec<MetricID>` of labels that contain data. Used for exact queries and targeted cleanup without full label scans.
- **`time_ranges`** (secondary index): all distinct windows in this epoch, sorted. Used for epoch range filtering (skip epochs that don't overlap the query interval) and cleanup ordering.

### Per-Key Store (per_key.rs)

Each aggregation_id gets its own `StoreKeyData` behind a per-key `RwLock`:

```
DashMap<aggregation_id, Arc<RwLock<StoreKeyData>>>

StoreKeyData {
    intern:           InternTable
    epochs:           BTreeMap<EpochID, EpochData>
    current_epoch_id: EpochID
    epoch_capacity:   Option<usize>   // None = unlimited
    max_epochs:       usize           // default 4
    read_counts:      Mutex<HashMap<TimestampRange, u64>>
}
```

`read_counts` is behind an inner `Mutex` so queries can hold a read lock on the outer `RwLock` and still update counts (brief inner lock, no write-lock upgrade needed).

### Global Store (global.rs)

Same per-key epoch structure, but all aggregation_ids share a single `Mutex<StoreData>`:

```
Mutex<StoreData>

StoreData {
    stores:      HashMap<aggregation_id, PerKeyState>
    read_counts: HashMap<aggregation_id, HashMap<TimestampRange, u64>>
}

PerKeyState {
    intern:           InternTable
    epochs:           BTreeMap<EpochID, EpochData>
    current_epoch_id: EpochID
    epoch_capacity:   Option<usize>
    max_epochs:       usize
}
```

No inner `Mutex` for `read_counts` — the outer `Mutex` already serializes all access.

---

## Theoretical Complexity

### Variables

| Symbol | Meaning |
|--------|---------|
| A | Number of distinct aggregation IDs |
| L | Number of distinct label combinations (cardinality) |
| N | Number of distinct time windows stored per (agg_id, label) |
| E | Number of epochs (bounded by `max_epochs`, default 4) |
| k | Number of results matched or entries removed |
| m | Number of labels present in a specific time window |
| V | Aggregate objects per (label, window) slot (typically 1) |

### Time Complexity

| Operation | Time | Notes |
|-----------|------|-------|
| **Insert** (single entry) | O(log N) | DashMap O(1) + RwLock O(1) + InternTable O(1) + BTreeMap O(log N) + BTreeSet O(log N) + sorted-Vec insert O(L) worst |
| **Insert** (batch B, same agg_id) | O(B · log N) | One write-lock acquisition amortized over B items |
| **Epoch rotation** (CircularBuffer) | O(1) amortized | BTreeMap insert new epoch + BTreeMap pop oldest |
| **Range query** | O(E · L · (log N + k)) | Per epoch: skip check O(1) + range scan per label O(log N + k_L); MetricID→label resolution O(L) |
| **Exact query** | O(E · m · log N) | Per epoch: reverse-index lookup O(1) + point get O(log N) per matching label; stops at first match |
| **CircularBuffer cleanup** | O(1) amortized | Epoch rotation drops entire oldest epoch |
| **ReadBased cleanup** | O(N + k · m) | Scan read_counts O(N) + targeted removals via window_to_ids O(k · m) |
| **get_earliest_timestamp** | O(A) | DashMap iteration with AtomicU64 loads |

### Space Complexity

| Structure | Space | Notes |
|-----------|-------|-------|
| `InternTable` | O(L) per agg_id | Stores each label string once |
| `label_map` (per epoch) | O(L · N · V) | Primary index across all epochs |
| `window_to_ids` | O(N · m) | Reverse index, bounded by epoch |
| `time_ranges` | O(N) per epoch | BTreeSet of distinct windows |
| `read_counts` | O(N) total | Counts keyed by TimestampRange |
| **Total** | **O(A · E · L · N · V)** | E bounded by `max_epochs` (default 4); dominated by label_map |

Arc-sharing means query results reference aggregate objects already in the store — no deep copies on read paths.

---

## Query Mechanics

### Range Query

For a query `[start, end]`:

1. Acquire **read lock** on `StoreKeyData` (concurrent queries run in parallel)
2. For each epoch in `epochs.values()`:
   - Skip if `min_tr.0 > end || max_tr.1 < start` (epoch range check, O(1) via BTreeSet first/last)
   - For each label in `label_map`, call `btree.range((start, 0)..=(end, u64::MAX))`, filter `tr.1 <= end`
   - Stream results directly into a `MetricBucketMap` (grouped by MetricID, no intermediate flat vec)
3. Resolve MetricIDs → label strings in one pass via `InternTable`
4. Lock inner `Mutex` briefly to update `read_counts`

### Exact Query

For exact match `(exact_start, exact_end)`:

1. Acquire **read lock**
2. Iterate epochs newest-first (`epochs.values().rev()`):
   - Use `window_to_ids.get(&range)` to get the sorted `Vec<MetricID>` of labels with that window
   - For each MetricID, use `label_map[id].get(&range)` — O(log N) point lookup
   - Stop at the first epoch that has the window (break after first match)
3. Resolve MetricIDs → labels, update `read_counts`

---

## Cleanup Policies

### CircularBuffer

Epoch-based eviction — O(1) amortized:

1. On first insert, set `epoch_capacity` from `num_aggregates_to_retain`
2. After each item insert, call `maybe_rotate_epoch()`:
   - If current epoch's `window_count() >= epoch_capacity`, open a new epoch (`current_epoch_id + 1`)
   - If `epochs.len() > max_epochs`, pop the oldest epoch (BTreeMap first entry) — O(1) drop of entire epoch
   - Purge dropped epoch's windows from `read_counts`

### ReadBased

Read-count triggered eviction:

1. Scan `read_counts` for windows with `count >= threshold`
2. For each such window, call `EpochData::remove_windows()`:
   - Remove from `time_ranges`, `window_to_ids`, and only the affected label BTrees (via sorted `Vec<MetricID>` from reverse index)
3. Drop any epochs that are now empty; re-create `current_epoch_id` entry if it was dropped

### NoCleanup

No eviction — data accumulates indefinitely.

---

## Concurrency (Per-Key Store)

| Operation | Lock acquired |
|-----------|--------------|
| **Insert** | `DashMap` shard lock (briefly) → `RwLock::write` for the duration of the batch |
| **Range/Exact query** | `DashMap` shard lock (briefly) → `RwLock::read` (concurrent queries run in parallel) → `Mutex::lock` on `read_counts` (briefly, while holding read lock) |
| **Cleanup** | Runs under the existing write lock; accesses `read_counts` via `Mutex::get_mut()` (no lock overhead — `&mut self` guarantees exclusivity) |

Multiple readers per aggregation_id can proceed concurrently. Writers only block readers of the same aggregation_id, not other aggregation_ids.
