# SimpleStore Index Design

## Overview

The `SimpleMapStore` uses an **inverted index** (label-primary) layout to store precomputed aggregates. This design aligns the storage structure with the query return type (`HashMap<Option<KeyByLabelValues>, Vec<TimestampedBucket>>`), eliminating the need for regrouping at query time.

## Data Structure

### Per-Key Store (`per_key.rs`)

Each `aggregation_id` maps to a `StoreKeyData` protected by an `RwLock`:

```
DashMap<aggregation_id, Arc<RwLock<StoreKeyData>>>

StoreKeyData {
    label_map:         HashMap<Option<KeyByLabelValues>, BTreeMap<(start, end), Vec<Arc<dyn AggregateCore>>>>
    window_to_labels:  HashMap<(start, end), HashSet<Option<KeyByLabelValues>>>
    time_ranges: BTreeSet<(start, end)>
    read_counts: Mutex<HashMap<(start, end), u64>>
}
```

- **`label_map`** (primary index): Inverted index from label key to a time-sorted BTreeMap of aggregates. Enables O(log n + k) range queries per label.
- **`window_to_labels`** (reverse index): For each time window, tracks exactly which labels contain data. Enables exact queries and cleanup to avoid full label scans.
- **`time_ranges`** (secondary index): All known timestamp ranges across all labels. Used for cleanup counting and read-count tracking.
- **`read_counts`**: Wrapped in `Mutex` so queries can use a read lock on the outer `RwLock` (only needs brief exclusive access to increment counts).

### Global Store (`global.rs`)

Same inverted index structure, but nested under a single `Mutex<StoreData>`:

```
Mutex<StoreData>

StoreData {
    store:            HashMap<aggregation_id, HashMap<Option<KeyByLabelValues>, BTreeMap<(start, end), Vec<Arc<dyn AggregateCore>>>>>
    window_to_labels: HashMap<aggregation_id, HashMap<(start, end), HashSet<Option<KeyByLabelValues>>>>
    time_ranges:      HashMap<aggregation_id, BTreeSet<(start, end)>>
    read_counts:      HashMap<aggregation_id, HashMap<(start, end), u64>>
}
```

No inner Mutex for `read_counts` since the outer Mutex already serializes all access.

## Theoretical Complexity

### Variables

| Symbol | Meaning |
|---|---|
| A | Number of distinct aggregation IDs |
| L | Number of distinct label combinations (cardinality) |
| N | Number of distinct time windows stored per (agg_id, label) |
| k | Number of results matched or entries removed in a given operation |
| m | Number of labels present in a specific time window |
| V | Number of aggregate objects stored per (label, window) slot (typically 1) |

### Time Complexity

| Operation | Time | Notes |
|---|---|---|
| **Insert** (single entry) | **O(log N)** | DashMap O(1) + RwLock O(1) + HashMap O(1) + BTreeMap O(log N) + BTreeSet O(log N) |
| **Insert** (batch of B entries, same agg_id) | **O(B · log N)** | One write-lock acquisition amortized over B items |
| **Range query** | **O(L · (log N + k))** | BTreeMap::range per label in O(log N + k_L); results already grouped by label |
| **Exact query** | **O(m · log N)** | window_to_labels lookup O(1) + BTreeMap point get O(log N) per matching label |
| **CircularBuffer cleanup** | **O(k · m)** amortized | BTreeSet iteration O(k) + targeted label-map removals via window_to_labels |
| **ReadBased cleanup** | **O(N + k · m)** | Full read_counts scan O(N) + targeted removals O(k · m) |
| **get_earliest_timestamp** | **O(A)** | DashMap iteration over A entries with atomic loads |

### Space Complexity

| Structure | Space | Notes |
|---|---|---|
| `label_map` | O(A · L · N · V) | Primary index: agg_id → label → BTreeMap(window → Vec<Arc<Agg>>) |
| `window_to_labels` | O(A · N · L) | Reverse index: agg_id → window → HashSet\<label\> |
| `time_ranges` | O(A · N) | Secondary index: agg_id → BTreeSet of all windows |
| `read_counts` | O(A · N) | agg_id → HashMap\<window, u64\> |
| **Total** | **O(A · L · N · V)** | Dominated by the primary label_map |

Arc-sharing means query results hold references into the store; no deep copies are made for read paths.

### Operation Complexity Summary

| Operation | Complexity |
|---|---|
| Range query | O(L × (log N + k)) via `BTreeMap::range()`, already grouped by label |
| Exact query | O(m × log N) where m = labels present in target window (via reverse index) |
| Insert | O(log N) BTreeMap insert per label |
| CircularBuffer cleanup | O(k × m) iterate first k from `BTreeSet` + targeted removals via `window_to_labels` |
| ReadBased cleanup | O(N + k × m) scan `read_counts` + targeted removals via `window_to_labels` |
| Space | O(A × L × N × V) — proportional to stored aggregates, not index overhead |

## Query Mechanics

### Range Query

For a query with `[start, end]`:

1. For each label in `label_map`, use `btree.range((start, 0)..=(end, u64::MAX))` to find candidate entries in O(log n)
2. Filter by `range_end <= end` (BTreeMap range only bounds `range_start`)
3. Results are already in chronological order (BTreeMap iteration order) and grouped by label
4. Update `read_counts` via the `time_ranges` secondary index

### Exact Query

For exact match `(exact_start, exact_end)`:

1. Use `window_to_labels` to get labels that actually have that window
2. For those labels only, use `btree.get(&(exact_start, exact_end))` for O(log n) lookup
2. Results are already grouped by label

## Cleanup Policies

### CircularBuffer

Retains the newest `configured_limit * 4` time ranges:

1. Check `time_ranges.len()` against the retention limit
2. Iterate `time_ranges` from the start (oldest first, already sorted by BTreeSet)
3. Remove excess entries from `time_ranges`, `read_counts`, and reverse index
4. Remove from only affected label BTrees using `window_to_labels` membership

### ReadBased

Removes entries that have been read `>= threshold` times:

1. Scan `read_counts` for entries meeting the threshold
2. Remove from `read_counts`, `time_ranges`, and reverse index
3. Remove from only affected label BTrees using `window_to_labels` membership

## Concurrency (Per-Key Store)

The per-key store uses a read-lock optimization:

- **Insert**: Acquires a write lock on the `RwLock` (exclusive access needed for `label_map` and `time_ranges`)
- **Query**: Acquires a read lock on the `RwLock` (multiple queries can run concurrently). Updates `read_counts` by briefly locking the inner `Mutex`
- **Cleanup**: Runs during insert (under write lock), accesses `read_counts` via `Mutex::get_mut()` (no lock needed since `&mut self` guarantees exclusive access)
