# beam_rs

A mini Apache Beam execution engine in **pure Rust** — zero external dependencies.

Built as a proof-of-concept to explore how a streaming database engine would implement the [Apache Beam programming model](https://beam.apache.org/documentation/programming-guide/) internally.

## What This Implements

| Beam Concept | Rust Type | Description |
|---|---|---|
| **PCollection** | `PCollection<T>` | Immutable collection of timestamped elements |
| **Map** | `Transforms::map()` | 1-to-1 element transformation |
| **FlatMap** | `Transforms::flat_map()` | 1-to-many element expansion |
| **Filter** | `Transforms::filter()` | Predicate-based element removal |
| **GroupByKey** | `Transforms::group_by_key()` | Shuffle + group by key |
| **CombinePerKey** | `Transforms::combine_per_key()` | GroupByKey + aggregation (like SQL `GROUP BY`) |
| **Fixed Windows** | `WindowingStrategy::Fixed` | Non-overlapping tumbling windows |
| **Sliding Windows** | `WindowingStrategy::Sliding` | Overlapping hopping windows |
| **Session Windows** | `WindowingStrategy::Session` | Gap-based dynamic windows |
| **Watermarks** | `Watermark` | Monotonic event-time progress tracker |
| **DirectRunner** | `DirectRunner` | Local single-machine pipeline executor |

## Quick Start

```bash
# Run the demo (5 pipelines: WordCount, Fixed/Sliding/Session Windows, Watermarks)
cargo run

# Run all 17 tests
cargo test
```

## Example Output

```
--- DEMO 2: Fixed Windows (10s tumbling) ---
  [0s-10s) buy: 3
  [0s-10s) hold: 1
  [0s-10s) sell: 2
  [10s-20s) buy: 1
  [10s-20s) hold: 1
  [10s-20s) sell: 2
  [20s-30s) buy: 2
  [20s-30s) sell: 1

--- DEMO 5: Watermark Progress ---
  Watermark at 11s:
    [0s-10s) COMPLETE
    [10s-20s) waiting...
    [20s-30s) waiting...
```

## Architecture

```
                    ┌─────────────────────────────┐
                    │     Pipeline Definition      │
                    │  (Runner-agnostic Beam API)  │
                    └──────────────┬──────────────┘
                                   │
                    ┌──────────────▼──────────────┐
                    │      WindowAssigner          │
                    │  Fixed │ Sliding │ Session   │
                    └──────────────┬──────────────┘
                                   │
                    ┌──────────────▼──────────────┐
                    │       DirectRunner           │
                    │  GroupByKey + CombinePerKey   │
                    │  per window                  │
                    └──────────────┬──────────────┘
                                   │
                    ┌──────────────▼──────────────┐
                    │        Watermark             │
                    │  Tracks event-time progress  │
                    │  Gates window emission       │
                    └─────────────────────────────┘
```

## Key Design Decisions

- **`div_euclid`** for fixed window assignment — correctly handles negative event timestamps (unlike truncating integer division)
- **`total_cmp`** for session window sorting — avoids panics on NaN timestamps
- **Monotonic watermark** — `advance()` never regresses, matching real Beam/Flink semantics
- **Zero dependencies** — the entire engine is `std` only

## Tests

17 unit tests covering every component:

```
test tests::pcollection_from_timestamped ... ok
test tests::pcollection_empty ... ok
test tests::map_preserves_timestamps ... ok
test tests::flat_map_expands_elements ... ok
test tests::filter_removes_elements ... ok
test tests::group_by_key_groups_correctly ... ok
test tests::combine_per_key_sums ... ok
test tests::fixed_window_assigns_correctly ... ok
test tests::fixed_window_negative_timestamps ... ok
test tests::fixed_window_empty_input ... ok
test tests::sliding_window_overlap ... ok
test tests::session_window_splits_on_gap ... ok
test tests::session_window_single_element ... ok
test tests::watermark_monotonic_advance ... ok
test tests::watermark_window_completion ... ok
test tests::runner_fixed_window_aggregation ... ok
test tests::window_display_format ... ok

test result: ok. 17 passed; 0 failed
```

## References

- [Apache Beam Programming Guide](https://beam.apache.org/documentation/programming-guide/)
- [Streaming Systems](https://www.oreilly.com/library/view/streaming-systems/9781491983867/) — Akidau, Chernyak, Lax (Chapter 6: Streams & Tables)
- [Apache Beam Portability Framework](https://beam.apache.org/roadmap/portability/)
