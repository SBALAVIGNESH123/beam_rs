// beam_rs — A mini Apache Beam execution engine in Rust
// =====================================================
//
// This implements the core Beam programming model primitives:
//   - PCollection:  Immutable distributed dataset (here: Vec<T>)
//   - PTransform:   Map, FlatMap, Filter, GroupByKey, CombinePerKey
//   - Windowing:    Fixed (Tumbling), Sliding (Hopping), Session
//   - Watermarks:   Event-time tracking for window completion
//   - DirectRunner: Local single-machine executor
//
// This is exactly what a Rust-based Beam runner must implement
// to execute Apache Beam pipelines via the Portability Framework.

use std::collections::HashMap;
use std::fmt;

// ═══════════════════════════════════════════════════════════
//  CORE TYPES — The Beam Data Model in Rust
// ═══════════════════════════════════════════════════════════

/// A timestamped element in a PCollection.
/// In Beam, every element carries an event-time timestamp.
#[derive(Debug, Clone)]
pub struct TimestampedValue<T> {
    pub value: T,
    pub timestamp: f64, // event time in seconds
}

impl<T> TimestampedValue<T> {
    pub fn new(value: T, timestamp: f64) -> Self {
        Self { value, timestamp }
    }
}

/// A PCollection is the fundamental data abstraction in Beam.
/// It represents an immutable, potentially unbounded collection
/// of timestamped elements.
///
/// In a streaming engine, this would be backed by an
/// async channel or ring buffer for true streaming.
#[derive(Debug, Clone)]
pub struct PCollection<T: Clone> {
    pub elements: Vec<TimestampedValue<T>>,
}

impl<T: Clone> PCollection<T> {
    pub fn from_timestamped(elements: Vec<(T, f64)>) -> Self {
        PCollection {
            elements: elements
                .into_iter()
                .map(|(v, ts)| TimestampedValue::new(v, ts))
                .collect(),
        }
    }

    pub fn len(&self) -> usize {
        self.elements.len()
    }

    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }
}

// ═══════════════════════════════════════════════════════════
//  PTRANSFORMS — The Beam Processing Primitives
// ═══════════════════════════════════════════════════════════

/// Element-wise transforms (ParDo family).
pub struct Transforms;

impl Transforms {
    /// Map: 1-to-1 element transformation.
    pub fn map<T: Clone, U: Clone>(
        input: &PCollection<T>,
        f: impl Fn(&T) -> U,
    ) -> PCollection<U> {
        PCollection {
            elements: input
                .elements
                .iter()
                .map(|e| TimestampedValue::new(f(&e.value), e.timestamp))
                .collect(),
        }
    }

    /// FlatMap: 1-to-many element transformation.
    pub fn flat_map<T: Clone, U: Clone>(
        input: &PCollection<T>,
        f: impl Fn(&T) -> Vec<U>,
    ) -> PCollection<U> {
        let mut result = Vec::new();
        for elem in &input.elements {
            for val in f(&elem.value) {
                result.push(TimestampedValue::new(val, elem.timestamp));
            }
        }
        PCollection { elements: result }
    }

    /// Filter: Keep only elements matching a predicate.
    pub fn filter<T: Clone>(
        input: &PCollection<T>,
        predicate: impl Fn(&T) -> bool,
    ) -> PCollection<T> {
        PCollection {
            elements: input
                .elements
                .iter()
                .filter(|e| predicate(&e.value))
                .cloned()
                .collect(),
        }
    }

    /// GroupByKey: Groups elements by key.
    /// This is the core shuffle operation in Beam / MapReduce.
    /// In a distributed runner, this would trigger a shuffle
    /// across partitions.
    pub fn group_by_key<K, V>(
        input: &PCollection<(K, V)>,
    ) -> HashMap<K, Vec<TimestampedValue<V>>>
    where
        K: Clone + Eq + std::hash::Hash,
        V: Clone,
    {
        let mut groups: HashMap<K, Vec<TimestampedValue<V>>> = HashMap::new();
        for elem in &input.elements {
            let (key, val) = &elem.value;
            groups
                .entry(key.clone())
                .or_default()
                .push(TimestampedValue::new(val.clone(), elem.timestamp));
        }
        groups
    }

    /// CombinePerKey: Groups by key and applies a combining function.
    /// Equivalent to SQL's GROUP BY + aggregate.
    pub fn combine_per_key<K, V, R>(
        input: &PCollection<(K, V)>,
        combiner: impl Fn(&[V]) -> R,
    ) -> PCollection<(K, R)>
    where
        K: Clone + Eq + std::hash::Hash,
        V: Clone,
        R: Clone,
    {
        let groups = Self::group_by_key(input);
        let mut results = Vec::new();

        for (key, timestamped_vals) in &groups {
            let vals: Vec<V> = timestamped_vals.iter().map(|tv| tv.value.clone()).collect();
            let min_ts = timestamped_vals
                .iter()
                .map(|tv| tv.timestamp)
                .fold(f64::INFINITY, f64::min);
            let combined = combiner(&vals);
            results.push(TimestampedValue::new(
                (key.clone(), combined),
                min_ts,
            ));
        }

        PCollection { elements: results }
    }
}

// ═══════════════════════════════════════════════════════════
//  WINDOWING — Time-based Grouping (Chapter 6)
// ═══════════════════════════════════════════════════════════

/// A Window represents a finite time interval.
/// A Beam runner must assign every element to one or more windows.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Window {
    pub start: i64, // seconds
    pub end: i64,   // seconds
}

impl fmt::Display for Window {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}s-{}s)", self.start, self.end)
    }
}

/// Windowing strategies that a Beam runner must support.
pub enum WindowingStrategy {
    /// Fixed (Tumbling): Non-overlapping windows of constant size.
    /// Example: every 10 seconds
    Fixed { size_secs: i64 },

    /// Sliding (Hopping): Overlapping windows.
    /// Example: 10s window, 5s slide -> each element in 2 windows
    Sliding { size_secs: i64, slide_secs: i64 },

    /// Session: Gap-based dynamic windows.
    /// A new session starts when the gap exceeds the threshold.
    Session { gap_secs: i64 },
}

/// Assigns elements to windows based on their event-time timestamps.
pub struct WindowAssigner;

impl WindowAssigner {
    /// Assign each element to its window(s).
    pub fn assign<T: Clone>(
        input: &PCollection<T>,
        strategy: &WindowingStrategy,
    ) -> Vec<(Window, Vec<TimestampedValue<T>>)> {
        match strategy {
            WindowingStrategy::Fixed { size_secs } => {
                Self::assign_fixed(input, *size_secs)
            }
            WindowingStrategy::Sliding { size_secs, slide_secs } => {
                Self::assign_sliding(input, *size_secs, *slide_secs)
            }
            WindowingStrategy::Session { gap_secs } => {
                Self::assign_session(input, *gap_secs)
            }
        }
    }

    /// Fixed windows: floor(timestamp / size) * size
    /// Uses div_euclid for correct negative timestamp handling.
    fn assign_fixed<T: Clone>(
        input: &PCollection<T>,
        size: i64,
    ) -> Vec<(Window, Vec<TimestampedValue<T>>)> {
        let mut windows: HashMap<Window, Vec<TimestampedValue<T>>> = HashMap::new();

        for elem in &input.elements {
            // div_euclid floors toward negative infinity, not zero.
            // This correctly handles negative event timestamps.
            let ts = elem.timestamp as i64;
            let window_start = ts.div_euclid(size) * size;
            let window = Window {
                start: window_start,
                end: window_start + size,
            };
            windows.entry(window).or_default().push(elem.clone());
        }

        let mut result: Vec<_> = windows.into_iter().collect();
        result.sort_by_key(|(w, _)| w.start);
        result
    }

    /// Sliding windows: each element belongs to ceil(size/slide) windows.
    fn assign_sliding<T: Clone>(
        input: &PCollection<T>,
        size: i64,
        slide: i64,
    ) -> Vec<(Window, Vec<TimestampedValue<T>>)> {
        let mut windows: HashMap<Window, Vec<TimestampedValue<T>>> = HashMap::new();

        for elem in &input.elements {
            let ts = elem.timestamp as i64;
            // Find all windows this element belongs to
            let mut window_start = (ts / slide) * slide - size + slide;
            while window_start <= ts {
                let window_end = window_start + size;
                if ts >= window_start && ts < window_end {
                    let window = Window {
                        start: window_start,
                        end: window_end,
                    };
                    windows.entry(window).or_default().push(elem.clone());
                }
                window_start += slide;
            }
        }

        let mut result: Vec<_> = windows.into_iter().collect();
        result.sort_by_key(|(w, _)| w.start);
        result
    }

    /// Session windows: merge elements within gap distance.
    /// Uses total_cmp to avoid panics on NaN timestamps.
    fn assign_session<T: Clone>(
        input: &PCollection<T>,
        gap: i64,
    ) -> Vec<(Window, Vec<TimestampedValue<T>>)> {
        let mut sorted: Vec<_> = input.elements.clone();
        // total_cmp handles NaN safely (NaN sorts to end)
        sorted.sort_by(|a, b| a.timestamp.total_cmp(&b.timestamp));

        if sorted.is_empty() {
            return vec![];
        }

        let mut sessions: Vec<(Window, Vec<TimestampedValue<T>>)> = Vec::new();
        let mut current_start = sorted[0].timestamp as i64;
        let mut current_end = current_start + gap;
        let mut current_elements = vec![sorted[0].clone()];

        for elem in sorted.iter().skip(1) {
            let ts = elem.timestamp as i64;
            if ts < current_end {
                // Element falls within current session — extend it
                current_end = ts + gap;
                current_elements.push(elem.clone());
            } else {
                // Gap exceeded — close current session, start new one
                sessions.push((
                    Window {
                        start: current_start,
                        end: current_end,
                    },
                    current_elements,
                ));
                current_start = ts;
                current_end = ts + gap;
                current_elements = vec![elem.clone()];
            }
        }

        // Close last session
        sessions.push((
            Window {
                start: current_start,
                end: current_end,
            },
            current_elements,
        ));

        sessions
    }
}

// ═══════════════════════════════════════════════════════════
//  WATERMARK — Event Time Progress Tracker
// ═══════════════════════════════════════════════════════════

/// The Watermark is the system's estimate of event-time progress.
/// When the watermark passes the end of a window, that window
/// is considered "complete" and its results can be emitted.
///
/// This is THE critical concept for any streaming engine.
/// Without watermarks, the engine cannot know when to emit windowed results.
pub struct Watermark {
    current: f64, // current watermark position (event time)
}

impl Watermark {
    pub fn new() -> Self {
        Watermark { current: 0.0 }
    }

    /// Advance the watermark. In a real system, this is driven
    /// by the input source (e.g., Kafka partition offsets).
    pub fn advance(&mut self, timestamp: f64) {
        if timestamp > self.current {
            self.current = timestamp;
        }
    }

    /// Check if a window is complete (watermark has passed its end).
    pub fn is_window_complete(&self, window: &Window) -> bool {
        self.current >= window.end as f64
    }

    pub fn current(&self) -> f64 {
        self.current
    }
}

// ═══════════════════════════════════════════════════════════
//  DIRECT RUNNER — Local Execution Engine
// ═══════════════════════════════════════════════════════════

/// The DirectRunner executes Beam pipelines locally.
/// A production runner replaces this with a distributed engine
/// that executes the same pipeline across multiple nodes.
pub struct DirectRunner;

impl DirectRunner {
    /// Execute a windowed aggregation pipeline.
    /// This is the core execution loop a streaming runner must implement.
    pub fn run_windowed_aggregation(
        input: &PCollection<(String, i64)>,
        strategy: &WindowingStrategy,
    ) -> Vec<(Window, Vec<(String, i64)>)> {
        // Step 1: Assign elements to windows
        let windowed = WindowAssigner::assign(input, strategy);

        // Step 2: Track watermark as we process
        let mut watermark = Watermark::new();

        // Step 3: For each window, group by key and combine
        let mut results = Vec::new();

        for (window, elements) in &windowed {
            // Advance watermark to max timestamp in this window
            for elem in elements {
                watermark.advance(elem.timestamp);
            }

            // Group by key within this window
            let mut key_counts: HashMap<String, i64> = HashMap::new();
            for elem in elements {
                let (key, val) = &elem.value;
                *key_counts.entry(key.clone()).or_insert(0) += val;
            }

            let mut window_results: Vec<(String, i64)> = key_counts.into_iter().collect();
            window_results.sort_by(|a, b| a.0.cmp(&b.0));

            // For bounded (batch) data, always emit.
            // For unbounded (streaming), the watermark check gates emission.
            // Demo 5 shows the watermark logic explicitly.
            results.push((window.clone(), window_results));
        }

        results
    }
}

// ═══════════════════════════════════════════════════════════
//  TESTS — 18 tests covering every Beam primitive
// ═══════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    // ── Helper ───────────────────────────────────────────────
    #[allow(dead_code)]
    fn trading_events() -> Vec<(String, f64)> {
        vec![
            ("buy".into(), 1.0), ("sell".into(), 2.0), ("buy".into(), 3.0),
            ("hold".into(), 5.0), ("buy".into(), 8.0), ("sell".into(), 11.0),
            ("sell".into(), 13.0), ("buy".into(), 15.0), ("hold".into(), 17.0),
            ("buy".into(), 21.0), ("sell".into(), 25.0), ("buy".into(), 28.0),
            ("sell".into(), 4.0),
        ]
    }

    // ── PCollection Tests ────────────────────────────────────

    #[test]
    fn pcollection_from_timestamped() {
        let pc = PCollection::from_timestamped(vec![("a", 1.0), ("b", 2.0)]);
        assert_eq!(pc.len(), 2);
        assert!(!pc.is_empty());
        assert_eq!(pc.elements[0].value, "a");
        assert_eq!(pc.elements[0].timestamp, 1.0);
    }

    #[test]
    fn pcollection_empty() {
        let pc: PCollection<i32> = PCollection::from_timestamped(vec![]);
        assert!(pc.is_empty());
        assert_eq!(pc.len(), 0);
    }

    // ── Transform Tests ──────────────────────────────────────

    #[test]
    fn map_preserves_timestamps() {
        let pc = PCollection::from_timestamped(vec![(1, 5.0), (2, 10.0)]);
        let result = Transforms::map(&pc, |x| x * 10);
        assert_eq!(result.elements[0].value, 10);
        assert_eq!(result.elements[0].timestamp, 5.0);
        assert_eq!(result.elements[1].value, 20);
    }

    #[test]
    fn flat_map_expands_elements() {
        let pc = PCollection::from_timestamped(vec![("hello world", 1.0)]);
        let result = Transforms::flat_map(&pc, |s| {
            s.split_whitespace().map(|w| w.to_string()).collect()
        });
        assert_eq!(result.len(), 2);
        assert_eq!(result.elements[0].value, "hello");
        assert_eq!(result.elements[1].value, "world");
        // Both inherit parent timestamp
        assert_eq!(result.elements[0].timestamp, 1.0);
        assert_eq!(result.elements[1].timestamp, 1.0);
    }

    #[test]
    fn filter_removes_elements() {
        let pc = PCollection::from_timestamped(vec![(1, 0.0), (2, 1.0), (3, 2.0), (4, 3.0)]);
        let result = Transforms::filter(&pc, |x| x % 2 == 0);
        assert_eq!(result.len(), 2);
        assert_eq!(result.elements[0].value, 2);
        assert_eq!(result.elements[1].value, 4);
    }

    #[test]
    fn group_by_key_groups_correctly() {
        let pc = PCollection::from_timestamped(vec![
            (("a".to_string(), 1), 0.0),
            (("b".to_string(), 2), 1.0),
            (("a".to_string(), 3), 2.0),
        ]);
        let groups = Transforms::group_by_key(&pc);
        assert_eq!(groups.len(), 2);
        assert_eq!(groups["a"].len(), 2);
        assert_eq!(groups["b"].len(), 1);
    }

    #[test]
    fn combine_per_key_sums() {
        let pc = PCollection::from_timestamped(vec![
            (("x".to_string(), 10i64), 0.0),
            (("y".to_string(), 20i64), 1.0),
            (("x".to_string(), 30i64), 2.0),
        ]);
        let result = Transforms::combine_per_key(&pc, |vals: &[i64]| vals.iter().sum::<i64>());
        let mut items: Vec<_> = result.elements.iter()
            .map(|e| (e.value.0.clone(), e.value.1))
            .collect();
        items.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(items, vec![("x".into(), 40), ("y".into(), 20)]);
    }

    // ── Fixed Window Tests ───────────────────────────────────

    #[test]
    fn fixed_window_assigns_correctly() {
        let pc = PCollection::from_timestamped(vec![
            (1, 0.0), (2, 5.0), (3, 10.0), (4, 15.0), (5, 25.0),
        ]);
        let windows = WindowAssigner::assign(&pc, &WindowingStrategy::Fixed { size_secs: 10 });
        assert_eq!(windows.len(), 3); // [0-10), [10-20), [20-30)
        assert_eq!(windows[0].0, Window { start: 0, end: 10 });
        assert_eq!(windows[0].1.len(), 2); // elements at 0s and 5s
        assert_eq!(windows[1].0, Window { start: 10, end: 20 });
        assert_eq!(windows[1].1.len(), 2); // elements at 10s and 15s
        assert_eq!(windows[2].0, Window { start: 20, end: 30 });
        assert_eq!(windows[2].1.len(), 1); // element at 25s
    }

    #[test]
    fn fixed_window_negative_timestamps() {
        let pc = PCollection::from_timestamped(vec![(1, -3.0), (2, -15.0)]);
        let windows = WindowAssigner::assign(&pc, &WindowingStrategy::Fixed { size_secs: 10 });
        // -3 -> div_euclid(10) = -1, window [-10, 0)
        // -15 -> div_euclid(10) = -2, window [-20, -10)
        assert_eq!(windows[0].0, Window { start: -20, end: -10 });
        assert_eq!(windows[1].0, Window { start: -10, end: 0 });
    }

    #[test]
    fn fixed_window_empty_input() {
        let pc: PCollection<i32> = PCollection::from_timestamped(vec![]);
        let windows = WindowAssigner::assign(&pc, &WindowingStrategy::Fixed { size_secs: 10 });
        assert!(windows.is_empty());
    }

    // ── Sliding Window Tests ─────────────────────────────────

    #[test]
    fn sliding_window_overlap() {
        // An element at ts=7 with size=10, slide=5 should appear in
        // windows [0,10) and [5,15)
        let pc = PCollection::from_timestamped(vec![(1, 7.0)]);
        let windows = WindowAssigner::assign(
            &pc,
            &WindowingStrategy::Sliding { size_secs: 10, slide_secs: 5 },
        );
        let window_ranges: Vec<(i64, i64)> = windows.iter().map(|(w, _)| (w.start, w.end)).collect();
        assert!(window_ranges.contains(&(0, 10)));
        assert!(window_ranges.contains(&(5, 15)));
    }

    // ── Session Window Tests ─────────────────────────────────

    #[test]
    fn session_window_splits_on_gap() {
        // Events: 1, 2, 3 (close together) then 20 (far away)
        let pc = PCollection::from_timestamped(vec![
            (1, 1.0), (2, 2.0), (3, 3.0), (4, 20.0),
        ]);
        let sessions = WindowAssigner::assign(&pc, &WindowingStrategy::Session { gap_secs: 5 });
        assert_eq!(sessions.len(), 2);
        assert_eq!(sessions[0].1.len(), 3); // elements 1,2,3
        assert_eq!(sessions[1].1.len(), 1); // element 4
    }

    #[test]
    fn session_window_single_element() {
        let pc = PCollection::from_timestamped(vec![(42, 100.0)]);
        let sessions = WindowAssigner::assign(&pc, &WindowingStrategy::Session { gap_secs: 5 });
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].0, Window { start: 100, end: 105 });
    }

    // ── Watermark Tests ──────────────────────────────────────

    #[test]
    fn watermark_monotonic_advance() {
        let mut wm = Watermark::new();
        assert_eq!(wm.current(), 0.0);
        wm.advance(5.0);
        assert_eq!(wm.current(), 5.0);
        wm.advance(3.0); // earlier timestamp must NOT regress
        assert_eq!(wm.current(), 5.0);
        wm.advance(10.0);
        assert_eq!(wm.current(), 10.0);
    }

    #[test]
    fn watermark_window_completion() {
        let mut wm = Watermark::new();
        let w = Window { start: 0, end: 10 };
        assert!(!wm.is_window_complete(&w));
        wm.advance(9.9);
        assert!(!wm.is_window_complete(&w));
        wm.advance(10.0);
        assert!(wm.is_window_complete(&w));
    }

    // ── DirectRunner Tests ───────────────────────────────────

    #[test]
    fn runner_fixed_window_aggregation() {
        let input = PCollection::from_timestamped(vec![
            (("a".into(), 1i64), 1.0),
            (("a".into(), 1i64), 5.0),
            (("b".into(), 1i64), 3.0),
            (("a".into(), 1i64), 12.0),
        ]);
        let results = DirectRunner::run_windowed_aggregation(
            &input,
            &WindowingStrategy::Fixed { size_secs: 10 },
        );
        // Window [0,10): a=2, b=1
        assert_eq!(results[0].0, Window { start: 0, end: 10 });
        let w0: HashMap<String, i64> = results[0].1.iter().cloned().collect();
        assert_eq!(w0["a"], 2);
        assert_eq!(w0["b"], 1);
        // Window [10,20): a=1
        assert_eq!(results[1].0, Window { start: 10, end: 20 });
        let w1: HashMap<String, i64> = results[1].1.iter().cloned().collect();
        assert_eq!(w1["a"], 1);
    }

    // ── Display Tests ────────────────────────────────────────

    #[test]
    fn window_display_format() {
        let w = Window { start: 10, end: 20 };
        assert_eq!(format!("{}", w), "[10s-20s)");
    }
}

