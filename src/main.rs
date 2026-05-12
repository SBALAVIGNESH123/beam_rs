use beam_rs::*;

fn main() {
    println!("============================================================");
    println!("  beam_rs: Apache Beam Execution Engine in Rust");
    println!("  A proof-of-concept for FlareDB's core pipeline engine");
    println!("============================================================\n");

    let events: Vec<(String, f64)> = vec![
        ("buy".into(),  1.0),  ("sell".into(), 2.0),  ("buy".into(),  3.0),
        ("hold".into(), 5.0),  ("buy".into(),  8.0),  ("sell".into(), 11.0),
        ("sell".into(), 13.0), ("buy".into(),  15.0), ("hold".into(), 17.0),
        ("buy".into(),  21.0), ("sell".into(), 25.0), ("buy".into(),  28.0),
        ("sell".into(), 4.0),
    ];

    let pcoll = PCollection::from_timestamped(events.clone());
    println!("Input: {} timestamped trading events\n", pcoll.len());

    // DEMO 1: WordCount
    println!("--- DEMO 1: WordCount via CombinePerKey ---");
    let paired: PCollection<(String, i64)> = Transforms::map(&pcoll, |w| (w.clone(), 1i64));
    let counts = Transforms::combine_per_key(&paired, |vals: &[i64]| vals.iter().sum::<i64>());
    let mut sorted: Vec<_> = counts.elements.iter().collect();
    sorted.sort_by(|a, b| a.value.0.cmp(&b.value.0));
    for e in &sorted { println!("  {}: {}", e.value.0, e.value.1); }

    // DEMO 2: Fixed Windows
    println!("\n--- DEMO 2: Fixed Windows (10s tumbling) ---");
    let paired_input = PCollection::from_timestamped(
        events.iter().map(|(w, ts)| ((w.clone(), 1i64), *ts)).collect(),
    );
    let results = DirectRunner::run_windowed_aggregation(
        &paired_input, &WindowingStrategy::Fixed { size_secs: 10 },
    );
    for (w, aggs) in &results { for (k, c) in aggs { println!("  {} {}: {}", w, k, c); } }

    // DEMO 3: Sliding Windows
    println!("\n--- DEMO 3: Sliding Windows (10s size, 5s slide) ---");
    let results = DirectRunner::run_windowed_aggregation(
        &paired_input, &WindowingStrategy::Sliding { size_secs: 10, slide_secs: 5 },
    );
    for (w, aggs) in &results { for (k, c) in aggs { println!("  {} {}: {}", w, k, c); } }

    // DEMO 4: Session Windows
    println!("\n--- DEMO 4: Session Windows (5s gap) ---");
    let results = DirectRunner::run_windowed_aggregation(
        &paired_input, &WindowingStrategy::Session { gap_secs: 5 },
    );
    for (w, aggs) in &results { for (k, c) in aggs { println!("  {} {}: {}", w, k, c); } }

    // DEMO 5: Watermark
    println!("\n--- DEMO 5: Watermark Progress ---");
    let mut wm = Watermark::new();
    let check = vec![
        Window { start: 0, end: 10 },
        Window { start: 10, end: 20 },
        Window { start: 20, end: 30 },
    ];
    for ts in [1.0, 5.0, 8.0, 11.0, 15.0, 22.0, 30.0] {
        wm.advance(ts);
        println!("  Watermark at {:.0}s:", wm.current());
        for w in &check {
            let s = if wm.is_window_complete(w) { "COMPLETE" } else { "waiting..." };
            println!("    {} {}", w, s);
        }
    }
}
