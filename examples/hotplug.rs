//! Manual hot-plug check for issue #4: connect, sync, then wait for the app-stream set to
//! change three times (a stream appearing, its info filling in, then it stopping) without
//! polling. Run on a dev machine with
//! PipeWire, and start/stop `pw-play` in another terminal:
//!
//! ```text
//! cargo run -p dmn-audio --example hotplug
//! ```

use std::time::{Duration, Instant};

use dmn_audio::PipeWireContext;

fn main() {
    let ctx = match PipeWireContext::connect() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
    };
    let mut table = ctx.wait_for_sync(Duration::from_secs(2));
    println!(
        "synced={} sources={} streams={} default={:?}",
        table.synced,
        table.sources().count(),
        table.streams().count(),
        table.default_source
    );
    let t0 = Instant::now();
    for _ in 0..3 {
        let before: Vec<String> = table
            .streams()
            .map(|s| format!("{} — {}", s.application, s.media_name))
            .collect();
        loop {
            table = ctx.wait_for_change(table.generation, Duration::from_secs(30));
            let now: Vec<String> = table
                .streams()
                .map(|s| format!("{} — {}", s.application, s.media_name))
                .collect();
            if now != before || table.disconnected.is_some() {
                println!(
                    "t+{:.2}s gen={} streams: {:?}",
                    t0.elapsed().as_secs_f64(),
                    table.generation,
                    now
                );
                break;
            }
            if t0.elapsed() > Duration::from_secs(60) {
                println!("timed out waiting for a stream change");
                return;
            }
        }
    }
}
