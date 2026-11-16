//! Capture one application's output stream, named by its `object.serial`, to a 32-bit float
//! WAV file (issue #8). With no serial it lists the application streams and who they belong to.
//!
//! ```text
//! cargo run --example app_stream                              # list streams
//! cargo run --example app_stream -- <serial> [secs] [out.wav] [channels]
//! ```
//!
//! The capture is 48 kHz; `channels` is 1 or 2 (default 2). It stops early if the stream ends.
//! Needs a running PipeWire session.

use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dmn_audio::{CaptureState, PipeWireContext, StreamCapture, StreamError};

const RATE: u32 = 48_000;

fn main() {
    if let Err(e) = run() {
        eprintln!("{e}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let ctx = Arc::new(PipeWireContext::connect()?);
    let table = ctx.wait_for_sync(Duration::from_secs(2));
    let Some(serial) = args.first() else {
        for (s, m) in table.streams_with_metadata() {
            println!(
                "serial {:?}: {} / {} (name set: {}, client pid {:?}, claimed pid {:?})",
                m.serial,
                s.application,
                s.media_name,
                m.application_name.is_some(),
                m.sec_pid,
                m.application_process_id,
            );
        }
        return Ok(());
    };
    let serial: u64 = serial.parse()?;
    let secs: u64 = args.get(1).map(|s| s.parse()).transpose()?.unwrap_or(5);
    let path = args.get(2).map_or("app_stream.wav", String::as_str);
    let channels: u32 = args.get(3).map(|s| s.parse()).transpose()?.unwrap_or(2);

    let mut capture = StreamCapture::new(Arc::clone(&ctx), serial, RATE, channels, 2_000)?;
    let mut reader = capture.start()?;
    capture.wait_streaming(Duration::from_secs(3))?;
    let status = capture.status();

    let mut out = hound::WavWriter::create(
        path,
        hound::WavSpec {
            channels: u16::try_from(channels)?,
            sample_rate: RATE,
            bits_per_sample: 32,
            sample_format: hound::SampleFormat::Float,
        },
    )?;
    let mut block = [0.0f32; 4800];
    let mut samples: u64 = 0;
    let start = Instant::now();
    let mut ended = None;
    while start.elapsed() < Duration::from_secs(secs) {
        let n = reader.pop(&mut block);
        for s in &block[..n] {
            out.write_sample(*s)?;
        }
        samples += n as u64;
        if status.target_gone() || status.state() == CaptureState::Error {
            ended = capture.wait_streaming(Duration::ZERO).err();
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    // Stop the producer before draining, so the tail is finite.
    capture.stop()?;
    // Whatever is still queued.
    loop {
        let n = reader.pop(&mut block);
        if n == 0 {
            break;
        }
        for s in &block[..n] {
            out.write_sample(*s)?;
        }
        samples += n as u64;
    }
    out.finalize()?;
    println!(
        "wrote {samples} samples ({} dropped) to {path}",
        status.dropped()
    );
    match ended {
        Some(StreamError::TargetGone { .. }) => println!("the stream ended"),
        Some(e) => return Err(e.into()),
        None => {}
    }
    Ok(())
}
