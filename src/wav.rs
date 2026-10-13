//! `WavSource`: a file standing in for a capture stream. Any rate and channel count in; 16 kHz
//! mono frames out, paced at an arbitrary multiple of wall-clock for demos and live-like tests,
//! or as fast as the disk allows for the eval harness.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use hound::{SampleFormat, WavReader, WavSpec};
use rubato::audioadapter::Adapter;
use rubato::audioadapter_buffers::direct::InterleavedSlice;
use rubato::{Fft, FixedSync, Indexing, Resampler};

use crate::frames::FrameSink;
use crate::source::{AudioError, AudioSource, Source};
use crate::{FRAME_LEN, FRAME_MS, SAMPLE_RATE};

/// How fast a `WavSource` plays.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Speed {
    /// One second of audio per second of wall-clock, like a live call. Equivalent to
    /// `Factor(1.0)`, kept as its own variant since it is the common case.
    RealTime,
    /// No pacing: frames are pushed as soon as they are decoded.
    Fast,
    /// `factor`× wall-clock; the caller (`dmn replay`) validates that it is positive and
    /// finite before constructing this.
    Factor(f32),
}

/// Input frames handed to the resampler per call. Large enough to amortise the FFT, small
/// enough (about 21 ms at 48 kHz) that the first frame is not delayed noticeably.
const RESAMPLE_CHUNK: usize = 1024;

pub struct WavSource {
    path: PathBuf,
    source: Source,
    speed: Speed,
    spec: WavSpec,
    duration: Duration,
    stop: Arc<AtomicBool>,
    finished: Arc<AtomicBool>,
    thread: Option<JoinHandle<Result<(), AudioError>>>,
}

impl WavSource {
    /// Open and validate the file. Nothing is read until `start`.
    pub fn open(path: impl AsRef<Path>, source: Source, speed: Speed) -> Result<Self, AudioError> {
        let path = path.as_ref().to_path_buf();
        let reader = WavReader::open(&path).map_err(|source| AudioError::Open {
            path: path.clone(),
            source,
        })?;
        let spec = reader.spec();
        if spec.channels == 0 || spec.sample_rate == 0 {
            return Err(AudioError::Unsupported {
                path,
                detail: format!("{} channels at {} Hz", spec.channels, spec.sample_rate),
            });
        }
        match (spec.sample_format, spec.bits_per_sample) {
            (SampleFormat::Float, 32) | (SampleFormat::Int, 8 | 16 | 24 | 32) => {}
            (f, b) => {
                return Err(AudioError::Unsupported {
                    path,
                    detail: format!("{f:?} {b}-bit samples"),
                });
            }
        }
        let frames = u64::from(reader.duration());
        let duration = Duration::from_secs_f64(frames as f64 / f64::from(spec.sample_rate));
        Ok(Self {
            path,
            source,
            speed,
            spec,
            duration,
            stop: Arc::new(AtomicBool::new(false)),
            finished: Arc::new(AtomicBool::new(false)),
            thread: None,
        })
    }

    /// Length of the file.
    pub fn duration(&self) -> Duration {
        self.duration
    }

    /// Sample rate and channel count of the file (before conversion).
    pub fn input_format(&self) -> (u32, u16) {
        (self.spec.sample_rate, self.spec.channels)
    }

    /// True while the reader thread is alive: started, and neither at the end of the file nor
    /// stopped nor failed. Lets a caller wait for the file without blocking in [`wait`](Self::wait),
    /// so it can also stop early (`dmn run --replay` quitting on `q`).
    pub fn is_running(&self) -> bool {
        self.thread.as_ref().is_some_and(|h| !h.is_finished())
    }

    /// Block until the file has been delivered in full (or `stop` was called), then return the
    /// thread's result. Tests and `dmn replay` use this; a live pipeline uses `stop`.
    pub fn wait(&mut self) -> Result<(), AudioError> {
        match self.thread.take() {
            Some(handle) => handle.join().map_err(|_| AudioError::ThreadPanicked)?,
            None => Ok(()),
        }
    }
}

impl AudioSource for WavSource {
    fn source(&self) -> Source {
        self.source
    }

    fn start(&mut self, sink: FrameSink) -> Result<(), AudioError> {
        if self.thread.is_some() {
            return Err(AudioError::AlreadyStarted);
        }
        let job = Job {
            path: self.path.clone(),
            spec: self.spec,
            speed: self.speed,
            stop: Arc::clone(&self.stop),
            finished: Arc::clone(&self.finished),
        };
        let name = format!("dmn-wav-{}", self.source.label().to_lowercase());
        let handle = std::thread::Builder::new()
            .name(name)
            .spawn(move || job.run(sink))
            .map_err(|e| AudioError::Spawn(e.to_string()))?;
        self.thread = Some(handle);
        Ok(())
    }

    fn stop(&mut self) -> Result<(), AudioError> {
        self.stop.store(true, Ordering::Release);
        self.wait()
    }

    fn is_finished(&self) -> bool {
        self.finished.load(Ordering::Acquire)
    }
}

impl Drop for WavSource {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.thread.take() {
            let _ = handle.join();
        }
    }
}

/// Everything the reader thread needs; lives on that thread.
struct Job {
    path: PathBuf,
    spec: WavSpec,
    speed: Speed,
    stop: Arc<AtomicBool>,
    finished: Arc<AtomicBool>,
}

impl Job {
    fn run(self, mut sink: FrameSink) -> Result<(), AudioError> {
        let reader = WavReader::open(&self.path).map_err(|source| AudioError::Open {
            path: self.path.clone(),
            source,
        })?;
        let channels = usize::from(self.spec.channels);
        let mut samples = MonoReader::new(reader, self.spec, self.path.clone());
        let mut converter = RateConverter::new(self.spec.sample_rate)?;
        let mut pacer = Pacer::new(self.speed);
        let mut pending: Vec<f32> = Vec::with_capacity(4 * FRAME_LEN);
        let mut block = vec![0.0f32; converter.input_len().max(1)];

        loop {
            if self.stop.load(Ordering::Acquire) {
                return Ok(());
            }
            let want = converter.input_len().max(1);
            block.resize(want, 0.0);
            let got = samples.read_mono(&mut block, channels)?;
            if got == 0 {
                break;
            }
            converter.convert(&block, got, &mut pending)?;
            self.emit_full_frames(&mut pending, &mut pacer, &mut sink);
        }

        converter.flush(&mut pending)?;
        self.emit_full_frames(&mut pending, &mut pacer, &mut sink);
        if !pending.is_empty() && !self.stop.load(Ordering::Acquire) {
            pending.resize(FRAME_LEN, 0.0);
            if !pacer.wait_for_next(&self.stop) {
                sink.push_frame(&pending);
            }
        }
        self.finished.store(true, Ordering::Release);
        Ok(())
    }

    fn emit_full_frames(&self, pending: &mut Vec<f32>, pacer: &mut Pacer, sink: &mut FrameSink) {
        let mut offset = 0;
        while pending.len() - offset >= FRAME_LEN {
            if self.stop.load(Ordering::Acquire) || pacer.wait_for_next(&self.stop) {
                break;
            }
            sink.push_frame(&pending[offset..offset + FRAME_LEN]);
            offset += FRAME_LEN;
        }
        pending.drain(..offset);
    }
}

/// Reads interleaved samples of any supported format as f32 in [-1, 1] and downmixes to mono
/// by averaging channels.
struct MonoReader {
    iter: Box<dyn Iterator<Item = Result<f32, hound::Error>> + Send>,
    path_for_errors: PathBuf,
}

impl MonoReader {
    fn new(
        reader: WavReader<std::io::BufReader<std::fs::File>>,
        spec: WavSpec,
        path: PathBuf,
    ) -> Self {
        let iter: Box<dyn Iterator<Item = Result<f32, hound::Error>> + Send> =
            match (spec.sample_format, spec.bits_per_sample) {
                (SampleFormat::Float, _) => Box::new(reader.into_samples::<f32>()),
                (SampleFormat::Int, bits) => {
                    // Full scale for a `bits`-bit signed integer; `open` validated 8 ≤ bits ≤ 32.
                    let scale = 1.0 / (1u64 << (bits.clamp(8, 32) - 1)) as f32;
                    Box::new(
                        reader
                            .into_samples::<i32>()
                            .map(move |s| s.map(|v| v as f32 * scale)),
                    )
                }
            };
        Self {
            iter,
            path_for_errors: path,
        }
    }

    /// Fill `out` with mono samples; returns how many were produced (fewer at end of file).
    fn read_mono(&mut self, out: &mut [f32], channels: usize) -> Result<usize, AudioError> {
        let mut produced = 0;
        'frames: for slot in out.iter_mut() {
            let mut acc = 0.0f32;
            for c in 0..channels {
                match self.iter.next() {
                    Some(Ok(v)) => acc += v,
                    Some(Err(source)) => {
                        return Err(AudioError::Read {
                            path: self.path_for_errors.clone(),
                            source,
                        });
                    }
                    None => {
                        // A truncated final frame (c > 0) is dropped rather than half-averaged.
                        let _ = c;
                        break 'frames;
                    }
                }
            }
            *slot = acc / channels as f32;
            produced += 1;
        }
        Ok(produced)
    }
}

/// Converts mono input at any rate to `SAMPLE_RATE`. Identity when the rate already matches.
enum RateConverter {
    Identity,
    Fft {
        resampler: Box<Fft<f32>>,
        /// Output samples still to discard: the filter's startup delay.
        skip: usize,
    },
}

impl RateConverter {
    fn new(rate_in: u32) -> Result<Self, AudioError> {
        if rate_in == SAMPLE_RATE {
            return Ok(Self::Identity);
        }
        let resampler = Fft::<f32>::new(
            rate_in as usize,
            SAMPLE_RATE as usize,
            RESAMPLE_CHUNK,
            1,
            FixedSync::Input,
        )
        .map_err(|e| AudioError::Resampler(e.to_string()))?;
        let skip = resampler.output_delay();
        Ok(Self::Fft {
            resampler: Box::new(resampler),
            skip,
        })
    }

    /// Input samples the next `convert` call wants.
    fn input_len(&self) -> usize {
        match self {
            Self::Identity => FRAME_LEN * 4,
            Self::Fft { resampler, .. } => resampler.input_frames_next(),
        }
    }

    /// Convert `block[..got]` and append the output to `out`.
    fn convert(&mut self, block: &[f32], got: usize, out: &mut Vec<f32>) -> Result<(), AudioError> {
        match self {
            Self::Identity => {
                out.extend_from_slice(&block[..got]);
                Ok(())
            }
            Self::Fft { resampler, skip } => {
                let input = InterleavedSlice::new(block, 1, block.len())
                    .map_err(|e| AudioError::Resampler(e.to_string()))?;
                let indexing = Indexing {
                    input_offset: 0,
                    output_offset: 0,
                    partial_len: (got < block.len()).then_some(got),
                    active_channels_mask: None,
                };
                let produced = resampler
                    .process(&input, Some(&indexing))
                    .map_err(|e| AudioError::Resampler(e.to_string()))?;
                let frames = produced.frames();
                let data = produced.take_data();
                let data = &data[..frames];
                let drop = (*skip).min(data.len());
                *skip -= drop;
                out.extend_from_slice(&data[drop..]);
                Ok(())
            }
        }
    }

    /// Push silence through the filter so the last real samples come out.
    fn flush(&mut self, out: &mut Vec<f32>) -> Result<(), AudioError> {
        match self {
            Self::Identity => Ok(()),
            Self::Fft { resampler, .. } => {
                let delay_in = resampler.output_delay() * resampler.input_frames_next()
                    / resampler.output_frames_next().max(1);
                let zeros = vec![0.0f32; resampler.input_frames_next()];
                let mut fed = 0;
                let mut tail = Vec::new();
                while fed < delay_in + 1 {
                    self.convert(&zeros, zeros.len(), &mut tail)?;
                    fed += zeros.len();
                }
                // Only the part of the tail that corresponds to real input is wanted; the rest
                // is silence the filter produced from our zeros. Keep exactly the delay's worth.
                let keep = match self {
                    Self::Fft { resampler, .. } => resampler.output_delay(),
                    Self::Identity => 0,
                };
                tail.truncate(keep);
                out.extend_from_slice(&tail);
                Ok(())
            }
        }
    }
}

/// Holds each frame until its wall-clock slot for `RealTime` and `Factor`; a no-op in `Fast`
/// mode. The deadline for frame `n` is always `start + n * frame_interval`, computed fresh from
/// the fixed start instant rather than accumulated from previous sleeps, so pacing cannot drift
/// over a long file.
struct Pacer {
    speed: Speed,
    /// Wall-clock seconds per 20 ms frame at this speed; unused when `speed` is `Fast`.
    frame_secs: f64,
    start: Instant,
    emitted: u64,
}

/// How often a long wait wakes up to recheck `stop`, so a tiny `--speed` factor (a multi-second
/// sleep per frame) still lets `WavSource::stop` return promptly instead of blocking until the
/// next deadline.
const STOP_POLL_INTERVAL: Duration = Duration::from_millis(100);

impl Pacer {
    fn new(speed: Speed) -> Self {
        let factor = match speed {
            Speed::Factor(f) => f64::from(f),
            Speed::RealTime | Speed::Fast => 1.0,
        };
        let frame_secs = f64::from(FRAME_MS) / 1000.0 / factor;
        Self {
            speed,
            frame_secs,
            start: Instant::now(),
            emitted: 0,
        }
    }

    /// Sleep until frame `self.emitted`'s deadline, waking every `STOP_POLL_INTERVAL` to check
    /// `stop`. Returns `true` if `stop` was seen, in which case the caller must not emit the
    /// frame. A deadline too far in the future to represent as an `Instant` (an absurdly small
    /// `Factor`, not reachable through `dmn replay`'s own validation but possible through the
    /// library API) is treated as already due rather than panicking: the source runs unpaced
    /// from that point instead of crashing.
    fn wait_for_next(&mut self, stop: &AtomicBool) -> bool {
        if !matches!(self.speed, Speed::Fast) {
            let due_secs = self.emitted as f64 * self.frame_secs;
            if let Some(due) = Self::deadline(self.start, due_secs) {
                loop {
                    if stop.load(Ordering::Acquire) {
                        return true;
                    }
                    let now = Instant::now();
                    if due <= now {
                        break;
                    }
                    std::thread::sleep((due - now).min(STOP_POLL_INTERVAL));
                }
            }
        }
        self.emitted += 1;
        false
    }

    /// `None` when `due_secs` cannot be turned into an `Instant` (not finite, or beyond what
    /// `Duration`/`Instant` can represent).
    fn deadline(start: Instant, due_secs: f64) -> Option<Instant> {
        if !due_secs.is_finite() {
            return None;
        }
        Duration::try_from_secs_f64(due_secs)
            .ok()
            .and_then(|d| start.checked_add(d))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frames::{Frames, frame_channel};
    use std::f32::consts::TAU;

    /// Write a WAV whose channel `c` holds `amps[c] * sin(2π f t)`.
    fn write_sine(path: &Path, rate: u32, amps: &[f32], hz: f32, secs: f32) {
        let spec = WavSpec {
            channels: amps.len() as u16,
            sample_rate: rate,
            bits_per_sample: 16,
            sample_format: SampleFormat::Int,
        };
        let mut w = hound::WavWriter::create(path, spec).unwrap();
        let n = (rate as f32 * secs) as usize;
        for i in 0..n {
            let t = i as f32 / rate as f32;
            for &a in amps {
                let v = (a * (TAU * hz * t).sin() * i16::MAX as f32) as i16;
                w.write_sample(v).unwrap();
            }
        }
        w.finalize().unwrap();
    }

    fn drain(frames: &mut Frames) -> Vec<f32> {
        let mut out = Vec::new();
        let mut buf = [0.0; FRAME_LEN];
        while frames.pop_frame(&mut buf) {
            out.extend_from_slice(&buf);
        }
        out
    }

    fn rms(x: &[f32]) -> f32 {
        (x.iter().map(|v| v * v).sum::<f32>() / x.len() as f32).sqrt()
    }

    #[test]
    fn stereo_48k_becomes_16k_mono_within_0_1_db() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("stereo48.wav");
        write_sine(&path, 48_000, &[0.5, 0.3], 440.0, 1.0);

        let mut src = WavSource::open(&path, Source::Interviewer, Speed::Fast).unwrap();
        assert_eq!(src.input_format(), (48_000, 2));
        assert_eq!(src.duration(), Duration::from_secs(1));
        let (sink, mut frames) = frame_channel(Source::Interviewer, 200);
        src.start(sink).unwrap();
        src.wait().unwrap();
        assert!(src.is_finished());

        let out = drain(&mut frames);
        assert_eq!(frames.overruns(), 0);
        let expected = SAMPLE_RATE as usize;
        assert!(
            (out.len() as i64 - expected as i64).unsigned_abs() as usize <= FRAME_LEN,
            "got {} samples, expected about {expected}",
            out.len()
        );

        // Reference downmix: (0.5 + 0.3) / 2 = 0.4 amplitude sine → RMS 0.4 / √2.
        let ref_rms = 0.4 / 2f32.sqrt();
        let body = &out[1600..14_400]; // 100 ms .. 900 ms, clear of edges
        let db = 20.0 * (rms(body) / ref_rms).log10();
        eprintln!("downmix level error: {db:.4} dB");
        assert!(db.abs() < 0.1, "level off by {db} dB");

        // Pitch survived: 440 Hz has 880 zero crossings per second.
        let crossings = body
            .windows(2)
            .filter(|w| (w[0] < 0.0) != (w[1] < 0.0))
            .count();
        let per_sec = crossings as f32 / (body.len() as f32 / SAMPLE_RATE as f32);
        assert!(
            (per_sec - 880.0).abs() < 10.0,
            "zero crossings/s = {per_sec}"
        );
    }

    #[test]
    fn sixteen_k_mono_passes_through_exactly() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mono16.wav");
        let spec = WavSpec {
            channels: 1,
            sample_rate: SAMPLE_RATE,
            bits_per_sample: 32,
            sample_format: SampleFormat::Float,
        };
        let input: Vec<f32> = (0..FRAME_LEN * 3 + 7).map(|i| i as f32 / 1000.0).collect();
        let mut w = hound::WavWriter::create(&path, spec).unwrap();
        for &v in &input {
            w.write_sample(v).unwrap();
        }
        w.finalize().unwrap();

        let mut src = WavSource::open(&path, Source::You, Speed::Fast).unwrap();
        let (sink, mut frames) = frame_channel(Source::You, 8);
        src.start(sink).unwrap();
        src.wait().unwrap();
        let out = drain(&mut frames);
        assert_eq!(
            out.len(),
            FRAME_LEN * 4,
            "last partial frame is zero-padded"
        );
        assert_eq!(&out[..input.len()], &input[..]);
        assert!(out[input.len()..].iter().all(|&v| v == 0.0));
    }

    #[test]
    fn fast_finishes_quickly_and_realtime_paces_at_one_x() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("half.wav");
        write_sine(&path, 44_100, &[0.2], 300.0, 0.5);

        let t = Instant::now();
        let mut fast = WavSource::open(&path, Source::You, Speed::Fast).unwrap();
        let (sink, mut frames) = frame_channel(Source::You, 100);
        fast.start(sink).unwrap();
        fast.wait().unwrap();
        let fast_elapsed = t.elapsed();
        let n_fast = drain(&mut frames).len();

        let t = Instant::now();
        let mut rt = WavSource::open(&path, Source::You, Speed::RealTime).unwrap();
        let (sink, mut frames) = frame_channel(Source::You, 100);
        rt.start(sink).unwrap();
        rt.wait().unwrap();
        let rt_elapsed = t.elapsed();
        let n_rt = drain(&mut frames).len();

        assert_eq!(n_fast, n_rt);
        assert!(
            fast_elapsed < Duration::from_millis(150),
            "fast took {fast_elapsed:?}"
        );
        assert!(
            rt_elapsed >= Duration::from_millis(450) && rt_elapsed < Duration::from_millis(900),
            "real-time took {rt_elapsed:?} for 0.5 s of audio"
        );
    }

    #[test]
    fn is_running_covers_start_to_end_and_an_early_stop() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("one.wav");
        write_sine(&path, 16_000, &[0.2], 300.0, 1.0);

        let mut src = WavSource::open(&path, Source::You, Speed::RealTime).unwrap();
        assert!(!src.is_running(), "not started");
        let (sink, _frames) = frame_channel(Source::You, 100);
        src.start(sink).unwrap();
        assert!(src.is_running());
        // Stopping a 1 s real-time file after 100 ms returns at once and ends the thread.
        std::thread::sleep(Duration::from_millis(100));
        let t = Instant::now();
        src.stop().unwrap();
        assert!(
            t.elapsed() < Duration::from_millis(200),
            "{:?}",
            t.elapsed()
        );
        assert!(!src.is_running());
        assert!(!src.is_finished(), "stopped, not delivered in full");
    }

    #[test]
    fn factor_paces_at_n_times_real_time() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("one.wav");
        write_sine(&path, 16_000, &[0.2], 300.0, 1.0);

        // 4× a 1 s file should land near 250 ms, not the ~1 s of RealTime or the near-0 of Fast.
        let t = Instant::now();
        let mut src = WavSource::open(&path, Source::You, Speed::Factor(4.0)).unwrap();
        let (sink, mut frames) = frame_channel(Source::You, 100);
        src.start(sink).unwrap();
        src.wait().unwrap();
        let elapsed = t.elapsed();
        assert_eq!(drain(&mut frames).len(), SAMPLE_RATE as usize);
        assert!(
            elapsed >= Duration::from_millis(200) && elapsed < Duration::from_millis(400),
            "4× took {elapsed:?} for 1 s of audio"
        );
    }

    #[test]
    fn factor_stays_drift_free_over_many_frames() {
        // A deadline computed as `start + n * interval` for every frame, rather than
        // accumulated from each sleep, so a long file does not creep behind schedule.
        // 8× on a 4 s file is 500 ms of wall clock over 200 frames: a naive per-sleep
        // accumulation (each sleep rounded up by the OS scheduler) would drift well past the
        // tolerance below by the last frame.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("long.wav");
        write_sine(&path, 16_000, &[0.2], 300.0, 4.0);

        let t = Instant::now();
        let mut src = WavSource::open(&path, Source::You, Speed::Factor(8.0)).unwrap();
        let (sink, frames) = frame_channel(Source::You, 300);
        src.start(sink).unwrap();
        src.wait().unwrap();
        let elapsed = t.elapsed();
        assert_eq!(frames.overruns(), 0);
        assert!(
            elapsed >= Duration::from_millis(450) && elapsed < Duration::from_millis(650),
            "8× took {elapsed:?} for 4 s of audio, expected about 500 ms"
        );
    }

    #[test]
    fn stop_ends_a_realtime_source_early() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("long.wav");
        write_sine(&path, 16_000, &[0.2], 300.0, 5.0);
        let mut src = WavSource::open(&path, Source::You, Speed::RealTime).unwrap();
        let (sink, frames) = frame_channel(Source::You, 100);
        src.start(sink).unwrap();
        assert!(matches!(
            src.start(frame_channel(Source::You, 1).0),
            Err(AudioError::AlreadyStarted)
        ));
        std::thread::sleep(Duration::from_millis(100));
        let t = Instant::now();
        src.stop().unwrap();
        assert!(
            t.elapsed() < Duration::from_millis(100),
            "stop joined promptly"
        );
        assert!(!src.is_finished());
        assert!(frames.queued_frames() >= 3 && frames.queued_frames() < 20);
    }

    /// Review of PR #35: an extreme `Factor` (not reachable through `dmn replay`'s own
    /// validation, but constructible through the library API) turned the per-frame deadline into
    /// a `Duration` too large to represent, which panicked in `Duration::from_secs_f64`. The
    /// source must degrade to running unpaced instead of crashing the thread.
    #[test]
    fn an_absurdly_small_factor_does_not_panic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("short.wav");
        write_sine(&path, 16_000, &[0.2], 300.0, 0.1);

        let mut src = WavSource::open(&path, Source::You, Speed::Factor(1e-30)).unwrap();
        let (sink, mut frames) = frame_channel(Source::You, 100);
        src.start(sink).unwrap();
        src.wait().unwrap();
        assert!(src.is_finished());
        assert!(!drain(&mut frames).is_empty());
    }

    /// Review of PR #35: pacing slept for the whole frame interval in one uninterruptible call,
    /// so a small `Factor` (a multi-second sleep per 20 ms frame) made `stop` block until the
    /// next deadline instead of returning promptly.
    #[test]
    fn stop_is_prompt_even_at_a_slow_factor() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("slow.wav");
        write_sine(&path, 16_000, &[0.2], 300.0, 5.0);

        // 0.001× makes each 20 ms frame a 20 s sleep; stop must not wait anywhere near that.
        let mut src = WavSource::open(&path, Source::You, Speed::Factor(0.001)).unwrap();
        let (sink, _frames) = frame_channel(Source::You, 100);
        src.start(sink).unwrap();
        std::thread::sleep(Duration::from_millis(50));
        let t = Instant::now();
        src.stop().unwrap();
        assert!(
            t.elapsed() < Duration::from_millis(500),
            "stop joined promptly, took {:?}",
            t.elapsed()
        );
        assert!(!src.is_finished());
    }

    #[test]
    fn small_ring_overruns_but_the_producer_never_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("one.wav");
        write_sine(&path, 16_000, &[0.2], 300.0, 1.0);
        let mut src = WavSource::open(&path, Source::You, Speed::Fast).unwrap();
        let (sink, frames) = frame_channel(Source::You, 2);
        let t = Instant::now();
        src.start(sink).unwrap();
        src.wait().unwrap();
        assert!(t.elapsed() < Duration::from_millis(200));
        assert_eq!(frames.queued_frames(), 2);
        assert_eq!(frames.overruns(), 50 - 2, "1 s = 50 frames, 2 kept");
    }

    #[test]
    fn unsupported_and_missing_files_are_typed_errors() {
        let dir = tempfile::tempdir().unwrap();
        let missing = WavSource::open(dir.path().join("nope.wav"), Source::You, Speed::Fast);
        assert!(matches!(missing, Err(AudioError::Open { .. })));
    }

    /// Copilot on PR #19: a `Builder::spawn` failure was reported as `ThreadPanicked`, which is a
    /// different fault with a different fix. The OS error must reach the caller intact. A real
    /// spawn failure needs the process to be out of threads, so this checks the mapping the
    /// start path uses rather than provoking one.
    #[test]
    fn a_spawn_failure_keeps_the_os_error_and_is_not_a_panic() {
        let io = std::io::Error::new(
            std::io::ErrorKind::OutOfMemory,
            "Resource temporarily unavailable",
        );
        let mapped = AudioError::Spawn(io.to_string());
        assert!(!matches!(mapped, AudioError::ThreadPanicked));
        assert!(
            mapped
                .to_string()
                .contains("Resource temporarily unavailable"),
            "{mapped}"
        );
        // A healthy start still spawns and finishes cleanly, so the new mapping is not hiding
        // a failure.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tone.wav");
        write_sine(&path, SAMPLE_RATE, &[0.5], 440.0, 0.05);
        let mut src = WavSource::open(&path, Source::You, Speed::Fast).unwrap();
        let (sink, _frames) = frame_channel(Source::You, 4);
        src.start(sink).unwrap();
        assert!(
            src.wait().is_ok(),
            "a healthy run is neither Spawn nor panic"
        );
    }
}
