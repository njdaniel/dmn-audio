//! The virtual microphone (#108): a PipeWire node named [`VMIC_NODE_NAME`] with
//! `media.class = Audio/Source`, so a call (Meet, Zoom, Teams, `pw-record --target dmn-voice`)
//! lists it as a microphone and hears whatever is written into it. `dmn speak` writes the
//! synthesized speech there; the user's own speakers stay silent.
//!
//! Three pieces, each on the thread that owns it:
//!
//! - [`VirtualMic`], the caller's handle. It lives on the thread that feeds the speech (the
//!   `dmn speak` main thread): `write` resamples a chunk from the TTS rate to the node's rate
//!   with [`LinearResampler`] and pushes it into the ring, waiting while the ring is full;
//!   `drain` waits until PipeWire has pulled the last sample; `close` (and `Drop`) takes the
//!   node out of the graph.
//! - `LoopVmic`, the `pw_stream` and its listeners, on the PipeWire loop thread, reached
//!   through [`crate::pipewire::Command`]s like the capture streams.
//! - [`VoiceReader`], the real-time side. The process callback dequeues an output buffer,
//!   pops samples from the lock-free ring into it, zero-fills what the ring could not supply
//!   (an underrun is silence, never a stall) and requeues. It allocates nothing, locks nothing
//!   and logs nothing; counters are relaxed atomics the feeding thread reads.
//!
//! The ring never drops queued speech. A lock-free SPSC producer cannot touch the consumer's
//! read index (SPEC § Threading rules says the same of capture), and dropping words from the
//! user's own sentence would be the wrong trade anyway: the writer waits, the decoder thread
//! behind it blocks on its bounded channel, and that blocks the HTTP body read. Backpressure
//! all the way to the server; `stalls` counts how often the writer had to wait.
//!
//! Node lifetime: the node exists from [`VirtualMic::open`] to `close`/`Drop`. `dmn speak`
//! opens it before sending the request, so the call (or `pw-record`) can pick it up during the
//! seconds the server takes to render, and closes it when the process ends; nothing lingers in
//! the audio settings between runs. A session that speaks many times (#109) opens one and
//! keeps it, which is what keeps a call's microphone selection stable.
//!
//! **Pass-through (#114).** With [`Passthrough`] the node also carries the user's own
//! microphone, so a call that selected `dmn-voice` hears them live until translate mode is on.
//! The microphone is a second capture stream (`LoopTap`, `dmn.passthrough`) on the same loop,
//! at the graph rate, so the call gets the microphone's full bandwidth, not the 16 kHz the
//! whisper lane runs at; its process callback pushes the samples into a second lock-free ring
//! ([`tap_ring`]) from PipeWire's data thread, and the playback callback's [`Mixer`] pops them
//! into the output under the speech: nothing in between, no thread to wake, about one quantum
//! of latency. The mixer gates the microphone with a per-sample gain that ramps over
//! [`PASSTHROUGH_FADE_MS`] whenever the switch flips, so neither the mode key nor a sentence
//! starting clicks, and the gate is shut whenever speech is in the ring, whatever the mode: the
//! call never hears both voices. A microphone backlog over [`TAP_MAX_BACKLOG_MS`] (the mic's
//! clock and the call's drifting apart, or nothing pulling for a while) is skipped down to
//! [`TAP_TARGET_BACKLOG_MS`] so the pass-through never lags; a full ring drops the newest
//! microphone samples, never the speech. Both callbacks still allocate, lock and log nothing.

use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use pipewire as pw;
use pw::spa;
use ringbuf::HeapRb;
use ringbuf::traits::{Consumer, Observer, Producer, Split};

use crate::capture::enum_format_pod;
use crate::pipewire::{Command, PipeWireContext};
use crate::source::AudioError;

/// `node.name` of the virtual microphone: what a call and `pw-record --target` select.
pub const VMIC_NODE_NAME: &str = "dmn-voice";
/// `node.description`: what the desktop's sound panel shows.
pub const VMIC_DESCRIPTION: &str = "Daemon voice (dmn speak)";
/// Bytes per F32LE mono frame.
const STRIDE: usize = 4;
/// Ring capacity handed out by default: 2 s at 48 kHz. The writer waits when it is full, so
/// this is how far ahead of real time the TTS output may run, not a latency.
pub const DEFAULT_RING_SECS: u32 = 2;
/// Largest quantum PipeWire's default `clock.quantum-limit` allows; the callback's fixed
/// scratch is this big and copies larger buffers in passes.
const SCRATCH_FRAMES: usize = 8192;
/// How long `open` waits for the stream to be connected (or refused).
const OPEN_TIMEOUT: Duration = Duration::from_secs(3);
/// How long `close` waits for the loop thread to destroy the stream.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(1);
/// How long `write` waits for the ring to make room before deciding nothing is pulling.
const STALL_TIMEOUT: Duration = Duration::from_secs(5);
/// Writer and drain poll interval while waiting on the real-time side.
const WAIT_TICK: Duration = Duration::from_millis(5);
/// #114: how long the microphone's gain takes to open or shut at a switch (the issue asks for
/// 5–10 ms): long enough not to click, short enough that a sentence starting over it is not
/// heard twice.
pub const PASSTHROUGH_FADE_MS: u32 = 8;
/// #114: microphone samples queued beyond this are skipped (the mic's clock and the call's
/// drift apart, or nothing pulled from the node for a while): the pass-through must not lag.
pub const TAP_MAX_BACKLOG_MS: u32 = 60;
/// #114: what the backlog is cut down to when it is skipped.
pub const TAP_TARGET_BACKLOG_MS: u32 = 20;
/// #114: the microphone ring's depth, 250 ms: well over the backlog it is trimmed to, so the
/// trimming (the playback side) and not the ring (the capture side) decides what is dropped.
const TAP_RING_MS: u32 = 250;
/// #114: a sample at or over this is sound, for the first-sound timestamps the latency test
/// reads (the ring's silence floor is 0).
const LOUD: f32 = 0.05;
/// #114: `node.name` of the pass-through's capture stream.
pub const TAP_NODE_NAME: &str = "dmn.passthrough";

type Prod = <HeapRb<f32> as Split>::Prod;
type Cons = <HeapRb<f32> as Split>::Cons;

/// Nanoseconds since `epoch`, for a first-sample timestamp from a real-time callback:
/// `Instant::elapsed` is a monotonic clock read (vDSO, no syscall). Never 0 in practice, and 0
/// would only mean "measure again next time".
fn stamp(epoch: Instant) -> u64 {
    epoch.elapsed().as_nanos() as u64
}

fn instant_of(epoch: Instant, ns: &AtomicU64) -> Option<Instant> {
    let ns = ns.load(Ordering::Relaxed);
    (ns != 0).then(|| epoch + Duration::from_nanos(ns))
}

/// #114: the microphone ring's counters, shared by its two ends and read by whoever reports.
/// Relaxed atomics only, on both real-time sides.
#[derive(Debug)]
pub struct TapStats {
    /// Samples the capture callback pushed.
    pushed: AtomicU64,
    /// Samples the capture callback found no room for (nothing pulled from the node, or the
    /// playback side fell behind the trim): the newest microphone samples, never speech.
    dropped: AtomicU64,
    /// Samples the playback side skipped to cut a backlog down ([`TAP_MAX_BACKLOG_MS`]).
    skipped: AtomicU64,
    /// Samples the playback side pulled (mixed in or discarded under a shut gate).
    popped: AtomicU64,
    /// Nanoseconds after `epoch` when the first sample at or over [`LOUD`] was pushed; 0 until
    /// then. With `first_out_ns`, the microphone's dwell in dmn.
    first_in_ns: AtomicU64,
    /// Nanoseconds after `epoch` when the first such sample went out with the gate open.
    first_out_ns: AtomicU64,
    epoch: Instant,
}

impl TapStats {
    fn new() -> Self {
        Self {
            pushed: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            skipped: AtomicU64::new(0),
            popped: AtomicU64::new(0),
            first_in_ns: AtomicU64::new(0),
            first_out_ns: AtomicU64::new(0),
            epoch: Instant::now(),
        }
    }

    pub fn pushed(&self) -> u64 {
        self.pushed.load(Ordering::Relaxed)
    }

    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    pub fn skipped(&self) -> u64 {
        self.skipped.load(Ordering::Relaxed)
    }

    pub fn popped(&self) -> u64 {
        self.popped.load(Ordering::Relaxed)
    }

    /// When the first sound reached the ring from the microphone, if any has.
    pub fn first_in(&self) -> Option<Instant> {
        instant_of(self.epoch, &self.first_in_ns)
    }

    /// When the first sound left the ring for the call, if any has.
    pub fn first_out(&self) -> Option<Instant> {
        instant_of(self.epoch, &self.first_out_ns)
    }

    /// The microphone's dwell in dmn: from its first sound reaching the ring to that sound
    /// leaving it for the call. What this crate adds to PipeWire's own capture and playback
    /// latency.
    pub fn dwell(&self) -> Option<Duration> {
        Some(
            self.first_out()?
                .saturating_duration_since(self.first_in()?),
        )
    }

    /// Stamp `first` once, when `samples` hold the first sound.
    fn note_loud(&self, first: &AtomicU64, samples: &[f32]) {
        if first.load(Ordering::Relaxed) == 0 && samples.iter().any(|s| s.abs() >= LOUD) {
            first.store(stamp(self.epoch), Ordering::Relaxed);
        }
    }
}

/// #114: the microphone ring's producer end, held by the capture callback. `push` is safe to
/// call from a real-time callback: no allocation, no lock, no logging, bounded time.
pub struct TapWriter {
    prod: Prod,
    stats: Arc<TapStats>,
}

impl TapWriter {
    /// Push as many of `samples` as fit and return that count; the rest are dropped and
    /// counted (a full ring is a call that is not pulling, or a backlog the other side is about
    /// to trim; either way the newest microphone samples are the ones to lose).
    pub fn push(&mut self, samples: &[f32]) -> usize {
        let n = self.prod.push_slice(samples);
        self.stats.pushed.fetch_add(n as u64, Ordering::Relaxed);
        if n < samples.len() {
            self.stats
                .dropped
                .fetch_add((samples.len() - n) as u64, Ordering::Relaxed);
        }
        self.stats.note_loud(&self.stats.first_in_ns, &samples[..n]);
        n
    }

    pub fn stats(&self) -> Arc<TapStats> {
        Arc::clone(&self.stats)
    }
}

/// #114: the microphone ring's consumer end, held by the playback callback's [`Mixer`]. Every
/// method is safe in a real-time callback.
pub struct TapReader {
    cons: Cons,
    stats: Arc<TapStats>,
}

impl TapReader {
    /// Fill `out` from the ring and zero the rest (an empty ring is silence). Returns how many
    /// real samples it had.
    pub fn pop(&mut self, out: &mut [f32]) -> usize {
        let got = self.cons.pop_slice(out);
        out[got..].fill(0.0);
        self.stats.popped.fetch_add(got as u64, Ordering::Relaxed);
        got
    }

    /// Discard up to `n` queued samples (the gate is shut: the microphone keeps flowing so
    /// what comes through when it opens is current).
    pub fn skip(&mut self, n: usize) -> usize {
        let n = self.cons.skip(n);
        self.stats.popped.fetch_add(n as u64, Ordering::Relaxed);
        n
    }

    /// Cut a backlog over `max` samples down to `target`, counting what went.
    pub fn trim(&mut self, max: usize, target: usize) {
        let queued = self.cons.occupied_len();
        if queued > max {
            let n = self.cons.skip(queued - target.min(max));
            self.stats.skipped.fetch_add(n as u64, Ordering::Relaxed);
        }
    }

    /// Samples queued.
    pub fn queued(&self) -> usize {
        self.cons.occupied_len()
    }

    pub fn stats(&self) -> Arc<TapStats> {
        Arc::clone(&self.stats)
    }
}

/// #114: build the microphone ring of `capacity` samples and split it into its two ends.
pub fn tap_ring(capacity: usize) -> (TapWriter, TapReader) {
    let (prod, cons) = HeapRb::<f32>::new(capacity.max(1)).split();
    let stats = Arc::new(TapStats::new());
    (
        TapWriter {
            prod,
            stats: Arc::clone(&stats),
        },
        TapReader { cons, stats },
    )
}

/// #114: what the playback callback reads: the speech ring, and under it the microphone ring
/// behind a gain that ramps rather than steps. Owned by the callback; everything in `fill` is
/// safe in real time: two lock-free rings, relaxed atomics, a fixed scratch block, one pass
/// over the buffer.
///
/// The gate is open only while pass-through is on (the mode, an atomic the feeding thread
/// flips) *and* no speech is in flight (`speaking`, set by the writer from a sentence's first
/// sample to its last). It opens and shuts over [`PASSTHROUGH_FADE_MS`]: a sentence starting
/// over the microphone fades it out under its first 8 ms rather than cutting it, and the mode
/// key is a fade either way. Shut, the microphone is still pulled and discarded, so the ring
/// never fills and what comes through when the gate opens is live.
pub struct Mixer {
    speech: VoiceReader,
    /// The microphone, with the mode switch; `None` for a node with no pass-through.
    tap: Option<(TapReader, Arc<AtomicBool>)>,
    /// The microphone's gain now, 0.0 (shut) to 1.0 (open).
    gain: f32,
    /// How far the gain moves per sample: one fade in [`PASSTHROUGH_FADE_MS`].
    step: f32,
    max_backlog: usize,
    target_backlog: usize,
    scratch: Box<[f32]>,
}

impl Mixer {
    /// A mixer at `rate` Hz over `speech`, with the microphone when `tap` is given.
    pub fn new(speech: VoiceReader, tap: Option<(TapReader, Arc<AtomicBool>)>, rate: u32) -> Self {
        let rate = rate.max(1);
        let fade_samples = (rate * PASSTHROUGH_FADE_MS / 1000).max(1);
        // The gate starts where the mode says, so a session that starts in pass-through is
        // heard from its first buffer, not after a fade from silence that nobody asked for.
        let gain = match &tap {
            Some((_, on)) if on.load(Ordering::Relaxed) => 1.0,
            _ => 0.0,
        };
        Self {
            speech,
            tap,
            gain,
            step: 1.0 / fade_samples as f32,
            max_backlog: (rate * TAP_MAX_BACKLOG_MS / 1000) as usize,
            target_backlog: (rate * TAP_TARGET_BACKLOG_MS / 1000) as usize,
            scratch: vec![0.0; SCRATCH_FRAMES].into_boxed_slice(),
        }
    }

    /// Fill `out`: the speech (silence where the ring had none), plus the microphone under its
    /// gain. Returns how many real speech samples it had, as [`VoiceReader::fill`] does.
    pub fn fill(&mut self, out: &mut [f32]) -> usize {
        let got = self.speech.fill(out);
        let Some((tap, on)) = self.tap.as_mut() else {
            return got;
        };
        let speaking = self.speech.stats.speaking.load(Ordering::Relaxed);
        let target = if on.load(Ordering::Relaxed) && !speaking {
            1.0
        } else {
            0.0
        };
        tap.trim(self.max_backlog, self.target_backlog);
        if self.gain == 0.0 && target == 0.0 {
            // Shut and staying shut: keep the microphone flowing past, hear none of it.
            tap.skip(out.len());
            return got;
        }
        let mut done = 0;
        while done < out.len() {
            let n = (out.len() - done).min(self.scratch.len());
            tap.pop(&mut self.scratch[..n]);
            let mut heard_from = n;
            for (i, (slot, &mic)) in out[done..done + n]
                .iter_mut()
                .zip(&self.scratch[..n])
                .enumerate()
            {
                if self.gain < target {
                    self.gain = (self.gain + self.step).min(target);
                } else if self.gain > target {
                    self.gain = (self.gain - self.step).max(target);
                }
                if self.gain > 0.0 {
                    *slot += mic * self.gain;
                    heard_from = heard_from.min(i);
                }
            }
            if heard_from < n {
                tap.stats
                    .note_loud(&tap.stats.first_out_ns, &self.scratch[heard_from..n]);
            }
            done += n;
        }
        got
    }

    /// The microphone's gain now, for tests.
    pub fn gain(&self) -> f32 {
        self.gain
    }
}

/// Counters shared by the ring's two ends and read by whoever reports. The real-time side
/// only does relaxed atomic loads, stores and adds on these.
#[derive(Debug)]
pub struct RingStats {
    /// Callbacks that found the ring empty while `speaking` was set: a whole buffer of silence,
    /// a gap in the speech. A short fill is not counted (the clip's last buffer looks the same),
    /// so a producer only slightly slower than real time shows as fewer gaps than it made.
    underruns: AtomicU64,
    /// Writer pushes that found the ring too full to take everything. The writer retries every
    /// 5 ms, so one wait of 100 ms counts about 20; it is a count of polls, not of waits.
    stalls: AtomicU64,
    /// Samples handed to PipeWire so far (real samples, not padding).
    pulled: AtomicU64,
    /// Set by the writer from its first push until the clip has drained; gates `underruns`
    /// so the silence before and after a clip is not counted as gaps in it.
    speaking: AtomicBool,
    /// Nanoseconds after `epoch` when the first real sample was pulled; 0 until then.
    first_pull_ns: AtomicU64,
    epoch: Instant,
}

impl RingStats {
    fn new() -> Self {
        Self {
            underruns: AtomicU64::new(0),
            stalls: AtomicU64::new(0),
            pulled: AtomicU64::new(0),
            speaking: AtomicBool::new(false),
            first_pull_ns: AtomicU64::new(0),
            epoch: Instant::now(),
        }
    }

    pub fn underruns(&self) -> u64 {
        self.underruns.load(Ordering::Relaxed)
    }

    pub fn stalls(&self) -> u64 {
        self.stalls.load(Ordering::Relaxed)
    }

    pub fn pulled(&self) -> u64 {
        self.pulled.load(Ordering::Relaxed)
    }

    /// When PipeWire pulled the first real sample, if it has.
    pub fn first_pull(&self) -> Option<Instant> {
        let ns = self.first_pull_ns.load(Ordering::Relaxed);
        (ns != 0).then(|| self.epoch + Duration::from_nanos(ns))
    }
}

/// Producer end: held by the feeding thread. Never blocks; `push` returns how much fit.
pub struct VoiceWriter {
    prod: Prod,
    stats: Arc<RingStats>,
}

impl VoiceWriter {
    /// Push as many of `samples` as fit and return that count. Counts a stall when it is less
    /// than all of them; the caller pushes the rest later. Nothing already queued is touched.
    pub fn push(&mut self, samples: &[f32]) -> usize {
        let n = self.prod.push_slice(samples);
        if n < samples.len() {
            self.stats.stalls.fetch_add(1, Ordering::Relaxed);
        }
        n
    }

    /// Samples the ring can still take.
    pub fn vacant(&self) -> usize {
        self.prod.vacant_len()
    }

    /// Open or close the underrun gate (see [`RingStats`]).
    pub fn set_speaking(&self, speaking: bool) {
        self.stats.speaking.store(speaking, Ordering::Relaxed);
    }

    pub fn stats(&self) -> Arc<RingStats> {
        Arc::clone(&self.stats)
    }
}

/// Consumer end: held by the process callback. `fill` is safe to call from a real-time
/// callback: no allocation, no lock, no logging, bounded time.
pub struct VoiceReader {
    cons: Cons,
    stats: Arc<RingStats>,
}

impl VoiceReader {
    /// Fill `out` from the ring and zero the rest. Returns how many real samples it had.
    pub fn fill(&mut self, out: &mut [f32]) -> usize {
        let got = self.cons.pop_slice(out);
        if got < out.len() {
            out[got..].fill(0.0);
            // A whole buffer of silence while speaking is a gap. A short fill is not: it is
            // the clip's last buffer, or a gap that the next callback counts as a whole one.
            if got == 0 && self.stats.speaking.load(Ordering::Relaxed) {
                self.stats.underruns.fetch_add(1, Ordering::Relaxed);
            }
        }
        if got > 0 {
            if self.stats.first_pull_ns.load(Ordering::Relaxed) == 0 {
                // `Instant::elapsed` is a monotonic clock read (vDSO, no syscall); never 0 in
                // practice, and 0 would only mean "measure again next time".
                let ns = self.stats.epoch.elapsed().as_nanos() as u64;
                self.stats.first_pull_ns.store(ns, Ordering::Relaxed);
            }
            self.stats.pulled.fetch_add(got as u64, Ordering::Relaxed);
        }
        got
    }

    /// Samples queued.
    pub fn queued(&self) -> usize {
        self.cons.occupied_len()
    }
}

/// Build a ring of `capacity` samples and split it into its two ends.
pub fn voice_ring(capacity: usize) -> (VoiceWriter, VoiceReader) {
    let (prod, cons) = HeapRb::<f32>::new(capacity.max(1)).split();
    let stats = Arc::new(RingStats::new());
    (
        VoiceWriter {
            prod,
            stats: Arc::clone(&stats),
        },
        VoiceReader { cons, stats },
    )
}

/// Linear-interpolation resampler for speech, `from` Hz to `to` Hz, continuous across chunks
/// (the last input sample is kept so the first output of the next chunk interpolates towards
/// it). Chatterbox emits 24 kHz; the graph runs at 48 kHz on the dev box, so this is a plain
/// 2× upsample there. Good enough for speech; a sine keeps its frequency (tested below).
#[derive(Debug, Clone)]
pub struct LinearResampler {
    from: u32,
    to: u32,
    /// Input samples per output sample.
    step: f64,
    /// Position of the next output in the virtual input `[prev, chunk...]`: 0 is `prev`,
    /// 1 is the chunk's first sample. Starts at 1 so the first output is the first input.
    pos: f64,
    prev: f32,
}

impl LinearResampler {
    pub fn new(from: u32, to: u32) -> Self {
        Self {
            from,
            to,
            step: f64::from(from.max(1)) / f64::from(to.max(1)),
            pos: 1.0,
            prev: 0.0,
        }
    }

    pub fn from_rate(&self) -> u32 {
        self.from
    }

    pub fn to_rate(&self) -> u32 {
        self.to
    }

    /// Resample `input` and append the output to `out`.
    pub fn process(&mut self, input: &[f32], out: &mut Vec<f32>) {
        if input.is_empty() {
            return;
        }
        let len = input.len() as f64;
        // The virtual input is `[prev, input...]`, indices 0..=len. Every output whose
        // position is at or before the chunk's last sample is computable now; the rest waits
        // for the next chunk (or stays unproduced at the very end: under one sample).
        let at = |i: usize| if i == 0 { self.prev } else { input[i - 1] };
        while self.pos <= len {
            let i = self.pos as usize;
            let frac = (self.pos - i as f64) as f32;
            let a = at(i);
            // frac > 0 implies pos < len, so i + 1 <= len is a real sample.
            let value = if frac > 0.0 {
                a + (at(i + 1) - a) * frac
            } else {
                a
            };
            out.push(value);
            self.pos += self.step;
        }
        self.prev = input[input.len() - 1];
        self.pos -= len;
    }
}

/// Lifecycle of the virtual microphone's stream, as the status reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VmicState {
    /// Not created, or closed.
    Idle,
    /// Connecting to the graph.
    Connecting,
    /// In the graph, with nothing linked to it yet: PipeWire does not run it until a call or
    /// `pw-record` selects it, so nothing is pulled from the ring meanwhile.
    Ready,
    /// Something is linked and samples are being pulled.
    Streaming,
    /// PipeWire reported an error; see [`VmicStatus::last_error`].
    Error,
}

impl VmicState {
    fn to_u8(self) -> u8 {
        match self {
            VmicState::Idle => 0,
            VmicState::Connecting => 1,
            VmicState::Ready => 2,
            VmicState::Streaming => 3,
            VmicState::Error => 4,
        }
    }

    fn from_u8(v: u8) -> Self {
        match v {
            1 => VmicState::Connecting,
            2 => VmicState::Ready,
            3 => VmicState::Streaming,
            4 => VmicState::Error,
            _ => VmicState::Idle,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            VmicState::Idle => "idle",
            VmicState::Connecting => "connecting",
            VmicState::Ready => "ready (nothing linked)",
            VmicState::Streaming => "streaming",
            VmicState::Error => "in error",
        }
    }
}

/// Shared, lock-free view of the stream for the feeding thread. The real-time callback reads
/// only `format_ok`.
#[derive(Debug)]
pub struct VmicStatus {
    state: AtomicU8,
    /// Set once PipeWire settled on F32LE mono at the requested rate; the callback emits
    /// nothing until then, so no buffer is ever filled with bytes of the wrong shape.
    format_ok: AtomicBool,
    rate: AtomicU32,
    last_error: Mutex<Option<String>>,
}

impl Default for VmicStatus {
    fn default() -> Self {
        Self {
            state: AtomicU8::new(VmicState::Idle.to_u8()),
            format_ok: AtomicBool::new(false),
            rate: AtomicU32::new(0),
            last_error: Mutex::new(None),
        }
    }
}

impl VmicStatus {
    pub fn state(&self) -> VmicState {
        VmicState::from_u8(self.state.load(Ordering::Acquire))
    }

    /// The negotiated rate, once PipeWire has settled it.
    pub fn rate(&self) -> Option<u32> {
        let rate = self.rate.load(Ordering::Acquire);
        (rate != 0).then_some(rate)
    }

    pub fn last_error(&self) -> Option<String> {
        self.last_error
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn set_state(&self, s: VmicState) {
        self.state.store(s.to_u8(), Ordering::Release);
    }

    pub(crate) fn set_error(&self, why: String) {
        *self.last_error.lock().unwrap_or_else(|e| e.into_inner()) = Some(why);
        self.set_state(VmicState::Error);
    }
}

/// What the loop thread needs to create the stream.
pub(crate) struct VmicSpec {
    pub(crate) node_name: String,
    pub(crate) description: String,
    pub(crate) rate: u32,
    pub(crate) status: Arc<VmicStatus>,
    /// #114: the pass-through's capture stream, with the ring end its callback fills.
    pub(crate) tap: Option<(TapSpec, TapWriter)>,
}

/// #114: what the loop thread needs to create the pass-through's capture stream.
pub(crate) struct TapSpec {
    /// `target.object`: the microphone's `node.name` (or a stream's, in tests); `None` follows
    /// PipeWire's default source.
    pub(crate) target: Option<String>,
    pub(crate) rate: u32,
    pub(crate) status: Arc<VmicStatus>,
}

/// The real-time side's state: the mixer over the rings and a fixed scratch block.
struct Outlet {
    mixer: Mixer,
    scratch: Box<[f32]>,
    status: Arc<VmicStatus>,
}

/// REAL-TIME CALLBACK (runs on PipeWire's data thread; `RT_PROCESS`).
/// Rules (CLAUDE.md, SPEC § Threading rules), reviewed call by call:
/// - no allocation: `dequeue_buffer` wraps a pointer, `datas_mut`/`data`/`chunk_mut` borrow
///   the mapped buffer, `Mixer::fill` pops both rings into preallocated scratch blocks, and
///   the samples are copied from there into the buffer as little-endian bytes;
/// - no locks: the rings are lock-free SPSC, counters, the format gate and the pass-through
///   switch are relaxed atomics;
/// - no logging, no syscalls (one monotonic clock read for each first-sample timestamp,
///   which is a vDSO call), bounded time (one pass over the buffer).
///
/// Dequeue, fill, requeue (the `Buffer` guard requeues on drop). An empty ring fills the
/// buffer with silence: the graph never waits on the server.
fn process(stream: &pw::stream::Stream, out: &mut Outlet) {
    let Some(mut buffer) = stream.dequeue_buffer() else {
        return;
    };
    // `requested` is how many frames the graph wants this cycle (0 on servers that do not
    // say, in which case the whole buffer is filled).
    let requested = buffer.requested() as usize;
    let format_ok = out.status.format_ok.load(Ordering::Acquire);
    let Some(data) = buffer.datas_mut().first_mut() else {
        return;
    };
    let mut frames = 0;
    if let Some(bytes) = data.data()
        && format_ok
    {
        let max_frames = bytes.len() / STRIDE;
        frames = if requested == 0 {
            max_frames
        } else {
            requested.min(max_frames)
        };
        let mut done = 0;
        while done < frames {
            let n = (frames - done).min(out.scratch.len());
            out.mixer.fill(&mut out.scratch[..n]);
            let dst = &mut bytes[done * STRIDE..(done + n) * STRIDE];
            for (slot, sample) in dst.chunks_exact_mut(STRIDE).zip(&out.scratch[..n]) {
                slot.copy_from_slice(&sample.to_le_bytes());
            }
            done += n;
        }
    }
    let chunk = data.chunk_mut();
    *chunk.offset_mut() = 0;
    *chunk.stride_mut() = STRIDE as i32;
    *chunk.size_mut() = (frames * STRIDE) as u32;
}

/// #114: the pass-through's real-time side: the microphone ring's producer and a fixed block
/// the buffer's bytes are decoded into.
struct TapInlet {
    writer: TapWriter,
    scratch: Box<[f32]>,
    status: Arc<VmicStatus>,
}

/// REAL-TIME CALLBACK (runs on PipeWire's data thread; `RT_PROCESS`): the pass-through's
/// capture. Rules (CLAUDE.md, SPEC § Threading rules), reviewed call by call:
/// - no allocation: `dequeue_buffer` wraps a pointer, `datas_mut`/`chunk`/`data` borrow the
///   mapped buffer, the samples are decoded into the preallocated `scratch` and pushed into
///   the preallocated ring;
/// - no locks: the ring is lock-free SPSC, counters and the format gate are relaxed atomics;
/// - no logging, no syscalls (one monotonic clock read for the first-sound timestamp, a vDSO
///   call), bounded time (one pass over the buffer).
///
/// Dequeue, copy, requeue (the `Buffer` guard requeues on drop). Until the format is settled
/// at the graph rate, mono F32, the bytes are not what the ring expects and are not copied.
fn tap_process(stream: &pw::stream::Stream, inlet: &mut TapInlet) {
    let Some(mut buffer) = stream.dequeue_buffer() else {
        return;
    };
    if !inlet.status.format_ok.load(Ordering::Acquire) {
        return;
    }
    let Some(data) = buffer.datas_mut().first_mut() else {
        return;
    };
    let offset = data.chunk().offset() as usize;
    let size = data.chunk().size() as usize;
    let Some(bytes) = data.data() else {
        return;
    };
    let end = offset.saturating_add(size).min(bytes.len());
    let start = offset.min(end);
    for pass in bytes[start..end].chunks(STRIDE * inlet.scratch.len()) {
        let mut n = 0;
        for (slot, c) in inlet.scratch.iter_mut().zip(pass.chunks_exact(STRIDE)) {
            *slot = f32::from_le_bytes([c[0], c[1], c[2], c[3]]);
            n += 1;
        }
        inlet.writer.push(&inlet.scratch[..n]);
    }
}

/// A stream's state and format events into `status`, gating its callback on `want_rate` Hz
/// mono F32LE. Shared by the node and its pass-through capture: the same `VmicStatus` shape
/// describes both.
fn status_listener(
    stream: &pw::stream::StreamRc,
    status: &Arc<VmicStatus>,
    want_rate: u32,
) -> Result<pw::stream::StreamListener<()>, String> {
    let state_status = Arc::clone(status);
    let format_status = Arc::clone(status);
    stream
        .add_local_listener::<()>()
        .state_changed(move |_, _, _old, new| match new {
            pw::stream::StreamState::Streaming => state_status.set_state(VmicState::Streaming),
            pw::stream::StreamState::Paused => state_status.set_state(VmicState::Ready),
            pw::stream::StreamState::Connecting => state_status.set_state(VmicState::Connecting),
            pw::stream::StreamState::Error(why) => state_status.set_error(why),
            pw::stream::StreamState::Unconnected => {}
        })
        .param_changed(move |_, _, id, param| {
            if id != spa::param::ParamType::Format.as_raw() {
                return;
            }
            let Some(param) = param else {
                format_status.format_ok.store(false, Ordering::Release);
                return;
            };
            let mut info = spa::param::audio::AudioInfoRaw::new();
            if info.parse(param).is_ok() {
                format_status.rate.store(info.rate(), Ordering::Release);
                let ok = info.rate() == want_rate
                    && info.channels() == 1
                    && info.format() == spa::param::audio::AudioFormat::F32LE;
                format_status.format_ok.store(ok, Ordering::Release);
                if !ok {
                    format_status.set_error(format!(
                        "negotiated {:?} {} Hz × {} ch, need F32LE {want_rate} Hz mono",
                        info.format(),
                        info.rate(),
                        info.channels()
                    ));
                }
            } else {
                format_status.format_ok.store(false, Ordering::Release);
            }
        })
        .register()
        .map_err(|e| e.to_string())
}

/// #114: the pass-through's capture stream and its listeners, owned by the PipeWire loop
/// thread beside the node it feeds. Like a `LoopCapture` but at the graph rate, and its
/// callback fills the microphone ring rather than a 20 ms frame channel.
pub(crate) struct LoopTap {
    // Field order is drop order: listeners before the stream they are attached to.
    _process: pw::stream::StreamListener<TapInlet>,
    _events: pw::stream::StreamListener<()>,
    stream: pw::stream::StreamRc,
    status: Arc<VmicStatus>,
}

impl LoopTap {
    fn open(core: &pw::core::CoreRc, spec: TapSpec, writer: TapWriter) -> Result<Self, String> {
        let mut props = pw::properties::PropertiesBox::new();
        props.insert(*pw::keys::MEDIA_TYPE, "Audio");
        props.insert(*pw::keys::MEDIA_CATEGORY, "Capture");
        props.insert(*pw::keys::MEDIA_ROLE, "Communication");
        props.insert(*pw::keys::NODE_NAME, TAP_NODE_NAME);
        // A 10 ms quantum, as the whisper lanes ask for: the pass-through's latency is quanta.
        let latency = format!("{}/{}", spec.rate / 100, spec.rate);
        props.insert(*pw::keys::NODE_LATENCY, latency.as_str());
        if let Some(target) = &spec.target {
            // A named microphone that is absent falls back to the default source, as the
            // whisper lane does (`Reattach`): the call hears the microphone whisper hears.
            // (`node.dont-fallback` would put the stream in error instead: "defined target
            // not found".)
            props.insert("target.object", target.as_str());
        }
        let stream = pw::stream::StreamRc::new(core.clone(), TAP_NODE_NAME, props)
            .map_err(|e| e.to_string())?;
        let inlet = TapInlet {
            writer,
            scratch: vec![0.0; SCRATCH_FRAMES].into_boxed_slice(),
            status: Arc::clone(&spec.status),
        };
        let process_listener = stream
            .add_local_listener_with_user_data(inlet)
            .process(tap_process)
            .register()
            .map_err(|e| e.to_string())?;
        let events_listener = status_listener(&stream, &spec.status, spec.rate)?;
        let pod = enum_format_pod(spec.rate)?;
        let mut params = [spa::pod::Pod::from_bytes(&pod).ok_or("bad EnumFormat pod")?];
        stream
            .connect(
                spa::utils::Direction::Input,
                None,
                pw::stream::StreamFlags::AUTOCONNECT
                    | pw::stream::StreamFlags::MAP_BUFFERS
                    | pw::stream::StreamFlags::RT_PROCESS,
                &mut params,
            )
            .map_err(|e| e.to_string())?;
        spec.status.set_state(VmicState::Connecting);
        Ok(Self {
            _process: process_listener,
            _events: events_listener,
            stream,
            status: spec.status,
        })
    }
}

impl Drop for LoopTap {
    fn drop(&mut self) {
        self.status.format_ok.store(false, Ordering::Release);
        // After this returns the data thread no longer calls `tap_process`.
        let _ = self.stream.disconnect();
        self.status.set_state(VmicState::Idle);
    }
}

/// The stream and its two listeners, owned by the PipeWire loop thread. The process listener
/// is the only one holding the [`Outlet`]; state and format events go to a second listener
/// with no user data, so the real-time thread and the loop thread never share a `&mut`.
/// #114: with pass-through, the microphone's capture stream lives and dies with it.
pub(crate) struct LoopVmic {
    // Field order is drop order: the capture first (its ring's reader is in the outlet), then
    // listeners before the stream they are attached to.
    _tap: Option<LoopTap>,
    _process: pw::stream::StreamListener<Outlet>,
    _events: pw::stream::StreamListener<()>,
    stream: pw::stream::StreamRc,
    status: Arc<VmicStatus>,
}

impl LoopVmic {
    /// Create and connect the stream on the loop thread, and the pass-through's capture when
    /// the spec has one. A capture that PipeWire refuses is reported on its own status and the
    /// node still plays speech.
    pub(crate) fn open(
        core: &pw::core::CoreRc,
        spec: VmicSpec,
        mixer: Mixer,
    ) -> Result<Self, String> {
        let mut props = pw::properties::PropertiesBox::new();
        props.insert(*pw::keys::MEDIA_TYPE, "Audio");
        props.insert(*pw::keys::MEDIA_CATEGORY, "Playback");
        props.insert(*pw::keys::MEDIA_ROLE, "Communication");
        // The one property that makes a playback stream a microphone: apps enumerate it with
        // the real capture devices and WirePlumber links them to it on request.
        props.insert(*pw::keys::MEDIA_CLASS, "Audio/Source");
        props.insert(*pw::keys::NODE_NAME, spec.node_name.as_str());
        props.insert(*pw::keys::NODE_DESCRIPTION, spec.description.as_str());
        props.insert(*pw::keys::NODE_NICK, spec.node_name.as_str());
        // What keeps the node off the speakers is the missing AUTOCONNECT flag on `connect`
        // below: the session manager never links it to a sink, only whatever selects it as a
        // microphone links to it. This flag only stops a later move to another target.
        props.insert(*pw::keys::NODE_DONT_RECONNECT, "true");
        let stream = pw::stream::StreamRc::new(core.clone(), &spec.node_name, props)
            .map_err(|e| e.to_string())?;

        let outlet = Outlet {
            mixer,
            scratch: vec![0.0; SCRATCH_FRAMES].into_boxed_slice(),
            status: Arc::clone(&spec.status),
        };
        let process_listener = stream
            .add_local_listener_with_user_data(outlet)
            .process(process)
            .register()
            .map_err(|e| e.to_string())?;
        let events_listener = status_listener(&stream, &spec.status, spec.rate)?;

        let pod = enum_format_pod(spec.rate)?;
        let mut params = [spa::pod::Pod::from_bytes(&pod).ok_or("bad EnumFormat pod")?];
        // Output direction, no AUTOCONNECT: a source is linked *from*, by whoever selects it.
        stream
            .connect(
                spa::utils::Direction::Output,
                None,
                pw::stream::StreamFlags::MAP_BUFFERS | pw::stream::StreamFlags::RT_PROCESS,
                &mut params,
            )
            .map_err(|e| e.to_string())?;
        spec.status.set_state(VmicState::Connecting);

        let tap = spec.tap.and_then(|(tap_spec, writer)| {
            let status = Arc::clone(&tap_spec.status);
            match LoopTap::open(core, tap_spec, writer) {
                Ok(tap) => Some(tap),
                Err(why) => {
                    status.set_error(why);
                    None
                }
            }
        });

        Ok(Self {
            _tap: tap,
            _process: process_listener,
            _events: events_listener,
            stream,
            status: spec.status,
        })
    }
}

impl Drop for LoopVmic {
    fn drop(&mut self) {
        self.status.format_ok.store(false, Ordering::Release);
        // After this returns the data thread no longer calls `process`; destroying the stream
        // (the fields drop next) removes the node from the graph.
        let _ = self.stream.disconnect();
        self.status.set_state(VmicState::Idle);
    }
}

/// What a clip's playback came to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VmicStats {
    /// Samples written at the node's rate.
    pub written: u64,
    /// Samples PipeWire pulled.
    pub pulled: u64,
    pub underruns: u64,
    pub stalls: u64,
    /// When PipeWire pulled the first real sample.
    pub first_pull: Option<Instant>,
}

/// #114: the pass-through a node is opened with: the user's own microphone into `dmn-voice`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Passthrough {
    /// The microphone's `node.name` (`target.object`; a stream's name works too, which is what
    /// the live test plays a tone through), or `None` for PipeWire's default source. A named
    /// one that is absent falls back to the default source, as the whisper lane does.
    pub target: Option<String>,
    /// Whether the microphone is heard from the first buffer (pass-through mode) or the gate
    /// starts shut (translate mode); [`VirtualMic::set_passthrough`] flips it later.
    pub on: bool,
}

/// #114: what the pass-through has done so far.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PassthroughStats {
    /// The capture stream's state: `Streaming` once the microphone is linked to it.
    pub state: VmicState,
    /// Microphone samples its callback pushed into the ring.
    pub pushed: u64,
    /// Samples that found no room (nothing pulling from the node, or a backlog about to be
    /// trimmed).
    pub dropped: u64,
    /// Samples the playback side skipped to cut a backlog down.
    pub skipped: u64,
    /// Samples the playback side pulled, heard or discarded.
    pub popped: u64,
    /// From the first sound reaching the ring to that sound leaving it for the call: what dmn
    /// adds to PipeWire's own latency.
    pub dwell: Option<Duration>,
    /// When the first sound reached the ring from the microphone.
    pub first_in: Option<Instant>,
    /// When the first sound left the ring for the call.
    pub first_out: Option<Instant>,
}

/// The virtual microphone, from the feeding thread's side. See the module doc for the node's
/// lifetime and the ring's rules.
pub struct VirtualMic {
    ctx: Arc<PipeWireContext>,
    key: Option<u64>,
    status: Arc<VmicStatus>,
    writer: VoiceWriter,
    rate: u32,
    resampler: Option<LinearResampler>,
    scratch: Vec<f32>,
    written: u64,
    /// #114: the pass-through's switch, its capture's status and its ring's counters.
    passthrough: Option<(Arc<AtomicBool>, Arc<VmicStatus>, Arc<TapStats>)>,
}

impl VirtualMic {
    /// Create the node at `rate` Hz (the graph rate, [`crate::NodeTable::graph_rate`]) with a
    /// ring of [`DEFAULT_RING_SECS`], and wait until it is in the graph. An error names what
    /// PipeWire refused; the node is gone again when this returns `Err`.
    pub fn open(ctx: Arc<PipeWireContext>, rate: u32) -> Result<Self, AudioError> {
        Self::open_with(ctx, rate, (rate * DEFAULT_RING_SECS) as usize)
    }

    /// [`Self::open`] with the ring capacity in samples.
    pub fn open_with(
        ctx: Arc<PipeWireContext>,
        rate: u32,
        ring_capacity: usize,
    ) -> Result<Self, AudioError> {
        Self::open_full(ctx, rate, ring_capacity, None)
    }

    /// #114: [`Self::open`] with the user's own microphone passing through the node.
    pub fn open_passthrough(
        ctx: Arc<PipeWireContext>,
        rate: u32,
        passthrough: Passthrough,
    ) -> Result<Self, AudioError> {
        Self::open_full(
            ctx,
            rate,
            (rate * DEFAULT_RING_SECS) as usize,
            Some(passthrough),
        )
    }

    fn open_full(
        ctx: Arc<PipeWireContext>,
        rate: u32,
        ring_capacity: usize,
        passthrough: Option<Passthrough>,
    ) -> Result<Self, AudioError> {
        let (writer, reader) = voice_ring(ring_capacity);
        let status = Arc::new(VmicStatus::default());
        let key = ctx.next_key();
        let mut tap = None;
        let mut tap_reader = None;
        let mut handle = None;
        if let Some(p) = passthrough {
            let (tap_writer, reader) = tap_ring((rate * TAP_RING_MS / 1000).max(1) as usize);
            let on = Arc::new(AtomicBool::new(p.on));
            let tap_status = Arc::new(VmicStatus::default());
            handle = Some((Arc::clone(&on), Arc::clone(&tap_status), tap_writer.stats()));
            tap_reader = Some((reader, on));
            tap = Some((
                TapSpec {
                    target: p.target,
                    rate,
                    status: tap_status,
                },
                tap_writer,
            ));
        }
        ctx.send(Command::OpenVmic {
            key,
            spec: VmicSpec {
                node_name: VMIC_NODE_NAME.to_owned(),
                description: VMIC_DESCRIPTION.to_owned(),
                rate,
                status: Arc::clone(&status),
                tap,
            },
            mixer: Mixer::new(reader, tap_reader, rate),
        })?;
        let mut mic = Self {
            ctx,
            key: Some(key),
            status,
            writer,
            rate,
            resampler: None,
            scratch: Vec::new(),
            written: 0,
            passthrough: handle,
        };
        let t = Instant::now();
        loop {
            match mic.status.state() {
                VmicState::Ready | VmicState::Streaming => return Ok(mic),
                VmicState::Error => {
                    let why = mic.status.last_error().unwrap_or_default();
                    let _ = mic.close_inner();
                    return Err(AudioError::VirtualMic(why));
                }
                VmicState::Idle | VmicState::Connecting => {
                    if t.elapsed() > OPEN_TIMEOUT {
                        let _ = mic.close_inner();
                        return Err(AudioError::VirtualMic(format!(
                            "the stream was still {} after {} ms",
                            mic.status.state().label(),
                            OPEN_TIMEOUT.as_millis()
                        )));
                    }
                    std::thread::sleep(WAIT_TICK);
                }
            }
        }
    }

    /// The rate the node plays at.
    pub fn rate(&self) -> u32 {
        self.rate
    }

    pub fn status(&self) -> Arc<VmicStatus> {
        Arc::clone(&self.status)
    }

    pub fn stats(&self) -> VmicStats {
        let s = self.writer.stats();
        VmicStats {
            written: self.written,
            pulled: s.pulled(),
            underruns: s.underruns(),
            stalls: s.stalls(),
            first_pull: s.first_pull(),
        }
    }

    /// #114: open or shut the microphone's gate (pass-through mode on or off). The playback
    /// callback fades over [`PASSTHROUGH_FADE_MS`] from its next buffer; speech in flight keeps
    /// the gate shut whatever this says. Nothing without a pass-through.
    pub fn set_passthrough(&self, on: bool) {
        if let Some((switch, ..)) = &self.passthrough {
            switch.store(on, Ordering::Relaxed);
        }
    }

    /// #114: whether the microphone's gate is set open; `None` for a node with no pass-through.
    pub fn passthrough(&self) -> Option<bool> {
        self.passthrough
            .as_ref()
            .map(|(switch, ..)| switch.load(Ordering::Relaxed))
    }

    /// #114: what the pass-through has done so far; `None` for a node without one.
    pub fn passthrough_stats(&self) -> Option<PassthroughStats> {
        let (_, status, stats) = self.passthrough.as_ref()?;
        Some(PassthroughStats {
            state: status.state(),
            pushed: stats.pushed(),
            dropped: stats.dropped(),
            skipped: stats.skipped(),
            popped: stats.popped(),
            dwell: stats.dwell(),
            first_in: stats.first_in(),
            first_out: stats.first_out(),
        })
    }

    /// #114: why the pass-through's capture is in error, if it is.
    pub fn passthrough_error(&self) -> Option<String> {
        let (_, status, _) = self.passthrough.as_ref()?;
        (status.state() == VmicState::Error).then(|| status.last_error().unwrap_or_default())
    }

    /// Queue `samples` (mono f32 at `sample_rate`) for playback, resampled to the node's rate.
    /// Waits while the ring is full; fails if PipeWire reports an error, or if nothing pulled
    /// from a full ring for [`STALL_TIMEOUT`] (the node is in the graph but nothing is linked
    /// to it, so the speech would never be heard).
    pub fn write(&mut self, samples: &[f32], sample_rate: u32) -> Result<(), AudioError> {
        if self
            .resampler
            .as_ref()
            .is_none_or(|r| r.from_rate() != sample_rate)
        {
            self.resampler = Some(LinearResampler::new(sample_rate, self.rate));
        }
        self.scratch.clear();
        if let Some(r) = self.resampler.as_mut() {
            r.process(samples, &mut self.scratch);
        }
        let mut rest: &[f32] = &self.scratch;
        let mut last_progress = Instant::now();
        while !rest.is_empty() {
            let n = self.writer.push(rest);
            rest = &rest[n..];
            if n > 0 {
                self.written += n as u64;
                self.writer.set_speaking(true);
                last_progress = Instant::now();
            }
            if rest.is_empty() {
                break;
            }
            self.check()?;
            if last_progress.elapsed() > STALL_TIMEOUT {
                return Err(AudioError::VirtualMic(format!(
                    "nothing pulled from {VMIC_NODE_NAME} for {} s while it was full ({}); is it selected as the microphone anywhere?",
                    STALL_TIMEOUT.as_secs(),
                    self.status.state().label()
                )));
            }
            std::thread::sleep(WAIT_TICK);
        }
        Ok(())
    }

    /// Wait until PipeWire has pulled everything written, or `timeout`. Returns whether it
    /// did; either way the underrun gate closes, so the silence after the clip is not counted.
    pub fn drain(&mut self, timeout: Duration) -> Result<bool, AudioError> {
        let stats = self.writer.stats();
        let t = Instant::now();
        let drained = loop {
            if stats.pulled() >= self.written {
                break true;
            }
            self.check()?;
            if t.elapsed() > timeout {
                break false;
            }
            std::thread::sleep(WAIT_TICK);
        };
        self.writer.set_speaking(false);
        Ok(drained)
    }

    /// Take the node out of the graph. `Drop` does the same without reporting.
    pub fn close(mut self) -> Result<(), AudioError> {
        self.close_inner()
    }

    fn check(&self) -> Result<(), AudioError> {
        match self.status.state() {
            VmicState::Error => Err(AudioError::VirtualMic(
                self.status.last_error().unwrap_or_default(),
            )),
            VmicState::Idle => Err(AudioError::VirtualMic(
                "the stream was closed (PipeWire loop gone?)".into(),
            )),
            _ => Ok(()),
        }
    }

    fn close_inner(&mut self) -> Result<(), AudioError> {
        let Some(key) = self.key.take() else {
            return Ok(());
        };
        if self.ctx.send(Command::CloseVmic { key }).is_err() {
            // The loop is gone, and the stream and node went with it.
            return Ok(());
        }
        let t = Instant::now();
        while self.status.state() != VmicState::Idle && t.elapsed() < CLOSE_TIMEOUT {
            std::thread::sleep(Duration::from_millis(2));
        }
        if self.status.state() != VmicState::Idle {
            return Err(AudioError::StopTimeout {
                state: self.status.state().label(),
                ms: CLOSE_TIMEOUT.as_millis() as u64,
            });
        }
        Ok(())
    }
}

impl Drop for VirtualMic {
    fn drop(&mut self) {
        let _ = self.close_inner();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn underrun_emits_silence_and_counts_only_while_speaking() {
        let (writer, mut reader) = voice_ring(100);
        let stats = writer.stats();
        let mut out = [1.0f32; 16];
        // Empty ring, gate closed: silence, no underrun (nothing is playing yet).
        let t = Instant::now();
        assert_eq!(reader.fill(&mut out), 0);
        assert!(out.iter().all(|&s| s == 0.0), "padding is silence");
        assert!(t.elapsed() < Duration::from_millis(5), "returned at once");
        assert_eq!(stats.underruns(), 0);
        assert_eq!(stats.pulled(), 0);
        assert!(stats.first_pull().is_none());

        // Gate open: a short fill pads with silence (the clip's last buffer, not a gap); an
        // empty fill while speaking is a gap and counts once per fill.
        writer.set_speaking(true);
        let mut writer = writer;
        assert_eq!(writer.push(&[0.5; 10]), 10);
        out.fill(1.0);
        assert_eq!(reader.fill(&mut out), 10);
        assert!(out[..10].iter().all(|&s| s == 0.5));
        assert!(out[10..].iter().all(|&s| s == 0.0));
        assert_eq!(stats.underruns(), 0, "a short last buffer is not a gap");
        assert_eq!(stats.pulled(), 10);
        assert!(
            stats.first_pull().is_some(),
            "first real sample timestamped"
        );
        out.fill(1.0);
        assert_eq!(reader.fill(&mut out), 0);
        assert!(out.iter().all(|&s| s == 0.0), "a gap is silence");
        assert_eq!(stats.underruns(), 1);
        assert_eq!(reader.fill(&mut out), 0);
        assert_eq!(stats.underruns(), 2);
        writer.set_speaking(false);
        assert_eq!(reader.fill(&mut out), 0);
        assert_eq!(stats.underruns(), 2, "gate closed: the tail is not a gap");
    }

    #[test]
    fn a_full_ring_takes_what_fits_keeps_what_is_queued_and_counts_the_stall() {
        let (mut writer, mut reader) = voice_ring(8);
        assert_eq!(writer.push(&[1.0; 6]), 6);
        assert_eq!(writer.stats().stalls(), 0);
        let t = Instant::now();
        assert_eq!(writer.push(&[2.0; 6]), 2, "only two slots were free");
        assert!(t.elapsed() < Duration::from_millis(5), "never blocks");
        assert_eq!(writer.stats().stalls(), 1);
        assert_eq!(writer.push(&[3.0; 1]), 0);
        assert_eq!(writer.stats().stalls(), 2);
        assert_eq!(writer.vacant(), 0);
        // Nothing queued was lost: the oldest samples come out first, in order.
        let mut out = [0.0f32; 8];
        assert_eq!(reader.fill(&mut out), 8);
        assert_eq!(&out[..6], &[1.0; 6]);
        assert_eq!(&out[6..], &[2.0; 2]);
        assert_eq!(reader.queued(), 0);
        // Draining frees exactly that many slots.
        assert_eq!(writer.push(&[4.0; 9]), 8);
    }

    /// 24 kHz → 48 kHz: a 440 Hz sine keeps its frequency and the length doubles.
    #[test]
    fn resampler_doubles_the_length_and_keeps_the_frequency() {
        let (from, to, hz) = (24_000u32, 48_000u32, 440.0f32);
        let secs = 0.5;
        let n_in = (from as f32 * secs) as usize;
        let input: Vec<f32> = (0..n_in)
            .map(|i| (std::f32::consts::TAU * hz * i as f32 / from as f32).sin())
            .collect();
        let mut r = LinearResampler::new(from, to);
        let mut out = Vec::new();
        // Fed in uneven chunks, as the decoder delivers them.
        for chunk in input.chunks(1_333) {
            r.process(chunk, &mut out);
        }
        let expect = n_in * to as usize / from as usize;
        assert!(
            out.len().abs_diff(expect) <= 2,
            "length {} ≈ {expect}",
            out.len()
        );
        // Frequency from zero crossings (the sine starts at 0 and the output is continuous).
        let crossings = out.windows(2).filter(|w| w[0] < 0.0 && w[1] >= 0.0).count();
        let measured = crossings as f32 / (out.len() as f32 / to as f32);
        assert!(
            (measured - hz).abs() <= 2.0,
            "{measured} Hz at {to} Hz, expected {hz}"
        );
        // Amplitude survives (linear interpolation of a 440 Hz sine at 24 kHz loses almost
        // nothing).
        let peak = out.iter().fold(0.0f32, |m, s| m.max(s.abs()));
        assert!(peak > 0.99, "peak {peak}");
        // Chunk boundaries are seamless: no sample-to-sample jump larger than the sine's own
        // slope allows.
        let max_step = out
            .windows(2)
            .map(|w| (w[1] - w[0]).abs())
            .fold(0.0, f32::max);
        let slope = std::f32::consts::TAU * hz / to as f32;
        assert!(
            max_step <= slope * 1.05,
            "max step {max_step} vs slope {slope}"
        );
        assert_eq!((r.from_rate(), r.to_rate()), (from, to));
    }

    #[test]
    fn resampler_at_the_same_rate_is_the_identity() {
        let mut r = LinearResampler::new(48_000, 48_000);
        let input: Vec<f32> = (0..1000).map(|i| i as f32).collect();
        let mut out = Vec::new();
        r.process(&input[..300], &mut out);
        r.process(&input[300..], &mut out);
        assert_eq!(out, input);
    }

    #[test]
    fn resampler_downsamples_too() {
        // 48 kHz → 16 kHz: a third of the samples, the ramp intact.
        let mut r = LinearResampler::new(48_000, 16_000);
        let input: Vec<f32> = (0..3000).map(|i| i as f32).collect();
        let mut out = Vec::new();
        r.process(&input, &mut out);
        assert_eq!(out.len(), 1000);
        assert_eq!(out[0], 0.0);
        assert_eq!(out[1], 3.0);
        assert_eq!(out[999], 2997.0);
    }

    #[test]
    fn state_labels_and_status_round_trip() {
        for s in [
            VmicState::Idle,
            VmicState::Connecting,
            VmicState::Ready,
            VmicState::Streaming,
            VmicState::Error,
        ] {
            assert_eq!(VmicState::from_u8(s.to_u8()), s);
            assert!(!s.label().is_empty());
        }
        let st = VmicStatus::default();
        assert_eq!(st.state(), VmicState::Idle);
        assert_eq!(st.rate(), None);
        st.set_error("refused".into());
        assert_eq!(st.state(), VmicState::Error);
        assert_eq!(st.last_error().as_deref(), Some("refused"));
    }

    // ---- #114: the pass-through's ring and the mixer -------------------------------------

    /// A test rate: 8 ms of fade is 8 samples, the backlog limits 60 and 20 samples.
    const RATE: u32 = 1_000;
    const FADE: usize = (RATE * PASSTHROUGH_FADE_MS / 1000) as usize;

    /// A mixer at [`RATE`] over a speech ring and a mic ring, with the switch.
    fn mixer(on: bool) -> (Mixer, VoiceWriter, TapWriter, Arc<AtomicBool>) {
        let (speech, speech_reader) = voice_ring(1_000);
        let (mic, mic_reader) = tap_ring(1_000);
        let switch = Arc::new(AtomicBool::new(on));
        let m = Mixer::new(speech_reader, Some((mic_reader, Arc::clone(&switch))), RATE);
        (m, speech, mic, switch)
    }

    #[test]
    fn the_tap_ring_drops_the_newest_mic_samples_when_full_and_counts_them() {
        let (mut w, mut r) = tap_ring(8);
        assert_eq!(w.push(&[0.1; 6]), 6);
        let t = Instant::now();
        assert_eq!(w.push(&[0.2; 6]), 2, "two slots were free");
        assert!(t.elapsed() < Duration::from_millis(5), "never blocks");
        let stats = w.stats();
        assert_eq!((stats.pushed(), stats.dropped()), (8, 4));
        assert!(stats.first_in().is_some(), "0.1 is sound");
        // The oldest come out first; an empty ring is silence.
        let mut out = [1.0f32; 10];
        assert_eq!(r.pop(&mut out), 8);
        assert_eq!(&out[..6], &[0.1; 6]);
        assert_eq!(&out[6..8], &[0.2; 2]);
        assert_eq!(&out[8..], &[0.0; 2]);
        assert_eq!(stats.popped(), 8);
        // Silence is not sound: the stamp waits for it.
        let (w2, _) = tap_ring(8);
        let mut w2 = w2;
        w2.push(&[0.0; 4]);
        assert!(w2.stats().first_in().is_none());
    }

    #[test]
    fn a_mic_backlog_is_skipped_down_so_pass_through_never_lags() {
        let (mut w, mut r) = tap_ring(1_000);
        let ramp: Vec<f32> = (0..200).map(|i| i as f32 / 1000.0).collect();
        w.push(&ramp);
        r.trim(60, 20);
        assert_eq!(r.queued(), 20);
        assert_eq!(r.stats().skipped(), 180);
        let mut out = [0.0; 20];
        r.pop(&mut out);
        assert_eq!(out[0], 0.180, "the newest 20 ms are what is left");
        // Under the limit nothing is touched.
        w.push(&ramp[..50]);
        r.trim(60, 20);
        assert_eq!(r.queued(), 50);
    }

    #[test]
    fn the_mixer_passes_the_mic_through_when_the_gate_is_open_and_fades_it_in_when_it_opens() {
        // Open from the start: the first buffer is the microphone, whole.
        let (mut m, _speech, mut mic, _) = mixer(true);
        assert_eq!(m.gain(), 1.0);
        mic.push(&[0.5; 40]);
        let mut out = [1.0f32; 40];
        assert_eq!(m.fill(&mut out), 0, "no speech");
        assert!(out.iter().all(|&s| (s - 0.5).abs() < 1e-6), "{out:?}");
        assert!(mic.stats().first_out().is_some());

        // Shut at the start (translate mode), then opened: a fade of FADE samples, no step.
        let (mut m, _speech, mut mic, switch) = mixer(false);
        assert_eq!(m.gain(), 0.0);
        mic.push(&[0.5; 40]);
        let mut out = [1.0f32; 20];
        m.fill(&mut out);
        assert!(
            out.iter().all(|&s| s == 0.0),
            "shut: silence, mic discarded"
        );
        assert_eq!(mic.stats().popped(), 20, "the mic kept flowing past");
        switch.store(true, Ordering::Relaxed);
        let mut out = [0.0f32; 20];
        m.fill(&mut out);
        for (i, s) in out.iter().enumerate() {
            let want = 0.5 * ((i + 1) as f32 / FADE as f32).min(1.0);
            assert!((s - want).abs() < 1e-5, "sample {i}: {s} ≠ {want}");
        }
        assert_eq!(m.gain(), 1.0);
    }

    /// The acceptance criterion: no microphone sample reaches the output while speech plays,
    /// whatever the mode; the hand-over is a fade, and the microphone is back after the drain.
    #[test]
    fn no_mic_sample_reaches_the_output_while_speech_plays_whatever_the_mode() {
        // The microphone is pushed a buffer at a time, as its callback does; a pile pushed at
        // once would be a backlog and trimmed.
        let (mut m, mut speech, mut mic, switch) = mixer(true);
        mic.push(&[0.5; 10]);
        let mut out = [0.0f32; 10];
        m.fill(&mut out);
        assert!(
            out.iter().all(|&s| (s - 0.5).abs() < 1e-6),
            "mic alone first"
        );

        // A sentence arrives: the writer sets `speaking` with its first push.
        speech.set_speaking(true);
        assert_eq!(speech.push(&[0.25; 100]), 100);
        mic.push(&[0.5; 30]);
        let mut out = [0.0f32; 30];
        assert_eq!(m.fill(&mut out), 30);
        for (i, s) in out.iter().enumerate() {
            let mic_gain = (1.0 - (i + 1) as f32 / FADE as f32).max(0.0);
            let want = 0.25 + 0.5 * mic_gain;
            assert!((s - want).abs() < 1e-5, "sample {i}: {s} ≠ {want}");
        }
        assert!(
            out[FADE..].iter().all(|&s| (s - 0.25).abs() < 1e-6),
            "after the fade only the speech is heard"
        );
        // The next buffer, still speaking: speech only, the mic flowing past unheard.
        let popped = mic.stats().popped();
        mic.push(&[0.5; 20]);
        let mut out = [0.0f32; 20];
        m.fill(&mut out);
        assert!(out.iter().all(|&s| (s - 0.25).abs() < 1e-6), "{out:?}");
        assert_eq!(mic.stats().popped(), popped + 20);
        // The speech ring runs dry mid-sentence (the server is slow): still no microphone.
        mic.push(&[0.5; 60]);
        let mut out = [0.0f32; 60];
        assert_eq!(m.fill(&mut out), 50, "the sentence's last 50 samples");
        assert!(out[..50].iter().all(|&s| (s - 0.25).abs() < 1e-6));
        assert!(
            out[50..].iter().all(|&s| s == 0.0),
            "then silence, not the mic"
        );

        // Translate mode (the switch off): speech, never the mic.
        switch.store(false, Ordering::Relaxed);
        speech.push(&[0.25; 20]);
        mic.push(&[0.5; 20]);
        let mut out = [0.0f32; 20];
        m.fill(&mut out);
        assert!(out.iter().all(|&s| (s - 0.25).abs() < 1e-6));
        speech.set_speaking(false);
        mic.push(&[0.5; 20]);
        let mut out = [0.0f32; 20];
        m.fill(&mut out);
        assert!(
            out.iter().all(|&s| s == 0.0),
            "translate mode: the mic stays out"
        );

        // Back in pass-through mode after the drain: the mic fades back in.
        switch.store(true, Ordering::Relaxed);
        mic.push(&[0.5; 20]);
        let mut out = [0.0f32; 20];
        m.fill(&mut out);
        assert!(out[0] > 0.0 && out[0] < 0.5 && out[FADE] == 0.5, "{out:?}");
    }

    #[test]
    fn a_switch_is_a_fade_not_a_step() {
        let (mut m, _speech, mut mic, switch) = mixer(true);
        mic.push(&[0.5; 10]);
        let mut out = [0.0f32; 10];
        m.fill(&mut out);
        switch.store(false, Ordering::Relaxed);
        mic.push(&[0.5; 20]);
        let mut out = [0.0f32; 20];
        m.fill(&mut out);
        let max_step = out
            .windows(2)
            .map(|w| (w[0] - w[1]).abs())
            .fold(0.0, f32::max);
        assert!(
            max_step <= 0.5 / FADE as f32 + 1e-5,
            "largest step {max_step} over a fade of {FADE} samples"
        );
        assert_eq!(out[FADE], 0.0, "shut after the fade");
        assert_eq!(m.gain(), 0.0);
    }

    #[test]
    fn a_node_without_pass_through_mixes_speech_alone() {
        let (mut speech, reader) = voice_ring(100);
        let mut m = Mixer::new(reader, None, RATE);
        speech.push(&[0.25; 10]);
        let mut out = [1.0f32; 20];
        assert_eq!(m.fill(&mut out), 10);
        assert!(out[..10].iter().all(|&s| s == 0.25));
        assert!(out[10..].iter().all(|&s| s == 0.0));
    }

    // ---- Live tests: need a running PipeWire server and pw-record. `just probe`.

    mod live {
        use super::*;
        use std::io::Read as _;
        use std::process::{Command as Proc, Stdio};

        const TONE_HZ: f32 = 440.0;

        /// Both tests create the one `dmn-voice`; `pw-record --target dmn-voice` would pick
        /// either, so they run one at a time.
        static ONE_AT_A_TIME: Mutex<()> = Mutex::new(());

        fn connect() -> Arc<PipeWireContext> {
            let ctx = Arc::new(PipeWireContext::connect().expect("PipeWire running"));
            assert!(ctx.wait_for_sync(Duration::from_secs(2)).synced);
            ctx
        }

        fn node_count() -> usize {
            let out = Proc::new("pw-dump").output().expect("pw-dump on PATH");
            String::from_utf8_lossy(&out.stdout)
                .matches(&format!("\"node.name\": \"{VMIC_NODE_NAME}\""))
                .count()
        }

        /// Record the virtual microphone as raw 48 kHz mono f32 on a pipe: nothing on disk,
        /// even in tests (CLAUDE.md).
        fn record(rate: u32) -> std::process::Child {
            Proc::new("pw-record")
                .args(["--target", VMIC_NODE_NAME, "--raw"])
                .args([
                    "--rate",
                    &rate.to_string(),
                    "--channels",
                    "1",
                    "--format",
                    "f32",
                ])
                .arg("-")
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .spawn()
                .expect("pw-record on PATH")
        }

        fn goertzel(w: &[f32], hz: f32, rate: f32) -> f32 {
            let k = 2.0 * (2.0 * std::f32::consts::PI * hz / rate).cos();
            let (mut s1, mut s2) = (0.0f32, 0.0f32);
            for &x in w {
                let s0 = x + k * s1 - s2;
                s2 = s1;
                s1 = s0;
            }
            s1 * s1 + s2 * s2 - k * s1 * s2
        }

        /// Speak a 1.5 s 440 Hz tone generated at 24 kHz (Chatterbox's rate) through
        /// `dmn-voice`, record the node with `pw-record --target dmn-voice`, and check what
        /// came out: loud enough, the right length, the right pitch at 48 kHz, and the node
        /// gone afterwards.
        #[test]
        #[ignore]
        fn live_vmic_plays_a_tone_pw_record_hears_and_the_node_leaves_the_graph() {
            let _serial = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
            let ctx = connect();
            let graph_rate = ctx.nodes().graph_rate();
            let before = node_count();
            let mut mic = VirtualMic::open(Arc::clone(&ctx), graph_rate).expect("vmic opens");
            assert_eq!(mic.rate(), graph_rate);
            assert_eq!(node_count(), before + 1, "dmn-voice is in the graph");
            assert!(matches!(
                mic.status().state(),
                VmicState::Ready | VmicState::Streaming
            ));

            let mut rec = record(graph_rate);
            let mut stdout = rec.stdout.take().unwrap();
            let reader = std::thread::spawn(move || {
                let mut buf = Vec::new();
                let _ = stdout.read_to_end(&mut buf);
                buf
            });
            // Let pw-record link before the tone starts, so its start is in the recording.
            std::thread::sleep(Duration::from_millis(500));

            let (tts_rate, secs) = (24_000u32, 1.5f32);
            let n = (tts_rate as f32 * secs) as usize;
            let tone: Vec<f32> = (0..n)
                .map(|i| 0.5 * (std::f32::consts::TAU * TONE_HZ * i as f32 / tts_rate as f32).sin())
                .collect();
            let t = Instant::now();
            for chunk in tone.chunks(4_800) {
                mic.write(chunk, tts_rate).expect("write");
            }
            assert!(
                mic.drain(Duration::from_secs(5)).expect("drain"),
                "all pulled"
            );
            let played = t.elapsed();
            let stats = mic.stats();
            std::thread::sleep(Duration::from_millis(300));
            let _ = rec.kill();
            let _ = rec.wait();
            let raw = reader.join().unwrap();
            mic.close().expect("close");
            assert_eq!(node_count(), before, "dmn-voice left the graph");

            // Real time: 1.5 s of audio took about 1.5 s to play (the ring holds 2 s, so the
            // writes themselves never waited; drain did).
            assert!(
                played >= Duration::from_millis(1_200) && played <= Duration::from_millis(2_500),
                "played in {played:?}"
            );
            let expect = (graph_rate as f32 * secs) as u64;
            assert!(
                stats.written.abs_diff(expect) <= 2,
                "written {} ≈ {expect} (the resampler holds back under one sample)",
                stats.written
            );
            assert_eq!(stats.pulled, stats.written);
            assert!(stats.first_pull.is_some());
            assert_eq!(stats.stalls, 0);

            let samples: Vec<f32> = raw
                .chunks_exact(4)
                .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                .collect();
            assert!(!samples.is_empty(), "pw-record heard the node");
            // Duration of the non-silent part, within 10 % of the tone.
            let loud: Vec<usize> = samples
                .iter()
                .enumerate()
                .filter(|(_, s)| s.abs() > 0.05)
                .map(|(i, _)| i)
                .collect();
            let (first, last) = (loud[0], *loud.last().unwrap());
            let heard = (last - first) as f32 / graph_rate as f32;
            assert!(
                (heard - secs).abs() <= secs * 0.1,
                "heard {heard:.3} s of tone, expected {secs} ± 10 %"
            );
            let tone_part = &samples[first..=last];
            let rms =
                (tone_part.iter().map(|s| s * s).sum::<f32>() / tone_part.len() as f32).sqrt();
            assert!(rms > 0.2, "rms {rms} (a 0.5 sine is 0.354)");
            // Pitch: 440 Hz dominates its neighbours (880 Hz would mean the 24 kHz samples
            // were played at 48 kHz, i.e. no resampling).
            let rate = graph_rate as f32;
            let on = goertzel(tone_part, TONE_HZ, rate);
            let off = goertzel(tone_part, 880.0, rate).max(goertzel(tone_part, 220.0, rate));
            assert!(on / off.max(1e-9) > 100.0, "440 Hz dominates: {}", on / off);
        }

        /// Whether a recorder linked to `dmn-voice` is linked again after the node is closed
        /// and a new one opened on a fresh connection, as a dmn restart does: (linked at
        /// first, linked again within 5 s). With `other_stream`, another stream (a second
        /// recorder on the default source, read and dropped) comes and goes while the node is
        /// gone, as anything that plays or records meanwhile does.
        fn relinked_after_a_restart(recorder: &[&str], other_stream: bool) -> (bool, bool) {
            let wait_streaming = |mic: &VirtualMic, within: Duration| {
                let t = Instant::now();
                while mic.status().state() != VmicState::Streaming && t.elapsed() < within {
                    std::thread::sleep(Duration::from_millis(20));
                }
                mic.status().state() == VmicState::Streaming
            };
            let ctx = connect();
            let rate = ctx.nodes().graph_rate();
            let mic = VirtualMic::open(Arc::clone(&ctx), rate).expect("vmic opens");
            // Raw samples on a pipe that is read and dropped: nothing on disk.
            let mut rec = Proc::new(recorder[0])
                .args(&recorder[1..])
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .spawn()
                .expect("recorder on PATH");
            let mut out = rec.stdout.take().unwrap();
            std::thread::spawn(move || {
                let mut buf = [0u8; 65_536];
                while out.read(&mut buf).is_ok_and(|n| n > 0) {}
            });
            let linked = wait_streaming(&mic, Duration::from_secs(3));
            drop(mic);
            drop(ctx);
            if other_stream {
                let mut other = Proc::new("pw-record")
                    .args(["--raw", "-"])
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .spawn()
                    .unwrap();
                std::thread::sleep(Duration::from_millis(400));
                let _ = other.kill();
                let _ = other.wait();
            }
            std::thread::sleep(Duration::from_millis(1_000));
            let ctx = connect();
            let mic = VirtualMic::open(Arc::clone(&ctx), rate).expect("vmic reopens");
            let relinked = wait_streaming(&mic, Duration::from_secs(5));
            let _ = rec.kill();
            let _ = rec.wait();
            (linked, relinked)
        }

        /// #113: `dmn-voice` across a dmn restart. The node is a new one each run (same
        /// `node.name`, new `object.serial`). WirePlumber relinks a stream that names it by
        /// name in its own properties (`target.object = "dmn-voice"`, as `pw-record --target`
        /// and a PulseAudio app opening the device by name do) only while nothing else happens:
        /// meanwhile the stream sits on the default source, and the first rescan (any stream
        /// that comes or goes) marks it to follow the default from then on, as PulseAudio did
        /// (`lutils.checkFollowDefault`). A stream with `node.dont-reconnect` (what a
        /// PulseAudio app gets when it asks not to be moved, as Chromium-based apps such as
        /// Discord do for a chosen device) is never linked again once handled
        /// (`linking/prepare-link.lua`), and a stream moved by hand is kept in the `default`
        /// metadata by serial, which a new node never matches. No property of the node changes
        /// any of it (a fixed `object.path`, `node.virtual`, `device.description` and
        /// `node.link-group` were tried), so `dmn run` says so on the status line instead.
        /// The first case depends on the graph staying quiet, which other tests running in
        /// parallel break, so it is printed, not asserted.
        #[test]
        #[ignore]
        fn live_a_reopened_dmn_voice_is_not_reliably_relinked() {
            let _serial = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
            let by_name = ["pw-record", "--target", VMIC_NODE_NAME, "--raw", "-"];
            let quiet = relinked_after_a_restart(&by_name, false);
            eprintln!(
                "by name, nothing else in the graph meanwhile: (linked, relinked) = {quiet:?}"
            );
            assert!(quiet.0, "linked at first");
            assert_eq!(
                relinked_after_a_restart(&by_name, true),
                (true, false),
                "by name, another stream meanwhile: it follows the default source from then on"
            );
            let dont_move = [
                "pw-record",
                "--target",
                VMIC_NODE_NAME,
                "-P",
                "{ node.dont-reconnect = true }",
                "--raw",
                "-",
            ];
            assert_eq!(
                relinked_after_a_restart(&dont_move, false),
                (true, false),
                "if this now relinks, PipeWire keeps a call's choice across a restart and the \
                 start-up notice in dmn-cli's speaker can go"
            );
        }

        /// #114, the issue's first criterion: a 440 Hz tone played by `pw-play` as a stream
        /// named `dmn-test-114-tone` (nothing on disk: raw samples on its stdin), captured by
        /// the pass-through's stream through the same PipeWire capture path the mic lane uses
        /// (`target.object` naming the node, as the Interviewer lane names an app's stream),
        /// written through the mixer into `dmn-voice`, and recorded there with `pw-record
        /// --target dmn-voice`: the right length, the right pitch at the graph rate, loud. The
        /// real microphone is never used: the tap names the tone's node and does not fall back.
        /// Prints the latency: the microphone's dwell in dmn (its first sound reaching the ring
        /// to that sound leaving it, both stamped in the callbacks) and the same sound reaching
        /// `pw-record`'s pipe (which adds `pw-record`'s own quantum and the pipe). The dwell
        /// must be under the issue's 50 ms.
        #[test]
        #[ignore]
        fn live_passthrough_carries_a_tone_from_a_capture_into_dmn_voice_within_50_ms() {
            let _serial = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
            const TONE_NODE: &str = "dmn-test-114-tone";
            let ctx = connect();
            let rate = ctx.nodes().graph_rate();

            // The microphone: a stream playing silence until `go`, then 1.5 s of tone and 0.3 s
            // of silence, at the graph rate so nothing resamples it on the way. It is in the
            // graph before the tap opens, so the tap can only ever link to it (a named target
            // that is absent falls back to the default source, which here would be the real
            // microphone).
            let secs = 1.5f32;
            let mut play = Proc::new("pw-play")
                .args(["--raw", "--rate", &rate.to_string(), "--channels", "1"])
                .args(["--format", "f32", "-P"])
                .arg(format!("{{ node.name = \"{TONE_NODE}\" }}"))
                .arg("-")
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("pw-play on PATH");
            let mut stdin = play.stdin.take().unwrap();
            let go = Arc::new(AtomicBool::new(false));
            let feeder = {
                let go = Arc::clone(&go);
                std::thread::spawn(move || {
                    use std::io::Write as _;
                    let silence = vec![0u8; rate as usize / 10 * STRIDE];
                    while !go.load(Ordering::Acquire) {
                        if stdin.write_all(&silence).is_err() {
                            return;
                        }
                    }
                    let n = (rate as f32 * secs) as usize;
                    let tone: Vec<u8> = (0..n)
                        .map(|i| {
                            0.5 * (std::f32::consts::TAU * TONE_HZ * i as f32 / rate as f32).sin()
                        })
                        .chain(std::iter::repeat_n(0.0, (rate as f32 * 0.3) as usize))
                        .flat_map(f32::to_le_bytes)
                        .collect();
                    let _ = stdin.write_all(&tone);
                })
            };
            let t = Instant::now();
            while !String::from_utf8_lossy(&Proc::new("pw-dump").output().unwrap().stdout)
                .contains(&format!("\"node.name\": \"{TONE_NODE}\""))
            {
                assert!(
                    t.elapsed() < Duration::from_secs(3),
                    "the tone's node never came"
                );
                std::thread::sleep(Duration::from_millis(50));
            }

            let mic = VirtualMic::open_passthrough(
                Arc::clone(&ctx),
                rate,
                Passthrough {
                    target: Some(TONE_NODE.to_owned()),
                    on: true,
                },
            )
            .expect("vmic opens");
            assert_eq!(mic.passthrough(), Some(true));

            // The call: pw-record on dmn-voice, each loud read stamped when it arrives.
            let mut rec = record(rate);
            let mut stdout = rec.stdout.take().unwrap();
            let reader = std::thread::spawn(move || {
                let (mut raw, mut first_loud, mut buf) = (Vec::new(), None, [0u8; 4096]);
                while let Ok(n) = stdout.read(&mut buf) {
                    if n == 0 {
                        break;
                    }
                    if first_loud.is_none()
                        && buf[..n]
                            .chunks_exact(4)
                            .any(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]).abs() >= LOUD)
                    {
                        first_loud = Some(Instant::now());
                    }
                    raw.extend_from_slice(&buf[..n]);
                }
                (raw, first_loud)
            });
            let t = Instant::now();
            loop {
                let linked = mic.status().state() == VmicState::Streaming
                    && mic
                        .passthrough_stats()
                        .is_some_and(|s| s.state == VmicState::Streaming);
                if linked {
                    break;
                }
                assert!(
                    t.elapsed() < Duration::from_secs(3),
                    "pw-record or the tap never linked: {:?} / {:?}",
                    mic.status().state(),
                    mic.passthrough_error()
                );
                std::thread::sleep(Duration::from_millis(20));
            }
            // Both links settled, and the silence has been flowing for a moment.
            std::thread::sleep(Duration::from_millis(300));
            go.store(true, Ordering::Release);
            let _ = feeder.join();
            let _ = play.wait();
            // The tail of the tone is still in the rings and pw-record's quantum.
            std::thread::sleep(Duration::from_millis(500));
            let stats = mic.passthrough_stats().expect("a pass-through");
            let _ = rec.kill();
            let _ = rec.wait();
            let (raw, heard_at) = reader.join().unwrap();
            let tap_error = mic.passthrough_error();
            mic.close().expect("close");

            assert_eq!(tap_error, None);
            assert!(stats.pushed > 0, "the capture pushed samples");
            let heard: Vec<f32> = raw
                .chunks_exact(4)
                .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                .collect();
            let loud: Vec<usize> = heard
                .iter()
                .enumerate()
                .filter(|(_, s)| s.abs() > LOUD)
                .map(|(i, _)| i)
                .collect();
            assert!(
                !loud.is_empty(),
                "pw-record heard the tone through dmn-voice"
            );
            let (first, last) = (loud[0], *loud.last().unwrap());
            let length = (last - first) as f32 / rate as f32;
            assert!(
                (length - secs).abs() <= secs * 0.1,
                "heard {length:.3} s of tone, expected {secs} ± 10 %"
            );
            let tone = &heard[first..=last];
            let rms = (tone.iter().map(|s| s * s).sum::<f32>() / tone.len() as f32).sqrt();
            assert!(rms > 0.2, "rms {rms} (a 0.5 sine is 0.354)");
            let on = goertzel(tone, TONE_HZ, rate as f32);
            let off = goertzel(tone, 880.0, rate as f32).max(goertzel(tone, 220.0, rate as f32));
            assert!(on / off.max(1e-9) > 100.0, "440 Hz dominates: {}", on / off);

            let dwell = stats.dwell.expect("the tone went in and out");
            let to_recorder = heard_at
                .zip(stats.first_in)
                .map(|(heard, entered)| heard.saturating_duration_since(entered));
            eprintln!(
                "pass-through latency: in dmn (capture callback → playback callback) {:.1} ms; \
                 capture callback → pw-record's pipe {}; {} samples pushed, {} dropped, {} skipped",
                dwell.as_secs_f64() * 1000.0,
                to_recorder.map_or("unknown".to_owned(), |d| format!(
                    "{:.1} ms",
                    d.as_secs_f64() * 1000.0
                )),
                stats.pushed,
                stats.dropped,
                stats.skipped
            );
            assert!(
                dwell < Duration::from_millis(50),
                "mic → dmn-voice took {dwell:?}, over the 50 ms target"
            );
        }

        /// Nothing linked: the node sits in the graph, nothing is pulled, and `write` reports
        /// that instead of blocking forever.
        #[test]
        #[ignore]
        fn live_vmic_with_no_listener_reports_the_stall() {
            let _serial = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
            let ctx = connect();
            let rate = ctx.nodes().graph_rate();
            // A 0.1 s ring so the stall shows within the timeout.
            let mut mic = VirtualMic::open_with(Arc::clone(&ctx), rate, rate as usize / 10)
                .expect("vmic opens");
            let silence = vec![0.0f32; rate as usize];
            let t = Instant::now();
            let err = mic.write(&silence, rate).unwrap_err();
            assert!(err.to_string().contains("nothing pulled"), "{err}");
            assert!(t.elapsed() >= STALL_TIMEOUT);
            assert_eq!(mic.stats().pulled, 0);
            mic.close().expect("close");
            assert_eq!(node_count(), 0);
        }
    }
}
