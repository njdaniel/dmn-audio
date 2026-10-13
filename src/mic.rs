//! Microphone capture at a rate the caller chooses: [`MicCapture`] records one microphone as mono
//! f32 at `rate` Hz and hands the samples over through a lock-free ring, with no framing and no
//! label. It sits beside [`crate::PipeWireSource`] (16 kHz, 20 ms frames, `FrameSink`) and
//! shares its machinery.
//!
//! Three pieces, each on the thread that owns it:
//!
//! - [`Reattach`], the same state machine [`crate::PipeWireSource`] uses, driven by this
//!   source's supervisor thread with a [`CaptureTarget::Mic`]. A named microphone that goes away
//!   is disconnected and reconnected when it returns; the default microphone has no
//!   `target.object`, so the session manager itself moves the stream to a new default. Two
//!   things `Reattach` does are worth knowing here. A named microphone that is absent when
//!   capture starts is stood in for by the default one until it appears
//!   ([`MicStatus::fallback`] says so). And the default microphone going away is not a
//!   `Disconnected`: the stream is left for the session manager to move, and the status shows
//!   `Connecting` until it has.
//! - `LoopMic`, the `pw_stream` and its listeners, on the PipeWire loop thread. The supervisor
//!   reaches it through [`crate::pipewire::Command`]s, and its stream lives from `start` to
//!   `stop`: a reconnect keeps the stream and with it the ring's writer.
//! - `mic_process`, the real-time side: it decodes the buffer's f32 samples into a fixed
//!   scratch block and pushes them into the ring. It allocates nothing, locks nothing and logs
//!   nothing; a full ring drops the incoming samples and counts them.
//!
//! The rate is asked of PipeWire, not computed here: the format pod names `rate` and the
//! adapter in front of the stream converts whatever the graph runs at. What PipeWire settles on
//! is checked in `param_changed` against rate, channel count and sample format, and the real-time
//! callback copies nothing until it matches. A stream that settles on something else is an
//! error ([`MicError::RateMismatch`]), never other-rate samples in the ring.

use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use pipewire as pw;
use pw::spa;
use ringbuf::HeapRb;
use ringbuf::traits::{Consumer, Observer, Producer, Split};
use std::ffi::CString;
use std::sync::atomic::AtomicBool;

use crate::capture::{
    Action, CaptureState, CaptureTarget, Reattach, enum_format_pod, stop_result,
    update_stream_props,
};
use crate::pipewire::{Command, PipeWireContext};
use crate::source::AudioError;

/// Lowest rate [`MicCapture`] accepts, in Hz.
pub const MIC_MIN_RATE: u32 = 8_000;
/// Highest rate [`MicCapture`] accepts, in Hz.
pub const MIC_MAX_RATE: u32 = 192_000;
/// A ring length that suits a caller with no reason to pick another, in milliseconds: long
/// enough that an overrun means the reader stalled, not that it was briefly late.
pub const DEFAULT_MIC_RING_MS: u32 = 2_000;
/// `node.name` of the capture stream, as `pw-cli` and patchbays show it.
pub const MIC_NODE_NAME: &str = "dmn.capture";

/// The stream's format, as `param_changed` last judged it. `REJECTED` is kept apart from
/// `UNSETTLED` because a stream that settled on the wrong format still goes on to `Streaming`:
/// its state must stay `Error`, and the reason must still be there to report.
const FORMAT_UNSETTLED: u8 = 0;
const FORMAT_OK: u8 = 1;
const FORMAT_REJECTED: u8 = 2;

/// Samples decoded per pass in the real-time callback; PipeWire's quantum is far below this.
const SCRATCH_FRAMES: usize = 8192;
/// Bytes per f32 sample.
const STRIDE: usize = 4;

type Prod = <HeapRb<f32> as Split>::Prod;
type Cons = <HeapRb<f32> as Split>::Cons;

/// Why a [`MicCapture`] could not be set up, or did not come up.
#[derive(Debug, thiserror::Error)]
pub enum MicError {
    /// The target is not a microphone; app streams and the sink monitor stay with
    /// [`crate::PipeWireSource`].
    #[error("not a microphone target")]
    NotAMic,
    /// The requested rate is outside [`MIC_MIN_RATE`]..=[`MIC_MAX_RATE`].
    #[error("unsupported sample rate {rate} Hz (need {MIC_MIN_RATE} to {MIC_MAX_RATE})")]
    UnsupportedRate { rate: u32 },
    /// PipeWire settled on another rate than the one asked for, so nothing is delivered.
    #[error("PipeWire negotiated {negotiated} Hz, {requested} Hz was requested")]
    RateMismatch { requested: u32, negotiated: u32 },
    /// PipeWire settled on the right rate but another channel count or sample format.
    #[error("{0}")]
    Format(String),
    /// The stream reported an error.
    #[error("capture stream: {0}")]
    Stream(String),
    /// The stream did not reach `Streaming` in time (no such microphone, no session manager).
    #[error("capture stream still {state} after {ms} ms")]
    NotStreaming { state: &'static str, ms: u64 },
    #[error(transparent)]
    Audio(#[from] AudioError),
}

/// How a requested rate relates to the graph's, as [`plan_rate`] decides it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RatePlan {
    /// The graph runs at the requested rate; samples pass through untouched.
    Native,
    /// The graph runs at `graph` Hz; PipeWire's adapter converts to the requested rate.
    Converted { graph: u32 },
}

/// Decide how `requested` Hz will be delivered when the graph runs at `graph_rate`. Fails for a
/// rate outside [`MIC_MIN_RATE`]..=[`MIC_MAX_RATE`]; the actual delivery is checked when the
/// stream's format settles.
pub fn plan_rate(requested: u32, graph_rate: u32) -> Result<RatePlan, MicError> {
    if !(MIC_MIN_RATE..=MIC_MAX_RATE).contains(&requested) {
        return Err(MicError::UnsupportedRate { rate: requested });
    }
    Ok(if requested == graph_rate {
        RatePlan::Native
    } else {
        RatePlan::Converted { graph: graph_rate }
    })
}

/// Only microphones: app streams and the sink monitor stay with [`crate::PipeWireSource`].
fn require_mic(target: &CaptureTarget) -> Result<(), MicError> {
    match target {
        CaptureTarget::Mic(_) => Ok(()),
        _ => Err(MicError::NotAMic),
    }
}

/// Check the settled stream format against what was asked for: `want` Hz, mono, F32LE.
pub(crate) fn check_format(
    want: u32,
    rate: u32,
    channels: u32,
    format: spa::param::audio::AudioFormat,
) -> Result<(), MicError> {
    if rate != want {
        return Err(MicError::RateMismatch {
            requested: want,
            negotiated: rate,
        });
    }
    if channels != 1 || format != spa::param::audio::AudioFormat::F32LE {
        return Err(MicError::Format(format!(
            "negotiated {format:?} {rate} Hz × {channels} ch, need F32LE {want} Hz mono"
        )));
    }
    Ok(())
}

/// Shared, lock-free view of one microphone capture. The real-time callback only touches the
/// atomics.
#[derive(Debug, Default)]
pub struct MicStatus {
    state: AtomicU8,
    /// The rate asked for, in Hz.
    requested_rate: AtomicU32,
    /// The rate PipeWire settled on, 0 until it has.
    negotiated_rate: AtomicU32,
    /// 1 when [`plan_rate`] said PipeWire converts, 0 when the graph runs at the rate.
    converted: AtomicU8,
    /// `FORMAT_OK` once `param_changed` has seen the format asked for; the callback copies
    /// nothing while it is anything else.
    format: AtomicU8,
    /// Whether PipeWire last reported the stream as `Streaming`. Kept apart from `state`, which
    /// a rejected format holds at `Error`: when a good format then arrives on the same stream,
    /// this says where `state` goes back to.
    running: AtomicBool,
    /// True while a named microphone that is not there is being stood in for by the default one.
    fallback: AtomicBool,
    pushed: AtomicU64,
    dropped: AtomicU64,
    last_error: Mutex<Option<String>>,
}

impl MicStatus {
    /// Where the capture is in its life; the same states as [`crate::CaptureStatus`].
    pub fn state(&self) -> CaptureState {
        CaptureState::from_u8(self.state.load(Ordering::Acquire))
    }

    /// The rate samples are delivered at: the requested rate, once PipeWire has settled on it.
    /// `None` before that, while disconnected, and after a mismatch (nothing is delivered).
    pub fn delivered_rate(&self) -> Option<u32> {
        self.delivering()
            .then(|| self.negotiated_rate.load(Ordering::Acquire))
    }

    /// True while the real-time callback copies samples: the format is the one asked for.
    fn delivering(&self) -> bool {
        self.format.load(Ordering::Acquire) == FORMAT_OK
    }

    /// The rate PipeWire settled on, even when it is not the one asked for. `None` until then.
    pub fn negotiated_rate(&self) -> Option<u32> {
        let rate = self.negotiated_rate.load(Ordering::Acquire);
        (rate != 0).then_some(rate)
    }

    /// The rate the caller asked for.
    pub fn requested_rate(&self) -> u32 {
        self.requested_rate.load(Ordering::Relaxed)
    }

    /// True when the graph runs at another rate and PipeWire converts (decided at `start`).
    pub fn converted(&self) -> bool {
        self.converted.load(Ordering::Relaxed) != 0
    }

    /// True while the capture was asked for a microphone by name, that microphone is not
    /// there, and the default one is being captured in its place. [`Reattach`] does this for a
    /// named microphone that is absent when capture starts, and moves to the named one when it
    /// appears; a caller for whom another microphone is worse than none checks here.
    pub fn fallback(&self) -> bool {
        self.fallback.load(Ordering::Acquire)
    }

    /// Samples pushed into the ring since the last [`MicCapture::start`].
    pub fn pushed(&self) -> u64 {
        self.pushed.load(Ordering::Relaxed)
    }

    /// Samples the ring had no room for and dropped (the newest ones), since the last
    /// [`MicCapture::start`].
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// The error PipeWire or the format check reported on the current connection; `None` once
    /// the stream is connected again.
    pub fn last_error(&self) -> Option<String> {
        self.last_error
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn set_state(&self, s: CaptureState) {
        self.state.store(s.to_u8(), Ordering::Release);
    }

    pub(crate) fn set_error(&self, why: String) {
        *self.last_error.lock().unwrap_or_else(|e| e.into_inner()) = Some(why);
        self.set_state(CaptureState::Error);
    }

    fn clear_error(&self) {
        *self.last_error.lock().unwrap_or_else(|e| e.into_inner()) = None;
    }

    /// A new capture begins: counters and the last error start over.
    fn restart(&self) {
        self.pushed.store(0, Ordering::Relaxed);
        self.dropped.store(0, Ordering::Relaxed);
        self.fallback.store(false, Ordering::Release);
        self.clear_error();
    }

    /// The stream is being connected, or connected again: nothing is delivered and no rate is
    /// known until a format settles, and what the last connection reported is over. Shuts the
    /// gate the real-time callback checks.
    fn connection_reset(&self) {
        self.format.store(FORMAT_UNSETTLED, Ordering::Release);
        self.negotiated_rate.store(0, Ordering::Release);
        self.running.store(false, Ordering::Release);
    }

    /// PipeWire withdrew the stream's format. The gate shuts. A rejection stands, with the rate
    /// that caused it, until a new format settles or the stream is connected again: the reason
    /// nothing is delivered must not turn into "unknown" on the way down.
    fn format_cleared(&self) {
        if self.format.load(Ordering::Acquire) != FORMAT_REJECTED {
            self.format.store(FORMAT_UNSETTLED, Ordering::Release);
            self.negotiated_rate.store(0, Ordering::Release);
        }
    }

    /// PipeWire settled on a format. The gate is set before anything is reported, so no buffer
    /// of the wrong shape is ever copied into the ring.
    fn format_settled(&self, rate: u32, channels: u32, format: spa::param::audio::AudioFormat) {
        self.negotiated_rate.store(rate, Ordering::Release);
        match check_format(self.requested_rate(), rate, channels, format) {
            Ok(()) => {
                let was = self.format.swap(FORMAT_OK, Ordering::AcqRel);
                if was == FORMAT_REJECTED {
                    // The same stream, renegotiated to what was asked for. PipeWire sends no
                    // new state for that, so the state the rejection held at `Error` is put
                    // back to where the stream is.
                    self.clear_error();
                    self.set_state(if self.running.load(Ordering::Acquire) {
                        CaptureState::Streaming
                    } else {
                        CaptureState::Connecting
                    });
                }
            }
            Err(e) => {
                self.format.store(FORMAT_REJECTED, Ordering::Release);
                self.set_error(e.to_string());
            }
        }
    }

    /// The stream reached `Streaming`. One whose format was rejected is running too, but
    /// nothing it carries is delivered: it stays in `Error`. One the supervisor has already
    /// called unplugged stays `Disconnected`: this event was on its way when the device went.
    fn stream_streaming(&self) {
        self.running.store(true, Ordering::Release);
        if self.state() != CaptureState::Disconnected
            && self.format.load(Ordering::Acquire) != FORMAT_REJECTED
        {
            self.set_state(CaptureState::Streaming);
        }
    }

    /// The stream is connecting or paused. Neither hides an unplugged device nor a rejected
    /// format, which only the next connection clears.
    fn stream_waiting(&self) {
        self.running.store(false, Ordering::Release);
        if self.state() != CaptureState::Disconnected
            && self.format.load(Ordering::Acquire) != FORMAT_REJECTED
        {
            self.set_state(CaptureState::Connecting);
        }
    }

    /// Why nothing is being delivered, when the reason is an error and not a wait.
    fn failure(&self) -> Option<MicError> {
        if self.state() != CaptureState::Error {
            return None;
        }
        let requested = self.requested_rate();
        let why = self.last_error().unwrap_or_default();
        Some(match self.negotiated_rate() {
            Some(negotiated) if negotiated != requested => MicError::RateMismatch {
                requested,
                negotiated,
            },
            _ if self.format.load(Ordering::Acquire) == FORMAT_REJECTED => MicError::Format(why),
            _ => MicError::Stream(why),
        })
    }
}

/// The ring's producer end, held by the capture callback.
pub(crate) struct MicWriter {
    prod: Prod,
    status: Arc<MicStatus>,
}

impl MicWriter {
    /// Push as many of `samples` as fit and return that count; the rest are dropped and counted.
    /// Real-time safe: no allocation, no lock, no logging, bounded time.
    pub(crate) fn push(&mut self, samples: &[f32]) -> usize {
        let n = self.prod.push_slice(samples);
        self.status.pushed.fetch_add(n as u64, Ordering::Relaxed);
        if n < samples.len() {
            self.status
                .dropped
                .fetch_add((samples.len() - n) as u64, Ordering::Relaxed);
        }
        n
    }
}

/// The ring's consumer end: the caller reads the microphone's samples here. Owned by one
/// thread; the capture callback is the only producer.
pub struct MicReader {
    cons: Cons,
    status: Arc<MicStatus>,
}

impl MicReader {
    /// Move up to `out.len()` queued samples into `out` and return how many. Never blocks.
    pub fn pop(&mut self, out: &mut [f32]) -> usize {
        self.cons.pop_slice(out)
    }

    /// Samples queued.
    pub fn queued(&self) -> usize {
        self.cons.occupied_len()
    }

    /// Samples the ring holds at most.
    pub fn capacity(&self) -> usize {
        self.cons.capacity().get()
    }

    /// The capture's status, as [`MicCapture::status`] returns it.
    pub fn status(&self) -> Arc<MicStatus> {
        Arc::clone(&self.status)
    }
}

/// Build the ring of `capacity` samples and split it into its two ends.
pub(crate) fn mic_ring(capacity: usize, status: Arc<MicStatus>) -> (MicWriter, MicReader) {
    let (prod, cons) = HeapRb::<f32>::new(capacity.max(1)).split();
    (
        MicWriter {
            prod,
            status: Arc::clone(&status),
        },
        MicReader { cons, status },
    )
}

/// What the loop thread needs to create the stream. The rate asked for travels in `status`.
pub(crate) struct MicSpec {
    pub(crate) status: Arc<MicStatus>,
}

/// The real-time side's state: the ring's producer and a fixed block the bytes are decoded into.
struct MicInlet {
    writer: MicWriter,
    scratch: Box<[f32]>,
    status: Arc<MicStatus>,
}

/// Decode little-endian f32 `bytes` through `scratch` into the ring. A trailing partial sample is
/// ignored. No allocation, no lock, bounded by `bytes.len()`.
fn push_le_bytes(writer: &mut MicWriter, scratch: &mut [f32], bytes: &[u8]) {
    for pass in bytes.chunks(STRIDE * scratch.len().max(1)) {
        let mut n = 0;
        for (slot, c) in scratch.iter_mut().zip(pass.chunks_exact(STRIDE)) {
            *slot = f32::from_le_bytes([c[0], c[1], c[2], c[3]]);
            n += 1;
        }
        writer.push(&scratch[..n]);
    }
}

/// REAL-TIME CALLBACK (runs on PipeWire's data thread; `RT_PROCESS`).
/// Rules (CLAUDE.md, SPEC § Threading rules), reviewed call by call:
/// - no allocation: `dequeue_buffer` wraps a pointer, `datas_mut`/`chunk`/`data` borrow the
///   mapped buffer, the samples are decoded into the preallocated `scratch` and pushed into
///   the preallocated ring;
/// - no locks: the ring is lock-free SPSC, counters and the format gate are relaxed or
///   acquire/release atomics;
/// - no logging, no syscalls, bounded time (one pass over the buffer).
///
/// Dequeue, copy, requeue (the `Buffer` guard requeues on drop). Until the format is settled at
/// the requested rate, mono F32, the bytes are not what the ring promises and are not copied.
fn mic_process(stream: &pw::stream::Stream, inlet: &mut MicInlet) {
    let Some(mut buffer) = stream.dequeue_buffer() else {
        return;
    };
    if !inlet.status.delivering() {
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
    push_le_bytes(&mut inlet.writer, &mut inlet.scratch, &bytes[start..end]);
}

/// The stream and its two listeners, owned by the PipeWire loop thread. The process listener is
/// the only one holding the [`MicInlet`]; state and format events go to a second listener with
/// no user data, so the real-time thread and the loop thread never share a `&mut`.
pub(crate) struct LoopMic {
    // Field order is drop order: listeners before the stream they are attached to.
    _process: pw::stream::StreamListener<MicInlet>,
    _events: pw::stream::StreamListener<()>,
    stream: pw::stream::StreamRc,
    status: Arc<MicStatus>,
    connected: bool,
}

impl LoopMic {
    /// Create the stream (not connected yet) on the loop thread.
    pub(crate) fn open(
        core: &pw::core::CoreRc,
        spec: MicSpec,
        writer: MicWriter,
    ) -> Result<Self, String> {
        let mut props = pw::properties::PropertiesBox::new();
        props.insert(*pw::keys::MEDIA_TYPE, "Audio");
        props.insert(*pw::keys::MEDIA_CATEGORY, "Capture");
        props.insert(*pw::keys::MEDIA_ROLE, "Communication");
        props.insert(*pw::keys::NODE_NAME, MIC_NODE_NAME);
        // A 10 ms quantum.
        let rate = spec.status.requested_rate();
        let latency = format!("{}/{}", rate / 100, rate);
        props.insert(*pw::keys::NODE_LATENCY, latency.as_str());
        let stream = pw::stream::StreamRc::new(core.clone(), MIC_NODE_NAME, props)
            .map_err(|e| e.to_string())?;

        let inlet = MicInlet {
            writer,
            scratch: vec![0.0; SCRATCH_FRAMES].into_boxed_slice(),
            status: Arc::clone(&spec.status),
        };
        let process_listener = stream
            .add_local_listener_with_user_data(inlet)
            .process(mic_process)
            .register()
            .map_err(|e| e.to_string())?;

        let state_status = Arc::clone(&spec.status);
        let format_status = Arc::clone(&spec.status);
        let events_listener = stream
            .add_local_listener::<()>()
            .state_changed(move |_, _, _old, new| match new {
                pw::stream::StreamState::Streaming => state_status.stream_streaming(),
                pw::stream::StreamState::Connecting | pw::stream::StreamState::Paused => {
                    state_status.stream_waiting()
                }
                pw::stream::StreamState::Error(why) => state_status.set_error(why),
                pw::stream::StreamState::Unconnected => {}
            })
            .param_changed(move |_, _, id, param| {
                if id != spa::param::ParamType::Format.as_raw() {
                    return;
                }
                let Some(param) = param else {
                    // The format was cleared: stop the callback until a new one is negotiated.
                    format_status.format_cleared();
                    return;
                };
                let mut info = spa::param::audio::AudioInfoRaw::new();
                if info.parse(param).is_err() {
                    format_status.format_cleared();
                    return;
                }
                format_status.format_settled(info.rate(), info.channels(), info.format());
            })
            .register()
            .map_err(|e| e.to_string())?;

        Ok(Self {
            _process: process_listener,
            _events: events_listener,
            stream,
            status: spec.status,
            connected: false,
        })
    }

    /// Point the stream at a new target (`None`: the default source) and (re)connect it.
    pub(crate) fn connect(&mut self, target_object: Option<&str>) {
        if let Err(e) = self.try_connect(target_object) {
            self.status.set_error(e);
        }
    }

    fn try_connect(&mut self, target_object: Option<&str>) -> Result<(), String> {
        self.unlink();
        self.status.clear_error();
        let target = target_object
            .map(|t| CString::new(t).map_err(|_| format!("target {t:?} contains a NUL byte")))
            .transpose()?;
        update_stream_props(
            &self.stream,
            &[
                (c"target.object", target.as_deref()),
                (c"stream.capture.sink", Some(c"false")),
            ],
        )?;
        let pod = enum_format_pod(self.status.requested_rate())?;
        let mut params = [spa::pod::Pod::from_bytes(&pod).ok_or("bad EnumFormat pod")?];
        self.stream
            .connect(
                spa::utils::Direction::Input,
                None,
                pw::stream::StreamFlags::AUTOCONNECT
                    | pw::stream::StreamFlags::MAP_BUFFERS
                    | pw::stream::StreamFlags::RT_PROCESS,
                &mut params,
            )
            .map_err(|e| e.to_string())?;
        self.connected = true;
        self.status.set_state(CaptureState::Connecting);
        Ok(())
    }

    /// The device is gone (the supervisor's `Disconnect`): unlink, and say so. The supervisor
    /// set `Disconnected` when it sent the command, but a `Streaming` event already on its way
    /// could have landed since; on this thread, after the unlink, none can.
    pub(crate) fn disconnect(&mut self) {
        self.unlink();
        self.status.set_state(CaptureState::Disconnected);
    }

    fn unlink(&mut self) {
        // A reconnect renegotiates the format, so the old verdict must not carry over.
        self.status.connection_reset();
        if self.connected {
            // After this returns the data thread no longer calls `mic_process`.
            let _ = self.stream.disconnect();
            self.connected = false;
        }
    }
}

impl Drop for LoopMic {
    fn drop(&mut self) {
        self.unlink();
        self.status.set_state(CaptureState::Idle);
    }
}

/// How often the supervisor re-checks when nothing changed (also its stop latency).
const SUPERVISE_TICK: Duration = Duration::from_millis(100);
/// How long `stop` waits for the loop thread to close the stream.
const STOP_TIMEOUT: Duration = Duration::from_secs(1);

/// A microphone captured as mono f32 at a rate the caller chooses, into a lock-free ring.
///
/// ```no_run
/// use std::sync::Arc;
/// use dmn_audio::{CaptureTarget, MicCapture, PipeWireContext};
///
/// let ctx = Arc::new(PipeWireContext::connect()?);
/// let mut mic = MicCapture::new(ctx, CaptureTarget::mic("default"), 48_000, 2_000)?;
/// let mut reader = mic.start()?;
/// let mut block = [0.0f32; 960];
/// let got = reader.pop(&mut block); // 0 until samples arrive
/// # let _ = got;
/// mic.stop()?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct MicCapture {
    ctx: Arc<PipeWireContext>,
    target: CaptureTarget,
    rate: u32,
    ring_samples: usize,
    status: Arc<MicStatus>,
    key: Option<u64>,
    stop: Arc<AtomicBool>,
    supervisor: Option<JoinHandle<()>>,
}

impl MicCapture {
    /// A capture of `target` (which must be [`CaptureTarget::Mic`]) at `rate` Hz, into a ring that
    /// holds `ring_ms` milliseconds. Nothing is opened until [`Self::start`].
    pub fn new(
        ctx: Arc<PipeWireContext>,
        target: CaptureTarget,
        rate: u32,
        ring_ms: u32,
    ) -> Result<Self, MicError> {
        require_mic(&target)?;
        plan_rate(rate, rate)?;
        let ring_samples = (u64::from(rate) * u64::from(ring_ms) / 1000).max(1) as usize;
        let status = Arc::new(MicStatus::default());
        status.requested_rate.store(rate, Ordering::Relaxed);
        Ok(Self {
            ctx,
            target,
            rate,
            ring_samples,
            status,
            key: None,
            stop: Arc::new(AtomicBool::new(false)),
            supervisor: None,
        })
    }

    /// Shared status: state, delivered rate, dropped samples, last error.
    pub fn status(&self) -> Arc<MicStatus> {
        Arc::clone(&self.status)
    }

    /// The microphone this captures.
    pub fn target(&self) -> &CaptureTarget {
        &self.target
    }

    /// Create the stream and start following the microphone. Returns the ring's reader at once;
    /// the stream connects in the background, see [`Self::wait_streaming`]. Calling it while
    /// started is an error. After [`Self::stop`] it may be called again: that is a new capture
    /// with a new ring, the status counters and last error start over, and the reader of the
    /// capture before it gets nothing more.
    pub fn start(&mut self) -> Result<MicReader, MicError> {
        if self.key.is_some() {
            return Err(AudioError::AlreadyStarted.into());
        }
        self.status.restart();
        let plan = plan_rate(self.rate, self.ctx.nodes().graph_rate())?;
        self.status
            .converted
            .store(u8::from(plan != RatePlan::Native), Ordering::Relaxed);
        let (writer, reader) = mic_ring(self.ring_samples, Arc::clone(&self.status));
        let key = self.ctx.next_key();
        self.ctx
            .send(Command::OpenMic {
                key,
                spec: MicSpec {
                    status: Arc::clone(&self.status),
                },
                writer,
            })
            .map_err(AudioError::from)?;
        self.key = Some(key);
        self.stop.store(false, Ordering::Release);

        let ctx = Arc::clone(&self.ctx);
        let stop = Arc::clone(&self.stop);
        let status = Arc::clone(&self.status);
        let mut reattach = Reattach::new(self.target.clone());
        let named = matches!(self.target, CaptureTarget::Mic(Some(_)));
        let handle = std::thread::Builder::new()
            .name("dmn-capture-mic".into())
            .spawn(move || {
                let mut table = ctx.nodes();
                while !stop.load(Ordering::Acquire) {
                    if let Some(action) = reattach.step(&table) {
                        let cmd = match action {
                            Action::Connect { target_object, .. } => {
                                // A named microphone connected with no target is `Reattach`
                                // standing the default one in for it.
                                status
                                    .fallback
                                    .store(named && target_object.is_none(), Ordering::Release);
                                status.set_state(CaptureState::Connecting);
                                Command::Connect {
                                    key,
                                    target_object,
                                    capture_sink: false,
                                }
                            }
                            Action::Disconnect => {
                                status.fallback.store(false, Ordering::Release);
                                status.set_state(CaptureState::Disconnected);
                                Command::Disconnect { key }
                            }
                        };
                        if ctx.send(cmd).is_err() {
                            status.set_error("PipeWire loop thread is gone".into());
                            break;
                        }
                    }
                    if table.disconnected.is_some() {
                        // The table is frozen; wait_for_change would return at once.
                        std::thread::sleep(SUPERVISE_TICK);
                    } else {
                        table = ctx.wait_for_change(table.generation, SUPERVISE_TICK);
                    }
                }
            })
            .map_err(|e| AudioError::Spawn(e.to_string()))?;
        self.supervisor = Some(handle);
        Ok(reader)
    }

    /// Block until the stream is delivering samples at the requested rate, or fail with the
    /// reason: [`MicError::RateMismatch`] when PipeWire settled on another rate,
    /// [`MicError::Format`] when it settled on the rate but not on mono f32,
    /// [`MicError::Stream`] for any other stream error, [`MicError::NotStreaming`] on timeout.
    pub fn wait_streaming(&self, timeout: Duration) -> Result<(), MicError> {
        let t = Instant::now();
        loop {
            let state = self.status.state();
            if state == CaptureState::Streaming && self.status.delivered_rate().is_some() {
                return Ok(());
            }
            if let Some(why) = self.status.failure() {
                return Err(why);
            }
            if t.elapsed() >= timeout {
                return Err(MicError::NotStreaming {
                    state: state.label(),
                    ms: timeout.as_millis() as u64,
                });
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// Stop following the microphone, close the stream and wait for the loop thread to remove its
    /// node from the graph. After it returns nothing writes into the ring. Idempotent.
    pub fn stop(&mut self) -> Result<(), AudioError> {
        self.stop.store(true, Ordering::Release);
        let panicked = self
            .supervisor
            .take()
            .map(|h| h.join().is_err())
            .unwrap_or(false);
        let mut final_state = None;
        if let Some(key) = self.key.take()
            && self.ctx.send(Command::Close { key }).is_ok()
        {
            let t = Instant::now();
            while self.status.state() != CaptureState::Idle && t.elapsed() < STOP_TIMEOUT {
                std::thread::sleep(Duration::from_millis(2));
            }
            // Never claim a clean stop we did not get: the caller may be about to drop the ring.
            final_state = Some(self.status.state());
        }
        stop_result(panicked, final_state, STOP_TIMEOUT)
    }
}

impl Drop for MicCapture {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipewire::{NodeTable, RegistryEvent};
    use crate::testutil::tone_hz;
    use spa::param::audio::AudioFormat;

    fn status() -> Arc<MicStatus> {
        Arc::new(MicStatus::default())
    }

    fn mic(t: &mut NodeTable, id: u32, name: &str) {
        t.apply(RegistryEvent::Added {
            id,
            props: vec![
                ("media.class".into(), "Audio/Source".into()),
                ("node.name".into(), name.into()),
            ],
        });
    }

    fn synced() -> NodeTable {
        let mut t = NodeTable::default();
        t.apply(RegistryEvent::Synced);
        t
    }

    #[test]
    fn the_pitch_of_a_tone_tells_the_rate_its_samples_were_taken_at() {
        let tone = |rate: u32| -> Vec<f32> {
            (0..rate)
                .map(|i| (2.0 * std::f32::consts::PI * 440.0 * i as f32 / rate as f32).sin())
                .collect()
        };
        for rate in [16_000, 44_100, 48_000] {
            let hz = tone_hz(&tone(rate), rate);
            assert!((hz - 440.0).abs() < 1.0, "{rate} Hz: measured {hz}");
        }
        // 48 kHz samples mistaken for 44.1 kHz ones are flat by the ratio of the rates.
        let hz = tone_hz(&tone(48_000), 44_100);
        assert!((hz - 404.25).abs() < 1.0, "measured {hz}");
        assert_eq!(tone_hz(&[0.0; 64], 48_000), 0.0, "silence has no pitch");
        // Faint noise before and after the tone does not change what is measured.
        let mut padded: Vec<f32> = (0..4_000)
            .map(|i| if i % 2 == 0 { 1e-4 } else { -1e-4 })
            .collect();
        padded.extend(tone(16_000));
        padded.extend((0..4_000).map(|i| if i % 3 == 0 { 2e-4 } else { -2e-4 }));
        let hz = tone_hz(&padded, 16_000);
        assert!(
            (hz - 440.0).abs() < 1.0,
            "with noise around it: measured {hz}"
        );
    }

    fn connect(target: Option<&str>) -> Option<Action> {
        Some(Action::Connect {
            target_object: target.map(str::to_owned),
            capture_sink: false,
            mode: crate::CaptureMode::Mic,
        })
    }

    #[test]
    fn a_full_ring_drops_the_newest_samples_and_counts_them() {
        let st = status();
        let (mut w, mut r) = mic_ring(8, Arc::clone(&st));
        assert_eq!(w.push(&[1.0; 6]), 6);
        assert_eq!(w.push(&[2.0; 6]), 2, "only two fit");
        assert_eq!((st.pushed(), st.dropped()), (8, 4));
        let mut out = [0.0; 16];
        assert_eq!(r.pop(&mut out), 8);
        assert_eq!(&out[..8], &[1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 2.0, 2.0]);
        assert_eq!(r.queued(), 0);
        assert_eq!(w.push(&[3.0; 8]), 8, "room again after the reader drained");
        assert_eq!((st.pushed(), st.dropped()), (16, 4));
        assert_eq!(r.capacity(), 8);
    }

    #[test]
    fn bytes_are_decoded_in_order_across_scratch_passes_and_a_partial_tail_is_ignored() {
        let st = status();
        let (mut w, mut r) = mic_ring(64, Arc::clone(&st));
        let samples: Vec<f32> = (0..10).map(|i| i as f32 * 0.5).collect();
        let mut bytes: Vec<u8> = samples.iter().flat_map(|s| s.to_le_bytes()).collect();
        bytes.extend_from_slice(&[0xAA, 0xBB]);
        let mut scratch = [0.0f32; 3];
        push_le_bytes(&mut w, &mut scratch, &bytes);
        let mut out = [0.0; 16];
        assert_eq!(r.pop(&mut out), 10);
        assert_eq!(&out[..10], &samples[..]);
        assert_eq!(st.dropped(), 0);
    }

    #[test]
    fn the_rate_plan_says_native_or_converted_and_refuses_absurd_rates() {
        assert_eq!(plan_rate(48_000, 48_000).unwrap(), RatePlan::Native);
        assert_eq!(
            plan_rate(48_000, 44_100).unwrap(),
            RatePlan::Converted { graph: 44_100 }
        );
        assert_eq!(
            plan_rate(16_000, 48_000).unwrap(),
            RatePlan::Converted { graph: 48_000 }
        );
        for bad in [0, 7_999, 192_001] {
            assert!(matches!(
                plan_rate(bad, 48_000),
                Err(MicError::UnsupportedRate { rate }) if rate == bad
            ));
        }
        // The graph rate comes from the table: forced, else clock, else 48 kHz.
        let mut t = synced();
        assert_eq!(t.graph_rate(), 48_000);
        t.apply(RegistryEvent::ClockRate {
            rate: Some(44_100),
            forced: false,
        });
        assert_eq!(
            plan_rate(48_000, t.graph_rate()).unwrap(),
            RatePlan::Converted { graph: 44_100 }
        );
    }

    #[test]
    fn only_the_requested_rate_mono_f32_opens_the_gate() {
        assert!(check_format(48_000, 48_000, 1, AudioFormat::F32LE).is_ok());
        assert!(matches!(
            check_format(48_000, 44_100, 1, AudioFormat::F32LE),
            Err(MicError::RateMismatch {
                requested: 48_000,
                negotiated: 44_100
            })
        ));
        assert!(matches!(
            check_format(48_000, 48_000, 2, AudioFormat::F32LE),
            Err(MicError::Format(_))
        ));
        assert!(matches!(
            check_format(48_000, 48_000, 1, AudioFormat::S16LE),
            Err(MicError::Format(_))
        ));
    }

    #[test]
    fn the_status_reports_a_rate_only_while_it_is_being_delivered() {
        let st = status();
        st.requested_rate.store(48_000, Ordering::Relaxed);
        assert_eq!((st.delivered_rate(), st.negotiated_rate()), (None, None));
        st.format_settled(44_100, 1, AudioFormat::F32LE);
        assert_eq!(st.delivered_rate(), None, "settled on the wrong rate");
        assert_eq!(st.negotiated_rate(), Some(44_100));
        st.connection_reset();
        assert_eq!((st.delivered_rate(), st.negotiated_rate()), (None, None));
        st.format_settled(48_000, 1, AudioFormat::F32LE);
        assert_eq!(st.delivered_rate(), Some(48_000));
        st.connection_reset();
        assert_eq!(st.delivered_rate(), None, "a reconnect shuts the gate");
    }

    /// PipeWire reports the format first and `Paused` and `Streaming` after it. A stream that
    /// settled on the wrong format still gets there, and must not look healthy when it does.
    #[test]
    fn a_rejected_format_stays_an_error_when_the_stream_goes_on_to_streaming() {
        for (rate, channels, format) in [
            (44_100, 1, AudioFormat::F32LE),
            (48_000, 2, AudioFormat::F32LE),
            (48_000, 1, AudioFormat::S16LE),
        ] {
            let st = status();
            st.requested_rate.store(48_000, Ordering::Relaxed);
            st.set_state(CaptureState::Connecting);
            st.format_settled(rate, channels, format);
            st.stream_waiting();
            st.stream_streaming();
            assert_eq!(
                st.state(),
                CaptureState::Error,
                "{rate} Hz × {channels} {format:?}"
            );
            assert_eq!(st.delivered_rate(), None);
            match st.failure() {
                Some(MicError::RateMismatch {
                    requested: 48_000,
                    negotiated,
                }) => assert_eq!((negotiated, rate), (44_100, 44_100)),
                Some(MicError::Format(why)) => {
                    assert_eq!(rate, 48_000);
                    assert!(why.contains("need F32LE 48000 Hz mono"), "{why}");
                }
                other => panic!("{rate} Hz × {channels} {format:?}: {other:?}"),
            }
            // The next connection starts clean.
            st.connection_reset();
            st.set_state(CaptureState::Connecting);
            st.format_settled(48_000, 1, AudioFormat::F32LE);
            st.stream_waiting();
            st.stream_streaming();
            assert_eq!(st.state(), CaptureState::Streaming);
            assert_eq!(st.delivered_rate(), Some(48_000));
            assert!(st.failure().is_none());
        }
    }

    /// PipeWire can settle a second time on the same stream. No state event comes with that, so
    /// the status has to recover by itself, to where the stream is.
    #[test]
    fn a_good_format_after_a_rejected_one_on_the_same_stream_recovers_without_a_state_event() {
        for running in [true, false] {
            let st = status();
            st.requested_rate.store(48_000, Ordering::Relaxed);
            st.set_state(CaptureState::Connecting);
            st.format_settled(48_000, 2, AudioFormat::F32LE);
            st.stream_waiting();
            if running {
                st.stream_streaming();
            }
            assert_eq!(st.state(), CaptureState::Error);
            st.format_settled(48_000, 1, AudioFormat::F32LE);
            let want = if running {
                CaptureState::Streaming
            } else {
                CaptureState::Connecting
            };
            assert_eq!(st.state(), want, "running: {running}");
            assert_eq!(st.delivered_rate(), Some(48_000));
            assert!(st.failure().is_none());
            assert_eq!(st.last_error(), None, "the rejection is over");
        }
    }

    /// The reason survives PipeWire withdrawing the format on the way down, and ends with the
    /// connection.
    #[test]
    fn a_rate_mismatch_is_still_one_after_the_format_is_cleared() {
        let st = status();
        st.requested_rate.store(48_000, Ordering::Relaxed);
        st.format_settled(44_100, 1, AudioFormat::F32LE);
        st.format_cleared();
        assert!(matches!(
            st.failure(),
            Some(MicError::RateMismatch {
                requested: 48_000,
                negotiated: 44_100
            })
        ));
        assert_eq!(st.delivered_rate(), None);
        // A healthy stream whose format is cleared just stops delivering.
        let st = status();
        st.requested_rate.store(48_000, Ordering::Relaxed);
        st.format_settled(48_000, 1, AudioFormat::F32LE);
        st.stream_streaming();
        st.format_cleared();
        assert_eq!((st.delivered_rate(), st.negotiated_rate()), (None, None));
        assert!(st.failure().is_none());
    }

    /// A `Streaming` event that was on its way when the device went must not undo
    /// `Disconnected`.
    #[test]
    fn a_late_streaming_event_does_not_undo_disconnected() {
        let st = status();
        st.requested_rate.store(48_000, Ordering::Relaxed);
        st.format_settled(48_000, 1, AudioFormat::F32LE);
        st.set_state(CaptureState::Disconnected);
        st.stream_streaming();
        assert_eq!(st.state(), CaptureState::Disconnected);
    }

    #[test]
    fn a_new_capture_starts_its_counters_and_its_error_over() {
        let st = status();
        let (mut w, _r) = mic_ring(4, Arc::clone(&st));
        w.push(&[0.0; 6]);
        st.set_error("boom".into());
        st.fallback.store(true, Ordering::Release);
        assert_eq!((st.pushed(), st.dropped()), (4, 2));
        st.restart();
        assert_eq!((st.pushed(), st.dropped()), (0, 0));
        assert_eq!(st.last_error(), None);
        assert!(!st.fallback());
    }

    #[test]
    fn a_stream_error_is_reported_as_such_and_waiting_does_not_hide_an_unplugged_device() {
        let st = status();
        st.requested_rate.store(48_000, Ordering::Relaxed);
        st.format_settled(48_000, 1, AudioFormat::F32LE);
        st.stream_streaming();
        assert!(st.failure().is_none());
        st.set_error("boom".into());
        assert_eq!(st.state(), CaptureState::Error);
        assert!(matches!(st.failure(), Some(MicError::Stream(why)) if why == "boom"));

        let st = status();
        st.set_state(CaptureState::Disconnected);
        st.stream_waiting();
        assert_eq!(st.state(), CaptureState::Disconnected);
    }

    #[test]
    fn only_microphone_targets_are_accepted() {
        assert!(require_mic(&CaptureTarget::mic("usb-mic")).is_ok());
        assert!(require_mic(&CaptureTarget::mic("default")).is_ok());
        for other in [
            CaptureTarget::interviewer(""),
            CaptureTarget::interviewer("zoom"),
            CaptureTarget::SinkMonitor,
        ] {
            assert!(matches!(require_mic(&other), Err(MicError::NotAMic)));
        }
    }

    #[test]
    fn the_default_mic_connects_without_a_target_once_and_follows_the_server() {
        let mut r = Reattach::new(CaptureTarget::mic("default"));
        let mut t = NodeTable::default();
        assert_eq!(r.step(&t), None, "nothing before the first enumeration");
        t.apply(RegistryEvent::Synced);
        assert_eq!(r.step(&t), connect(None));
        assert_eq!(r.step(&t), None);
        // The default microphone itself coming and going is the session manager's to follow:
        // `Reattach` neither disconnects nor reconnects, so the status never says
        // `Disconnected` for it (the stream's own state shows `Connecting` meanwhile).
        mic(&mut t, 40, "built-in");
        assert_eq!(r.step(&t), None);
        t.apply(RegistryEvent::Removed(40));
        assert_eq!(r.step(&t), None);
        t.apply(RegistryEvent::Disconnected("gone".into()));
        assert_eq!(r.step(&t), Some(Action::Disconnect));
        assert_eq!(r.step(&t), None);
    }

    #[test]
    fn a_named_mic_that_unplugs_is_disconnected_and_found_again_when_it_returns() {
        let mut r = Reattach::new(CaptureTarget::mic("usb-mic"));
        let mut t = synced();
        mic(&mut t, 40, "usb-mic");
        assert_eq!(r.step(&t), connect(Some("usb-mic")));
        assert_eq!(r.step(&t), None);

        t.apply(RegistryEvent::Removed(40));
        assert_eq!(r.step(&t), Some(Action::Disconnect));
        assert_eq!(r.step(&t), None, "stays disconnected, no fallback");

        // Plugged back in under a new id.
        mic(&mut t, 41, "usb-mic");
        assert_eq!(r.step(&t), connect(Some("usb-mic")));
    }

    #[test]
    fn a_named_mic_absent_at_start_falls_back_to_the_default_then_follows_the_named_one() {
        let mut r = Reattach::new(CaptureTarget::mic("usb-mic"));
        let mut t = synced();
        assert_eq!(r.step(&t), connect(None));
        mic(&mut t, 42, "usb-mic");
        assert_eq!(r.step(&t), connect(Some("usb-mic")));
    }

    mod live {
        use super::*;
        use crate::testutil::{Loopback, peak, wait_for};
        use std::process::Command as Proc;

        /// Play a 440 Hz tone for `secs` into the node named `target`; the tone is generated in
        /// memory and piped to pw-play.
        fn play_into(target: &str, secs: f32) -> std::process::Child {
            use std::io::Write as _;
            let mut child = Proc::new("pw-play")
                .args([
                    "--raw",
                    "--rate",
                    "48000",
                    "--channels",
                    "1",
                    "--format",
                    "f32",
                ])
                .args(["--volume", "0.5", "--target", target, "-"])
                .stdin(std::process::Stdio::piped())
                .spawn()
                .expect("pw-play on PATH");
            let mut stdin = child.stdin.take().unwrap();
            let bytes: Vec<u8> = (0..(48_000.0 * secs) as usize)
                .flat_map(|i| {
                    (0.5 * (2.0 * std::f32::consts::PI * 440.0 * i as f32 / 48_000.0).sin())
                        .to_le_bytes()
                })
                .collect();
            std::thread::spawn(move || {
                let _ = stdin.write_all(&bytes);
            });
            child
        }

        /// Drain the reader for `dur` and return the samples that came.
        fn drain(r: &mut MicReader, dur: Duration) -> Vec<f32> {
            let mut all = Vec::new();
            let mut buf = [0.0f32; 4800];
            let t = Instant::now();
            while t.elapsed() < dur {
                let got = r.pop(&mut buf);
                all.extend_from_slice(&buf[..got]);
                std::thread::sleep(Duration::from_millis(10));
            }
            all
        }

        /// The 440 Hz tone must come out at 440 Hz when its samples are read at `rate`, and
        /// about `rate` of them must arrive per second: both fail if the stream is delivering
        /// another rate than the one it says.
        fn assert_tone_at(samples: &[f32], rate: u32, secs: f32, what: &str) {
            let hz = tone_hz(samples, rate);
            let per_sec = samples.len() as f32 / secs;
            println!(
                "{what}: {} samples ({per_sec:.0}/s), peak {:.2}, tone {hz:.1} Hz at {rate} Hz",
                samples.len(),
                peak(samples)
            );
            assert!(peak(samples) > 0.05, "{what}: the tone reached the ring");
            assert!(
                (hz - 440.0).abs() < 9.0,
                "{what}: 440 Hz tone measured as {hz} Hz"
            );
            assert!(
                (per_sec - rate as f32).abs() < rate as f32 * 0.15,
                "{what}: {per_sec} samples a second, want about {rate}"
            );
        }

        /// Needs a running PipeWire session with `pw-loopback` and `pw-play`. Makes
        /// its own virtual source, plays a tone into it, captures at 48 kHz, removes and
        /// recreates the source and checks capture resumes. Touches no real device and leaves
        /// the default source alone; the virtual source goes with its process on every exit
        /// path.
        #[test]
        #[ignore = "needs PipeWire, pw-loopback and pw-play; makes and removes its own virtual source"]
        fn live_named_virtual_source_is_captured_at_48k_and_resumes_after_replug() {
            let name = format!("dmn-test-mic-{}", std::process::id());
            let ctx = Arc::new(PipeWireContext::connect().expect("PipeWire running"));
            assert!(ctx.wait_for_sync(Duration::from_secs(2)).synced);

            let first = Loopback::new(&name);
            wait_for("source in the table", Duration::from_secs(3), || {
                ctx.nodes().sources().any(|n| n.name == name)
            });
            let mut mic = MicCapture::new(
                Arc::clone(&ctx),
                CaptureTarget::Mic(Some(name.clone())),
                48_000,
                2_000,
            )
            .unwrap();
            let st = mic.status();
            let mut reader = mic.start().unwrap();
            mic.wait_streaming(Duration::from_secs(3)).unwrap();
            assert_eq!(st.delivered_rate(), Some(48_000));

            let mut tone = play_into(&format!("{name}-sink"), 1.5);
            let heard = drain(&mut reader, Duration::from_millis(1000));
            let _ = tone.kill();
            let _ = tone.wait();
            assert_tone_at(&heard, 48_000, 1.0, "first run");
            assert_eq!(st.dropped(), 0, "the reader kept up");
            assert!(!st.fallback(), "the named source itself, not the default");
            // Counted: everything read was counted as pushed, and nothing else was.
            let pushed = st.pushed() as usize;
            assert!(
                pushed >= heard.len() && pushed <= heard.len() + reader.queued() + 4_800,
                "pushed {pushed}, read {}",
                heard.len()
            );

            // Unplug: the capture reports it and the ring stops filling.
            drop(first);
            wait_for("disconnected", Duration::from_secs(3), || {
                st.state() == CaptureState::Disconnected
            });
            assert_eq!(
                st.delivered_rate(),
                None,
                "nothing is delivered while unplugged"
            );
            let _ = drain(&mut reader, Duration::from_millis(100));
            let idle = drain(&mut reader, Duration::from_millis(300));
            assert!(idle.is_empty(), "the ring stopped filling");

            // Replug: capture resumes without reopening anything.
            let _second = Loopback::new(&name);
            wait_for("streaming again", Duration::from_secs(5), || {
                st.state() == CaptureState::Streaming
            });
            let _ = drain(&mut reader, Duration::from_millis(100));
            let mut tone = play_into(&format!("{name}-sink"), 1.5);
            let heard = drain(&mut reader, Duration::from_millis(1000));
            let _ = tone.kill();
            let _ = tone.wait();
            assert_tone_at(&heard, 48_000, 1.0, "after the replug");

            mic.stop().unwrap();
            assert_eq!(st.state(), CaptureState::Idle);
            assert_eq!(st.delivered_rate(), None, "nothing is delivered after stop");
        }

        /// Needs a running PipeWire session with `pw-loopback` and `pw-play`. Asks for a rate the
        /// graph is not running at, from its own virtual source, and checks by the pitch of a
        /// tone that the samples really are at that rate.
        #[test]
        #[ignore = "needs PipeWire, pw-loopback and pw-play; makes and removes its own virtual source"]
        fn live_a_rate_other_than_the_graphs_is_converted_by_pipewire() {
            let name = format!("dmn-test-rate-{}", std::process::id());
            let ctx = Arc::new(PipeWireContext::connect().expect("PipeWire running"));
            assert!(ctx.wait_for_sync(Duration::from_secs(2)).synced);
            let _src = Loopback::new(&name);
            wait_for("source in the table", Duration::from_secs(3), || {
                ctx.nodes().sources().any(|n| n.name == name)
            });
            let mut mic = MicCapture::new(
                Arc::clone(&ctx),
                CaptureTarget::Mic(Some(name.clone())),
                16_000,
                1_000,
            )
            .unwrap();
            let st = mic.status();
            let mut reader = mic.start().unwrap();
            mic.wait_streaming(Duration::from_secs(3)).unwrap();
            println!(
                "graph {} Hz, converted {}, delivered {:?}",
                ctx.nodes().graph_rate(),
                st.converted(),
                st.delivered_rate()
            );
            assert_eq!(st.delivered_rate(), Some(16_000));
            assert_ne!(
                ctx.nodes().graph_rate(),
                16_000,
                "this test needs a graph at another rate"
            );
            assert!(st.converted());
            let _ = drain(&mut reader, Duration::from_millis(100));
            let mut tone = play_into(&format!("{name}-sink"), 1.5);
            let heard = drain(&mut reader, Duration::from_millis(1000));
            let _ = tone.kill();
            let _ = tone.wait();
            assert_tone_at(&heard, 16_000, 1.0, "converted to 16 kHz");
            mic.stop().unwrap();
        }
    }
}
