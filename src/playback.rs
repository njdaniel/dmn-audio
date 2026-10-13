//! Playback at a rate the caller chooses: [`Playback`] plays mono f32 at `rate` Hz, written by
//! the caller into a lock-free ring ([`PlaybackWriter`]), to the default sink or to a sink the
//! caller names. It is the other half of [`crate::MicCapture`] and follows its shape; it sits
//! beside [`crate::VirtualMic`], which is an `Audio/Source` other programs record from, where
//! this is an ordinary playback stream that PipeWire links to a sink.
//!
//! Three pieces, each on the thread that owns it:
//!
//! - [`SinkFollow`], a pure state machine driven by this playback's supervisor thread. A named
//!   sink that goes away is disconnected and reconnected when it returns; the default sink has
//!   no `target.object`, so the session manager itself moves the stream when the default
//!   changes. (`Reattach` decides for capture targets and looks only at sources, so sinks have
//!   their own, smaller decision.)
//! - `LoopPlayback`, the `pw_stream` and its listeners, on the PipeWire loop thread. The
//!   supervisor reaches it through [`crate::pipewire::Command`]s, and its stream lives from
//!   `start` to `stop`: a reconnect keeps the stream and with it the ring's reader.
//! - `playback_process`, the real-time side: it pops the ring into a fixed scratch block and
//!   copies that into the buffer PipeWire handed out, padding what the ring could not supply
//!   with silence. It allocates nothing, locks nothing and logs nothing; an underrun is
//!   silence and a count, never a stall.
//!
//! The rate is asked of PipeWire, not computed here: the format pod names `rate` and the adapter
//! in front of the stream converts to whatever the sink runs at. What PipeWire settles on is
//! checked in `param_changed` against rate, channel count and sample format, and the real-time
//! callback plays nothing (a zero-length buffer) until it matches. A stream that settles on
//! something else is an error ([`PlaybackError::RateMismatch`]), never audio at another speed.

use std::ffi::CString;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use pipewire as pw;
use pw::spa;
use ringbuf::HeapRb;
use ringbuf::traits::{Consumer, Observer, Producer, Split};

use crate::capture::{CaptureState, enum_format_pod, stop_result, update_stream_props};
use crate::pipewire::{Command, NodeTable, PipeWireContext};
use crate::source::AudioError;

/// Lowest rate [`Playback`] accepts, in Hz.
pub const PLAYBACK_MIN_RATE: u32 = 8_000;
/// Highest rate [`Playback`] accepts, in Hz.
pub const PLAYBACK_MAX_RATE: u32 = 192_000;
/// A ring length that suits a caller with no reason to pick another, in milliseconds. The ring's
/// length is the most audio that can be waiting, so a caller playing live audio picks a short
/// one: what is queued when a sink goes away is played when it comes back.
pub const DEFAULT_PLAYBACK_RING_MS: u32 = 2_000;
/// `node.name` of the playback stream, as `pw-cli` and patchbays show it.
pub const PLAYBACK_NODE_NAME: &str = "dmn.playback";

/// The stream's format, as `param_changed` last judged it; the same three values and the same
/// reason as in `mic.rs`: a stream that settled on the wrong format still goes on to
/// `Streaming`, and its state must stay `Error`.
const FORMAT_UNSETTLED: u8 = 0;
const FORMAT_OK: u8 = 1;
const FORMAT_REJECTED: u8 = 2;

/// Samples copied per pass in the real-time callback; PipeWire's quantum is far below this.
const SCRATCH_FRAMES: usize = 8192;
/// Bytes per f32 sample.
const STRIDE: usize = 4;

type Prod = <HeapRb<f32> as Split>::Prod;
type Cons = <HeapRb<f32> as Split>::Cons;

/// Why a [`Playback`] could not be set up, or did not come up.
#[derive(Debug, thiserror::Error)]
pub enum PlaybackError {
    /// The requested rate is outside [`PLAYBACK_MIN_RATE`]..=[`PLAYBACK_MAX_RATE`].
    #[error("unsupported sample rate {rate} Hz (need {PLAYBACK_MIN_RATE} to {PLAYBACK_MAX_RATE})")]
    UnsupportedRate { rate: u32 },
    /// PipeWire settled on another rate than the one asked for, so nothing is played.
    #[error("PipeWire negotiated {negotiated} Hz, {requested} Hz was requested")]
    RateMismatch { requested: u32, negotiated: u32 },
    /// PipeWire settled on the right rate but another channel count or sample format.
    #[error("{0}")]
    Format(String),
    /// The stream reported an error.
    #[error("playback stream: {0}")]
    Stream(String),
    /// The stream did not reach `Streaming` in time (no such sink, no session manager).
    #[error("playback stream still {state} after {ms} ms")]
    NotStreaming { state: &'static str, ms: u64 },
    #[error(transparent)]
    Audio(#[from] AudioError),
}

/// Where a [`Playback`] plays to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SinkTarget {
    /// PipeWire's default sink. The stream carries no `target.object`, so the session manager
    /// moves it whenever the default changes.
    Default,
    /// The sink with this `node.name` (as [`crate::NodeTable::sinks`] lists it). While it is
    /// absent nothing is played, and the stream is never moved to another sink.
    Named(String),
}

impl SinkTarget {
    /// The target from a config string: "default" or "" means PipeWire's default sink, anything
    /// else is a sink's `node.name`.
    pub fn from_config(sink: &str) -> Self {
        match sink.trim() {
            "" | "default" => SinkTarget::Default,
            name => SinkTarget::Named(name.to_owned()),
        }
    }
}

/// One decision from [`SinkFollow::step`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SinkAction {
    /// (Re)connect the stream. `target_object` is written to `target.object` (removed when
    /// `None`, which is the default sink).
    Connect { target_object: Option<String> },
    /// Disconnect and wait: the named sink is gone.
    Disconnect,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SinkBound {
    /// Nothing decided yet.
    Nothing,
    /// Linked to the sink with this registry id.
    Node(u32),
    /// Linked to PipeWire's default sink (no `target.object`).
    Default,
    /// The named sink is not there; disconnected until it appears.
    Lost,
}

/// Decides, from registry snapshots, what the playback stream should be linked to. Pure: no
/// PipeWire, no threads; every call with an unchanged table returns `None`. Unlike
/// [`crate::Reattach`] for a microphone, a named sink that is absent never falls back to the
/// default one: audio meant for the headset must not come out of the speakers.
#[derive(Debug, Clone)]
pub struct SinkFollow {
    target: SinkTarget,
    bound: SinkBound,
}

impl SinkFollow {
    /// A follower of `target`, with nothing decided yet.
    pub fn new(target: SinkTarget) -> Self {
        Self {
            target,
            bound: SinkBound::Nothing,
        }
    }

    /// The sink being followed.
    pub fn target(&self) -> &SinkTarget {
        &self.target
    }

    /// What to do now, if anything, given the table.
    pub fn step(&mut self, table: &NodeTable) -> Option<SinkAction> {
        if table.disconnected.is_some() {
            return match self.bound {
                SinkBound::Nothing | SinkBound::Lost => None,
                _ => {
                    self.bound = SinkBound::Lost;
                    Some(SinkAction::Disconnect)
                }
            };
        }
        // Deciding "not found" before the first enumeration would report a sink missing that is
        // only not listed yet.
        if !table.synced {
            return None;
        }
        match &self.target {
            SinkTarget::Default => {
                if self.bound == SinkBound::Default {
                    return None;
                }
                self.bound = SinkBound::Default;
                Some(SinkAction::Connect {
                    target_object: None,
                })
            }
            SinkTarget::Named(name) => match table.sinks().find(|n| &n.name == name) {
                Some(n) => {
                    if self.bound == SinkBound::Node(n.id) {
                        return None;
                    }
                    self.bound = SinkBound::Node(n.id);
                    Some(SinkAction::Connect {
                        target_object: Some(name.clone()),
                    })
                }
                None => {
                    if self.bound == SinkBound::Lost {
                        return None;
                    }
                    self.bound = SinkBound::Lost;
                    Some(SinkAction::Disconnect)
                }
            },
        }
    }
}

/// Check the settled stream format against what was asked for: `want` Hz, mono, F32LE.
fn check_format(
    want: u32,
    rate: u32,
    channels: u32,
    format: spa::param::audio::AudioFormat,
) -> Result<(), PlaybackError> {
    if rate != want {
        return Err(PlaybackError::RateMismatch {
            requested: want,
            negotiated: rate,
        });
    }
    if channels != 1 || format != spa::param::audio::AudioFormat::F32LE {
        return Err(PlaybackError::Format(format!(
            "negotiated {format:?} {rate} Hz × {channels} ch, need F32LE {want} Hz mono"
        )));
    }
    Ok(())
}

/// Shared, lock-free view of one playback. The real-time callback only touches the atomics.
#[derive(Debug, Default)]
pub struct PlaybackStatus {
    state: AtomicU8,
    /// The rate asked for, in Hz.
    requested_rate: AtomicU32,
    /// The rate PipeWire settled on, 0 until it has.
    negotiated_rate: AtomicU32,
    /// `FORMAT_OK` once `param_changed` has seen the format asked for; the callback plays
    /// nothing while it is anything else.
    format: AtomicU8,
    /// Whether PipeWire last reported the stream as `Streaming`. Kept apart from `state`, which
    /// a rejected format holds at `Error`: when a good format then arrives on the same stream,
    /// this says where `state` goes back to.
    running: AtomicBool,
    /// Whether the caller is feeding a clip: gates `underruns`.
    active: AtomicBool,
    played: AtomicU64,
    silence: AtomicU64,
    underruns: AtomicU64,
    stalls: AtomicU64,
    last_error: Mutex<Option<String>>,
}

impl PlaybackStatus {
    /// Where the playback is in its life; the same states as [`crate::CaptureStatus`].
    /// `Disconnected` means the named sink is not there.
    pub fn state(&self) -> CaptureState {
        CaptureState::from_u8(self.state.load(Ordering::Acquire))
    }

    /// The rate samples are played at: the requested rate, once PipeWire has settled on it.
    /// `None` before that, while disconnected, and after a mismatch (nothing is played).
    pub fn playing_rate(&self) -> Option<u32> {
        self.delivering()
            .then(|| self.negotiated_rate.load(Ordering::Acquire))
    }

    /// True while the real-time callback plays the ring: the format is the one asked for.
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

    /// Samples taken from the ring and played since the last [`Playback::start`], as are the
    /// three counts below.
    pub fn played(&self) -> u64 {
        self.played.load(Ordering::Relaxed)
    }

    /// Samples of silence played because the ring was short, whether or not the caller was
    /// feeding a clip.
    pub fn silence(&self) -> u64 {
        self.silence.load(Ordering::Relaxed)
    }

    /// Callbacks that found the ring short of what the graph asked for while the writer was
    /// active ([`PlaybackWriter::set_active`]): each played silence, or a silent tail, in place
    /// of audio the caller had not delivered in time. Callbacks while the writer is idle are
    /// not counted; the silence between clips is not a fault.
    pub fn underruns(&self) -> u64 {
        self.underruns.load(Ordering::Relaxed)
    }

    /// Writes that found the ring too full to take everything.
    pub fn stalls(&self) -> u64 {
        self.stalls.load(Ordering::Relaxed)
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

    /// A new playback begins: counters and the last error start over.
    fn restart(&self) {
        for counter in [&self.played, &self.silence, &self.underruns, &self.stalls] {
            counter.store(0, Ordering::Relaxed);
        }
        self.active.store(false, Ordering::Relaxed);
        self.clear_error();
    }

    /// The stream is being connected, or connected again: nothing is played and no rate is
    /// known until a format settles, and what the last connection reported is over. Shuts the
    /// gate the real-time callback checks.
    fn connection_reset(&self) {
        self.format.store(FORMAT_UNSETTLED, Ordering::Release);
        self.negotiated_rate.store(0, Ordering::Release);
        self.running.store(false, Ordering::Release);
    }

    /// PipeWire withdrew the stream's format. The gate shuts. A rejection stands, with the rate
    /// that caused it, until a new format settles or the stream is connected again: the reason
    /// nothing is played must not turn into "unknown" on the way down.
    fn format_cleared(&self) {
        if self.format.load(Ordering::Acquire) != FORMAT_REJECTED {
            self.format.store(FORMAT_UNSETTLED, Ordering::Release);
            self.negotiated_rate.store(0, Ordering::Release);
        }
    }

    /// PipeWire settled on a format. The gate is set before anything is reported, so no buffer
    /// is ever filled from the ring at a rate other than the one asked for.
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
    /// plays nothing: it stays in `Error`. One the supervisor has already called gone stays
    /// `Disconnected`: this event was on its way when the sink went.
    fn stream_streaming(&self) {
        self.running.store(true, Ordering::Release);
        if self.state() != CaptureState::Disconnected
            && self.format.load(Ordering::Acquire) != FORMAT_REJECTED
        {
            self.set_state(CaptureState::Streaming);
        }
    }

    /// The stream is connecting or paused. Neither hides a missing sink nor a rejected format,
    /// which only the next connection clears.
    fn stream_waiting(&self) {
        self.running.store(false, Ordering::Release);
        if self.state() != CaptureState::Disconnected
            && self.format.load(Ordering::Acquire) != FORMAT_REJECTED
        {
            self.set_state(CaptureState::Connecting);
        }
    }

    /// Why nothing is being played, when the reason is an error and not a wait.
    fn failure(&self) -> Option<PlaybackError> {
        if self.state() != CaptureState::Error {
            return None;
        }
        let requested = self.requested_rate();
        let why = self.last_error().unwrap_or_default();
        Some(match self.negotiated_rate() {
            Some(negotiated) if negotiated != requested => PlaybackError::RateMismatch {
                requested,
                negotiated,
            },
            _ if self.format.load(Ordering::Acquire) == FORMAT_REJECTED => {
                PlaybackError::Format(why)
            }
            _ => PlaybackError::Stream(why),
        })
    }
}

/// The ring's producer end: the caller writes the samples to play here. Owned by one thread;
/// the playback callback is the only consumer.
pub struct PlaybackWriter {
    prod: Prod,
    status: Arc<PlaybackStatus>,
}

impl PlaybackWriter {
    /// Push as many of `samples` as fit and return that count; the caller keeps the rest and
    /// writes it later. Counts a stall when it is less than all of them. Never blocks, and
    /// nothing already queued is touched.
    pub fn write(&mut self, samples: &[f32]) -> usize {
        let n = self.prod.push_slice(samples);
        if n < samples.len() {
            self.status.stalls.fetch_add(1, Ordering::Relaxed);
        }
        n
    }

    /// Samples the ring can still take.
    pub fn vacant(&self) -> usize {
        self.prod.vacant_len()
    }

    /// Samples queued and not yet played.
    pub fn queued(&self) -> usize {
        self.prod.occupied_len()
    }

    /// Samples the ring holds at most.
    pub fn capacity(&self) -> usize {
        self.prod.capacity().get()
    }

    /// Say whether a clip is being fed. While it is, a callback that finds the ring short counts
    /// as an underrun in [`PlaybackStatus::underruns`]; while it is not, the silence is only
    /// counted in [`PlaybackStatus::silence`]. Set it before the first write and clear it
    /// with the last.
    pub fn set_active(&self, active: bool) {
        self.status.active.store(active, Ordering::Relaxed);
    }

    /// The playback's status, as [`Playback::status`] returns it.
    pub fn status(&self) -> Arc<PlaybackStatus> {
        Arc::clone(&self.status)
    }
}

/// The ring's consumer end, held by the playback callback.
pub(crate) struct PlaybackReader {
    cons: Cons,
    status: Arc<PlaybackStatus>,
}

impl PlaybackReader {
    /// Fill `out` from the ring and zero the rest; return how many real samples it had. An
    /// underrun is silence, never a wait. Real-time safe: no allocation, no lock, no logging,
    /// bounded time.
    pub(crate) fn fill(&mut self, out: &mut [f32]) -> usize {
        let got = self.cons.pop_slice(out);
        self.status.played.fetch_add(got as u64, Ordering::Relaxed);
        if got < out.len() {
            out[got..].fill(0.0);
            self.status
                .silence
                .fetch_add((out.len() - got) as u64, Ordering::Relaxed);
            if self.status.active.load(Ordering::Relaxed) {
                self.status.underruns.fetch_add(1, Ordering::Relaxed);
            }
        }
        got
    }
}

/// Build the ring of `capacity` samples and split it into its two ends.
pub(crate) fn playback_ring(
    capacity: usize,
    status: Arc<PlaybackStatus>,
) -> (PlaybackWriter, PlaybackReader) {
    let (prod, cons) = HeapRb::<f32>::new(capacity.max(1)).split();
    (
        PlaybackWriter {
            prod,
            status: Arc::clone(&status),
        },
        PlaybackReader { cons, status },
    )
}

/// What the loop thread needs to create the stream. The rate asked for travels in `status`.
pub(crate) struct PlaybackSpec {
    pub(crate) status: Arc<PlaybackStatus>,
}

/// The real-time side's state: the ring's consumer and a fixed block the samples pass through.
struct Outlet {
    reader: PlaybackReader,
    scratch: Box<[f32]>,
    status: Arc<PlaybackStatus>,
}

/// REAL-TIME CALLBACK (runs on PipeWire's data thread; `RT_PROCESS`).
/// Rules (CLAUDE.md, SPEC § Threading rules), reviewed call by call:
/// - no allocation: `dequeue_buffer` wraps a pointer, `datas_mut`/`data`/`chunk_mut` borrow
///   the mapped buffer, `PlaybackReader::fill` pops the ring into the preallocated `scratch`,
///   and the samples are copied from there into the buffer as little-endian bytes;
/// - no locks: the ring is lock-free SPSC, counters, the format gate and the active flag are
///   relaxed or acquire/release atomics;
/// - no logging, no syscalls, bounded time (one pass over the buffer).
///
/// Dequeue, fill, requeue (the `Buffer` guard requeues on drop). An empty ring fills the
/// buffer with silence: the graph never waits on the caller. Until the format is settled at the
/// requested rate, mono F32, the buffer is returned empty (size 0) and the ring is not touched.
fn playback_process(stream: &pw::stream::Stream, out: &mut Outlet) {
    let Some(mut buffer) = stream.dequeue_buffer() else {
        return;
    };
    // `requested` is how many frames the graph wants this cycle (0 on servers that do not
    // say, in which case the whole buffer is filled).
    let requested = buffer.requested() as usize;
    let delivering = out.status.delivering();
    let Some(data) = buffer.datas_mut().first_mut() else {
        return;
    };
    let mut frames = 0;
    if delivering && let Some(bytes) = data.data() {
        let max_frames = bytes.len() / STRIDE;
        frames = if requested == 0 {
            max_frames
        } else {
            requested.min(max_frames)
        };
        let mut done = 0;
        while done < frames {
            let n = (frames - done).min(out.scratch.len());
            out.reader.fill(&mut out.scratch[..n]);
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

/// The stream and its two listeners, owned by the PipeWire loop thread. The process listener is
/// the only one holding the [`Outlet`]; state and format events go to a second listener with
/// no user data, so the real-time thread and the loop thread never share a `&mut`.
pub(crate) struct LoopPlayback {
    // Field order is drop order: listeners before the stream they are attached to.
    _process: pw::stream::StreamListener<Outlet>,
    _events: pw::stream::StreamListener<()>,
    stream: pw::stream::StreamRc,
    status: Arc<PlaybackStatus>,
    connected: bool,
}

impl LoopPlayback {
    /// Create the stream (not connected yet) on the loop thread.
    pub(crate) fn open(
        core: &pw::core::CoreRc,
        spec: PlaybackSpec,
        reader: PlaybackReader,
    ) -> Result<Self, String> {
        let mut props = pw::properties::PropertiesBox::new();
        props.insert(*pw::keys::MEDIA_TYPE, "Audio");
        props.insert(*pw::keys::MEDIA_CATEGORY, "Playback");
        props.insert(*pw::keys::MEDIA_ROLE, "Communication");
        props.insert(*pw::keys::NODE_NAME, PLAYBACK_NODE_NAME);
        // A 10 ms quantum.
        let rate = spec.status.requested_rate();
        let latency = format!("{}/{}", rate / 100, rate);
        props.insert(*pw::keys::NODE_LATENCY, latency.as_str());
        let stream = pw::stream::StreamRc::new(core.clone(), PLAYBACK_NODE_NAME, props)
            .map_err(|e| e.to_string())?;

        let outlet = Outlet {
            reader,
            scratch: vec![0.0; SCRATCH_FRAMES].into_boxed_slice(),
            status: Arc::clone(&spec.status),
        };
        let process_listener = stream
            .add_local_listener_with_user_data(outlet)
            .process(playback_process)
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

    /// Point the stream at a new target (`None`: the default sink) and (re)connect it.
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
        // A named sink is strict: if it is missing or goes away the stream must not be moved to
        // the default one (the speakers), it waits for `SinkFollow` to reconnect it. The default
        // sink is the opposite: the session manager has to be free to move the stream.
        let strict = target.as_deref().map(|_| c"true");
        update_stream_props(
            &self.stream,
            &[
                (c"target.object", target.as_deref()),
                (c"node.dont-fallback", strict),
                (c"node.dont-reconnect", strict),
            ],
        )?;
        let pod = enum_format_pod(self.status.requested_rate())?;
        let mut params = [spa::pod::Pod::from_bytes(&pod).ok_or("bad EnumFormat pod")?];
        self.stream
            .connect(
                spa::utils::Direction::Output,
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

    /// The sink is gone (the supervisor's `Disconnect`): unlink, and say so. The supervisor set
    /// `Disconnected` when it sent the command, but a `Streaming` event already on its way
    /// could have landed since; on this thread, after the unlink, none can.
    pub(crate) fn disconnect(&mut self) {
        self.unlink();
        self.status.set_state(CaptureState::Disconnected);
    }

    fn unlink(&mut self) {
        // A reconnect renegotiates the format, so the old verdict must not carry over.
        self.status.connection_reset();
        if self.connected {
            // After this returns the data thread no longer calls `playback_process`.
            let _ = self.stream.disconnect();
            self.connected = false;
        }
    }
}

impl Drop for LoopPlayback {
    fn drop(&mut self) {
        // Destroying the stream (the fields drop after this) removes the node from the graph.
        self.unlink();
        self.status.set_state(CaptureState::Idle);
    }
}

/// How often the supervisor re-checks when nothing changed (also its stop latency).
const SUPERVISE_TICK: Duration = Duration::from_millis(100);
/// How long `stop` waits for the loop thread to close the stream.
const STOP_TIMEOUT: Duration = Duration::from_secs(1);

/// Mono f32 played at a rate the caller chooses, from a lock-free ring, to the default sink or
/// to a named one.
///
/// ```no_run
/// use std::sync::Arc;
/// use std::time::Duration;
/// use dmn_audio::{PipeWireContext, Playback, SinkTarget};
///
/// let ctx = Arc::new(PipeWireContext::connect()?);
/// let mut out = Playback::new(ctx, SinkTarget::Default, 48_000, 2_000)?;
/// let mut writer = out.start()?;
/// out.wait_streaming(Duration::from_secs(3))?;
/// writer.set_active(true);
/// let queued = writer.write(&[0.0f32; 960]); // 20 ms; the ring takes what fits
/// # let _ = queued;
/// out.stop()?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct Playback {
    ctx: Arc<PipeWireContext>,
    target: SinkTarget,
    rate: u32,
    ring_samples: usize,
    status: Arc<PlaybackStatus>,
    key: Option<u64>,
    stop: Arc<AtomicBool>,
    supervisor: Option<JoinHandle<()>>,
}

impl Playback {
    /// A playback to `target` at `rate` Hz, from a ring that holds `ring_ms` milliseconds.
    /// Nothing is opened until [`Self::start`].
    pub fn new(
        ctx: Arc<PipeWireContext>,
        target: SinkTarget,
        rate: u32,
        ring_ms: u32,
    ) -> Result<Self, PlaybackError> {
        if !(PLAYBACK_MIN_RATE..=PLAYBACK_MAX_RATE).contains(&rate) {
            return Err(PlaybackError::UnsupportedRate { rate });
        }
        let ring_samples = (u64::from(rate) * u64::from(ring_ms) / 1000).max(1) as usize;
        let status = Arc::new(PlaybackStatus::default());
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

    /// Shared status: state, played rate, underruns, last error.
    pub fn status(&self) -> Arc<PlaybackStatus> {
        Arc::clone(&self.status)
    }

    /// The sink this plays to.
    pub fn target(&self) -> &SinkTarget {
        &self.target
    }

    /// The rate the caller writes at, in Hz.
    pub fn rate(&self) -> u32 {
        self.rate
    }

    /// Create the stream and start following the sink. Returns the ring's writer at once; the
    /// stream connects in the background, see [`Self::wait_streaming`]. Calling it while
    /// started is an error. After [`Self::stop`] it may be called again: that is a new playback
    /// with a new, empty ring, the status counters and last error start over, and what the
    /// writer of the playback before it writes goes nowhere.
    pub fn start(&mut self) -> Result<PlaybackWriter, PlaybackError> {
        if self.key.is_some() {
            return Err(AudioError::AlreadyStarted.into());
        }
        self.status.restart();
        let (writer, reader) = playback_ring(self.ring_samples, Arc::clone(&self.status));
        let key = self.ctx.next_key();
        self.ctx
            .send(Command::OpenPlayback {
                key,
                spec: PlaybackSpec {
                    status: Arc::clone(&self.status),
                },
                reader,
            })
            .map_err(AudioError::from)?;
        self.key = Some(key);
        self.stop.store(false, Ordering::Release);

        let ctx = Arc::clone(&self.ctx);
        let stop = Arc::clone(&self.stop);
        let status = Arc::clone(&self.status);
        let mut follow = SinkFollow::new(self.target.clone());
        let handle = std::thread::Builder::new()
            .name("dmn-playback".into())
            .spawn(move || {
                let mut table = ctx.nodes();
                while !stop.load(Ordering::Acquire) {
                    if let Some(action) = follow.step(&table) {
                        let cmd = match action {
                            SinkAction::Connect { target_object } => {
                                status.set_state(CaptureState::Connecting);
                                Command::ConnectPlayback { key, target_object }
                            }
                            SinkAction::Disconnect => {
                                status.set_state(CaptureState::Disconnected);
                                Command::DisconnectPlayback { key }
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
        Ok(writer)
    }

    /// Block until the stream is playing at the requested rate, or fail with the reason:
    /// [`PlaybackError::RateMismatch`] when PipeWire settled on another rate,
    /// [`PlaybackError::Format`] when it settled on the rate but not on mono f32,
    /// [`PlaybackError::Stream`] for any other stream error, [`PlaybackError::NotStreaming`] on
    /// timeout (a named sink that is not there ends up here, in state `disconnected`).
    pub fn wait_streaming(&self, timeout: Duration) -> Result<(), PlaybackError> {
        let t = Instant::now();
        loop {
            let state = self.status.state();
            if state == CaptureState::Streaming && self.status.playing_rate().is_some() {
                return Ok(());
            }
            if let Some(why) = self.status.failure() {
                return Err(why);
            }
            if t.elapsed() >= timeout {
                return Err(PlaybackError::NotStreaming {
                    state: state.label(),
                    ms: timeout.as_millis() as u64,
                });
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// Stop following the sink, close the stream and wait for the loop thread to remove its node
    /// from the graph. After it returns nothing reads the ring. Idempotent.
    pub fn stop(&mut self) -> Result<(), AudioError> {
        self.stop.store(true, Ordering::Release);
        let panicked = self
            .supervisor
            .take()
            .map(|h| h.join().is_err())
            .unwrap_or(false);
        let mut final_state = None;
        if let Some(key) = self.key.take()
            && self.ctx.send(Command::ClosePlayback { key }).is_ok()
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

impl Drop for Playback {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipewire::RegistryEvent;
    use spa::param::audio::AudioFormat;

    fn status() -> Arc<PlaybackStatus> {
        Arc::new(PlaybackStatus::default())
    }

    fn sink(t: &mut NodeTable, id: u32, name: &str) {
        t.apply(RegistryEvent::Added {
            id,
            props: vec![
                ("media.class".into(), "Audio/Sink".into()),
                ("node.name".into(), name.into()),
            ],
        });
    }

    fn synced() -> NodeTable {
        let mut t = NodeTable::default();
        t.apply(RegistryEvent::Synced);
        t
    }

    fn connect(target: Option<&str>) -> Option<SinkAction> {
        Some(SinkAction::Connect {
            target_object: target.map(str::to_owned),
        })
    }

    #[test]
    fn an_underrun_plays_silence_and_is_counted_only_while_a_clip_is_being_fed() {
        let st = status();
        let (mut w, mut r) = playback_ring(100, Arc::clone(&st));
        let mut out = [1.0f32; 16];
        // Empty ring, writer idle: silence, nothing counted as a fault.
        let t = Instant::now();
        assert_eq!(r.fill(&mut out), 0);
        assert!(out.iter().all(|&s| s == 0.0), "padding is silence");
        assert!(t.elapsed() < Duration::from_millis(5), "returned at once");
        assert_eq!((st.underruns(), st.played(), st.silence()), (0, 0, 16));

        // Active: a short fill is real samples then silence, and counts; so does an empty one.
        w.set_active(true);
        assert_eq!(w.write(&[0.5; 10]), 10);
        out.fill(1.0);
        assert_eq!(r.fill(&mut out), 10);
        assert!(out[..10].iter().all(|&s| s == 0.5));
        assert!(out[10..].iter().all(|&s| s == 0.0));
        assert_eq!((st.underruns(), st.played(), st.silence()), (1, 10, 22));
        out.fill(1.0);
        assert_eq!(r.fill(&mut out), 0);
        assert!(out.iter().all(|&s| s == 0.0), "a gap is silence");
        assert_eq!((st.underruns(), st.silence()), (2, 38));

        // A full buffer is no underrun, whatever the gate says.
        assert_eq!(w.write(&[0.25; 16]), 16);
        assert_eq!(r.fill(&mut out), 16);
        assert_eq!(st.underruns(), 2);

        // Writer idle again: the tail is only silence.
        w.set_active(false);
        assert_eq!(r.fill(&mut out), 0);
        assert_eq!((st.underruns(), st.silence()), (2, 54));
        assert_eq!(st.played(), 26);
    }

    #[test]
    fn samples_come_out_in_order_across_fills_and_a_full_ring_refuses_and_counts_a_stall() {
        let st = status();
        let (mut w, mut r) = playback_ring(8, Arc::clone(&st));
        assert_eq!((w.capacity(), w.vacant(), w.queued()), (8, 8, 0));
        let first: Vec<f32> = (0..6).map(|i| i as f32).collect();
        assert_eq!(w.write(&first), 6);
        assert_eq!(st.stalls(), 0);
        assert_eq!(w.write(&[6.0, 7.0, 8.0, 9.0]), 2, "only two fit");
        assert_eq!(st.stalls(), 1);
        assert_eq!((w.vacant(), w.queued()), (0, 8));
        let mut out = [9.0f32; 5];
        assert_eq!(r.fill(&mut out), 5);
        assert_eq!(out, [0.0, 1.0, 2.0, 3.0, 4.0]);
        assert_eq!(
            w.write(&[8.0, 9.0]),
            2,
            "room again, and what was queued is intact"
        );
        let mut out = [9.0f32; 8];
        assert_eq!(r.fill(&mut out), 5);
        assert_eq!(&out[..5], &[5.0, 6.0, 7.0, 8.0, 9.0]);
        assert_eq!(&out[5..], &[0.0, 0.0, 0.0]);
    }

    #[test]
    fn only_the_requested_rate_mono_f32_opens_the_gate() {
        assert!(check_format(48_000, 48_000, 1, AudioFormat::F32LE).is_ok());
        assert!(matches!(
            check_format(48_000, 44_100, 1, AudioFormat::F32LE),
            Err(PlaybackError::RateMismatch {
                requested: 48_000,
                negotiated: 44_100
            })
        ));
        assert!(matches!(
            check_format(48_000, 48_000, 2, AudioFormat::F32LE),
            Err(PlaybackError::Format(_))
        ));
        assert!(matches!(
            check_format(48_000, 48_000, 1, AudioFormat::S16LE),
            Err(PlaybackError::Format(_))
        ));
    }

    #[test]
    fn the_status_reports_a_rate_only_while_it_is_being_played() {
        let st = status();
        st.requested_rate.store(48_000, Ordering::Relaxed);
        assert_eq!((st.playing_rate(), st.negotiated_rate()), (None, None));
        st.format_settled(44_100, 1, AudioFormat::F32LE);
        assert_eq!(st.playing_rate(), None, "settled on the wrong rate");
        assert_eq!(st.negotiated_rate(), Some(44_100));
        st.connection_reset();
        assert_eq!((st.playing_rate(), st.negotiated_rate()), (None, None));
        st.format_settled(48_000, 1, AudioFormat::F32LE);
        assert_eq!(st.playing_rate(), Some(48_000));
        st.connection_reset();
        assert_eq!(st.playing_rate(), None, "a reconnect shuts the gate");
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
            assert_eq!(st.playing_rate(), Some(48_000));
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
            Some(PlaybackError::RateMismatch {
                requested: 48_000,
                negotiated: 44_100
            })
        ));
        assert_eq!(st.playing_rate(), None);
        // A healthy stream whose format is cleared just stops playing.
        let st = status();
        st.requested_rate.store(48_000, Ordering::Relaxed);
        st.format_settled(48_000, 1, AudioFormat::F32LE);
        st.stream_streaming();
        st.format_cleared();
        assert_eq!((st.playing_rate(), st.negotiated_rate()), (None, None));
        assert!(st.failure().is_none());
    }

    /// A `Streaming` event that was on its way when the sink went must not undo `Disconnected`.
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
    fn a_new_playback_starts_its_counters_and_its_error_over() {
        let st = status();
        let (mut w, mut r) = playback_ring(4, Arc::clone(&st));
        w.set_active(true);
        w.write(&[0.5; 6]);
        r.fill(&mut [0.0; 8]);
        st.set_error("boom".into());
        assert_eq!(
            (st.played(), st.silence(), st.underruns(), st.stalls()),
            (4, 4, 1, 1)
        );
        st.restart();
        assert_eq!(
            (st.played(), st.silence(), st.underruns(), st.stalls()),
            (0, 0, 0, 0)
        );
        assert_eq!(st.last_error(), None);
        r.fill(&mut [0.0; 8]);
        assert_eq!(st.underruns(), 0, "and the writer is idle again");
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
            assert_eq!(st.playing_rate(), None);
            match st.failure() {
                Some(PlaybackError::RateMismatch {
                    requested: 48_000,
                    negotiated,
                }) => assert_eq!((negotiated, rate), (44_100, 44_100)),
                Some(PlaybackError::Format(why)) => {
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
            assert_eq!(st.playing_rate(), Some(48_000));
            assert!(st.failure().is_none());
        }
    }

    #[test]
    fn a_stream_error_is_reported_as_such_and_waiting_does_not_hide_a_missing_sink() {
        let st = status();
        st.requested_rate.store(48_000, Ordering::Relaxed);
        st.format_settled(48_000, 1, AudioFormat::F32LE);
        st.stream_streaming();
        assert!(st.failure().is_none());
        st.set_error("boom".into());
        assert_eq!(st.state(), CaptureState::Error);
        assert!(matches!(st.failure(), Some(PlaybackError::Stream(why)) if why == "boom"));

        let st = status();
        st.set_state(CaptureState::Disconnected);
        st.stream_waiting();
        assert_eq!(st.state(), CaptureState::Disconnected);
    }

    #[test]
    fn a_sink_target_comes_from_a_config_string() {
        assert_eq!(SinkTarget::from_config(""), SinkTarget::Default);
        assert_eq!(SinkTarget::from_config(" default "), SinkTarget::Default);
        assert_eq!(
            SinkTarget::from_config("alsa_output.usb"),
            SinkTarget::Named("alsa_output.usb".into())
        );
    }

    #[test]
    fn the_default_sink_connects_without_a_target_once_and_follows_the_server() {
        let mut f = SinkFollow::new(SinkTarget::Default);
        let mut t = NodeTable::default();
        assert_eq!(f.step(&t), None, "nothing before the first enumeration");
        t.apply(RegistryEvent::Synced);
        assert_eq!(f.step(&t), connect(None));
        assert_eq!(f.step(&t), None);
        // The default moves to another sink: the session manager moves the stream, we do nothing.
        sink(&mut t, 70, "speakers");
        t.apply(RegistryEvent::DefaultSink(Some("speakers".into())));
        assert_eq!(f.step(&t), None);
        sink(&mut t, 71, "headset");
        t.apply(RegistryEvent::DefaultSink(Some("headset".into())));
        assert_eq!(f.step(&t), None);
        t.apply(RegistryEvent::Removed(71));
        t.apply(RegistryEvent::DefaultSink(Some("speakers".into())));
        assert_eq!(f.step(&t), None);
        // The server going away is the one thing that disconnects it.
        t.apply(RegistryEvent::Disconnected("gone".into()));
        assert_eq!(f.step(&t), Some(SinkAction::Disconnect));
        assert_eq!(f.step(&t), None);
    }

    #[test]
    fn a_named_sink_that_goes_away_is_disconnected_and_found_again_when_it_returns() {
        let mut f = SinkFollow::new(SinkTarget::Named("headset".into()));
        let mut t = synced();
        sink(&mut t, 80, "headset");
        sink(&mut t, 81, "speakers");
        t.apply(RegistryEvent::DefaultSink(Some("speakers".into())));
        assert_eq!(f.step(&t), connect(Some("headset")));
        assert_eq!(f.step(&t), None);
        // The default changing is not the named sink's business.
        t.apply(RegistryEvent::DefaultSink(Some("headset".into())));
        t.apply(RegistryEvent::DefaultSink(Some("speakers".into())));
        assert_eq!(f.step(&t), None);

        t.apply(RegistryEvent::Removed(80));
        assert_eq!(f.step(&t), Some(SinkAction::Disconnect));
        assert_eq!(
            f.step(&t),
            None,
            "stays disconnected, no fallback to the speakers"
        );

        // Plugged back in under a new id.
        sink(&mut t, 82, "headset");
        assert_eq!(f.step(&t), connect(Some("headset")));
        assert_eq!(f.step(&t), None);
    }

    #[test]
    fn a_named_sink_absent_at_start_is_reported_missing_and_not_replaced_by_the_default() {
        let mut f = SinkFollow::new(SinkTarget::Named("headset".into()));
        let mut t = NodeTable::default();
        assert_eq!(f.step(&t), None, "not enumerated yet is not missing");
        t.apply(RegistryEvent::Synced);
        sink(&mut t, 90, "speakers");
        t.apply(RegistryEvent::DefaultSink(Some("speakers".into())));
        assert_eq!(f.step(&t), Some(SinkAction::Disconnect));
        assert_eq!(f.step(&t), None);
        sink(&mut t, 91, "headset");
        assert_eq!(f.step(&t), connect(Some("headset")));
    }

    #[test]
    fn a_named_sink_that_comes_back_under_a_new_id_without_a_gap_is_reconnected() {
        let mut f = SinkFollow::new(SinkTarget::Named("headset".into()));
        let mut t = synced();
        sink(&mut t, 80, "headset");
        assert_eq!(f.step(&t), connect(Some("headset")));
        // Removed and re-added between two looks of the supervisor.
        t.apply(RegistryEvent::Removed(80));
        sink(&mut t, 83, "headset");
        assert_eq!(f.step(&t), connect(Some("headset")));
        assert_eq!(f.step(&t), None);
    }

    #[test]
    fn a_named_sink_is_disconnected_when_the_server_goes_away() {
        let mut f = SinkFollow::new(SinkTarget::Named("headset".into()));
        let mut t = synced();
        sink(&mut t, 80, "headset");
        assert_eq!(f.step(&t), connect(Some("headset")));
        t.apply(RegistryEvent::Disconnected("gone".into()));
        assert_eq!(f.step(&t), Some(SinkAction::Disconnect));
        assert_eq!(f.step(&t), None);
    }

    /// A tone through the ring and out of a callback-sized fill keeps its pitch: the ring and the
    /// fill add and drop nothing.
    #[test]
    fn a_tone_written_to_the_ring_comes_out_of_fills_of_any_size_unchanged() {
        use crate::testutil::tone_hz;
        let rate = 48_000u32;
        let tone: Vec<f32> = (0..rate)
            .map(|i| (2.0 * std::f32::consts::PI * 440.0 * i as f32 / rate as f32).sin())
            .collect();
        let st = status();
        let (mut w, mut r) = playback_ring(rate as usize, st);
        assert_eq!(w.write(&tone), tone.len());
        let mut heard = Vec::new();
        let mut buf = [0.0f32; 480];
        for size in [480, 441, 7, 480].into_iter().cycle() {
            let n = r.fill(&mut buf[..size]);
            heard.extend_from_slice(&buf[..n]);
            if n < size {
                break;
            }
        }
        assert_eq!(heard, tone);
        assert!((tone_hz(&heard, rate) - 440.0).abs() < 1.0);
    }

    mod live {
        use super::*;
        use crate::mic::MicCapture;
        use crate::testutil::{Loopback, peak, wait_for};
        use crate::{CaptureTarget, PipeWireContext};
        use std::process::Command as Proc;

        const TONE_HZ: f32 = 440.0;

        /// Feed `secs` of a 440 Hz tone into the writer as it has room, while draining `mic`'s
        /// reader; return what the microphone heard.
        fn play_and_record(
            writer: &mut PlaybackWriter,
            reader: &mut crate::mic::MicReader,
            rate: u32,
            secs: f32,
        ) -> Vec<f32> {
            let total = (rate as f32 * secs) as usize;
            let tone: Vec<f32> = (0..total)
                .map(|i| {
                    0.3 * (2.0 * std::f32::consts::PI * TONE_HZ * i as f32 / rate as f32).sin()
                })
                .collect();
            let mut sent = 0;
            let mut heard = Vec::new();
            let mut buf = [0.0f32; 4800];
            writer.set_active(true);
            let t = Instant::now();
            let limit = Duration::from_secs_f32(secs + 1.0);
            while t.elapsed() < limit {
                if sent < total {
                    sent += writer.write(&tone[sent..]);
                } else if writer.queued() == 0 {
                    writer.set_active(false);
                }
                let got = reader.pop(&mut buf);
                heard.extend_from_slice(&buf[..got]);
                std::thread::sleep(Duration::from_millis(10));
            }
            writer.set_active(false);
            heard
        }

        /// The names `pw-link -l` shows linked from the output ports of the node `node`.
        fn linked_from(node: &str) -> Vec<String> {
            let out = Proc::new("pw-link").arg("-l").output().expect("pw-link");
            let text = String::from_utf8_lossy(&out.stdout).into_owned();
            let mut targets = Vec::new();
            let mut on = false;
            for line in text.lines() {
                if !line.starts_with(' ') {
                    on = line.starts_with(&format!("{node}:"));
                } else if on && let Some((_, to)) = line.trim().split_once("|-> ") {
                    targets.push(to.to_owned());
                }
            }
            targets
        }

        /// Refuse to go on, before a sample is written, unless the playback is linked to
        /// `sink` and to nothing else.
        fn assert_linked_only_to(sink: &str) {
            let links = linked_from(PLAYBACK_NODE_NAME);
            assert!(
                !links.is_empty() && links.iter().all(|l| l.starts_with(&format!("{sink}:"))),
                "dmn.playback is linked to {links:?}, wanted only {sink}"
            );
        }

        fn assert_tone(heard: &[f32], rate: u32, what: &str) {
            let hz = crate::testutil::tone_hz(heard, rate);
            println!(
                "{what}: {} samples, peak {:.2}, tone {hz:.1} Hz",
                heard.len(),
                peak(heard)
            );
            assert!(peak(heard) > 0.05, "{what}: the tone came out");
            assert!(
                (hz - TONE_HZ).abs() < 9.0,
                "{what}: 440 Hz tone measured as {hz} Hz"
            );
        }

        /// Needs a running PipeWire session with `pw-loopback` and `pw-link`. Makes its own
        /// virtual sink and source (nothing is heard, no real device is touched, the default
        /// sink is left alone), plays a tone to that sink by name, records it from the source,
        /// removes and recreates the sink and checks playback resumes.
        #[test]
        #[ignore = "needs PipeWire, pw-loopback and pw-link; plays only to a sink it creates"]
        fn live_playback_to_a_named_virtual_sink_arrives_at_pitch_and_resumes_after_replug() {
            let name = format!("dmn-test-play-{}", std::process::id());
            let sink_name = format!("{name}-sink");
            let ctx = Arc::new(PipeWireContext::connect().expect("PipeWire running"));
            assert!(ctx.wait_for_sync(Duration::from_secs(2)).synced);

            let first = Loopback::new(&name);
            wait_for(
                "sink and source in the table",
                Duration::from_secs(3),
                || {
                    let t = ctx.nodes();
                    t.sinks().any(|n| n.name == sink_name) && t.sources().any(|n| n.name == name)
                },
            );
            let mut mic = MicCapture::new(
                Arc::clone(&ctx),
                CaptureTarget::Mic(Some(name.clone())),
                48_000,
                2_000,
            )
            .unwrap();
            let mut heard_by = mic.start().unwrap();
            mic.wait_streaming(Duration::from_secs(3)).unwrap();

            let mut play = Playback::new(
                Arc::clone(&ctx),
                SinkTarget::Named(sink_name.clone()),
                48_000,
                2_000,
            )
            .unwrap();
            let st = play.status();
            let mut writer = play.start().unwrap();
            play.wait_streaming(Duration::from_secs(3)).unwrap();
            assert_eq!(st.playing_rate(), Some(48_000));
            assert_linked_only_to(&sink_name);

            let heard = play_and_record(&mut writer, &mut heard_by, 48_000, 1.0);
            assert_tone(&heard, 48_000, "first run");
            assert!(st.played() >= 40_000, "played {}", st.played());

            // Unplug the sink: the status says so and the ring is no longer read.
            drop(first);
            wait_for("disconnected", Duration::from_secs(3), || {
                st.state() == CaptureState::Disconnected
            });
            assert_eq!(st.playing_rate(), None, "nothing is played while unplugged");
            let before = st.played();
            std::thread::sleep(Duration::from_millis(200));
            assert_eq!(st.played(), before, "the ring is not read");
            assert!(
                linked_from(PLAYBACK_NODE_NAME).is_empty(),
                "not moved to another sink: {:?}",
                linked_from(PLAYBACK_NODE_NAME)
            );

            // Replug: playback resumes without reopening anything.
            let _second = Loopback::new(&name);
            wait_for("streaming again", Duration::from_secs(5), || {
                st.state() == CaptureState::Streaming && st.playing_rate().is_some()
            });
            assert_linked_only_to(&sink_name);
            wait_for("microphone back", Duration::from_secs(5), || {
                mic.status().delivered_rate().is_some()
            });
            let _ = heard_by.pop(&mut [0.0; 48_000]);
            let heard = play_and_record(&mut writer, &mut heard_by, 48_000, 1.0);
            assert_tone(&heard, 48_000, "after the replug");

            play.stop().unwrap();
            assert_eq!(st.state(), CaptureState::Idle);
            assert!(
                !ctx.nodes()
                    .streams()
                    .any(|s| s.node_name == PLAYBACK_NODE_NAME),
                "the node left the graph"
            );
            mic.stop().unwrap();
        }

        /// Sets the `default.audio.sink` metadata entry, or deletes it for `None`.
        fn set_default_sink(to: Option<&str>) {
            let mut cmd = Proc::new("pw-metadata");
            match to {
                Some(name) => cmd.args([
                    "0",
                    "default.audio.sink",
                    &format!("{{ \"name\": \"{name}\" }}"),
                    "Spa:String:JSON",
                ]),
                None => cmd.args(["-d", "0", "default.audio.sink"]),
            };
            let out = cmd.output().expect("pw-metadata on PATH");
            assert!(out.status.success(), "pw-metadata failed");
        }

        /// Wait until the playback stream is linked to `sink` and to nothing else.
        fn wait_linked_only_to(sink: &str) {
            wait_for(
                "the stream to be on the sink",
                Duration::from_secs(5),
                || {
                    let links = linked_from(PLAYBACK_NODE_NAME);
                    !links.is_empty() && links.iter().all(|l| l.starts_with(&format!("{sink}:")))
                },
            );
        }

        /// CHANGES THE SESSION'S DEFAULT SINK while it runs, to two virtual sinks of its own in
        /// turn, and puts back what it found on every exit path. Everything else playing on the
        /// machine moves with the default, so it goes quiet for those seconds: run it on a
        /// development machine with nothing playing. The test itself is silent: it writes no
        /// sample until the stream is linked to one of its own sinks and to nothing else.
        ///
        /// Needs a running PipeWire session with a session manager, `pw-loopback`, `pw-link`
        /// and `pw-metadata`. It checks that a stream playing to "the default" is moved by the
        /// session manager when the default changes, without being reopened.
        ///
        /// Not run when it was written (it needs the default sink changed on a machine in
        /// use), so whether writing `default.audio.sink` moves the default on a given session
        /// manager is itself something the first run of this test finds out.
        #[test]
        #[ignore = "CHANGES THE DEFAULT SINK while it runs (to its own virtual sinks, then back); needs PipeWire, pw-loopback, pw-link and pw-metadata"]
        fn live_playback_to_the_default_sink_follows_it_when_the_default_changes() {
            let pid = std::process::id();
            let (a, b) = (
                format!("dmn-test-follow-a-{pid}"),
                format!("dmn-test-follow-b-{pid}"),
            );
            let (a_sink, b_sink) = (format!("{a}-sink"), format!("{b}-sink"));
            let ctx = Arc::new(PipeWireContext::connect().expect("PipeWire running"));
            assert!(ctx.wait_for_sync(Duration::from_secs(2)).synced);

            let _first = Loopback::new(&a);
            let _second = Loopback::new(&b);
            wait_for(
                "both virtual sinks in the table",
                Duration::from_secs(3),
                || {
                    let t = ctx.nodes();
                    [&a_sink, &b_sink]
                        .iter()
                        .all(|s| t.sinks().any(|n| &n.name == *s))
                        && [&a, &b].iter().all(|s| t.sources().any(|n| &n.name == *s))
                },
            );

            /// Puts the default sink back as it was found, on every exit path. Declared after
            /// the loopbacks so that it runs before they go.
            struct Restore(Option<String>);
            impl Drop for Restore {
                fn drop(&mut self) {
                    let mut cmd = Proc::new("pw-metadata");
                    match &self.0 {
                        Some(name) => cmd.args([
                            "0",
                            "default.audio.sink",
                            &format!("{{ \"name\": \"{name}\" }}"),
                            "Spa:String:JSON",
                        ]),
                        None => cmd.args(["-d", "0", "default.audio.sink"]),
                    };
                    let _ = cmd.output();
                }
            }
            let _restore = Restore(ctx.nodes().default_sink);

            let record = |name: &str| {
                let mut mic = MicCapture::new(
                    Arc::clone(&ctx),
                    CaptureTarget::Mic(Some(name.to_owned())),
                    48_000,
                    2_000,
                )
                .unwrap();
                let reader = mic.start().unwrap();
                mic.wait_streaming(Duration::from_secs(3)).unwrap();
                (mic, reader)
            };
            let (mut mic_a, mut heard_on_a) = record(&a);
            let (mut mic_b, mut heard_on_b) = record(&b);

            // The default is the first virtual sink before the stream exists, so the stream is
            // never linked to a real device.
            set_default_sink(Some(&a_sink));
            wait_for(
                "the default to be the first sink",
                Duration::from_secs(3),
                || ctx.nodes().default_sink.as_deref() == Some(a_sink.as_str()),
            );
            let mut play =
                Playback::new(Arc::clone(&ctx), SinkTarget::Default, 48_000, 2_000).unwrap();
            let st = play.status();
            let mut writer = play.start().unwrap();
            play.wait_streaming(Duration::from_secs(3)).unwrap();
            wait_linked_only_to(&a_sink);
            let heard = play_and_record(&mut writer, &mut heard_on_a, 48_000, 1.0);
            assert_tone(&heard, 48_000, "on the first default");

            // Move the default: the stream follows without being reopened.
            set_default_sink(Some(&b_sink));
            wait_for(
                "the default to be the second sink",
                Duration::from_secs(3),
                || ctx.nodes().default_sink.as_deref() == Some(b_sink.as_str()),
            );
            wait_linked_only_to(&b_sink);
            wait_for("playing again", Duration::from_secs(5), || {
                st.state() == CaptureState::Streaming && st.playing_rate().is_some()
            });
            let _ = heard_on_b.pop(&mut [0.0; 48_000]);
            let heard = play_and_record(&mut writer, &mut heard_on_b, 48_000, 1.0);
            assert_tone(&heard, 48_000, "after the default moved");

            play.stop().unwrap();
            mic_a.stop().unwrap();
            mic_b.stop().unwrap();
        }
    }
}
