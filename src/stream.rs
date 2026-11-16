//! Capture of one application's output stream at a rate and channel count the caller chooses:
//! [`StreamCapture`] records the `Stream/Output/Audio` node named by its `object.serial` as f32
//! samples, interleaved when stereo, and hands them over through a lock-free ring (#8). It is
//! [`crate::MicCapture`]'s shape applied to an application stream, for a caller that captures
//! several applications at once and mixes them itself.
//!
//! What it can be pointed at is the point of the type. Its only target is a serial: it takes
//! no [`crate::CaptureTarget`], has no `monitor_fallback`, and every connection it makes writes
//! `target.object = <serial>` and `stream.capture.sink = false` (see `ConnectProps`), with
//! `node.dont-fallback`, `node.dont-reconnect` and `node.dont-move` set so the session manager
//! neither picks another node when the target is missing nor moves the capture later. It
//! never looks a stream up by name and never re-targets: when the node with that serial goes,
//! the capture stops delivering, its status says the target is gone, and it stays that way.
//! A serial is unique for the node's lifetime, so a new stream is a new capture.
//!
//! The serial is checked against the app streams in the [`crate::NodeTable`] before anything
//! connects (by [`StreamCapture::start`], by the supervisor, and again on the loop thread as it
//! connects), and refused when a source, sink or other stream is *named* like it, because
//! `target.object` matches `node.name` as well as `object.serial`.
//!
//! Three pieces, each on the thread that owns it, as in `mic.rs`:
//!
//! - `SerialFollow`, a pure state machine on the capture's supervisor thread: connect once
//!   when the serial is listed, report the target gone (once, for good) when it is not.
//! - `LoopStream`, the `pw_stream` and its listeners, on the PipeWire loop thread, reached
//!   through [`crate::pipewire::Command`]s. The loop thread also retires it the moment the
//!   registry removes its target.
//! - `stream_process`, the real-time side: it decodes the buffer's f32 frames into a fixed
//!   scratch block and pushes whole frames into the ring. It allocates nothing, locks nothing
//!   and logs nothing; a full ring drops the incoming frames and counts their samples.
//!
//! The format is asked of PipeWire, not computed here: the format pod names the rate and the
//! channel count, and the adapter in front of the stream converts. What PipeWire settles on is
//! checked in `param_changed`, and the real-time callback copies nothing until it matches; a
//! stream that settles on another rate, channel count or sample format is an error that says
//! which ([`StreamError::RateMismatch`], [`StreamError::ChannelMismatch`],
//! [`StreamError::Format`]), never samples in another format.

use std::ffi::{CStr, CString};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use pipewire as pw;
use pw::spa;
use ringbuf::HeapRb;
use ringbuf::traits::{Consumer, Observer, Producer, Split};

use crate::capture::{CaptureState, stop_result, update_stream_props};
use crate::mic::{MIC_MAX_RATE, MIC_MIN_RATE};
use crate::pipewire::{AppStream, Command, NodeTable, PipeWireContext};
use crate::source::AudioError;

/// Lowest rate [`StreamCapture`] accepts, in Hz (the same as [`crate::MicCapture`]).
pub const STREAM_MIN_RATE: u32 = MIC_MIN_RATE;
/// Highest rate [`StreamCapture`] accepts, in Hz (the same as [`crate::MicCapture`]).
pub const STREAM_MAX_RATE: u32 = MIC_MAX_RATE;
/// Most channels [`StreamCapture`] delivers: mono or stereo.
pub const STREAM_MAX_CHANNELS: u32 = 2;
/// Longest ring [`StreamCapture`] accepts, in milliseconds.
pub const STREAM_MAX_RING_MS: u32 = 60_000;
/// A ring length that suits a caller with no reason to pick another, in milliseconds.
pub const DEFAULT_STREAM_RING_MS: u32 = 2_000;
/// `node.name` of every stream capture, as `pw-cli` and patchbays show it.
pub const STREAM_NODE_NAME: &str = "dmn.stream-capture";

/// The stream's format, as `param_changed` last judged it; the same three values and the same
/// reason as in `mic.rs`.
const FORMAT_UNSETTLED: u8 = 0;
const FORMAT_OK: u8 = 1;
const FORMAT_REJECTED: u8 = 2;

/// Which part of a rejected format was wrong, so the error can say which.
const REJECT_NONE: u8 = 0;
const REJECT_RATE: u8 = 1;
const REJECT_CHANNELS: u8 = 2;
const REJECT_SAMPLE_FORMAT: u8 = 3;

/// Frames decoded per pass in the real-time callback; PipeWire's quantum is far below this.
const SCRATCH_FRAMES: usize = 8192;
/// Bytes per f32 sample.
const STRIDE: usize = 4;

type Prod = <HeapRb<f32> as Split>::Prod;
type Cons = <HeapRb<f32> as Split>::Cons;

/// Why a [`StreamCapture`] could not be set up, did not come up, or stopped delivering.
#[derive(Debug, thiserror::Error)]
pub enum StreamError {
    /// The requested rate is outside [`STREAM_MIN_RATE`]..=[`STREAM_MAX_RATE`].
    #[error("unsupported sample rate {rate} Hz (need {STREAM_MIN_RATE} to {STREAM_MAX_RATE})")]
    UnsupportedRate { rate: u32 },
    /// The requested channel count is neither 1 nor 2.
    #[error("unsupported channel count {channels} (need 1 or 2)")]
    UnsupportedChannels { channels: u32 },
    /// The requested ring length is 0 or above [`STREAM_MAX_RING_MS`].
    #[error("unsupported ring length {ms} ms (need 1 to {STREAM_MAX_RING_MS})")]
    InvalidRingLength { ms: u32 },
    /// The registry has not finished its first enumeration, so the serial cannot be checked
    /// yet; see [`PipeWireContext::wait_for_sync`].
    #[error("the PipeWire registry has not finished its first enumeration")]
    NotSynced,
    /// The PipeWire server went away.
    #[error("the PipeWire server went away: {0}")]
    Disconnected(String),
    /// No application output stream (`Stream/Output/Audio`) has this `object.serial`. A sink,
    /// a source or any other node with that serial is not a target either.
    #[error("no application output stream has object.serial {serial}")]
    UnknownSerial { serial: u64 },
    /// A source, sink or other stream has the serial as its `node.name`; `target.object` could
    /// resolve to it, so the capture refuses.
    #[error("object.serial {serial} is also another node's node.name; refusing the target")]
    AmbiguousSerial { serial: u64 },
    /// The stream with this serial was removed; nothing more is delivered, ever.
    #[error("the application stream with object.serial {serial} is gone")]
    TargetGone { serial: u64 },
    /// PipeWire settled on another rate than the one asked for, so nothing is delivered.
    #[error("PipeWire negotiated {negotiated} Hz, {requested} Hz was requested")]
    RateMismatch { requested: u32, negotiated: u32 },
    /// PipeWire settled on another channel count than the one asked for.
    #[error("PipeWire negotiated {negotiated} channels, {requested} were requested")]
    ChannelMismatch { requested: u32, negotiated: u32 },
    /// PipeWire settled on the right rate and channels but not on F32LE samples.
    #[error("{0}")]
    Format(String),
    /// The stream reported an error.
    #[error("stream capture: {0}")]
    Stream(String),
    /// The stream did not reach `Streaming` in time.
    #[error("stream capture still {state} after {ms} ms")]
    NotStreaming { state: &'static str, ms: u64 },
    #[error(transparent)]
    Audio(#[from] AudioError),
}

/// Check the caller's parameters and return the ring's length in samples: `ring_ms` of
/// `channels`-sample frames at `rate` Hz, a whole number of frames.
pub(crate) fn ring_samples(rate: u32, channels: u32, ring_ms: u32) -> Result<usize, StreamError> {
    if !(STREAM_MIN_RATE..=STREAM_MAX_RATE).contains(&rate) {
        return Err(StreamError::UnsupportedRate { rate });
    }
    if !(1..=STREAM_MAX_CHANNELS).contains(&channels) {
        return Err(StreamError::UnsupportedChannels { channels });
    }
    if !(1..=STREAM_MAX_RING_MS).contains(&ring_ms) {
        return Err(StreamError::InvalidRingLength { ms: ring_ms });
    }
    let frames = (u64::from(rate) * u64::from(ring_ms) / 1000).max(1);
    Ok((frames * u64::from(channels)) as usize)
}

/// The app stream `serial` names in `table`, if it can be targeted: it must be a listed
/// `Stream/Output/Audio` node, and no other tracked node may be named like the serial.
pub(crate) fn resolve_serial(table: &NodeTable, serial: u64) -> Result<&AppStream, StreamError> {
    let stream = table
        .stream_by_serial(serial)
        .ok_or(StreamError::UnknownSerial { serial })?;
    if table.node_named(&serial.to_string(), stream.id) {
        return Err(StreamError::AmbiguousSerial { serial });
    }
    Ok(stream)
}

/// Check the settled stream format against what was asked for: `want_rate` Hz,
/// `want_channels` channels, F32LE. The rate is judged first, then the channels, then the
/// sample format.
pub(crate) fn check_format(
    want_rate: u32,
    want_channels: u32,
    rate: u32,
    channels: u32,
    format: spa::param::audio::AudioFormat,
) -> Result<(), StreamError> {
    if rate != want_rate {
        return Err(StreamError::RateMismatch {
            requested: want_rate,
            negotiated: rate,
        });
    }
    if channels != want_channels {
        return Err(StreamError::ChannelMismatch {
            requested: want_channels,
            negotiated: channels,
        });
    }
    if format != spa::param::audio::AudioFormat::F32LE {
        return Err(StreamError::Format(format!(
            "negotiated {format:?} {rate} Hz × {channels} ch, need F32LE {want_rate} Hz × \
             {want_channels} ch"
        )));
    }
    Ok(())
}

/// `SPA_PARAM_EnumFormat` with one fixed choice: F32LE (interleaved), `rate` Hz, `channels`
/// channels positioned MONO or FL FR. Built as `capture::enum_format_pod` builds its mono one.
pub(crate) fn stream_format_pod(rate: u32, channels: u32) -> Result<Vec<u8>, String> {
    let mut position = [0u32; spa::sys::SPA_AUDIO_MAX_CHANNELS as usize];
    match channels {
        1 => position[0] = spa::sys::SPA_AUDIO_CHANNEL_MONO,
        2 => {
            position[0] = spa::sys::SPA_AUDIO_CHANNEL_FL;
            position[1] = spa::sys::SPA_AUDIO_CHANNEL_FR;
        }
        n => return Err(format!("{n} channels: only mono and stereo")),
    }
    let mut info = spa::param::audio::AudioInfoRaw::new();
    info.set_format(spa::param::audio::AudioFormat::F32LE);
    info.set_rate(rate);
    info.set_channels(channels);
    info.set_position(position);
    let obj = spa::pod::Object {
        type_: spa::utils::SpaTypes::ObjectParamFormat.as_raw(),
        id: spa::param::ParamType::EnumFormat.as_raw(),
        properties: info.into(),
    };
    spa::pod::serialize::PodSerializer::serialize(
        std::io::Cursor::new(Vec::new()),
        &spa::pod::Value::Object(obj),
    )
    .map(|(cursor, _)| cursor.into_inner())
    .map_err(|e| format!("EnumFormat pod: {e:?}"))
}

/// The stream properties every connection of a [`StreamCapture`] writes. Built from a serial
/// and nothing else, so there is no input for which it names a sink, a monitor or a name.
pub(crate) struct ConnectProps {
    target: CString,
}

impl ConnectProps {
    pub(crate) fn for_serial(serial: u64) -> Result<Self, String> {
        CString::new(serial.to_string())
            .map(|target| Self { target })
            .map_err(|e| format!("serial {serial}: {e}"))
    }

    /// `target.object` is the serial; `stream.capture.sink` is false; the session manager may
    /// neither fall back to another node nor reconnect nor move the capture.
    pub(crate) fn entries(&self) -> [(&'static CStr, Option<&CStr>); 5] {
        [
            (c"target.object", Some(self.target.as_c_str())),
            (c"stream.capture.sink", Some(c"false")),
            (c"node.dont-fallback", Some(c"true")),
            (c"node.dont-reconnect", Some(c"true")),
            (c"node.dont-move", Some(c"true")),
        ]
    }
}

/// Shared, lock-free view of one stream capture. The real-time callback only touches the
/// atomics.
#[derive(Debug, Default)]
pub struct StreamStatus {
    state: AtomicU8,
    /// The `object.serial` captured.
    serial: AtomicU64,
    requested_rate: AtomicU32,
    requested_channels: AtomicU32,
    /// What PipeWire settled on, 0 until it has.
    negotiated_rate: AtomicU32,
    negotiated_channels: AtomicU32,
    /// `FORMAT_OK` once `param_changed` has seen the format asked for; the callback copies
    /// nothing while it is anything else.
    format: AtomicU8,
    /// Which part of a rejected format was wrong (`REJECT_*`).
    rejection: AtomicU8,
    /// Whether PipeWire last reported the stream as `Streaming`.
    running: AtomicBool,
    /// Set for good when the target was removed (or vanished before it was connected). The
    /// callback copies nothing once it is set.
    target_gone: AtomicBool,
    /// Set for good when a terminal error was reported; shuts the delivery gate and keeps the
    /// error from being hidden by later state events. Cleared only by a new start.
    failed: AtomicBool,
    pushed: AtomicU64,
    dropped: AtomicU64,
    last_error: Mutex<Option<String>>,
}

impl StreamStatus {
    /// Where the capture is in its life; the same states as [`crate::CaptureStatus`].
    /// `Disconnected` means the target is gone (see [`Self::target_gone`]); it is never
    /// followed by a reconnect.
    pub fn state(&self) -> CaptureState {
        CaptureState::from_u8(self.state.load(Ordering::Acquire))
    }

    /// The `object.serial` this captures.
    pub fn serial(&self) -> u64 {
        self.serial.load(Ordering::Relaxed)
    }

    /// True once the target stream was removed: nothing is delivered any more, and nothing
    /// will be. Cleared only by a new [`StreamCapture::start`].
    pub fn target_gone(&self) -> bool {
        self.target_gone.load(Ordering::Acquire)
    }

    /// The rate the caller asked for, in Hz.
    pub fn requested_rate(&self) -> u32 {
        self.requested_rate.load(Ordering::Relaxed)
    }

    /// The channel count the caller asked for.
    pub fn requested_channels(&self) -> u32 {
        self.requested_channels.load(Ordering::Relaxed)
    }

    /// `(rate, channels)` as delivered: the requested ones, once PipeWire has settled on them.
    /// `None` before that, after a mismatch, and once the target is gone.
    pub fn delivered_format(&self) -> Option<(u32, u32)> {
        if !self.delivering() {
            return None;
        }
        self.negotiated_format()
    }

    /// `(rate, channels)` PipeWire settled on, even when not the ones asked for.
    pub fn negotiated_format(&self) -> Option<(u32, u32)> {
        let rate = self.negotiated_rate.load(Ordering::Acquire);
        let channels = self.negotiated_channels.load(Ordering::Acquire);
        (rate != 0).then_some((rate, channels))
    }

    /// Samples (not frames) pushed into the ring since the last [`StreamCapture::start`].
    pub fn pushed(&self) -> u64 {
        self.pushed.load(Ordering::Relaxed)
    }

    /// Samples the ring had no room for and dropped (the newest ones, whole frames at a time),
    /// since the last [`StreamCapture::start`].
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// The error PipeWire or the format check reported on the current connection.
    pub fn last_error(&self) -> Option<String> {
        self.last_error
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// True while the real-time callback copies samples: the format is the one asked for and
    /// the target is still there.
    fn delivering(&self) -> bool {
        self.format.load(Ordering::Acquire) == FORMAT_OK && !self.target_gone() && !self.failed()
    }

    fn set_state(&self, s: CaptureState) {
        self.state.store(s.to_u8(), Ordering::Release);
    }

    pub(crate) fn set_error(&self, why: String) {
        *self.last_error.lock().unwrap_or_else(|e| e.into_inner()) = Some(why);
        self.set_state(CaptureState::Error);
    }

    /// A terminal error from the stream, the supervisor or a failed connect or refusal. The
    /// first terminal outcome wins: once the target is gone, or an error already stands, later
    /// events (the session manager failing the orphaned stream, a late state change, a later
    /// removal) do not replace it. The delivery gate shuts for good: nothing is delivered
    /// until a new [`StreamCapture::start`]. A format rejection already standing keeps its
    /// own reason.
    pub(crate) fn stream_error(&self, why: String) {
        if self.target_gone() || self.failed() {
            return;
        }
        self.failed.store(true, Ordering::Release);
        if self.state() != CaptureState::Error {
            self.set_error(why);
        }
    }

    /// An error that can pass: PipeWire settled on a format other than the one asked for. The
    /// gate is shut while it stands and a later good format reopens it.
    fn reject_error(&self, why: String) {
        if !self.target_gone() && !self.failed() {
            self.set_error(why);
        }
    }

    /// True once a terminal error ([`Self::stream_error`]) stands.
    fn failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }

    fn clear_error(&self) {
        *self.last_error.lock().unwrap_or_else(|e| e.into_inner()) = None;
    }

    /// A new capture begins: counters, the last error and the target-gone mark start over.
    fn restart(&self) {
        self.pushed.store(0, Ordering::Relaxed);
        self.dropped.store(0, Ordering::Relaxed);
        self.target_gone.store(false, Ordering::Release);
        self.failed.store(false, Ordering::Release);
        self.clear_error();
        self.connection_reset();
        // A previous run's `Error` or `Disconnected` must not be mistaken for this one's.
        self.set_state(CaptureState::Connecting);
    }

    /// The stream is being connected or unlinked: nothing is delivered until a format settles.
    fn connection_reset(&self) {
        self.format.store(FORMAT_UNSETTLED, Ordering::Release);
        self.rejection.store(REJECT_NONE, Ordering::Release);
        self.negotiated_rate.store(0, Ordering::Release);
        self.negotiated_channels.store(0, Ordering::Release);
        self.running.store(false, Ordering::Release);
    }

    /// PipeWire withdrew the format. The gate shuts; a rejection stands with its reason.
    fn format_cleared(&self) {
        if self.format.load(Ordering::Acquire) != FORMAT_REJECTED {
            self.format.store(FORMAT_UNSETTLED, Ordering::Release);
            self.negotiated_rate.store(0, Ordering::Release);
            self.negotiated_channels.store(0, Ordering::Release);
        }
    }

    /// PipeWire settled on a format. The gate is set before anything is reported, so no buffer
    /// of the wrong shape is ever copied into the ring.
    fn format_settled(&self, rate: u32, channels: u32, format: spa::param::audio::AudioFormat) {
        self.negotiated_rate.store(rate, Ordering::Release);
        self.negotiated_channels.store(channels, Ordering::Release);
        match check_format(
            self.requested_rate(),
            self.requested_channels(),
            rate,
            channels,
            format,
        ) {
            Ok(()) => {
                self.rejection.store(REJECT_NONE, Ordering::Release);
                let was = self.format.swap(FORMAT_OK, Ordering::AcqRel);
                if was == FORMAT_REJECTED && !self.target_gone() && !self.failed() {
                    // Renegotiated on the same stream; no state event comes with that.
                    self.clear_error();
                    self.set_state(if self.running.load(Ordering::Acquire) {
                        CaptureState::Streaming
                    } else {
                        CaptureState::Connecting
                    });
                }
            }
            Err(e) => {
                let kind = match e {
                    StreamError::RateMismatch { .. } => REJECT_RATE,
                    StreamError::ChannelMismatch { .. } => REJECT_CHANNELS,
                    _ => REJECT_SAMPLE_FORMAT,
                };
                self.rejection.store(kind, Ordering::Release);
                self.format.store(FORMAT_REJECTED, Ordering::Release);
                self.reject_error(e.to_string());
            }
        }
    }

    /// The stream reached `Streaming`. A rejected format or a terminal error keeps it in
    /// `Error`; a gone target keeps it `Disconnected`.
    fn stream_streaming(&self) {
        self.running.store(true, Ordering::Release);
        if !self.target_gone()
            && !self.failed()
            && self.state() != CaptureState::Disconnected
            && self.format.load(Ordering::Acquire) != FORMAT_REJECTED
        {
            self.set_state(CaptureState::Streaming);
        }
    }

    /// The stream is connecting or paused; that hides neither a rejection nor a gone target.
    fn stream_waiting(&self) {
        self.running.store(false, Ordering::Release);
        if !self.target_gone()
            && !self.failed()
            && self.state() != CaptureState::Disconnected
            && self.format.load(Ordering::Acquire) != FORMAT_REJECTED
        {
            self.set_state(CaptureState::Connecting);
        }
    }

    /// The target is gone, for good: the gate shuts at once and the state says so. A terminal
    /// error that already stands is kept as the reason instead.
    fn mark_gone(&self) {
        if self.failed() {
            return;
        }
        self.target_gone.store(true, Ordering::Release);
        self.set_state(CaptureState::Disconnected);
    }

    /// The loop thread retired the stream (unlinked it for good). A terminal error or a
    /// rejection that stands is not overwritten with `Disconnected`.
    fn retired(&self) {
        if self.state() != CaptureState::Error {
            self.set_state(CaptureState::Disconnected);
        }
    }

    /// Why nothing is being delivered, when the reason is final and not a wait.
    fn failure(&self) -> Option<StreamError> {
        if self.target_gone() {
            return Some(StreamError::TargetGone {
                serial: self.serial(),
            });
        }
        if self.state() != CaptureState::Error {
            return None;
        }
        let why = self.last_error().unwrap_or_default();
        if self.format.load(Ordering::Acquire) != FORMAT_REJECTED {
            return Some(StreamError::Stream(why));
        }
        Some(match self.rejection.load(Ordering::Acquire) {
            REJECT_RATE => StreamError::RateMismatch {
                requested: self.requested_rate(),
                negotiated: self.negotiated_rate.load(Ordering::Acquire),
            },
            REJECT_CHANNELS => StreamError::ChannelMismatch {
                requested: self.requested_channels(),
                negotiated: self.negotiated_channels.load(Ordering::Acquire),
            },
            _ => StreamError::Format(why),
        })
    }
}

/// The ring's producer end, held by the capture callback.
pub(crate) struct StreamWriter {
    prod: Prod,
    channels: usize,
    status: Arc<StreamStatus>,
}

impl StreamWriter {
    /// Push as many whole frames of `samples` as fit and return how many samples that was; the
    /// rest are dropped and counted. `samples` holds whole frames. Real-time safe: no
    /// allocation, no lock, no logging, bounded time.
    pub(crate) fn push(&mut self, samples: &[f32]) -> usize {
        let room = self.prod.vacant_len() / self.channels * self.channels;
        let take = samples.len().min(room);
        let n = self.prod.push_slice(&samples[..take]);
        self.status.pushed.fetch_add(n as u64, Ordering::Relaxed);
        if n < samples.len() {
            self.status
                .dropped
                .fetch_add((samples.len() - n) as u64, Ordering::Relaxed);
        }
        n
    }
}

/// The ring's consumer end: the caller reads the stream's samples here, interleaved when
/// stereo (left, right, left, …). Owned by one thread; the capture callback is the only
/// producer.
pub struct StreamReader {
    cons: Cons,
    channels: usize,
    rate: u32,
    status: Arc<StreamStatus>,
}

impl StreamReader {
    /// Move whole frames into `out`, as many as are queued and fit, and return how many
    /// samples that was (a multiple of [`Self::channels`]). Never blocks; an empty ring pops 0.
    pub fn pop(&mut self, out: &mut [f32]) -> usize {
        let want = out.len() / self.channels * self.channels;
        let queued = self.cons.occupied_len() / self.channels * self.channels;
        let n = want.min(queued);
        self.cons.pop_slice(&mut out[..n])
    }

    /// Samples queued.
    pub fn queued(&self) -> usize {
        self.cons.occupied_len()
    }

    /// Samples the ring holds at most.
    pub fn capacity(&self) -> usize {
        self.cons.capacity().get()
    }

    /// Samples per frame: 1 (mono) or 2 (stereo, interleaved left then right).
    pub fn channels(&self) -> u32 {
        self.channels as u32
    }

    /// The rate the samples are delivered at, in Hz.
    pub fn rate(&self) -> u32 {
        self.rate
    }

    /// The capture's status, as [`StreamCapture::status`] returns it.
    pub fn status(&self) -> Arc<StreamStatus> {
        Arc::clone(&self.status)
    }
}

/// Build a ring of `capacity` samples (rounded up to a whole frame) and split it.
pub(crate) fn stream_ring(
    capacity: usize,
    channels: u32,
    status: Arc<StreamStatus>,
) -> (StreamWriter, StreamReader) {
    let ch = (channels as usize).max(1);
    let capacity = capacity.max(1).div_ceil(ch) * ch;
    let (prod, cons) = HeapRb::<f32>::new(capacity).split();
    let rate = status.requested_rate();
    (
        StreamWriter {
            prod,
            channels: ch,
            status: Arc::clone(&status),
        },
        StreamReader {
            cons,
            channels: ch,
            rate,
            status,
        },
    )
}

/// What the loop thread needs to create the stream. Serial, rate and channels travel in
/// `status`.
pub(crate) struct StreamSpec {
    pub(crate) status: Arc<StreamStatus>,
}

/// The real-time side's state: the ring's producer and a fixed block the bytes are decoded
/// into.
struct Inlet {
    writer: StreamWriter,
    scratch: Box<[f32]>,
    status: Arc<StreamStatus>,
}

/// Decode little-endian f32 frames from `bytes` through `scratch` into the ring, whole frames
/// per pass so a frame is never split. A trailing partial frame is ignored. No allocation, no
/// lock, bounded by `bytes.len()`.
fn push_frames_le(writer: &mut StreamWriter, scratch: &mut [f32], bytes: &[u8]) {
    let channels = writer.channels;
    let frame_bytes = STRIDE * channels;
    let frames_per_pass = scratch.len() / channels;
    if frames_per_pass == 0 {
        return;
    }
    let whole = bytes.len() / frame_bytes * frame_bytes;
    for pass in bytes[..whole].chunks(frames_per_pass * frame_bytes) {
        let mut n = 0;
        for (slot, c) in scratch.iter_mut().zip(pass.chunks_exact(STRIDE)) {
            *slot = f32::from_le_bytes([c[0], c[1], c[2], c[3]]);
            n += 1;
        }
        writer.push(&scratch[..n]);
    }
}

/// The callback's copy step: nothing unless the gate is open (format as asked, target there).
fn deliver(inlet: &mut Inlet, bytes: &[u8]) {
    if !inlet.status.delivering() {
        return;
    }
    push_frames_le(&mut inlet.writer, &mut inlet.scratch, bytes);
}

/// REAL-TIME CALLBACK (runs on PipeWire's data thread; `RT_PROCESS`).
/// Rules (CLAUDE.md, SPEC § Threading rules), reviewed call by call:
/// - no allocation: `dequeue_buffer` wraps a pointer, `datas_mut`/`chunk`/`data` borrow the
///   mapped buffer, the frames are decoded into the preallocated `scratch` and pushed into
///   the preallocated ring;
/// - no locks: the ring is lock-free SPSC, counters and the gate are relaxed or
///   acquire/release atomics;
/// - no logging, no syscalls, bounded time (one pass over the buffer).
///
/// Dequeue, copy, requeue (the `Buffer` guard requeues on drop).
fn stream_process(stream: &pw::stream::Stream, inlet: &mut Inlet) {
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
    deliver(inlet, &bytes[start..end]);
}

/// The stream and its two listeners, owned by the PipeWire loop thread, as `LoopMic` is.
pub(crate) struct LoopStream {
    // Field order is drop order: listeners before the stream they are attached to.
    _process: pw::stream::StreamListener<Inlet>,
    _events: pw::stream::StreamListener<()>,
    stream: pw::stream::StreamRc,
    status: Arc<StreamStatus>,
    connected: bool,
    /// Set once the target is gone: this stream is never connected again.
    retired: bool,
}

impl LoopStream {
    /// Create the stream (not connected yet) on the loop thread.
    pub(crate) fn open(
        core: &pw::core::CoreRc,
        spec: StreamSpec,
        writer: StreamWriter,
    ) -> Result<Self, String> {
        let mut props = pw::properties::PropertiesBox::new();
        props.insert(*pw::keys::MEDIA_TYPE, "Audio");
        props.insert(*pw::keys::MEDIA_CATEGORY, "Capture");
        props.insert(*pw::keys::NODE_NAME, STREAM_NODE_NAME);
        // From the start, not only once connected: never the sink monitor, never a fallback,
        // never moved.
        props.insert("stream.capture.sink", "false");
        props.insert("node.dont-fallback", "true");
        props.insert(*pw::keys::NODE_DONT_RECONNECT, "true");
        props.insert("node.dont-move", "true");
        // A 10 ms quantum.
        let rate = spec.status.requested_rate();
        let latency = format!("{}/{}", rate / 100, rate);
        props.insert(*pw::keys::NODE_LATENCY, latency.as_str());
        let stream = pw::stream::StreamRc::new(core.clone(), STREAM_NODE_NAME, props)
            .map_err(|e| e.to_string())?;

        let channels = spec.status.requested_channels().max(1) as usize;
        let inlet = Inlet {
            writer,
            scratch: vec![0.0; SCRATCH_FRAMES * channels].into_boxed_slice(),
            status: Arc::clone(&spec.status),
        };
        let process_listener = stream
            .add_local_listener_with_user_data(inlet)
            .process(stream_process)
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
                pw::stream::StreamState::Error(why) => state_status.stream_error(why),
                pw::stream::StreamState::Unconnected => {}
            })
            .param_changed(move |_, _, id, param| {
                if id != spa::param::ParamType::Format.as_raw() {
                    return;
                }
                let Some(param) = param else {
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
            retired: false,
        })
    }

    /// Connect to the app stream with `serial`. A retired stream (its target gone) is never
    /// connected again.
    pub(crate) fn connect(&mut self, serial: u64) {
        if self.retired || self.status.target_gone() || self.status.failed() {
            return;
        }
        if let Err(e) = self.try_connect(serial) {
            self.status.stream_error(e);
            self.disconnect();
        }
    }

    /// The loop thread re-checked `serial` with [`resolve_serial`] right before connecting and
    /// it no longer resolves: fail closed, never connect. A serial that is no longer an app
    /// stream means the target was removed (gone, for good); any other refusal (an ambiguous
    /// name appeared meanwhile) is an error with its own message. Either way the stream is
    /// retired and nothing else is targeted.
    pub(crate) fn refuse(&mut self, why: StreamError) {
        match why {
            StreamError::UnknownSerial { .. } => self.target_removed(self.status.serial()),
            other => {
                self.status.stream_error(other.to_string());
                self.disconnect();
            }
        }
    }

    fn try_connect(&mut self, serial: u64) -> Result<(), String> {
        self.unlink();
        self.status.clear_error();
        let props = ConnectProps::for_serial(serial)?;
        update_stream_props(&self.stream, &props.entries())?;
        let pod = stream_format_pod(
            self.status.requested_rate(),
            self.status.requested_channels(),
        )?;
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

    /// The capture is over (the supervisor's `DisconnectStream`): unlink for good. The state
    /// becomes `Disconnected` unless an error stands: a refusal, a lost server or a format
    /// rejection keeps its reason for [`StreamCapture::wait_streaming`].
    pub(crate) fn disconnect(&mut self) {
        self.retired = true;
        self.unlink();
        self.status.retired();
    }

    /// The registry removed the app stream with `serial`. If that is this capture's target,
    /// shut the gate, say so, and unlink for good.
    pub(crate) fn target_removed(&mut self, serial: u64) {
        if self.status.serial() == serial {
            self.status.mark_gone();
            self.disconnect();
        }
    }

    fn unlink(&mut self) {
        self.status.connection_reset();
        if self.connected {
            // After this returns the data thread no longer calls `stream_process`.
            let _ = self.stream.disconnect();
            self.connected = false;
        }
    }
}

impl Drop for LoopStream {
    fn drop(&mut self) {
        // Destroying the stream (the fields drop after this) removes the node from the graph.
        self.unlink();
        self.status.set_state(CaptureState::Idle);
    }
}

/// One decision from `SerialFollow::step`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FollowAction {
    /// Connect to the serial (only ever once).
    Connect,
    /// The target is gone, or was gone before it could be connected. Final.
    Gone,
    /// The PipeWire server went away. Final.
    ServerLost(String),
    /// The serial cannot be targeted safely. Final.
    Refuse(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Followed {
    /// Nothing decided yet.
    Nothing,
    /// Connected to the node with this registry id.
    Bound(u32),
    /// Over: nothing will ever be connected again.
    Done,
}

/// Decides, from registry snapshots, whether the capture connects or its target is gone.
/// Pure: no PipeWire, no threads. It never re-targets: there is one serial, connected at most
/// once, and once it is gone every later step is `None`.
#[derive(Debug, Clone)]
pub(crate) struct SerialFollow {
    serial: u64,
    state: Followed,
}

impl SerialFollow {
    pub(crate) fn new(serial: u64) -> Self {
        Self {
            serial,
            state: Followed::Nothing,
        }
    }

    /// True once nothing more can happen.
    pub(crate) fn finished(&self) -> bool {
        self.state == Followed::Done
    }

    pub(crate) fn step(&mut self, table: &NodeTable) -> Option<FollowAction> {
        if self.state == Followed::Done {
            return None;
        }
        if let Some(why) = &table.disconnected {
            self.state = Followed::Done;
            return Some(FollowAction::ServerLost(why.clone()));
        }
        if !table.synced {
            return None;
        }
        match self.state {
            Followed::Nothing => match resolve_serial(table, self.serial) {
                Ok(stream) => {
                    self.state = Followed::Bound(stream.id);
                    Some(FollowAction::Connect)
                }
                Err(e @ StreamError::AmbiguousSerial { .. }) => {
                    self.state = Followed::Done;
                    Some(FollowAction::Refuse(e.to_string()))
                }
                Err(_) => {
                    self.state = Followed::Done;
                    Some(FollowAction::Gone)
                }
            },
            Followed::Bound(id) => match table.stream_by_serial(self.serial) {
                Some(s) if s.id == id => None,
                // Removed, or (impossible while the node lives) another node with the serial.
                _ => {
                    self.state = Followed::Done;
                    Some(FollowAction::Gone)
                }
            },
            Followed::Done => None,
        }
    }
}

/// How often the supervisor re-checks when nothing changed (also its stop latency).
const SUPERVISE_TICK: Duration = Duration::from_millis(100);
/// How long `stop` waits for the loop thread to close the stream.
const STOP_TIMEOUT: Duration = Duration::from_secs(1);

/// One application output stream (`Stream/Output/Audio`), named by its `object.serial`,
/// captured as f32 at a rate and channel count the caller chooses (interleaved when stereo),
/// into a lock-free ring. It cannot capture a monitor or a sink, never re-targets, and stops
/// for good when its stream goes; see the module documentation.
///
/// Several can run at once on one [`PipeWireContext`], each with its own stream, ring and
/// supervisor; starting or stopping one does not touch the others.
///
/// ```no_run
/// use std::sync::Arc;
/// use std::time::Duration;
/// use dmn_audio::{PipeWireContext, StreamCapture};
///
/// let ctx = Arc::new(PipeWireContext::connect()?);
/// let table = ctx.wait_for_sync(Duration::from_secs(2));
/// let Some(serial) = table
///     .streams_with_metadata()
///     .find(|(_, m)| m.application_name.as_deref() == Some("Firefox"))
///     .and_then(|(_, m)| m.serial)
/// else {
///     return Ok(());
/// };
/// let mut capture = StreamCapture::new(Arc::clone(&ctx), serial, 48_000, 2, 2_000)?;
/// let mut reader = capture.start()?;
/// capture.wait_streaming(Duration::from_secs(3))?;
/// let mut block = [0.0f32; 1920]; // 20 ms of 48 kHz stereo
/// let got = reader.pop(&mut block); // whole frames, left then right
/// # let _ = got;
/// capture.stop()?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct StreamCapture {
    ctx: Arc<PipeWireContext>,
    serial: u64,
    rate: u32,
    channels: u32,
    ring_samples: usize,
    status: Arc<StreamStatus>,
    key: Option<u64>,
    stop: Arc<AtomicBool>,
    supervisor: Option<JoinHandle<()>>,
}

impl StreamCapture {
    /// A capture of the app stream whose `object.serial` is `serial`, at `rate` Hz with
    /// `channels` channels (1 or 2), into a ring that holds `ring_ms` milliseconds. Nothing is
    /// opened, and the serial not yet checked, until [`Self::start`].
    pub fn new(
        ctx: Arc<PipeWireContext>,
        serial: u64,
        rate: u32,
        channels: u32,
        ring_ms: u32,
    ) -> Result<Self, StreamError> {
        let ring_samples = ring_samples(rate, channels, ring_ms)?;
        let status = Arc::new(StreamStatus::default());
        status.serial.store(serial, Ordering::Relaxed);
        status.requested_rate.store(rate, Ordering::Relaxed);
        status.requested_channels.store(channels, Ordering::Relaxed);
        Ok(Self {
            ctx,
            serial,
            rate,
            channels,
            ring_samples,
            status,
            key: None,
            stop: Arc::new(AtomicBool::new(false)),
            supervisor: None,
        })
    }

    /// Shared status: state, delivered format, target gone, dropped samples, last error.
    pub fn status(&self) -> Arc<StreamStatus> {
        Arc::clone(&self.status)
    }

    /// The `object.serial` this captures.
    pub fn serial(&self) -> u64 {
        self.serial
    }

    /// The rate samples are delivered at, in Hz.
    pub fn rate(&self) -> u32 {
        self.rate
    }

    /// Samples per frame: 1 or 2.
    pub fn channels(&self) -> u32 {
        self.channels
    }

    /// Check the serial against the app streams listed now, create the stream and connect it
    /// to that serial. Returns the ring's reader at once; the stream connects in the
    /// background, see [`Self::wait_streaming`].
    ///
    /// Fails with [`StreamError::NotSynced`] before the first enumeration,
    /// [`StreamError::UnknownSerial`] when no app stream has the serial (a sink's or source's
    /// serial included), [`StreamError::AmbiguousSerial`] when a node is named like it, and
    /// [`StreamError::Disconnected`] when the server is gone. Calling it while started is an
    /// error. After [`Self::stop`] it may be called again: a new capture with a new ring.
    pub fn start(&mut self) -> Result<StreamReader, StreamError> {
        if self.key.is_some() {
            return Err(AudioError::AlreadyStarted.into());
        }
        let table = self.ctx.nodes();
        if let Some(why) = &table.disconnected {
            return Err(StreamError::Disconnected(why.clone()));
        }
        if !table.synced {
            return Err(StreamError::NotSynced);
        }
        resolve_serial(&table, self.serial)?;

        self.status.restart();
        let (writer, reader) =
            stream_ring(self.ring_samples, self.channels, Arc::clone(&self.status));
        let key = self.ctx.next_key();
        self.ctx
            .send(Command::OpenStream {
                key,
                spec: StreamSpec {
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
        let serial = self.serial;
        let mut follow = SerialFollow::new(serial);
        let handle = std::thread::Builder::new()
            .name("dmn-capture-stream".into())
            .spawn(move || {
                let mut table = ctx.nodes();
                while !stop.load(Ordering::Acquire) {
                    if let Some(action) = follow.step(&table) {
                        let cmd = match action {
                            FollowAction::Connect => Command::ConnectStream { key, serial },
                            FollowAction::Gone => {
                                status.mark_gone();
                                Command::DisconnectStream { key }
                            }
                            FollowAction::ServerLost(why) => {
                                status.stream_error(format!("PipeWire server went away: {why}"));
                                Command::DisconnectStream { key }
                            }
                            FollowAction::Refuse(why) => {
                                status.stream_error(why);
                                Command::DisconnectStream { key }
                            }
                        };
                        if ctx.send(cmd).is_err() {
                            status.stream_error("PipeWire loop thread is gone".into());
                            break;
                        }
                    }
                    if follow.finished() {
                        // Nothing will ever be connected again; `stop` closes the stream.
                        break;
                    }
                    table = ctx.wait_for_change(table.generation, SUPERVISE_TICK);
                }
            })
            .map_err(|e| AudioError::Spawn(e.to_string()))?;
        self.supervisor = Some(handle);
        Ok(reader)
    }

    /// Block until the stream delivers the requested format, or fail with the reason:
    /// [`StreamError::TargetGone`] when the stream went, [`StreamError::RateMismatch`],
    /// [`StreamError::ChannelMismatch`] or [`StreamError::Format`] when PipeWire settled on
    /// something else, [`StreamError::Stream`] for any other error,
    /// [`StreamError::NotStreaming`] on timeout.
    pub fn wait_streaming(&self, timeout: Duration) -> Result<(), StreamError> {
        let t = Instant::now();
        loop {
            if let Some(why) = self.status.failure() {
                return Err(why);
            }
            let state = self.status.state();
            if state == CaptureState::Streaming && self.status.delivered_format().is_some() {
                return Ok(());
            }
            if t.elapsed() >= timeout {
                return Err(StreamError::NotStreaming {
                    state: state.label(),
                    ms: timeout.as_millis() as u64,
                });
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// Stop the supervisor, close the stream and wait (at most a second) for the loop thread
    /// to remove its node from the graph. After it returns nothing writes into the ring.
    /// Idempotent.
    pub fn stop(&mut self) -> Result<(), AudioError> {
        self.stop.store(true, Ordering::Release);
        let panicked = self
            .supervisor
            .take()
            .map(|h| h.join().is_err())
            .unwrap_or(false);
        let mut final_state = None;
        if let Some(key) = self.key.take()
            && self.ctx.send(Command::CloseStream { key }).is_ok()
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

impl Drop for StreamCapture {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipewire::{REGISTRY_CLIENT_KEY, RegistryEvent};
    use spa::param::audio::AudioFormat;

    fn status(rate: u32, channels: u32) -> Arc<StreamStatus> {
        let st = Arc::new(StreamStatus::default());
        st.serial.store(150, Ordering::Relaxed);
        st.requested_rate.store(rate, Ordering::Relaxed);
        st.requested_channels.store(channels, Ordering::Relaxed);
        st
    }

    /// The callback's state over a ring of `capacity` samples, with a scratch block of
    /// `scratch_frames` frames, and the ring's reader.
    fn make_inlet(
        st: &Arc<StreamStatus>,
        capacity: usize,
        scratch_frames: usize,
    ) -> (Inlet, StreamReader) {
        let channels = st.requested_channels() as usize;
        let (writer, reader) = stream_ring(capacity, channels as u32, Arc::clone(st));
        let scratch = vec![0.0; scratch_frames * channels].into_boxed_slice();
        let status = Arc::clone(st);
        let inlet = Inlet {
            writer,
            scratch,
            status,
        };
        (inlet, reader)
    }

    /// What kind of format error `e` is.
    fn kind(e: &StreamError) -> &'static str {
        match e {
            StreamError::RateMismatch { .. } => "rate",
            StreamError::ChannelMismatch { .. } => "channels",
            StreamError::Format(_) => "format",
            _ => "other",
        }
    }

    fn le(samples: &[f32]) -> Vec<u8> {
        samples.iter().flat_map(|s| s.to_le_bytes()).collect()
    }

    fn props(p: &[(&str, &str)]) -> Vec<(String, String)> {
        p.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn app(t: &mut NodeTable, id: u32, application: &str, serial: &str) {
        t.apply(RegistryEvent::Added {
            id,
            props: props(&[
                ("media.class", "Stream/Output/Audio"),
                ("application.name", application),
                ("node.name", application),
                ("object.serial", serial),
            ]),
        });
    }

    fn synced() -> NodeTable {
        let mut t = NodeTable::default();
        t.apply(RegistryEvent::Synced);
        t
    }

    #[test]
    fn the_connect_properties_name_the_serial_and_never_a_sink_or_monitor() {
        for serial in [0, 1, 42, 4_294_967_296, u64::MAX] {
            let p = ConnectProps::for_serial(serial).unwrap();
            let pairs = p.entries();
            let entries: Vec<(&str, Option<&str>)> = pairs
                .iter()
                .map(|(k, v)| (k.to_str().unwrap(), v.map(|v| v.to_str().unwrap())))
                .collect();
            let get = |key: &str| {
                entries
                    .iter()
                    .find(|(k, _)| *k == key)
                    .and_then(|(_, v)| *v)
            };
            let target = get("target.object").expect("a target");
            assert_eq!(target, serial.to_string(), "the target is the serial");
            assert!(target.bytes().all(|b| b.is_ascii_digit()), "{target}");
            assert_eq!(get("stream.capture.sink"), Some("false"));
            for key in [
                "node.dont-fallback",
                "node.dont-reconnect",
                "node.dont-move",
            ] {
                assert_eq!(get(key), Some("true"), "{key}");
            }
            // Nothing else is set, and nothing names a monitor or a sink.
            let keys: Vec<&str> = entries.iter().map(|(k, _)| *k).collect();
            let want = [
                "target.object",
                "stream.capture.sink",
                "node.dont-fallback",
                "node.dont-reconnect",
                "node.dont-move",
            ];
            assert_eq!(keys, want);
            for (k, v) in &entries {
                let v = v.unwrap_or_default();
                assert!(!v.contains("monitor") && !v.contains("sink"), "{k}={v}");
            }
        }
    }

    #[test]
    fn the_format_pod_asks_for_the_requested_rate_and_channel_count() {
        for (rate, channels) in [(48_000, 2), (48_000, 1), (16_000, 1), (44_100, 2)] {
            let bytes = stream_format_pod(rate, channels).unwrap();
            let pod = spa::pod::Pod::from_bytes(&bytes).expect("a pod");
            let mut info = spa::param::audio::AudioInfoRaw::new();
            assert!(info.parse(pod).is_ok(), "{rate} Hz × {channels}");
            assert_eq!(info.rate(), rate);
            assert_eq!(info.channels(), channels);
            assert_eq!(info.format(), AudioFormat::F32LE);
        }
        assert!(stream_format_pod(48_000, 3).is_err());
        assert!(stream_format_pod(48_000, 0).is_err());
    }

    #[test]
    fn the_parameters_are_checked_and_the_ring_holds_whole_frames() {
        assert_eq!(ring_samples(48_000, 2, 2_000).unwrap(), 192_000);
        assert_eq!(ring_samples(16_000, 1, 1_000).unwrap(), 16_000);
        assert_eq!(ring_samples(8_000, 2, 1).unwrap(), 16, "8 frames of 2");
        for bad in [0, 7_999, 192_001] {
            assert!(matches!(
                ring_samples(bad, 2, 1_000),
                Err(StreamError::UnsupportedRate { rate }) if rate == bad
            ));
        }
        for bad in [0, 3, 8] {
            assert!(matches!(
                ring_samples(48_000, bad, 1_000),
                Err(StreamError::UnsupportedChannels { channels }) if channels == bad
            ));
        }
        for bad in [0, STREAM_MAX_RING_MS + 1] {
            assert!(matches!(
                ring_samples(48_000, 2, bad),
                Err(StreamError::InvalidRingLength { ms }) if ms == bad
            ));
        }
        // An odd capacity is rounded up to a whole stereo frame.
        let (_w, r) = stream_ring(7, 2, status(48_000, 2));
        assert_eq!(r.capacity(), 8);
        assert_eq!((r.channels(), r.rate()), (2, 48_000));
    }

    #[test]
    fn only_the_requested_rate_channels_and_f32_open_the_gate() {
        assert!(check_format(48_000, 2, 48_000, 2, AudioFormat::F32LE).is_ok());
        assert!(check_format(16_000, 1, 16_000, 1, AudioFormat::F32LE).is_ok());
        assert!(matches!(
            check_format(48_000, 2, 44_100, 2, AudioFormat::F32LE),
            Err(StreamError::RateMismatch {
                requested: 48_000,
                negotiated: 44_100
            })
        ));
        assert!(matches!(
            check_format(48_000, 2, 48_000, 1, AudioFormat::F32LE),
            Err(StreamError::ChannelMismatch {
                requested: 2,
                negotiated: 1
            })
        ));
        assert!(matches!(
            check_format(48_000, 1, 48_000, 2, AudioFormat::F32LE),
            Err(StreamError::ChannelMismatch {
                requested: 1,
                negotiated: 2
            })
        ));
        assert!(matches!(
            check_format(48_000, 2, 48_000, 2, AudioFormat::S16LE),
            Err(StreamError::Format(why)) if why.contains("need F32LE 48000 Hz × 2 ch")
        ));
    }

    /// A stream that settles on another format delivers nothing, says which part was wrong,
    /// stays in error when it goes on to `Streaming`, and the next good format delivers.
    #[test]
    fn a_mismatched_format_delivers_zero_samples_and_says_which() {
        let frame = le(&[0.25, -0.25]);
        for (rate, channels, format, want, says) in [
            (44_100, 2, AudioFormat::F32LE, "rate", "44100 Hz, 48000"),
            (48_000, 1, AudioFormat::F32LE, "channels", "1 channels"),
            (48_000, 2, AudioFormat::S16LE, "format", "need F32LE"),
        ] {
            let st = status(48_000, 2);
            let (mut inlet, mut reader) = make_inlet(&st, 64, 4);
            st.set_state(CaptureState::Connecting);
            // Nothing settled yet: nothing delivered.
            deliver(&mut inlet, &frame);
            st.format_settled(rate, channels, format);
            st.stream_waiting();
            st.stream_streaming();
            deliver(&mut inlet, &frame);
            assert_eq!(st.state(), CaptureState::Error, "{want}");
            assert_eq!(st.pushed(), 0, "{want}: zero samples");
            assert_eq!(reader.pop(&mut [0.0; 8]), 0, "{want}");
            assert_eq!(st.delivered_format(), None);
            assert_eq!(st.negotiated_format(), Some((rate, channels)));
            let failure = st.failure().expect(want);
            assert_eq!(kind(&failure), want);
            assert!(failure.to_string().contains(says), "{failure}");
            // The reason survives the format being withdrawn on the way down.
            st.format_cleared();
            assert_eq!(st.failure().as_ref().map(kind), Some(want));
            // The same stream renegotiated to what was asked for delivers.
            st.format_settled(48_000, 2, AudioFormat::F32LE);
            assert_eq!(st.state(), CaptureState::Streaming, "{want}");
            assert_eq!(st.delivered_format(), Some((48_000, 2)));
            deliver(&mut inlet, &frame);
            assert_eq!(reader.pop(&mut [0.0; 8]), 2);
            assert!(st.failure().is_none());
        }
    }

    #[test]
    fn stereo_decodes_in_order_across_scratch_passes_and_ignores_a_partial_tail() {
        let st = status(48_000, 2);
        // A scratch block of 2 frames, so 7 frames take 4 passes.
        let (mut inlet, mut reader) = make_inlet(&st, 64, 2);
        st.format_settled(48_000, 2, AudioFormat::F32LE);
        let frames: Vec<f32> = (0..14)
            .map(|i| if i % 2 == 0 { i as f32 } else { -(i as f32) })
            .collect();
        let mut bytes = le(&frames);
        // A partial frame: one whole left sample and two stray bytes. None of it is delivered.
        bytes.extend_from_slice(&99.0f32.to_le_bytes());
        bytes.extend_from_slice(&[0xAA, 0xBB]);
        deliver(&mut inlet, &bytes);
        let mut out = [0.0; 32];
        assert_eq!(reader.pop(&mut out), 14);
        assert_eq!(&out[..14], &frames[..], "left, right, left, … in order");
        assert_eq!((st.pushed(), st.dropped()), (14, 0));
        // Mono: a trailing partial sample is ignored too.
        let st = status(16_000, 1);
        let (mut inlet, mut reader) = make_inlet(&st, 64, 3);
        st.format_settled(16_000, 1, AudioFormat::F32LE);
        let samples: Vec<f32> = (0..10).map(|i| i as f32 * 0.5).collect();
        let mut bytes = le(&samples);
        bytes.extend_from_slice(&[0x01, 0x02, 0x03]);
        deliver(&mut inlet, &bytes);
        assert_eq!(reader.pop(&mut out), 10);
        assert_eq!(&out[..10], &samples[..]);
    }

    #[test]
    fn a_full_ring_drops_whole_frames_and_counts_them_and_an_empty_one_pops_zero() {
        let st = status(48_000, 2);
        let (mut w, mut r) = stream_ring(6, 2, Arc::clone(&st));
        let mut out = [9.0f32; 8];
        assert_eq!(r.pop(&mut out), 0, "empty");
        assert_eq!(out, [9.0; 8], "untouched");
        assert_eq!(w.push(&[1.0, -1.0, 2.0, -2.0]), 4);
        assert_eq!(w.push(&[3.0, -3.0, 4.0, -4.0]), 2, "one frame fits");
        assert_eq!((st.pushed(), st.dropped()), (6, 2));
        // The reader takes whole frames only: an odd buffer leaves the last slot alone.
        let mut odd = [0.0f32; 5];
        assert_eq!(r.pop(&mut odd), 4);
        assert_eq!(&odd[..4], &[1.0, -1.0, 2.0, -2.0]);
        assert_eq!(r.queued(), 2);
        assert_eq!(r.pop(&mut odd), 2);
        assert_eq!(&odd[..2], &[3.0, -3.0], "the frame stayed together");
        assert_eq!(r.pop(&mut odd), 0, "empty again");
        // Room again after the reader drained.
        assert_eq!(w.push(&[5.0, -5.0, 6.0, -6.0, 7.0, -7.0]), 6);
        assert_eq!((st.pushed(), st.dropped()), (12, 2));
        assert_eq!(
            r.pop(&mut [0.0; 1]),
            0,
            "a buffer shorter than a frame takes nothing"
        );
    }

    #[test]
    fn only_a_listed_app_stream_serial_can_be_targeted() {
        let mut t = synced();
        app(&mut t, 50, "Firefox", "150");
        assert_eq!(resolve_serial(&t, 150).unwrap().id, 50);
        assert!(matches!(
            resolve_serial(&t, 151),
            Err(StreamError::UnknownSerial { serial: 151 })
        ));
        // A sink's serial is not a target: sinks are never searched.
        t.apply(RegistryEvent::Added {
            id: 60,
            props: props(&[
                ("media.class", "Audio/Sink"),
                ("node.name", "speakers"),
                ("object.serial", "160"),
            ]),
        });
        assert!(matches!(
            resolve_serial(&t, 160),
            Err(StreamError::UnknownSerial { serial: 160 })
        ));
        // A sink *named* like the serial makes target.object ambiguous: refused.
        t.apply(RegistryEvent::Added {
            id: 61,
            props: props(&[("media.class", "Audio/Sink"), ("node.name", "150")]),
        });
        assert!(matches!(
            resolve_serial(&t, 150),
            Err(StreamError::AmbiguousSerial { serial: 150 })
        ));
        let mut f = SerialFollow::new(150);
        assert!(matches!(f.step(&t), Some(FollowAction::Refuse(_))));
        assert!(f.finished());
        assert_eq!(f.step(&t), None);
        // A client is never a target either.
        let mut t = synced();
        t.apply(RegistryEvent::Added {
            id: 70,
            props: props(&[("object.serial", "170"), (REGISTRY_CLIENT_KEY, "true")]),
        });
        assert!(resolve_serial(&t, 170).is_err());
    }

    #[test]
    fn the_follower_connects_once_and_a_removed_target_is_gone_for_good() {
        let mut f = SerialFollow::new(150);
        let mut t = NodeTable::default();
        app(&mut t, 50, "Firefox", "150");
        assert_eq!(f.step(&t), None, "nothing before the first enumeration");
        t.apply(RegistryEvent::Synced);
        assert_eq!(f.step(&t), Some(FollowAction::Connect));
        assert_eq!(f.step(&t), None, "connected once");
        // Other streams of the same app come and go: not this capture's business.
        app(&mut t, 51, "Firefox", "151");
        t.apply(RegistryEvent::Removed(51));
        assert_eq!(f.step(&t), None);
        // The target goes.
        app(&mut t, 52, "Firefox", "152");
        t.apply(RegistryEvent::Removed(50));
        assert_eq!(f.step(&t), Some(FollowAction::Gone));
        assert!(f.finished());
        // It never reconnects: not to the app's other stream, not to anything.
        assert_eq!(f.step(&t), None);
        app(&mut t, 53, "Firefox", "150");
        assert_eq!(f.step(&t), None, "not even to a node claiming the serial");
        t.apply(RegistryEvent::Disconnected("gone".into()));
        assert_eq!(f.step(&t), None);
    }

    #[test]
    fn a_serial_absent_at_the_first_look_is_gone_and_server_loss_is_final() {
        let mut f = SerialFollow::new(150);
        let t = synced();
        assert_eq!(f.step(&t), Some(FollowAction::Gone));
        assert_eq!(f.step(&t), None);

        let mut f = SerialFollow::new(150);
        let mut t = synced();
        app(&mut t, 50, "Firefox", "150");
        assert_eq!(f.step(&t), Some(FollowAction::Connect));
        t.apply(RegistryEvent::Disconnected("server gone".into()));
        assert_eq!(
            f.step(&t),
            Some(FollowAction::ServerLost("server gone".into()))
        );
        assert!(f.finished());
        assert_eq!(f.step(&t), None);
    }

    /// The status says "gone" and nothing undoes it: no late stream event, no format, no
    /// error from the session manager failing the orphaned stream.
    #[test]
    fn a_gone_target_shuts_the_gate_and_stays_gone() {
        let st = status(48_000, 2);
        let (mut inlet, mut reader) = make_inlet(&st, 64, 4);
        st.format_settled(48_000, 2, AudioFormat::F32LE);
        st.stream_streaming();
        deliver(&mut inlet, &le(&[0.5, 0.5]));
        assert_eq!(reader.pop(&mut [0.0; 8]), 2);

        st.mark_gone();
        assert!(st.target_gone());
        assert_eq!(st.state(), CaptureState::Disconnected);
        deliver(&mut inlet, &le(&[0.5, 0.5]));
        assert_eq!(reader.pop(&mut [0.0; 8]), 0, "nothing after gone");
        assert_eq!(st.delivered_format(), None);
        st.stream_streaming();
        st.stream_waiting();
        st.stream_error("no target node available".into());
        st.format_settled(44_100, 2, AudioFormat::F32LE);
        st.format_settled(48_000, 2, AudioFormat::F32LE);
        assert_eq!(st.state(), CaptureState::Disconnected);
        let failure = st.failure();
        assert!(matches!(
            failure,
            Some(StreamError::TargetGone { serial: 150 })
        ));
        deliver(&mut inlet, &le(&[0.5, 0.5]));
        assert_eq!(st.pushed(), 2, "still nothing");
        // A new capture starts over.
        st.restart();
        assert!(!st.target_gone());
        assert_eq!((st.pushed(), st.dropped()), (0, 0));
        assert!(st.failure().is_none());
    }

    /// Two captures on one table: each has its own ring, status and follower; one's target
    /// going does not touch the other.
    #[test]
    fn captures_are_independent() {
        let mut t = synced();
        app(&mut t, 50, "Firefox", "150");
        app(&mut t, 51, "Spotify", "151");
        let (a, b) = (status(48_000, 2), status(48_000, 2));
        b.serial.store(151, Ordering::Relaxed);
        let (mut ia, mut ra) = make_inlet(&a, 64, 4);
        let (mut ib, mut rb) = make_inlet(&b, 64, 4);
        let (mut fa, mut fb) = (SerialFollow::new(150), SerialFollow::new(151));
        assert_eq!(fa.step(&t), Some(FollowAction::Connect));
        assert_eq!(fb.step(&t), Some(FollowAction::Connect));
        for st in [&a, &b] {
            st.format_settled(48_000, 2, AudioFormat::F32LE);
            st.stream_streaming();
        }
        deliver(&mut ia, &le(&[0.1, 0.1]));
        deliver(&mut ib, &le(&[0.2, 0.2, 0.3, 0.3]));
        let mut out = [0.0; 8];
        assert_eq!(ra.pop(&mut out), 2);
        assert_eq!(&out[..2], &[0.1, 0.1]);
        assert_eq!(rb.pop(&mut out), 4);
        assert_eq!(&out[..4], &[0.2, 0.2, 0.3, 0.3]);

        // B's stream goes.
        t.apply(RegistryEvent::Removed(51));
        assert_eq!(fa.step(&t), None, "A carries on");
        assert_eq!(fb.step(&t), Some(FollowAction::Gone));
        b.mark_gone();
        deliver(&mut ia, &le(&[0.4, 0.4]));
        deliver(&mut ib, &le(&[0.5, 0.5]));
        assert_eq!(ra.pop(&mut out), 2, "A still delivers");
        assert_eq!(rb.pop(&mut out), 0, "B delivers nothing");
        assert_eq!(a.state(), CaptureState::Streaming);
        assert!(!a.target_gone() && b.target_gone());
        assert!(a.failure().is_none());
    }

    /// A terminal error (a refusal, a lost server, the stream's own error) shuts the gate for
    /// good, keeps its reason through the loop thread's retirement of the stream and through
    /// late state and format events, and a new start clears it.
    #[test]
    fn a_terminal_error_survives_retirement_and_late_events_and_closes_the_gate() {
        let frame = le(&[0.25, -0.25]);
        for why in [
            "refused: ambiguous",
            "PipeWire server went away: x",
            "stream broke",
        ] {
            let st = status(48_000, 2);
            let (mut inlet, mut reader) = make_inlet(&st, 64, 4);
            st.format_settled(48_000, 2, AudioFormat::F32LE);
            st.stream_streaming();
            deliver(&mut inlet, &frame);
            assert_eq!(reader.pop(&mut [0.0; 8]), 2, "{why}: delivering before");

            // The error arrives while the format gate is valid: delivery stops at once.
            st.stream_error(why.into());
            assert_eq!(st.state(), CaptureState::Error, "{why}");
            assert!(!st.target_gone());
            assert_eq!(st.delivered_format(), None, "{why}: gate shut");
            deliver(&mut inlet, &frame);
            assert_eq!(
                reader.pop(&mut [0.0; 8]),
                0,
                "{why}: zero delivery after error"
            );
            assert_eq!(st.pushed(), 2, "{why}");

            // Loop thread retires the stream (DisconnectStream), late state and format events
            // follow, the target is removed afterwards: none hides the error.
            st.retired();
            assert_eq!(st.state(), CaptureState::Error, "{why}: not Disconnected");
            st.connection_reset();
            st.stream_streaming();
            st.stream_waiting();
            st.format_settled(48_000, 2, AudioFormat::F32LE);
            st.mark_gone();
            st.stream_error("a later, different error".into());
            assert_eq!(st.state(), CaptureState::Error, "{why}");
            assert!(!st.target_gone(), "{why}: gone does not replace an error");
            deliver(&mut inlet, &frame);
            assert_eq!(reader.pop(&mut [0.0; 8]), 0, "{why}: still nothing");
            match st.failure() {
                Some(StreamError::Stream(got)) => assert_eq!(got, why),
                other => panic!("{why}: {other:?}"),
            }
            // A new capture starts over.
            st.restart();
            assert!(st.failure().is_none());
            st.format_settled(48_000, 2, AudioFormat::F32LE);
            st.stream_streaming();
            assert_eq!(st.state(), CaptureState::Streaming);
            deliver(&mut inlet, &frame);
            assert_eq!(
                reader.pop(&mut [0.0; 8]),
                2,
                "{why}: delivers after restart"
            );
        }
    }

    /// With nothing wrong, retiring the stream is `Disconnected`; a standing format rejection
    /// keeps its reason when the stream is retired and when a terminal error comes after it.
    #[test]
    fn retirement_is_disconnected_unless_an_error_stands_and_a_rejection_keeps_its_reason() {
        let st = status(48_000, 2);
        st.stream_streaming();
        st.retired();
        assert_eq!(st.state(), CaptureState::Disconnected);
        assert!(st.failure().is_none());

        let st = status(48_000, 2);
        st.format_settled(44_100, 2, AudioFormat::F32LE);
        assert_eq!(st.state(), CaptureState::Error);
        st.stream_error("session manager failed the stream".into());
        st.retired();
        assert_eq!(st.state(), CaptureState::Error);
        assert_eq!(st.failure().as_ref().map(kind), Some("rate"));
        // Terminal now: the good format no longer reopens the gate.
        st.format_settled(48_000, 2, AudioFormat::F32LE);
        assert_eq!(st.delivered_format(), None);
        assert_eq!(st.state(), CaptureState::Error);
    }

    /// The loop thread revalidates with `resolve_serial` right before it connects, so a name
    /// that appears (or a target that goes) between the supervisor's decision and the connect
    /// is caught: ambiguity is a refusal that says so, removal is "unknown", and no other node
    /// (another stream of the app, a source, a sink) is ever substituted.
    #[test]
    fn the_connect_time_check_catches_ambiguity_and_removal_without_falling_back() {
        let mut t = synced();
        app(&mut t, 50, "Firefox", "150");
        app(&mut t, 51, "Firefox", "151");
        let mut f = SerialFollow::new(150);
        assert_eq!(f.step(&t), Some(FollowAction::Connect));
        assert!(resolve_serial(&t, 150).is_ok());

        // A source named like the serial appears after the supervisor decided.
        t.apply(RegistryEvent::Added {
            id: 61,
            props: props(&[("media.class", "Audio/Source"), ("node.name", "150")]),
        });
        let e = resolve_serial(&t, 150).map(|s| s.id).unwrap_err();
        assert!(matches!(e, StreamError::AmbiguousSerial { serial: 150 }));
        assert!(e.to_string().contains("150"), "a useful message: {e}");
        // The other stream of the same app is untouched and still resolves on its own.
        assert_eq!(resolve_serial(&t, 151).unwrap().id, 51);

        // The target is removed: unknown, and the app's other stream is not a substitute.
        t.apply(RegistryEvent::Removed(61));
        t.apply(RegistryEvent::Removed(50));
        let e = resolve_serial(&t, 150).map(|s| s.id).unwrap_err();
        assert!(matches!(e, StreamError::UnknownSerial { serial: 150 }));
        // Nothing but a stream carrying the serial resolves, whatever else is listed.
        t.apply(RegistryEvent::Added {
            id: 62,
            props: props(&[
                ("media.class", "Audio/Sink"),
                ("node.name", "monitor-me"),
                ("object.serial", "150"),
            ]),
        });
        assert!(resolve_serial(&t, 150).is_err());
    }

    // ---- Live tests: need a running PipeWire session, pw-play, pw-loopback, pw-link and pw-cli.
    // They play only into a temporary silent virtual stereo sink of their own (every player is
    // given that exact sink as `--target`); the default devices are never touched and nothing
    // is heard.

    mod live {
        use super::*;
        use std::process::{Child, Command as Proc, Stdio};

        const RATE: u32 = 48_000;

        /// The tone players, killed on every exit path.
        struct Players(Vec<Child>);

        impl Drop for Players {
            fn drop(&mut self) {
                for c in &mut self.0 {
                    let _ = c.kill();
                    let _ = c.wait();
                }
            }
        }

        /// A silent virtual stereo device made by `pw-loopback`, like `testutil::Loopback` but
        /// `[ FL FR ]`: an `Audio/Sink` called `name-sink` and an `Audio/Source` called
        /// `name`, neither connected to hardware. Killed, and so the nodes removed, on every
        /// exit path (drop), a panic included.
        struct StereoLoopback {
            child: Child,
            sink: String,
        }

        impl StereoLoopback {
            fn new(name: &str) -> Self {
                let sink = format!("{name}-sink");
                let child = Proc::new("pw-loopback")
                    .args(["-n", name, "-m", "[ FL FR ]"])
                    .arg(format!(
                        "--capture-props=media.class=Audio/Sink node.name={sink}"
                    ))
                    .arg(format!(
                        "--playback-props=media.class=Audio/Source node.name={name}"
                    ))
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .spawn()
                    .expect("pw-loopback on PATH");
                StereoLoopback { child, sink }
            }
        }

        impl Drop for StereoLoopback {
            fn drop(&mut self) {
                let _ = self.child.kill();
                let _ = self.child.wait();
            }
        }

        /// Run `cmd args` and return its stdout as text.
        fn output_of(cmd: &str, args: &[&str]) -> String {
            let out = Proc::new(cmd).args(args).output().expect("tool on PATH");
            String::from_utf8_lossy(&out.stdout).into_owned()
        }

        /// How many input ports of stream captures `pw-link -i` lists (two per stereo capture
        /// once it is connected; none once its node is gone).
        fn capture_ports() -> usize {
            let pat = format!("{STREAM_NODE_NAME}:");
            output_of("pw-link", &["-i"])
                .lines()
                .filter(|l| l.contains(&pat))
                .count()
        }

        /// How many stream capture nodes `pw-cli ls Node` lists.
        fn capture_nodes() -> usize {
            let want = format!("node.name = \"{STREAM_NODE_NAME}\"");
            output_of("pw-cli", &["ls", "Node"])
                .lines()
                .filter(|l| l.trim() == want)
                .count()
        }

        /// Play `secs` of a 48 kHz stereo tone, `left` Hz on the left and `right` Hz on the
        /// right, as application `app`, to exactly the sink `target` (`--target`: never the
        /// default one). Generated in memory and piped to pw-play: no audio on disk.
        fn play(target: &str, app: &str, left: f32, right: f32, secs: f32) -> Child {
            use std::io::Write as _;
            let mut child = Proc::new("pw-play")
                .args([
                    "--raw",
                    "--rate",
                    "48000",
                    "--channels",
                    "2",
                    "--format",
                    "f32",
                ])
                .args(["--target", target])
                .args(["--volume", "0.1", "-P"])
                .arg(format!(
                    "{{ application.name = \"{app}\" media.name = \"dmn stream test\" }}"
                ))
                .arg("-")
                .stdin(Stdio::piped())
                .spawn()
                .expect("pw-play on PATH");
            let mut stdin = child.stdin.take().unwrap();
            let tau = 2.0 * std::f32::consts::PI;
            let bytes: Vec<u8> = (0..(RATE as f32 * secs) as usize)
                .flat_map(|i| {
                    let t = i as f32 / RATE as f32;
                    let l = 0.5 * (tau * left * t).sin();
                    let r = 0.5 * (tau * right * t).sin();
                    l.to_le_bytes().into_iter().chain(r.to_le_bytes())
                })
                .collect();
            std::thread::spawn(move || {
                let _ = stdin.write_all(&bytes);
            });
            child
        }

        /// Goertzel power of `hz` in `w` at 48 kHz.
        fn goertzel(w: &[f32], hz: f32) -> f32 {
            let k = 2.0 * (2.0 * std::f32::consts::PI * hz / RATE as f32).cos();
            let (mut s1, mut s2) = (0.0f32, 0.0f32);
            for &x in w {
                let s0 = x + k * s1 - s2;
                s2 = s1;
                s1 = s0;
            }
            s1 * s1 + s2 * s2 - k * s1 * s2
        }

        /// One channel of interleaved stereo.
        fn channel(stereo: &[f32], ch: usize) -> Vec<f32> {
            stereo.chunks_exact(2).map(|f| f[ch]).collect()
        }

        /// In the loudest 0.25 s window of `mono`: its RMS, and how much stronger `want` Hz is
        /// than the strongest of `others`.
        fn dominance(mono: &[f32], want: f32, others: &[f32]) -> (f32, f32) {
            let win = RATE as usize / 4;
            mono.chunks(win)
                .filter(|w| w.len() == win)
                .map(|w| {
                    let rms = (w.iter().map(|s| s * s).sum::<f32>() / w.len() as f32).sqrt();
                    let off = others
                        .iter()
                        .map(|&hz| goertzel(w, hz))
                        .fold(1e-12f32, f32::max);
                    (rms, goertzel(w, want) / off)
                })
                .fold((0.0, 0.0), |best, x| if x.0 > best.0 { x } else { best })
        }

        fn wait_for(what: &str, dur: Duration, mut ok: impl FnMut() -> bool) {
            let t = Instant::now();
            while !ok() {
                assert!(t.elapsed() < dur, "timed out waiting for {what}");
                std::thread::sleep(Duration::from_millis(20));
            }
        }

        /// The app stream of `app` once it is listed with a serial.
        fn find(ctx: &PipeWireContext, app: &str) -> (u64, crate::StreamMetadata) {
            let mut found = None;
            wait_for(
                &format!("{app} in the table"),
                Duration::from_secs(3),
                || {
                    for (s, m) in ctx.nodes().streams_with_metadata() {
                        if s.application == app && m.serial.is_some() {
                            found = Some(m);
                        }
                    }
                    found.is_some()
                },
            );
            let m = found.unwrap();
            (m.serial.unwrap(), m)
        }

        /// The owner metadata of the app stream with `serial`, while it is listed.
        fn meta_of(ctx: &PipeWireContext, serial: u64) -> Option<crate::StreamMetadata> {
            let t = ctx.nodes();
            let id = t.stream_by_serial(serial)?.id;
            t.stream_metadata(id)
        }

        /// A 48 kHz stereo capture of `serial` with a 2 s ring.
        fn open(ctx: &Arc<PipeWireContext>, serial: u64) -> StreamCapture {
            match StreamCapture::new(Arc::clone(ctx), serial, RATE, 2, 2_000) {
                Ok(c) => c,
                Err(e) => panic!("{e}"),
            }
        }

        /// Three `pw-play` processes play different stereo tones into one temporary silent
        /// virtual stereo sink (each given it as an exact `--target`; the default devices are
        /// untouched). Two are captured by serial at 48 kHz stereo; the third is not, yet it
        /// shares that sink, so a capture of the sink's monitor would contain it (and the other
        /// capture's tone) in both. Each capture holds its own two tones on the right channels
        /// and none of the others', so nothing came from a monitor. Killing one player ends
        /// only its capture ("target gone"); the other goes on. The capture nodes and their
        /// ports are watched in the real graph (`pw-link -i`, `pw-cli ls Node`) and are gone
        /// after `stop` and after drop.
        #[test]
        #[ignore = "needs PipeWire, pw-play, pw-loopback, pw-link and pw-cli; silent: plays only into a virtual sink it creates"]
        fn live_two_streams_are_captured_apart_by_serial_and_one_ending_leaves_the_other() {
            let pid = std::process::id();
            let (app_a, app_b, app_c) = (
                format!("dmn-stream-a-{pid}"),
                format!("dmn-stream-b-{pid}"),
                format!("dmn-stream-c-{pid}"),
            );
            // A: 440 | 550, B: 660 | 770, C (never captured): 990 | 990.
            let all = [440.0, 550.0, 660.0, 770.0, 990.0];
            let ctx = Arc::new(PipeWireContext::connect().expect("PipeWire running"));
            assert!(ctx.wait_for_sync(Duration::from_secs(2)).synced);
            let device = StereoLoopback::new(&format!("dmn-stream-test-{pid}"));
            let sink = device.sink.clone();
            wait_for("the virtual sink", Duration::from_secs(5), || {
                ctx.nodes().sinks().any(|s| s.name == sink)
            });
            let mut players = Players(vec![
                play(&sink, &app_a, 440.0, 550.0, 12.0),
                play(&sink, &app_b, 660.0, 770.0, 12.0),
                play(&sink, &app_c, 990.0, 990.0, 12.0),
            ]);
            let (pid_a, pid_b) = (players.0[0].id(), players.0[1].id());
            let (serial_a, meta_a) = find(&ctx, &app_a);
            let (serial_b, meta_b) = find(&ctx, &app_b);
            // What the graph says about who owns each stream (record it in the PR).
            wait_for("owner metadata", Duration::from_secs(3), || {
                let a = meta_of(&ctx, serial_a).and_then(|m| m.sec_pid);
                let b = meta_of(&ctx, serial_b).and_then(|m| m.sec_pid);
                a.is_some() && b.is_some()
            });
            for (what, serial, pid) in [("A", serial_a, pid_a), ("B", serial_b, pid_b)] {
                let m = meta_of(&ctx, serial).unwrap();
                println!("{what}: player pid {pid}, metadata {m:?}");
                assert_eq!(m.application_process_id, Some(pid), "{what}");
                assert_eq!(m.sec_pid, Some(pid), "{what}: a native client's own pid");
                assert!(m.application_name.is_some(), "{what}");
            }
            assert_eq!(meta_a.application_name.as_deref(), Some(app_a.as_str()));
            assert_eq!(meta_b.application_name.as_deref(), Some(app_b.as_str()));

            // What stream captures already exist (another process's, say) is not ours to judge.
            let (base_ports, base_nodes) = (capture_ports(), capture_nodes());
            let mut cap_a = open(&ctx, serial_a);
            let mut cap_b = open(&ctx, serial_b);
            let (st_a, st_b) = (cap_a.status(), cap_b.status());
            let mut ra = cap_a.start().unwrap();
            let mut rb = cap_b.start().unwrap();
            cap_a.wait_streaming(Duration::from_secs(3)).unwrap();
            cap_b.wait_streaming(Duration::from_secs(3)).unwrap();
            wait_for("two captures in the graph", Duration::from_secs(3), || {
                capture_ports() == base_ports + 4 && capture_nodes() == base_nodes + 2
            });
            assert_eq!(st_a.delivered_format(), Some((RATE, 2)));
            assert_eq!(st_b.delivered_format(), Some((RATE, 2)));

            let (mut got_a, mut got_b) = (Vec::new(), Vec::new());
            let mut buf = [0.0f32; 9600];
            let t = Instant::now();
            while t.elapsed() < Duration::from_millis(1500) {
                let n = ra.pop(&mut buf);
                got_a.extend_from_slice(&buf[..n]);
                let n = rb.pop(&mut buf);
                got_b.extend_from_slice(&buf[..n]);
                std::thread::sleep(Duration::from_millis(10));
            }
            for (what, got, l, r) in [("A", &got_a, 440.0, 550.0), ("B", &got_b, 660.0, 770.0)] {
                let per_sec = got.len() as f32 / 2.0 / 1.5;
                println!("{what}: {} samples ({per_sec:.0} frames/s)", got.len());
                assert!(
                    (per_sec - RATE as f32).abs() < RATE as f32 * 0.15,
                    "{what}: {per_sec} frames a second"
                );
                for (ch, want) in [(0, l), (1, r)] {
                    let others: Vec<f32> = all.iter().copied().filter(|&f| f != want).collect();
                    let (rms, ratio) = dominance(&channel(got, ch), want, &others);
                    println!("{what} ch{ch}: rms {rms:.4}, {want} Hz ratio {ratio:.0}");
                    assert!(rms > 0.002, "{what} ch{ch}: the tone reached the capture");
                    assert!(
                        ratio > 100.0,
                        "{what} ch{ch}: {want} Hz dominates, nothing of the others"
                    );
                }
            }
            assert_eq!((st_a.dropped(), st_b.dropped()), (0, 0));

            // B's player dies: B's capture is gone, A's carries on.
            let _ = players.0[1].kill();
            let _ = players.0[1].wait();
            wait_for("B gone", Duration::from_secs(3), || st_b.target_gone());
            assert_eq!(st_b.state(), CaptureState::Disconnected);
            assert!(matches!(
                cap_b.wait_streaming(Duration::from_millis(10)),
                Err(StreamError::TargetGone { serial }) if serial == serial_b
            ));
            let _ = rb.pop(&mut buf);
            let b_pushed = st_b.pushed();
            let a_pushed = st_a.pushed();
            std::thread::sleep(Duration::from_millis(400));
            assert_eq!(st_b.pushed(), b_pushed, "B delivers nothing more");
            assert!(st_a.pushed() > a_pushed, "A still delivers");
            assert_eq!(st_a.state(), CaptureState::Streaming);
            assert!(!st_a.target_gone());

            // Stopping B, whose target is gone, leaves A alone too.
            cap_b.stop().unwrap();
            assert_eq!(st_b.state(), CaptureState::Idle);
            let a_pushed = st_a.pushed();
            std::thread::sleep(Duration::from_millis(200));
            assert!(st_a.pushed() > a_pushed, "A still delivers after B stopped");
            // The graph, not the node table (which never lists a recording stream): B's two
            // input ports and its node are gone, A's remain.
            wait_for("B's capture ports gone", Duration::from_secs(3), || {
                capture_ports() == base_ports + 2 && capture_nodes() == base_nodes + 1
            });
            cap_a.stop().unwrap();
            assert_eq!(st_a.state(), CaptureState::Idle, "stop closed the stream");
            // Dropping stops too: restart A's serial is not possible (its tone still plays, so a
            // new capture of the same serial is), and drop must leave nothing behind either.
            let mut again = open(&ctx, serial_a);
            let _r = again.start().unwrap();
            again.wait_streaming(Duration::from_secs(3)).unwrap();
            wait_for("a capture's ports", Duration::from_secs(3), || {
                capture_ports() == base_ports + 2 && capture_nodes() == base_nodes + 1
            });
            drop(again);
            wait_for(
                "every capture node and port gone",
                Duration::from_secs(3),
                || capture_ports() == base_ports && capture_nodes() == base_nodes,
            );
        }
    }
}
