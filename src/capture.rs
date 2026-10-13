//! Live capture: [`PipeWireSource`] records one side of the conversation from PipeWire as 16 kHz
//! mono f32 (docs/SPEC.md § Audio capture on PipeWire, Capture (M1)).
//!
//! Three pieces, each on the thread that owns it:
//!
//! - [`Reattach`], a pure state machine. It reads [`NodeTable`] snapshots and decides when to
//!   connect, retarget or disconnect. It runs on the source's supervisor thread and is what the
//!   unit tests drive with synthetic registry events.
//! - `LoopCapture`, the `pw_stream` and its listeners. It lives on the PipeWire loop thread; the
//!   supervisor reaches it through [`crate::pipewire::Command`]s.
//! - [`Accum`], the real-time side. The process callback dequeues a buffer, copies its f32
//!   samples into a fixed 20 ms array, pushes full frames into the [`FrameSink`] and requeues.
//!   It allocates nothing, locks nothing and logs nothing.
//!
//! One stream per source for its whole life: a retarget is `disconnect`, update `target.object`
//! and `stream.capture.sink`, `connect`. Keeping the stream keeps its process listener, and the
//! listener owns the only `FrameSink`, so frames keep flowing into the same ring across
//! reconnects.

use std::ffi::CString;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use pipewire as pw;
use pw::spa;

use crate::frames::FrameSink;
use crate::pipewire::{Command, NodeTable, PipeWireContext};
use crate::source::{AudioError, AudioSource, Source};
use crate::{FRAME_LEN, SAMPLE_RATE};

/// What a [`PipeWireSource`] records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CaptureTarget {
    /// A microphone: an `Audio/Source` by `node.name`, or `None` for PipeWire's default source
    /// (the session manager follows default changes by itself).
    Mic(Option<String>),
    /// One application's playback stream, matched by `application.name` (case-insensitive).
    /// With `monitor_fallback`, the default sink monitor is captured until the app appears;
    /// this is the SPEC's "no app stream selected or found" fallback.
    App {
        application: String,
        monitor_fallback: bool,
    },
    /// Everything the default sink plays (`stream.capture.sink = true`).
    SinkMonitor,
}

impl CaptureTarget {
    /// The interviewer target from `[audio].app_stream`: an empty name means the sink monitor.
    pub fn interviewer(app_stream: &str) -> Self {
        if app_stream.trim().is_empty() {
            CaptureTarget::SinkMonitor
        } else {
            CaptureTarget::App {
                application: app_stream.to_owned(),
                monitor_fallback: true,
            }
        }
    }

    /// The mic target from `[audio].mic`: "default" or "" means PipeWire's default source.
    pub fn mic(mic: &str) -> Self {
        match mic.trim() {
            "" | "default" => CaptureTarget::Mic(None),
            name => CaptureTarget::Mic(Some(name.to_owned())),
        }
    }
}

/// What the capture is currently attached to. The UI shows a "capturing all system audio" badge
/// for [`CaptureMode::SinkMonitor`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaptureMode {
    None,
    Mic,
    AppStream,
    SinkMonitor,
}

/// Lifecycle of a capture, as the status dots show it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaptureState {
    /// Not started, or stopped.
    Idle,
    /// Connected and waiting for the session manager to link and start the stream.
    Connecting,
    /// Frames are flowing.
    Streaming,
    /// The target went away; waiting for it to come back.
    Disconnected,
    /// PipeWire reported an error; see [`CaptureStatus::last_error`].
    Error,
}

impl CaptureState {
    fn to_u8(self) -> u8 {
        match self {
            CaptureState::Idle => 0,
            CaptureState::Connecting => 1,
            CaptureState::Streaming => 2,
            CaptureState::Disconnected => 3,
            CaptureState::Error => 4,
        }
    }

    fn from_u8(v: u8) -> Self {
        match v {
            1 => CaptureState::Connecting,
            2 => CaptureState::Streaming,
            3 => CaptureState::Disconnected,
            4 => CaptureState::Error,
            _ => CaptureState::Idle,
        }
    }

    /// Short name for error messages and status lines.
    pub fn label(self) -> &'static str {
        match self {
            CaptureState::Idle => "idle",
            CaptureState::Connecting => "connecting",
            CaptureState::Streaming => "streaming",
            CaptureState::Disconnected => "disconnected",
            CaptureState::Error => "in error",
        }
    }
}

impl CaptureMode {
    fn to_u8(self) -> u8 {
        match self {
            CaptureMode::None => 0,
            CaptureMode::Mic => 1,
            CaptureMode::AppStream => 2,
            CaptureMode::SinkMonitor => 3,
        }
    }

    fn from_u8(v: u8) -> Self {
        match v {
            1 => CaptureMode::Mic,
            2 => CaptureMode::AppStream,
            3 => CaptureMode::SinkMonitor,
            _ => CaptureMode::None,
        }
    }
}

/// Shared, lock-free view of one capture for the status bar and `dmn doctor`. The real-time
/// callback only touches `frames`.
#[derive(Debug, Default)]
pub struct CaptureStatus {
    state: AtomicU8,
    mode: AtomicU8,
    frames: AtomicU64,
    rate: AtomicU32,
    channels: AtomicU32,
    /// Set by `param_changed` once PipeWire has settled on 16 kHz mono F32; the real-time
    /// callback reads it and drops buffers while it is false, so a format we did not ask for
    /// never reaches the ring as if it were 16 kHz mono.
    format_ok: AtomicBool,
    /// Buffers the callback dropped because the negotiated format was wrong.
    bad_format_buffers: AtomicU64,
    last_error: Mutex<Option<String>>,
}

impl CaptureStatus {
    pub fn state(&self) -> CaptureState {
        CaptureState::from_u8(self.state.load(Ordering::Acquire))
    }

    pub fn mode(&self) -> CaptureMode {
        CaptureMode::from_u8(self.mode.load(Ordering::Acquire))
    }

    /// True while capturing the whole default sink rather than one app.
    pub fn capturing_all_system_audio(&self) -> bool {
        self.mode() == CaptureMode::SinkMonitor
    }

    /// Frames pushed into the sink since start (including ones the ring then dropped).
    pub fn frames(&self) -> u64 {
        self.frames.load(Ordering::Relaxed)
    }

    /// The negotiated format as `(rate, channels)`, once PipeWire has settled it.
    pub fn format(&self) -> Option<(u32, u32)> {
        let rate = self.rate.load(Ordering::Acquire);
        (rate != 0).then(|| (rate, self.channels.load(Ordering::Acquire)))
    }

    pub fn last_error(&self) -> Option<String> {
        self.last_error
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// True once PipeWire has negotiated the format we asked for. The callback pushes nothing
    /// until then.
    pub fn format_ok(&self) -> bool {
        self.format_ok.load(Ordering::Acquire)
    }

    /// Buffers dropped because the negotiated format was not 16 kHz mono F32.
    pub fn bad_format_buffers(&self) -> u64 {
        self.bad_format_buffers.load(Ordering::Relaxed)
    }

    pub(crate) fn set_format_ok(&self, ok: bool) {
        self.format_ok.store(ok, Ordering::Release);
    }

    pub(crate) fn set_state(&self, s: CaptureState) {
        self.state.store(s.to_u8(), Ordering::Release);
    }

    pub(crate) fn set_mode(&self, m: CaptureMode) {
        self.mode.store(m.to_u8(), Ordering::Release);
    }

    pub(crate) fn set_error(&self, why: String) {
        *self.last_error.lock().unwrap_or_else(|e| e.into_inner()) = Some(why);
        self.set_state(CaptureState::Error);
    }
}

/// One decision from [`Reattach::step`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// (Re)connect the stream. `target_object` is written to `target.object` (removed when
    /// `None`); `capture_sink` to `stream.capture.sink`.
    Connect {
        target_object: Option<String>,
        capture_sink: bool,
        mode: CaptureMode,
    },
    /// Disconnect and wait: the target is gone.
    Disconnect,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Bound {
    /// Nothing decided yet.
    Nothing,
    /// Linked to this registry id.
    Node(u32),
    /// Linked to PipeWire's default source (no `target.object`).
    Default,
    /// Linked to the default sink monitor.
    Monitor,
    /// The target went away; disconnected until it (or its replacement) appears.
    Lost,
}

/// Decides, from registry snapshots, what the capture stream should be linked to. Pure: no
/// PipeWire, no threads; every call with an unchanged table returns `None`.
#[derive(Debug, Clone)]
pub struct Reattach {
    target: CaptureTarget,
    bound: Bound,
}

impl Reattach {
    pub fn new(target: CaptureTarget) -> Self {
        Self {
            target,
            bound: Bound::Nothing,
        }
    }

    pub fn target(&self) -> &CaptureTarget {
        &self.target
    }

    pub fn step(&mut self, table: &NodeTable) -> Option<Action> {
        if table.disconnected.is_some() {
            return match self.bound {
                Bound::Nothing | Bound::Lost => None,
                _ => {
                    self.bound = Bound::Lost;
                    Some(Action::Disconnect)
                }
            };
        }
        // Deciding "not found" before the first enumeration would fall back for no reason.
        if !table.synced {
            return None;
        }
        match &self.target {
            CaptureTarget::SinkMonitor => {
                self.bind(Bound::Monitor, None, true, CaptureMode::SinkMonitor)
            }
            CaptureTarget::Mic(None) => self.bind(Bound::Default, None, false, CaptureMode::Mic),
            CaptureTarget::Mic(Some(name)) => {
                match table.sources().find(|n| &n.name == name) {
                    Some(n) => {
                        let target = Some(n.name.clone());
                        self.bind(Bound::Node(n.id), target, false, CaptureMode::Mic)
                    }
                    None => match self.bound {
                        Bound::Node(_) => self.lose(),
                        // Named mic absent at start: the default source is better than silence.
                        Bound::Nothing => self.bind(Bound::Default, None, false, CaptureMode::Mic),
                        _ => None,
                    },
                }
            }
            CaptureTarget::App {
                application,
                monitor_fallback,
            } => {
                let found = table
                    .streams()
                    .filter(|s| s.application.eq_ignore_ascii_case(application))
                    // Newest wins: an app that restarted has a higher id than its ghost.
                    .max_by_key(|s| s.id);
                match found {
                    Some(s) => {
                        let target = s.serial.clone().unwrap_or_else(|| s.node_name.clone());
                        self.bind(
                            Bound::Node(s.id),
                            Some(target),
                            false,
                            CaptureMode::AppStream,
                        )
                    }
                    None => match self.bound {
                        Bound::Node(_) => self.lose(),
                        Bound::Nothing if *monitor_fallback => {
                            self.bind(Bound::Monitor, None, true, CaptureMode::SinkMonitor)
                        }
                        _ => None,
                    },
                }
            }
        }
    }

    fn bind(
        &mut self,
        to: Bound,
        target_object: Option<String>,
        capture_sink: bool,
        mode: CaptureMode,
    ) -> Option<Action> {
        if self.bound == to {
            return None;
        }
        self.bound = to;
        Some(Action::Connect {
            target_object,
            capture_sink,
            mode,
        })
    }

    fn lose(&mut self) -> Option<Action> {
        self.bound = Bound::Lost;
        Some(Action::Disconnect)
    }
}

/// The real-time accumulator: PipeWire hands over whatever quantum it runs at (160 samples at
/// the 10 ms latency we ask for, but not guaranteed); this cuts it into 20 ms frames. Fixed-size
/// state, no allocation.
pub(crate) struct Accum {
    sink: FrameSink,
    buf: [f32; FRAME_LEN],
    fill: usize,
    status: Arc<CaptureStatus>,
}

impl Accum {
    pub(crate) fn new(sink: FrameSink, status: Arc<CaptureStatus>) -> Self {
        Self {
            sink,
            buf: [0.0; FRAME_LEN],
            fill: 0,
            status,
        }
    }

    /// Whether the negotiated format is the one we asked for. One relaxed atomic load; safe in
    /// the real-time callback.
    #[inline]
    pub(crate) fn format_ok(&self) -> bool {
        self.status.format_ok.load(Ordering::Acquire)
    }

    /// Count a buffer dropped for a wrong format. One relaxed atomic add, no allocation.
    #[inline]
    pub(crate) fn count_bad_format(&self) {
        self.status
            .bad_format_buffers
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Append little-endian f32 samples. A trailing partial sample (never produced by F32LE
    /// buffers) is ignored.
    pub(crate) fn push_le_bytes(&mut self, bytes: &[u8]) {
        for c in bytes.chunks_exact(4) {
            self.push_sample(f32::from_le_bytes([c[0], c[1], c[2], c[3]]));
        }
    }

    #[cfg(test)]
    pub(crate) fn push_samples(&mut self, samples: &[f32]) {
        for &s in samples {
            self.push_sample(s);
        }
    }

    #[inline]
    fn push_sample(&mut self, s: f32) {
        self.buf[self.fill] = s;
        self.fill += 1;
        if self.fill == FRAME_LEN {
            self.sink.push_frame(&self.buf);
            self.status.frames.fetch_add(1, Ordering::Relaxed);
            self.fill = 0;
        }
    }
}

/// REAL-TIME CALLBACK (runs on PipeWire's data thread; `RT_PROCESS`).
/// Rules (CLAUDE.md, SPEC § Threading rules), reviewed call by call:
/// - no allocation: `dequeue_buffer` wraps a pointer, `datas_mut`/`chunk`/`data` borrow the
///   mapped buffer, `Accum` writes into its fixed `[f32; FRAME_LEN]`, `FrameSink::push_frame`
///   copies into the preallocated ring;
/// - no locks: the ring is lock-free SPSC, counters are relaxed atomics;
/// - no logging, no syscalls, bounded time (one pass over the buffer).
///
/// Dequeue, copy, requeue (the `Buffer` guard requeues on drop). Nothing else goes here.
fn process(stream: &pw::stream::Stream, acc: &mut Accum) {
    let Some(mut buffer) = stream.dequeue_buffer() else {
        return;
    };
    // Dropping the buffer here requeues it; we just do not copy its samples. Until the format
    // is settled at 16 kHz mono F32, its bytes are not what the ring expects.
    if !acc.format_ok() {
        acc.count_bad_format();
        return;
    }
    let Some(data) = buffer.datas_mut().first_mut() else {
        return;
    };
    let offset = data.chunk().offset() as usize;
    let size = data.chunk().size() as usize;
    if let Some(bytes) = data.data() {
        let end = offset.saturating_add(size).min(bytes.len());
        let start = offset.min(end);
        acc.push_le_bytes(&bytes[start..end]);
    }
}

/// What `PipeWireSource::stop` returns: a panicked supervisor first, then a stream that never
/// reached `Idle` inside the timeout (its real-time callback may still be running).
/// `final_state` is `None` when no Close was sent, i.e. there was nothing to wait for.
fn stop_result(
    panicked: bool,
    final_state: Option<CaptureState>,
    timeout: Duration,
) -> Result<(), AudioError> {
    if panicked {
        return Err(AudioError::ThreadPanicked);
    }
    match final_state {
        Some(state) if state != CaptureState::Idle => Err(AudioError::StopTimeout {
            state: state.label(),
            ms: timeout.as_millis() as u64,
        }),
        _ => Ok(()),
    }
}

/// The stream and its two listeners, owned by the PipeWire loop thread. The process listener
/// is the only one holding the [`Accum`]; state and format events go to a second listener with
/// no user data, so the real-time thread and the loop thread never share a `&mut`.
pub(crate) struct LoopCapture {
    // Field order is drop order: listeners before the stream they are attached to.
    _process: pw::stream::StreamListener<Accum>,
    _events: pw::stream::StreamListener<()>,
    stream: pw::stream::StreamRc,
    status: Arc<CaptureStatus>,
    connected: bool,
}

impl LoopCapture {
    /// Create the stream (not connected yet) on the loop thread.
    pub(crate) fn open(
        core: &pw::core::CoreRc,
        spec: OpenSpec,
        sink: FrameSink,
    ) -> Result<Self, String> {
        let mut props = pw::properties::PropertiesBox::new();
        props.insert(*pw::keys::MEDIA_TYPE, "Audio");
        props.insert(*pw::keys::MEDIA_CATEGORY, "Capture");
        props.insert(*pw::keys::MEDIA_ROLE, "Communication");
        props.insert(*pw::keys::NODE_NAME, spec.node_name.as_str());
        props.insert(*pw::keys::NODE_LATENCY, "160/16000");
        if spec.strict {
            // An interviewer stream must never drift to the mic or another app on its own.
            props.insert("node.dont-fallback", "true");
            props.insert(*pw::keys::NODE_DONT_RECONNECT, "true");
        }
        let stream = pw::stream::StreamRc::new(core.clone(), &spec.node_name, props)
            .map_err(|e| e.to_string())?;

        let process_listener = stream
            .add_local_listener_with_user_data(Accum::new(sink, Arc::clone(&spec.status)))
            .process(process)
            .register()
            .map_err(|e| e.to_string())?;

        let state_status = Arc::clone(&spec.status);
        let format_status = Arc::clone(&spec.status);
        let events_listener = stream
            .add_local_listener::<()>()
            .state_changed(move |_, _, _old, new| match new {
                pw::stream::StreamState::Streaming => {
                    state_status.set_state(CaptureState::Streaming)
                }
                pw::stream::StreamState::Connecting | pw::stream::StreamState::Paused => {
                    if state_status.state() != CaptureState::Disconnected {
                        state_status.set_state(CaptureState::Connecting)
                    }
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
                    format_status.set_format_ok(false);
                    return;
                };
                let mut info = spa::param::audio::AudioInfoRaw::new();
                if info.parse(param).is_ok() {
                    format_status
                        .channels
                        .store(info.channels(), Ordering::Release);
                    format_status.rate.store(info.rate(), Ordering::Release);
                    // The sample format matters as much as the rate: S16LE at 16 kHz mono
                    // would open the gate and be read as f32 (Copilot on PR #32).
                    let ok = info.rate() == SAMPLE_RATE
                        && info.channels() == 1
                        && info.format() == spa::param::audio::AudioFormat::F32LE;
                    // Gate the real-time callback before reporting, so no buffer of the wrong
                    // shape is ever copied into the 16 kHz mono ring.
                    format_status.set_format_ok(ok);
                    if !ok {
                        format_status.set_error(format!(
                            "negotiated {:?} {} Hz × {} ch, need F32LE {SAMPLE_RATE} Hz mono (dropping buffers)",
                            info.format(),
                            info.rate(),
                            info.channels()
                        ));
                    }
                } else {
                    format_status.set_format_ok(false);
                }
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

    /// Point the stream at a new target and (re)connect it.
    pub(crate) fn connect(&mut self, target_object: Option<&str>, capture_sink: bool) {
        if let Err(e) = self.try_connect(target_object, capture_sink) {
            self.status.set_error(e);
        }
    }

    fn try_connect(
        &mut self,
        target_object: Option<&str>,
        capture_sink: bool,
    ) -> Result<(), String> {
        self.disconnect();
        let target = target_object
            .map(|t| CString::new(t).map_err(|_| format!("target {t:?} contains a NUL byte")))
            .transpose()?;
        let sink_flag = if capture_sink { c"true" } else { c"false" };
        update_stream_props(
            &self.stream,
            &[
                (c"target.object", target.as_deref()),
                (c"stream.capture.sink", Some(sink_flag)),
            ],
        )?;
        let pod = enum_format_pod(SAMPLE_RATE)?;
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

    pub(crate) fn disconnect(&mut self) {
        // A reconnect renegotiates the format, so the old verdict must not carry over into the
        // new stream's first buffers.
        self.status.set_format_ok(false);
        if self.connected {
            // After this returns the data thread no longer calls `process`.
            let _ = self.stream.disconnect();
            self.connected = false;
        }
    }
}

impl Drop for LoopCapture {
    fn drop(&mut self) {
        self.disconnect();
        self.status.set_state(CaptureState::Idle);
        self.status.set_mode(CaptureMode::None);
    }
}

/// What the loop thread needs to create a stream.
pub(crate) struct OpenSpec {
    pub(crate) node_name: String,
    /// Set `node.dont-fallback` / `node.dont-reconnect` (interviewer streams).
    pub(crate) strict: bool,
    pub(crate) status: Arc<CaptureStatus>,
}

/// `SPA_PARAM_EnumFormat` with one fixed choice: F32LE, `rate` Hz, mono. PipeWire's adapter
/// resamples and downmixes whatever the target produces (capture asks for 16 kHz; the virtual
/// microphone of `vmic.rs` asks for the graph rate).
pub(crate) fn enum_format_pod(rate: u32) -> Result<Vec<u8>, String> {
    let mut info = spa::param::audio::AudioInfoRaw::new();
    info.set_format(spa::param::audio::AudioFormat::F32LE);
    info.set_rate(rate);
    info.set_channels(1);
    let mut position = [0u32; spa::sys::SPA_AUDIO_MAX_CHANNELS as usize];
    position[0] = spa::sys::SPA_AUDIO_CHANNEL_MONO;
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

/// `pw_stream_update_properties` with optional values: `None` removes the key. The `pipewire`
/// crate has no safe wrapper for it, and a retarget needs it: the stream (and with it the
/// listener that owns the `FrameSink`) must survive, only its target changes.
#[allow(unsafe_code)] // CLAUDE.md allows unsafe in dmn-audio; this is its only use.
fn update_stream_props(
    stream: &pw::stream::Stream,
    entries: &[(&std::ffi::CStr, Option<&std::ffi::CStr>)],
) -> Result<(), String> {
    let items: Vec<spa::sys::spa_dict_item> = entries
        .iter()
        .map(|(k, v)| spa::sys::spa_dict_item {
            key: k.as_ptr(),
            value: v.map_or(std::ptr::null(), |v| v.as_ptr()),
        })
        .collect();
    let dict = spa::sys::spa_dict {
        flags: 0,
        n_items: items.len() as u32,
        items: items.as_ptr(),
    };
    // SAFETY: `stream` is a live pw_stream owned by the loop thread, which is the thread running
    // this call. `dict` and `items` point at C strings borrowed from `entries`; all of them
    // outlive the call, and pw_stream_update_properties copies every key and value into the
    // stream's own properties before returning (a NULL value removes the key). The function
    // does not retain the pointers.
    let r = unsafe { pw::sys::pw_stream_update_properties(stream.as_raw_ptr(), &dict) };
    if r < 0 {
        Err(format!(
            "pw_stream_update_properties: {}",
            std::io::Error::from_raw_os_error(-r)
        ))
    } else {
        Ok(())
    }
}

/// A live capture of one conversation side from PipeWire. Implements [`AudioSource`]:
/// `start` creates the stream on the context's loop thread and starts a supervisor thread that
/// follows the registry; `stop` tears both down.
pub struct PipeWireSource {
    ctx: Arc<PipeWireContext>,
    source: Source,
    target: CaptureTarget,
    status: Arc<CaptureStatus>,
    key: Option<u64>,
    stop: Arc<AtomicBool>,
    supervisor: Option<JoinHandle<()>>,
}

impl PipeWireSource {
    pub fn new(ctx: Arc<PipeWireContext>, source: Source, target: CaptureTarget) -> Self {
        Self {
            ctx,
            source,
            target,
            status: Arc::new(CaptureStatus::default()),
            key: None,
            stop: Arc::new(AtomicBool::new(false)),
            supervisor: None,
        }
    }

    /// Shared status: state, mode (for the system-audio badge), frame count, format.
    pub fn status(&self) -> Arc<CaptureStatus> {
        Arc::clone(&self.status)
    }

    pub fn target(&self) -> &CaptureTarget {
        &self.target
    }
}

/// How often the supervisor re-checks when nothing changed (also its stop latency).
const SUPERVISE_TICK: Duration = Duration::from_millis(100);
/// How long `stop` waits for the loop thread to close the stream.
const STOP_TIMEOUT: Duration = Duration::from_secs(1);

impl AudioSource for PipeWireSource {
    fn source(&self) -> Source {
        self.source
    }

    fn start(&mut self, sink: FrameSink) -> Result<(), AudioError> {
        if self.key.is_some() {
            return Err(AudioError::AlreadyStarted);
        }
        let key = self.ctx.next_key();
        let strict = matches!(
            self.target,
            CaptureTarget::App { .. } | CaptureTarget::SinkMonitor
        );
        self.ctx.send(Command::Open {
            key,
            spec: OpenSpec {
                node_name: format!("dmn.{}", self.source.label().to_ascii_lowercase()),
                strict,
                status: Arc::clone(&self.status),
            },
            sink,
        })?;
        self.key = Some(key);

        let ctx = Arc::clone(&self.ctx);
        let stop = Arc::clone(&self.stop);
        let status = Arc::clone(&self.status);
        let mut reattach = Reattach::new(self.target.clone());
        let handle = std::thread::Builder::new()
            .name(format!(
                "dmn-capture-{}",
                self.source.label().to_ascii_lowercase()
            ))
            .spawn(move || {
                let mut table = ctx.nodes();
                while !stop.load(Ordering::Acquire) {
                    if let Some(action) = reattach.step(&table) {
                        let cmd = match action {
                            Action::Connect {
                                target_object,
                                capture_sink,
                                mode,
                            } => {
                                status.set_mode(mode);
                                status.set_state(CaptureState::Connecting);
                                Command::Connect {
                                    key,
                                    target_object,
                                    capture_sink,
                                }
                            }
                            Action::Disconnect => {
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
        Ok(())
    }

    fn stop(&mut self) -> Result<(), AudioError> {
        self.stop.store(true, Ordering::Release);
        let panicked = self
            .supervisor
            .take()
            .map(|h| h.join().is_err())
            .unwrap_or(false);
        let mut timed_out = None;
        if let Some(key) = self.key.take()
            && self.ctx.send(Command::Close { key }).is_ok()
        {
            // Wait for the loop thread to destroy the stream, so that after `stop` returns
            // nothing writes into the ring any more. If the loop is gone, the stream went
            // with it.
            let t = std::time::Instant::now();
            while self.status.state() != CaptureState::Idle && t.elapsed() < STOP_TIMEOUT {
                std::thread::sleep(Duration::from_millis(2));
            }
            // Never claim a clean stop we did not get: the caller may be about to drop the
            // ring the callback writes into.
            timed_out = Some(self.status.state());
        }
        stop_result(panicked, timed_out, STOP_TIMEOUT)
    }

    fn is_finished(&self) -> bool {
        false
    }
}

impl Drop for PipeWireSource {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frames::frame_channel;
    use crate::pipewire::RegistryEvent;

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

    fn mic(t: &mut NodeTable, id: u32, name: &str) {
        t.apply(RegistryEvent::Added {
            id,
            props: props(&[("media.class", "Audio/Source"), ("node.name", name)]),
        });
    }

    fn synced() -> NodeTable {
        let mut t = NodeTable::default();
        t.apply(RegistryEvent::Synced);
        t
    }

    fn connect(target: Option<&str>, sink: bool, mode: CaptureMode) -> Option<Action> {
        Some(Action::Connect {
            target_object: target.map(str::to_owned),
            capture_sink: sink,
            mode,
        })
    }

    #[test]
    fn nothing_is_decided_before_the_first_enumeration() {
        let mut r = Reattach::new(CaptureTarget::interviewer("zoom"));
        let mut t = NodeTable::default();
        app(&mut t, 50, "zoom", "150");
        assert_eq!(r.step(&t), None, "no fallback decision before sync");
        t.apply(RegistryEvent::Synced);
        assert_eq!(
            r.step(&t),
            connect(Some("150"), false, CaptureMode::AppStream)
        );
    }

    #[test]
    fn app_stream_connects_by_serial_case_insensitively_and_only_once() {
        let mut r = Reattach::new(CaptureTarget::interviewer("firefox"));
        let mut t = synced();
        app(&mut t, 50, "Firefox", "150");
        assert_eq!(
            r.step(&t),
            connect(Some("150"), false, CaptureMode::AppStream)
        );
        assert_eq!(r.step(&t), None, "unchanged table, no action");
        // Unrelated streams come and go without touching the capture.
        app(&mut t, 51, "Spotify", "151");
        t.apply(RegistryEvent::Removed(51));
        assert_eq!(r.step(&t), None);
    }

    #[test]
    fn missing_app_falls_back_to_the_sink_monitor_then_switches_when_it_appears() {
        let mut r = Reattach::new(CaptureTarget::interviewer("zoom"));
        let mut t = synced();
        assert_eq!(r.step(&t), connect(None, true, CaptureMode::SinkMonitor));
        assert_eq!(r.step(&t), None);
        app(&mut t, 60, "zoom", "160");
        assert_eq!(
            r.step(&t),
            connect(Some("160"), false, CaptureMode::AppStream)
        );
    }

    #[test]
    fn app_that_disappears_disconnects_and_reattaches_by_application_name() {
        let mut r = Reattach::new(CaptureTarget::interviewer("teams-for-linux"));
        let mut t = synced();
        app(&mut t, 70, "teams-for-linux", "170");
        assert!(matches!(r.step(&t), Some(Action::Connect { .. })));

        // Call ends: the node is removed. Disconnect, do not fall back to system audio.
        t.apply(RegistryEvent::Removed(70));
        assert_eq!(r.step(&t), Some(Action::Disconnect));
        assert_eq!(r.step(&t), None, "stays disconnected");

        // App restarts with a new id and serial: reattach to it.
        app(&mut t, 90, "teams-for-linux", "190");
        assert_eq!(
            r.step(&t),
            connect(Some("190"), false, CaptureMode::AppStream)
        );
    }

    #[test]
    fn newest_stream_of_an_app_wins() {
        let mut r = Reattach::new(CaptureTarget::interviewer("Firefox"));
        let mut t = synced();
        app(&mut t, 10, "Firefox", "110");
        app(&mut t, 12, "Firefox", "112");
        assert_eq!(
            r.step(&t),
            connect(Some("112"), false, CaptureMode::AppStream)
        );
        // The older one going away changes nothing.
        t.apply(RegistryEvent::Removed(10));
        assert_eq!(r.step(&t), None);
    }

    #[test]
    fn app_without_fallback_waits_silently() {
        let mut r = Reattach::new(CaptureTarget::App {
            application: "zoom".into(),
            monitor_fallback: false,
        });
        let mut t = synced();
        assert_eq!(r.step(&t), None);
        app(&mut t, 5, "zoom", "105");
        assert_eq!(
            r.step(&t),
            connect(Some("105"), false, CaptureMode::AppStream)
        );
    }

    #[test]
    fn empty_app_stream_means_sink_monitor_connected_once() {
        let target = CaptureTarget::interviewer("");
        assert_eq!(target, CaptureTarget::SinkMonitor);
        let mut r = Reattach::new(target);
        let mut t = synced();
        assert_eq!(r.step(&t), connect(None, true, CaptureMode::SinkMonitor));
        app(&mut t, 1, "zoom", "101");
        assert_eq!(r.step(&t), None, "monitor mode ignores app streams");
    }

    #[test]
    fn default_mic_connects_without_a_target_once() {
        assert_eq!(CaptureTarget::mic("default"), CaptureTarget::Mic(None));
        let mut r = Reattach::new(CaptureTarget::mic("default"));
        let mut t = synced();
        assert_eq!(r.step(&t), connect(None, false, CaptureMode::Mic));
        mic(&mut t, 3, "usb-mic");
        t.apply(RegistryEvent::DefaultSource(Some("usb-mic".into())));
        assert_eq!(
            r.step(&t),
            None,
            "the session manager follows the default itself"
        );
    }

    #[test]
    fn named_mic_falls_back_to_default_then_follows_the_named_node() {
        let mut r = Reattach::new(CaptureTarget::mic("yeti"));
        let mut t = synced();
        assert_eq!(
            r.step(&t),
            connect(None, false, CaptureMode::Mic),
            "absent: default"
        );
        mic(&mut t, 8, "yeti");
        assert_eq!(r.step(&t), connect(Some("yeti"), false, CaptureMode::Mic));
        t.apply(RegistryEvent::Removed(8));
        assert_eq!(r.step(&t), Some(Action::Disconnect), "unplugged");
        mic(&mut t, 9, "yeti");
        assert_eq!(
            r.step(&t),
            connect(Some("yeti"), false, CaptureMode::Mic),
            "replugged"
        );
    }

    #[test]
    fn server_loss_disconnects_once() {
        let mut r = Reattach::new(CaptureTarget::interviewer("zoom"));
        let mut t = synced();
        app(&mut t, 4, "zoom", "104");
        assert!(r.step(&t).is_some());
        t.apply(RegistryEvent::Disconnected("server gone".into()));
        assert_eq!(r.step(&t), Some(Action::Disconnect));
        assert_eq!(r.step(&t), None);
    }

    #[test]
    fn accumulator_cuts_arbitrary_quanta_into_20ms_frames_in_order() {
        let status = Arc::new(CaptureStatus::default());
        let (sink, mut frames) = frame_channel(Source::Interviewer, 10);
        let mut acc = Accum::new(sink, Arc::clone(&status));
        // 3.5 frames of a ramp, delivered in PipeWire-ish uneven chunks (as LE bytes).
        let total = FRAME_LEN * 7 / 2;
        let ramp: Vec<f32> = (0..total).map(|i| i as f32).collect();
        let bytes: Vec<u8> = ramp.iter().flat_map(|s| s.to_le_bytes()).collect();
        for chunk in bytes.chunks(4 * 77) {
            acc.push_le_bytes(chunk);
        }
        assert_eq!(status.frames(), 3);
        let mut out = [0.0; FRAME_LEN];
        for f in 0..3 {
            assert!(frames.pop_frame(&mut out));
            assert_eq!(out[0], (f * FRAME_LEN) as f32);
            assert_eq!(out[FRAME_LEN - 1], ((f + 1) * FRAME_LEN - 1) as f32);
        }
        assert!(
            !frames.pop_frame(&mut out),
            "the half frame stays in the accumulator"
        );
        acc.push_samples(&vec![0.0; FRAME_LEN / 2]);
        assert!(frames.pop_frame(&mut out));
        assert_eq!(out[0], (3 * FRAME_LEN) as f32);
    }

    #[test]
    fn stop_reports_a_stream_that_never_went_idle() {
        // Copilot on PR #25: stop() returned Ok(()) after the timeout, so a caller could drop
        // the ring while the real-time callback was still writing into it.
        let t = Duration::from_millis(1000);
        assert!(stop_result(false, Some(CaptureState::Idle), t).is_ok());
        assert!(stop_result(false, None, t).is_ok(), "nothing to wait for");

        for state in [
            CaptureState::Streaming,
            CaptureState::Connecting,
            CaptureState::Error,
            CaptureState::Disconnected,
        ] {
            let err = stop_result(false, Some(state), t).unwrap_err();
            assert!(
                matches!(err, AudioError::StopTimeout { ms: 1000, .. }),
                "{state:?} -> {err}"
            );
            assert!(err.to_string().contains(state.label()), "{err}");
        }
        // A panicked supervisor still wins: it is the more serious failure.
        assert!(matches!(
            stop_result(true, Some(CaptureState::Streaming), t).unwrap_err(),
            AudioError::ThreadPanicked
        ));
    }

    #[test]
    fn the_callback_pushes_nothing_until_the_format_is_the_one_we_asked_for() {
        // Copilot on PR #25: a mismatch only set the status to Error while `process` kept
        // copying those buffers into the 16 kHz mono ring.
        let status = Arc::new(CaptureStatus::default());
        let (sink, mut frames) = frame_channel(Source::Interviewer, 10);
        let mut acc = Accum::new(sink, Arc::clone(&status));
        let samples = vec![0.5f32; FRAME_LEN * 2];

        // Fresh status: nothing negotiated yet, so the callback must drop.
        assert!(!status.format_ok(), "starts closed");
        for _ in 0..2 {
            if acc.format_ok() {
                acc.push_samples(&samples);
            } else {
                acc.count_bad_format();
            }
        }
        let mut out = [0.0; FRAME_LEN];
        assert!(
            !frames.pop_frame(&mut out),
            "no frames while the gate is shut"
        );
        assert_eq!(status.frames(), 0);
        assert_eq!(status.bad_format_buffers(), 2);

        // 16 kHz mono negotiated: the gate opens.
        status.set_format_ok(true);
        assert!(acc.format_ok());
        acc.push_samples(&samples);
        assert_eq!(status.frames(), 2);
        assert!(frames.pop_frame(&mut out));
        assert_eq!(out[0], 0.5);

        // A renegotiation to something else shuts it again.
        status.set_format_ok(false);
        assert!(!acc.format_ok());
        acc.count_bad_format();
        assert_eq!(status.bad_format_buffers(), 3);
        assert_eq!(status.frames(), 2, "no further frames were pushed");
    }

    #[test]
    fn enum_format_pod_serialises() {
        let pod = enum_format_pod(SAMPLE_RATE).unwrap();
        assert!(spa::pod::Pod::from_bytes(&pod).is_some());
    }

    #[test]
    fn status_reports_mode_and_badge() {
        let s = CaptureStatus::default();
        assert_eq!(s.state(), CaptureState::Idle);
        assert_eq!(s.mode(), CaptureMode::None);
        s.set_mode(CaptureMode::SinkMonitor);
        assert!(s.capturing_all_system_audio());
        s.set_error("boom".into());
        assert_eq!(s.state(), CaptureState::Error);
        assert_eq!(s.last_error().as_deref(), Some("boom"));
        assert_eq!(s.format(), None);
    }

    // ---- Live tests: need a running PipeWire server and pw-play. `just probe`. Each plays a
    // quiet 440 Hz tone (volume 0.1) through the default output for at most 2 s.

    mod live {
        use super::*;
        use std::process::{Child, Command as Proc};
        use std::time::Instant;

        /// Play `secs` of a 440 Hz tone as `app`. The tone is generated in memory and piped to
        /// pw-play as raw 16 kHz mono f32: no audio on disk, even in tests (CLAUDE.md).
        fn play(secs: f32, app: &str) -> Child {
            use std::io::Write as _;
            let mut child = Proc::new("pw-play")
                .args([
                    "--raw",
                    "--rate",
                    "16000",
                    "--channels",
                    "1",
                    "--format",
                    "f32",
                ])
                .args(["--volume", "0.1", "-P"])
                .arg(format!(
                    "{{ application.name = \"{app}\" media.name = \"dmn test tone\" }}"
                ))
                .arg("-")
                .stdin(std::process::Stdio::piped())
                .spawn()
                .expect("pw-play on PATH");
            let mut stdin = child.stdin.take().unwrap();
            let bytes: Vec<u8> = (0..(SAMPLE_RATE as f32 * secs) as usize)
                .flat_map(|i| {
                    let v =
                        (2.0 * std::f32::consts::PI * 440.0 * i as f32 / SAMPLE_RATE as f32).sin();
                    (0.5 * v).to_le_bytes()
                })
                .collect();
            // Larger than a pipe buffer; pw-play reads it as it plays. EOF ends playback.
            std::thread::spawn(move || {
                let _ = stdin.write_all(&bytes);
            });
            child
        }

        /// Drain frames for `dur`, returning them concatenated.
        fn collect(frames: &mut crate::Frames, dur: Duration) -> Vec<f32> {
            let mut all = Vec::new();
            let mut out = [0.0; FRAME_LEN];
            let t = Instant::now();
            while t.elapsed() < dur {
                while frames.pop_frame(&mut out) {
                    all.extend_from_slice(&out);
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            all
        }

        /// Goertzel power of `hz` in `w`.
        fn goertzel(w: &[f32], hz: f32) -> f32 {
            let k = 2.0 * (2.0 * std::f32::consts::PI * hz / SAMPLE_RATE as f32).cos();
            let (mut s1, mut s2) = (0.0f32, 0.0f32);
            for &x in w {
                let s0 = x + k * s1 - s2;
                s2 = s1;
                s1 = s0;
            }
            s1 * s1 + s2 * s2 - k * s1 * s2
        }

        /// RMS of the loudest 0.25 s window, and how much stronger 440 Hz is there than
        /// 300 Hz and 600 Hz.
        fn tone(samples: &[f32]) -> (f32, f32) {
            let win = SAMPLE_RATE as usize / 4;
            samples
                .chunks(win)
                .filter(|w| w.len() == win)
                .map(|w| {
                    let rms = (w.iter().map(|s| s * s).sum::<f32>() / w.len() as f32).sqrt();
                    let off = goertzel(w, 300.0).max(goertzel(w, 600.0)).max(1e-12);
                    (rms, goertzel(w, 440.0) / off)
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

        #[test]
        #[ignore]
        fn live_app_stream_capture_receives_the_tone() {
            let app = format!("dmn-test-{}", std::process::id());
            let ctx = Arc::new(PipeWireContext::connect().expect("PipeWire running"));
            assert!(ctx.wait_for_sync(Duration::from_secs(2)).synced);

            let mut player = play(2.0, &app);
            let mut src = PipeWireSource::new(
                Arc::clone(&ctx),
                Source::Interviewer,
                CaptureTarget::App {
                    application: app.clone(),
                    monitor_fallback: false,
                },
            );
            let status = src.status();
            let (sink, mut frames) = crate::frame_channel(Source::Interviewer, 200);
            src.start(sink).unwrap();
            wait_for("streaming", Duration::from_secs(2), || {
                status.state() == CaptureState::Streaming
            });
            assert_eq!(status.mode(), CaptureMode::AppStream);
            assert!(!status.capturing_all_system_audio());
            let got = collect(&mut frames, Duration::from_millis(1000));
            let _ = player.wait();
            src.stop().unwrap();

            assert_eq!(
                status.format(),
                Some((SAMPLE_RATE, 1)),
                "negotiated 16 kHz mono"
            );
            let (rms, ratio) = tone(&got);
            assert!(rms > 0.01, "tone reached the capture: rms {rms}");
            assert!(ratio > 100.0, "440 Hz dominates: ratio {ratio}");
            assert_eq!(frames.overruns(), 0);
        }

        #[test]
        #[ignore]
        fn live_app_stream_disconnects_and_reattaches_when_the_app_restarts() {
            let app = format!("dmn-reattach-{}", std::process::id());
            let ctx = Arc::new(PipeWireContext::connect().expect("PipeWire running"));
            assert!(ctx.wait_for_sync(Duration::from_secs(2)).synced);

            let mut src = PipeWireSource::new(
                Arc::clone(&ctx),
                Source::Interviewer,
                CaptureTarget::App {
                    application: app.clone(),
                    monitor_fallback: false,
                },
            );
            let status = src.status();
            let (sink, mut frames) = crate::frame_channel(Source::Interviewer, 400);
            src.start(sink).unwrap();

            // The mic runs alongside and must not notice the app coming and going. Its frames
            // are counted, never kept.
            let mut mic =
                PipeWireSource::new(Arc::clone(&ctx), Source::You, CaptureTarget::mic("default"));
            let mic_status = mic.status();
            let (mic_sink, mut mic_frames) = crate::frame_channel(Source::You, 400);
            mic.start(mic_sink).unwrap();
            wait_for("mic streaming", Duration::from_secs(2), || {
                mic_status.state() == CaptureState::Streaming
            });

            // First run of the "app".
            let mut first = play(1.0, &app);
            wait_for("first stream", Duration::from_secs(2), || {
                status.state() == CaptureState::Streaming
            });
            let _ = first.wait();
            let before = status.frames();
            wait_for(
                "disconnect after the app exits",
                Duration::from_secs(2),
                || status.state() == CaptureState::Disconnected,
            );
            let mic_at_disconnect = mic_status.frames();
            let _ = collect(&mut frames, Duration::from_millis(300));
            let _ = collect(&mut mic_frames, Duration::from_millis(1));
            assert!(
                mic_status.frames() >= mic_at_disconnect + 10,
                "mic kept running while the app was gone ({} -> {})",
                mic_at_disconnect,
                mic_status.frames()
            );
            assert_eq!(mic_status.state(), CaptureState::Streaming);

            // The app comes back as a new node with the same application.name.
            let mut second = play(1.0, &app);
            wait_for("reattach", Duration::from_secs(2), || {
                status.state() == CaptureState::Streaming
            });
            let got = collect(&mut frames, Duration::from_millis(700));
            let _ = second.wait();
            src.stop().unwrap();
            mic.stop().unwrap();

            assert!(before > 0, "frames flowed on the first run");
            let (rms, ratio) = tone(&got);
            assert!(rms > 0.01, "audio flows after reattach: rms {rms}");
            assert!(ratio > 100.0, "440 Hz dominates: ratio {ratio}");
        }

        /// Opens the default mic for 0.5 s and only counts frames; nothing is kept.
        #[test]
        #[ignore]
        fn live_default_mic_delivers_frames() {
            let ctx = Arc::new(PipeWireContext::connect().expect("PipeWire running"));
            assert!(ctx.wait_for_sync(Duration::from_secs(2)).synced);
            let mut src =
                PipeWireSource::new(Arc::clone(&ctx), Source::You, CaptureTarget::mic("default"));
            let status = src.status();
            let (sink, mut frames) = crate::frame_channel(Source::You, 100);
            src.start(sink).unwrap();
            wait_for("mic streaming", Duration::from_secs(2), || {
                status.state() == CaptureState::Streaming
            });
            let n = collect(&mut frames, Duration::from_millis(500)).len() / FRAME_LEN;
            assert_eq!(status.mode(), CaptureMode::Mic);
            src.stop().unwrap();
            assert!(n >= 15, "about 25 frames in 0.5 s, got {n}");
            assert_eq!(status.state(), CaptureState::Idle, "stop closes the stream");
            assert_eq!(status.mode(), CaptureMode::None);
        }
    }
}
