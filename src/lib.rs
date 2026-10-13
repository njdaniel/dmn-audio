//! Audio capture, playback to a sink, and the virtual microphone.
//!
//! Input: a PipeWire node (mic `Audio/Source` or an app's `Stream/Output/Audio`) or a WAV file,
//! and for [`MicCapture`] a microphone and a sample rate the caller picks;
//! for the virtual microphone (#108), mono f32 speech at any rate (the TTS server's 24 kHz),
//! and (#114) the user's own microphone, captured again at the graph rate for pass-through;
//! for [`Playback`], mono f32 at a rate the caller picks, written by the caller into a ring, and
//! the sink to play it to (the default one, or one named by its `node.name`).
//! Output: 16 kHz mono f32 samples, one labelled stream per source (You / Interviewer); or, from
//! [`MicCapture`], unlabelled mono f32 at the requested rate in a lock-free ring; for the
//! virtual microphone, a PipeWire `Audio/Source` node named `dmn-voice` that plays that speech,
//! and the microphone while pass-through is on and nothing is being spoken, at the graph rate
//! to whatever selects it as its microphone; from [`Playback`], that audio played at the
//! requested rate to the sink, silence when the ring runs dry, with the sinks listed in
//! [`NodeTable`].
//! Owns: the PipeWire main-loop thread, one lock-free `ringbuf` producer per capture stream
//! (and one per [`MicCapture`], with its supervisor thread), the lock-free ring a [`Playback`]'s
//! callback reads (with its supervisor thread),
//! the lock-free ring the virtual microphone's callback reads, and the lock-free ring between
//! the pass-through's capture callback and that same playback callback.
//! The process callbacks allocate nothing, lock nothing, log nothing.
//!
//! Shape (issue #3): an [`AudioSource`] is started with a [`FrameSink`] and pushes 20 ms frames
//! ([`FRAME_LEN`] samples at [`SAMPLE_RATE`]) into an SPSC ring; the consumer drains them through
//! [`Frames`] on its own thread. [`WavSource`] is the file-backed implementation every later
//! stage is tested against. [`PipeWireContext`] (issue #4) runs the PipeWire loop thread and
//! keeps the live [`NodeTable`] of mics and app streams. [`PipeWireSource`] (issue #5) captures a
//! mic, one app's output stream (by `target.object`) or the default sink monitor on that loop
//! thread, and re-attaches when the app restarts; [`PwRecordSource`] is the `pw-record`
//! emergency fallback behind the same trait. [`VirtualMic`] (#108) is the first output: samples
//! written to it are resampled by [`LinearResampler`] on the writer's thread into a ring the
//! playback callback drains; an underrun is silence, never a stall. With a [`Passthrough`]
//! (#114) the playback callback's [`Mixer`] also pops the microphone's ring, filled by a second
//! capture stream's callback, under a gain that fades at every switch and is shut while speech
//! plays. [`MicCapture`] is the microphone at a rate of the caller's choosing: no 20 ms framing
//! and no [`Source`] label, samples popped from a [`MicReader`]. PipeWire converts to the rate
//! (the format pod asks for it), the callback copies nothing until the negotiated format is
//! that rate, and a stream that settles elsewhere is a [`MicError::RateMismatch`], never samples
//! at another rate. It follows hot-plug with the same [`Reattach`] as [`PipeWireSource`]; a full
//! ring drops the new samples and counts them in [`MicStatus`].
//!
//! [`Playback`] (issue #2) is the other direction at a rate of the caller's choosing: samples
//! written to a [`PlaybackWriter`] are played by an ordinary playback stream that PipeWire
//! links to the default sink, or to a sink named by [`SinkTarget::Named`]. The format pod asks
//! for the rate and the callback plays nothing until the negotiated format is that rate, mono
//! f32; a stream that settles elsewhere is a [`PlaybackError::RateMismatch`]. An underrun is
//! silence and a count in [`PlaybackStatus`]. [`SinkFollow`] reconnects a named sink that
//! returns (it never falls back to the default); the default sink is followed by the session
//! manager. [`NodeTable::sinks`] and [`NodeTable::default_playback`] list outputs and mark the
//! default.

// CLAUDE.md: no unwrap/expect outside tests and main.
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

mod capture;
mod frames;
mod mic;
mod pipewire;
mod playback;
mod pwrecord;
mod source;
#[cfg(test)]
mod testutil;
mod vmic;
mod wav;

pub use capture::{
    Action, CaptureMode, CaptureState, CaptureStatus, CaptureTarget, PipeWireSource, Reattach,
};
pub use frames::{FrameSink, Frames, frame_channel};
pub use mic::{
    DEFAULT_MIC_RING_MS, MIC_MAX_RATE, MIC_MIN_RATE, MIC_NODE_NAME, MicCapture, MicError,
    MicReader, MicStatus, RatePlan, plan_rate,
};
pub use pipewire::{
    AppStream, CaptureNode, DEFAULT_GRAPH_RATE, NodeTable, PipeWireContext, PipeWireError,
    RegistryEvent, SinkNode, parse_default_name, parse_rate,
};
pub use playback::{
    DEFAULT_PLAYBACK_RING_MS, PLAYBACK_MAX_RATE, PLAYBACK_MIN_RATE, PLAYBACK_NODE_NAME, Playback,
    PlaybackError, PlaybackStatus, PlaybackWriter, SinkAction, SinkFollow, SinkTarget,
};
pub use pwrecord::PwRecordSource;
pub use source::{AudioError, AudioSource, Source};
pub use vmic::{
    DEFAULT_RING_SECS, LinearResampler, Mixer, PASSTHROUGH_FADE_MS, Passthrough, PassthroughStats,
    RingStats, TAP_MAX_BACKLOG_MS, TAP_NODE_NAME, TAP_TARGET_BACKLOG_MS, TapReader, TapStats,
    TapWriter, VMIC_DESCRIPTION, VMIC_NODE_NAME, VirtualMic, VmicState, VmicStats, VmicStatus,
    VoiceReader, VoiceWriter, tap_ring, voice_ring,
};
pub use wav::{Speed, WavSource};

/// Sample rate every source delivers, in Hz.
pub const SAMPLE_RATE: u32 = 16_000;
/// Frame length in milliseconds.
pub const FRAME_MS: u32 = 20;
/// Samples per frame: 16 kHz × 20 ms.
pub const FRAME_LEN: usize = (SAMPLE_RATE * FRAME_MS / 1000) as usize;
/// Ring capacity handed to a source by default: 2 s of audio, so an overrun means the consumer
/// stalled for 2 s, not that it was briefly late.
pub const DEFAULT_CAPACITY_FRAMES: usize = 100;
