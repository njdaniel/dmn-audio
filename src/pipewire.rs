//! PipeWire discovery: one OS thread runs the PipeWire main loop; a registry listener keeps a
//! live [`NodeTable`] of capture sources, playback sinks and app output streams (docs/SPEC.md § Audio capture on
//! PipeWire, Discovery). The same loop thread owns the capture streams of
//! [`crate::PipeWireSource`] (issue #5), which it creates, retargets and closes on
//! [`Command`]s, the playback stream behind [`crate::VirtualMic`] (#108), the one behind
//! [`crate::Playback`], and the per-application capture streams of [`crate::StreamCapture`].
//!
//! Who a stream belongs to (issue #8): app stream nodes are bound for their full info, and so
//! are PipeWire *clients*, because `pipewire.sec.pid` is set by the protocol on the client
//! object; a node reaches its client through its `client.id`. [`NodeTable::stream_metadata`]
//! joins the two: the security pid comes from the client only (a node's own
//! `pipewire.sec.pid` is application supplied and ignored), the claimed process id says where
//! it came from ([`PropertyOrigin`]). For a
//! PulseAudio-API application the owning client is the per-connection client `pipewire-pulse`
//! makes, so its `pipewire.sec.pid` is the pid of `pipewire-pulse`, not of the application;
//! the application's own pid is in `application.process.id`, which the application (or
//! `pipewire-pulse` on its behalf) sets and which is therefore not authenticated.
//!
//! Threading: everything PipeWire-owned (`MainLoopRc`, `CoreRc`, `RegistryRc`, proxies,
//! listeners) lives on the loop thread and is `!Send`; the table is shared through an
//! `Arc<Mutex<_>>` plus a `Condvar`, and the table's `generation` counter is the change
//! signal. `dmn-session`'s tokio `watch` mirrors that generation later; this crate never depends
//! on tokio. Shutdown sends [`Command::Quit`] over a [`pipewire::channel`], the loop thread
//! quits its own `MainLoop`, and `Drop` joins it.
//!
//! No `unsafe` here: the `pipewire` crate's `Rc` API covers everything discovery needs. The one
//! `unsafe` call capture needs lives in `capture.rs`.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use pipewire as pw;
use pw::client::Client;
use pw::metadata::Metadata;
use pw::node::Node;
use pw::proxy::Listener;
use pw::types::ObjectType;

use crate::capture::{LoopCapture, OpenSpec};
use crate::frames::FrameSink;
use crate::mic::{LoopMic, MicSpec, MicWriter};
use crate::playback::{LoopPlayback, PlaybackReader, PlaybackSpec};
use crate::stream::{LoopStream, StreamSpec, StreamWriter, resolve_serial};
use crate::vmic::{LoopVmic, Mixer, VmicSpec};

#[derive(Debug, thiserror::Error)]
pub enum PipeWireError {
    /// libpipewire refused to start a loop or context (library missing at runtime, etc.).
    #[error("PipeWire init: {0}")]
    Init(String),
    /// No server on the socket. `dmn doctor` prints the fix.
    #[error("cannot connect to the PipeWire server: {0}")]
    Connect(String),
    #[error("PipeWire loop thread ended before reporting its state")]
    ThreadGone,
    /// The loop thread no longer accepts commands (it quit or the server went away).
    #[error("PipeWire loop thread is not accepting commands")]
    LoopGone,
}

/// A microphone or other capture node (`media.class = Audio/Source`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaptureNode {
    /// Registry global id; valid until the node is removed.
    pub id: u32,
    /// `node.name`, the stable identifier config refers to.
    pub name: String,
    /// `node.description`, what the picker shows.
    pub description: String,
}

/// An output device or other playback node (`media.class = Audio/Sink`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkNode {
    /// Registry global id; valid until the node is removed.
    pub id: u32,
    /// `node.name`, the stable identifier config refers to and `target.object` takes.
    pub name: String,
    /// `node.description`, what the picker shows.
    pub description: String,
}

/// An application's playback stream (`media.class = Stream/Output/Audio`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppStream {
    pub id: u32,
    /// `application.name`, e.g. "Firefox", "zoom".
    pub application: String,
    /// `media.name`, e.g. "Google Meet" or "Playback".
    pub media_name: String,
    /// `node.name`. Not unique across apps with several streams, so capture prefers `serial`.
    pub node_name: String,
    /// `object.serial`, the preferred `target.object` value (unique for the node's lifetime).
    pub serial: Option<String>,
}

/// Where a value in [`StreamMetadata`] was read from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PropertyOrigin {
    /// The `Stream/Output/Audio` node's own properties.
    Node,
    /// The properties of the PipeWire client that owns the node (the node's `client.id`).
    Client,
}

/// Who an application stream belongs to, as [`NodeTable::stream_metadata`] reports it (#8).
/// Kept apart from [`AppStream`] so that type's fields, and code that builds it, stay as they
/// are. Every value is an `Option`: PipeWire sets none of them for certain.
///
/// Provenance, which matters for any rule built on these:
///
/// - `sec_pid` (`pipewire.sec.pid`) is **only ever read from the owning client** (the node's
///   `client.id`). The protocol sets it on the client object from the peer credentials of its
///   socket; it cannot be forged by the application. A node carrying a property of the same
///   name is application supplied (a live probe showed a node with a forged
///   `pipewire.sec.pid=42` while its client held the real pid), so that value is never read,
///   never preferred and never used as a fallback: with no client known (absent, removed,
///   or the node names none) `sec_pid` is `None`. It names the process holding the
///   connection; for a PulseAudio-API application that is `pipewire-pulse`, not the
///   application.
/// - `application.process.id` is set by the application (or by `pipewire-pulse` from the
///   PulseAudio client's own property list); it names the application for a PulseAudio-API
///   stream too, but it is a claim, not a credential. Read from the node, else from the
///   client. Never use it for a security decision.
/// - `application_name` is the node's `application.name` exactly as set, and `None` when the
///   node has none (or an empty one): [`AppStream::application`] then holds a fallback
///   (`application.process.binary`, or `"unknown"`), which a caller matching on the name
///   must not mistake for the name.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct StreamMetadata {
    /// Registry global id of the stream node, as [`AppStream::id`].
    pub id: u32,
    /// `object.serial` as a number, the only thing [`crate::StreamCapture`] targets.
    pub serial: Option<u64>,
    /// `client.id` of the node: the registry id of the client that owns it.
    pub client_id: Option<u32>,
    /// The node's `application.name`, `None` when it was not really set.
    pub application_name: Option<String>,
    /// `application.process.id`: the pid the application claims.
    pub application_process_id: Option<u32>,
    /// Where `application_process_id` came from.
    pub application_process_id_origin: Option<PropertyOrigin>,
    /// The owning client's `pipewire.sec.pid`, the trusted pid of the process holding the
    /// PipeWire connection (for a PulseAudio-API application, `pipewire-pulse`). `None` when
    /// the client is unknown or has none; a node's own property of that name is ignored.
    pub sec_pid: Option<u32>,
}

/// Key the loop thread adds, with the value `"true"`, to the properties of a
/// [`RegistryEvent::Added`] for a PipeWire *client* global, so [`NodeTable::apply`] can tell a
/// client from a node without a new event variant. It is not a PipeWire property. A later
/// [`RegistryEvent::Updated`] or [`RegistryEvent::Removed`] for that id reaches the client by
/// its id (registry ids are unique among live globals).
pub const REGISTRY_CLIENT_KEY: &str = "dmn.registry.client";

/// What a stream node itself says about its owner. Values are merged as they arrive: the
/// registry global carries a subset of the node's properties, its bound info the rest.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct NodeOwner {
    client_id: Option<u32>,
    application_name: Option<String>,
    process_id: Option<u32>,
}

/// What a client says about itself.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ClientOwner {
    process_id: Option<u32>,
    sec_pid: Option<u32>,
}

fn prop<'a>(props: &'a [(String, String)], key: &str) -> Option<&'a str> {
    props
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
}

/// A registry id such as `client.id`.
fn parse_id(v: &str) -> Option<u32> {
    v.trim().parse().ok()
}

/// A process id; 0 is no process.
fn parse_pid(v: &str) -> Option<u32> {
    parse_id(v).filter(|&p| p != 0)
}

/// A name that is really there: absent, empty and blank are all "not set".
fn parse_name(v: &str) -> Option<String> {
    (!v.trim().is_empty()).then(|| v.to_owned())
}

/// Fold property `key` into `slot`. An absent key preserves the slot (a partial update); a
/// key that is present replaces it, and one whose value does not parse (empty, malformed,
/// zero pid) clears it. True when the slot changed.
fn merge_key<T: PartialEq>(
    slot: &mut Option<T>,
    props: &[(String, String)],
    key: &str,
    parse: impl Fn(&str) -> Option<T>,
) -> bool {
    let Some(raw) = prop(props, key) else {
        return false;
    };
    let v = parse(raw);
    if *slot == v {
        return false;
    }
    *slot = v;
    true
}

/// `object.serial` as a number.
fn parse_serial(v: Option<&str>) -> Option<u64> {
    v?.trim().parse().ok()
}

impl NodeOwner {
    fn merge(&mut self, props: &[(String, String)]) -> bool {
        // `pipewire.sec.pid` on a node is application supplied: deliberately not read.
        let mut changed = merge_key(&mut self.client_id, props, "client.id", parse_id);
        changed |= merge_key(
            &mut self.application_name,
            props,
            "application.name",
            parse_name,
        );
        changed |= merge_key(
            &mut self.process_id,
            props,
            "application.process.id",
            parse_pid,
        );
        changed
    }
}

impl ClientOwner {
    fn merge(&mut self, props: &[(String, String)]) -> bool {
        let mut changed = merge_key(
            &mut self.process_id,
            props,
            "application.process.id",
            parse_pid,
        );
        changed |= merge_key(&mut self.sec_pid, props, "pipewire.sec.pid", parse_pid);
        changed
    }
}

/// Whether any tracked stream node names `client` as its owner: only then does a change to the
/// client change what [`NodeTable::stream_metadata`] reports.
fn client_referenced(owners: &BTreeMap<u32, NodeOwner>, client: u32) -> bool {
    owners.values().any(|o| o.client_id == Some(client))
}

/// The node's claim when it has one, else the client's, with where it came from. Only for
/// untrusted claims such as `application.process.id`, never for `pipewire.sec.pid`.
fn pick(node: Option<u32>, client: Option<u32>) -> (Option<u32>, Option<PropertyOrigin>) {
    match (node, client) {
        (Some(v), _) => (Some(v), Some(PropertyOrigin::Node)),
        (None, Some(v)) => (Some(v), Some(PropertyOrigin::Client)),
        (None, None) => (None, None),
    }
}

/// One registry event, decoupled from libpipewire so the table logic is testable without a
/// server. The loop thread produces these; [`NodeTable::apply`] consumes them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistryEvent {
    /// A global appeared with these properties. Nodes are classified by `media.class`; a
    /// client is recognised by [`REGISTRY_CLIENT_KEY`]; everything else is ignored.
    Added {
        id: u32,
        props: Vec<(String, String)>,
    },
    /// A bound node delivered its full info. The registry global only carries a subset of the
    /// node's properties (`media.name`, for one, is missing), so these are merged in.
    Updated {
        id: u32,
        props: Vec<(String, String)>,
    },
    /// The global with this id went away.
    Removed(u32),
    /// The `default.audio.source` metadata changed; `None` clears it.
    DefaultSource(Option<String>),
    /// The `default.audio.sink` metadata changed; `None` clears it.
    DefaultSink(Option<String>),
    /// The `settings` metadata's `clock.rate` (the graph's sample rate) changed; `None` clears
    /// it. `forced` carries `clock.force-rate`, which wins while it is set.
    ClockRate { rate: Option<u32>, forced: bool },
    /// The initial registry enumeration finished (core `done` for our sync).
    Synced,
    /// The server connection is gone; the table is frozen from now on.
    Disconnected(String),
}

/// Live view of what PipeWire offers. Cheap to clone; read through
/// [`PipeWireContext::nodes`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NodeTable {
    sources: BTreeMap<u32, CaptureNode>,
    streams: BTreeMap<u32, AppStream>,
    sinks: BTreeMap<u32, SinkNode>,
    /// Owner properties of each app stream, by the stream's id (#8).
    stream_owners: BTreeMap<u32, NodeOwner>,
    /// PipeWire clients, by registry id: what a stream's `client.id` points at (#8).
    clients: BTreeMap<u32, ClientOwner>,
    /// `node.name` of PipeWire's default source, from the `default` metadata object.
    pub default_source: Option<String>,
    /// `node.name` of PipeWire's default sink, from the same `default` metadata object.
    pub default_sink: Option<String>,
    /// The graph's sample rate (`clock.rate` in the `settings` metadata), if seen.
    pub clock_rate: Option<u32>,
    /// `clock.force-rate`, if set and non-zero: the rate the graph is pinned to.
    pub force_rate: Option<u32>,
    /// True once the first enumeration is complete.
    pub synced: bool,
    /// Set when the server went away; the table stops changing.
    pub disconnected: Option<String>,
    /// Incremented on every change; the change signal downstream watches.
    pub generation: u64,
}

impl NodeTable {
    /// Capture sources, ordered by id.
    pub fn sources(&self) -> impl Iterator<Item = &CaptureNode> {
        self.sources.values()
    }

    /// App output streams, ordered by id.
    pub fn streams(&self) -> impl Iterator<Item = &AppStream> {
        self.streams.values()
    }

    /// Playback sinks (`Audio/Sink`), ordered by id.
    pub fn sinks(&self) -> impl Iterator<Item = &SinkNode> {
        self.sinks.values()
    }

    /// Who the app stream with registry id `id` belongs to; `None` when no such stream is in
    /// the table. Joined from the node and its owning client each time it is asked, so it
    /// follows both as their properties arrive, change and go; see [`StreamMetadata`] for
    /// where each value comes from and what it can be trusted for.
    pub fn stream_metadata(&self, id: u32) -> Option<StreamMetadata> {
        let stream = self.streams.get(&id)?;
        Some(self.metadata_of(stream))
    }

    /// App output streams with their [`StreamMetadata`], ordered by id.
    pub fn streams_with_metadata(&self) -> impl Iterator<Item = (&AppStream, StreamMetadata)> {
        let all = self.streams.values();
        all.map(move |s| (s, self.metadata_of(s)))
    }

    /// The join behind [`Self::stream_metadata`]: the process id claim from the node, else
    /// its client; the security pid from the owning client only.
    fn metadata_of(&self, stream: &AppStream) -> StreamMetadata {
        let id = stream.id;
        let node = self.stream_owners.get(&id).cloned().unwrap_or_default();
        let owner = node.client_id.and_then(|c| self.clients.get(&c));
        let client = owner.cloned().unwrap_or_default();
        let (application_process_id, application_process_id_origin) =
            pick(node.process_id, client.process_id);
        let sec_pid = client.sec_pid;
        StreamMetadata {
            id,
            serial: parse_serial(stream.serial.as_deref()),
            client_id: node.client_id,
            application_name: node.application_name,
            application_process_id,
            application_process_id_origin,
            sec_pid,
        }
    }

    /// The app output stream (`Stream/Output/Audio`) whose `object.serial` is `serial`, if it
    /// is in the table. Only app streams are searched: a sink or source with that serial is
    /// never returned.
    pub fn stream_by_serial(&self, serial: u64) -> Option<&AppStream> {
        let has = |s: &&AppStream| parse_serial(s.serial.as_deref()) == Some(serial);
        self.streams.values().find(has)
    }

    /// Whether a tracked source, sink or app stream other than `except_id` has the `node.name`
    /// `name`. [`crate::StreamCapture`] refuses a serial that some node is also named, since
    /// `target.object` matches names as well as serials.
    pub(crate) fn node_named(&self, name: &str, except_id: u32) -> bool {
        let source = self.sources.values().any(|n| n.name == name);
        let sink = self.sinks.values().any(|n| n.name == name);
        let named = |s: &AppStream| s.id != except_id && s.node_name == name;
        source || sink || self.streams.values().any(named)
    }

    /// The default sink, if its metadata was seen and it is in the table.
    pub fn default_playback(&self) -> Option<&SinkNode> {
        let name = self.default_sink.as_deref()?;
        self.sinks.values().find(|n| n.name == name)
    }

    /// The default capture source, if its metadata was seen and it is in the table.
    pub fn default_capture(&self) -> Option<&CaptureNode> {
        let name = self.default_source.as_deref()?;
        self.sources.values().find(|n| n.name == name)
    }

    /// The rate the graph runs at: `clock.force-rate` when set, else `clock.rate`, else
    /// PipeWire's own default of 48 kHz.
    pub fn graph_rate(&self) -> u32 {
        self.force_rate
            .or(self.clock_rate)
            .unwrap_or(DEFAULT_GRAPH_RATE)
    }

    /// Fold one event in. Returns true when something observable changed.
    pub fn apply(&mut self, event: RegistryEvent) -> bool {
        if self.disconnected.is_some() {
            return false;
        }
        let changed = match event {
            RegistryEvent::Added { id, props } => {
                let get = |key: &str| {
                    props
                        .iter()
                        .find(|(k, _)| k == key)
                        .map(|(_, v)| v.as_str())
                };
                match get("media.class") {
                    Some("Audio/Source") => {
                        let name = get("node.name").unwrap_or_default().to_owned();
                        let description = get("node.description")
                            .or_else(|| get("node.nick"))
                            .unwrap_or(&name)
                            .to_owned();
                        self.sources.insert(
                            id,
                            CaptureNode {
                                id,
                                name,
                                description,
                            },
                        );
                        true
                    }
                    Some("Audio/Sink") => {
                        let name = get("node.name").unwrap_or_default().to_owned();
                        let description = get("node.description")
                            .or_else(|| get("node.nick"))
                            .unwrap_or(&name)
                            .to_owned();
                        self.sinks.insert(
                            id,
                            SinkNode {
                                id,
                                name,
                                description,
                            },
                        );
                        true
                    }
                    Some("Stream/Output/Audio") => {
                        let application = get("application.name")
                            .or_else(|| get("application.process.binary"))
                            .unwrap_or("unknown")
                            .to_owned();
                        let media_name = get("media.name").unwrap_or_default().to_owned();
                        let node_name = get("node.name").unwrap_or_default().to_owned();
                        let serial = get("object.serial").map(str::to_owned);
                        self.streams.insert(
                            id,
                            AppStream {
                                id,
                                application,
                                media_name,
                                node_name,
                                serial,
                            },
                        );
                        let mut owner = NodeOwner::default();
                        owner.merge(&props);
                        self.stream_owners.insert(id, owner);
                        true
                    }
                    _ if get(REGISTRY_CLIENT_KEY) == Some("true") => {
                        let mut client = ClientOwner::default();
                        client.merge(&props);
                        let before = self.clients.insert(id, client.clone());
                        before.unwrap_or_default() != client
                            && client_referenced(&self.stream_owners, id)
                    }
                    _ => false,
                }
            }
            RegistryEvent::Updated { id, props } => {
                let get = |key: &str| {
                    props
                        .iter()
                        .find(|(k, _)| k == key)
                        .map(|(_, v)| v.as_str())
                };
                let mut changed = false;
                if let Some(s) = self.streams.get_mut(&id) {
                    for (field, key) in [
                        (&mut s.application, "application.name"),
                        (&mut s.media_name, "media.name"),
                        (&mut s.node_name, "node.name"),
                    ] {
                        if let Some(v) = get(key)
                            && field != v
                        {
                            *field = v.to_owned();
                            changed = true;
                        }
                    }
                    if let Some(owner) = self.stream_owners.get_mut(&id)
                        && owner.merge(&props)
                    {
                        changed = true;
                    }
                } else if self.clients.contains_key(&id) {
                    let referenced = client_referenced(&self.stream_owners, id);
                    let merged = self.clients.get_mut(&id).is_some_and(|c| c.merge(&props));
                    changed = merged && referenced;
                } else if let Some(n) = self.sources.get_mut(&id)
                    && let Some(d) = get("node.description")
                    && n.description != d
                {
                    n.description = d.to_owned();
                    changed = true;
                } else if let Some(n) = self.sinks.get_mut(&id)
                    && let Some(d) = get("node.description")
                    && n.description != d
                {
                    n.description = d.to_owned();
                    changed = true;
                }
                changed
            }
            RegistryEvent::Removed(id) => {
                self.stream_owners.remove(&id);
                // A client that goes takes what it gave the streams it owned with it.
                let removed = self.clients.remove(&id).unwrap_or_default();
                let client_gone =
                    removed != ClientOwner::default() && client_referenced(&self.stream_owners, id);
                self.sources.remove(&id).is_some()
                    | self.streams.remove(&id).is_some()
                    | self.sinks.remove(&id).is_some()
                    | client_gone
            }
            RegistryEvent::DefaultSource(name) => {
                let changed = self.default_source != name;
                self.default_source = name;
                changed
            }
            RegistryEvent::DefaultSink(name) => {
                let changed = self.default_sink != name;
                self.default_sink = name;
                changed
            }
            RegistryEvent::ClockRate { rate, forced } => {
                let slot = if forced {
                    &mut self.force_rate
                } else {
                    &mut self.clock_rate
                };
                let changed = *slot != rate;
                *slot = rate;
                changed
            }
            RegistryEvent::Synced => {
                let changed = !self.synced;
                self.synced = true;
                changed
            }
            RegistryEvent::Disconnected(why) => {
                self.disconnected = Some(why);
                true
            }
        };
        if changed {
            self.generation += 1;
        }
        changed
    }
}

/// Extract `name` from the JSON value of a `default.audio.source` metadata entry, which looks
/// like `{ "name": "alsa_input.pci-0000_00_1f.3.analog-stereo" }`. Hand-parsed: the shape is
/// fixed and dmn-audio has no JSON dependency.
pub fn parse_default_name(value: &str) -> Option<String> {
    let after_key = value.split_once("\"name\"")?.1;
    let after_colon = after_key.split_once(':')?.1;
    let start = after_colon.find('"')? + 1;
    let rest = &after_colon[start..];
    let end = rest.find('"')?;
    let name = &rest[..end];
    (!name.is_empty()).then(|| name.to_owned())
}

/// PipeWire's default `clock.rate` when the `settings` metadata does not say.
pub const DEFAULT_GRAPH_RATE: u32 = 48_000;

/// Parse a `settings` metadata rate value (`"48000"`, or a JSON number). Zero, which is how
/// `clock.force-rate` says "not forced", and anything unparseable are `None`.
pub fn parse_rate(value: &str) -> Option<u32> {
    let rate: u32 = value.trim().trim_matches('"').parse().ok()?;
    (rate != 0).then_some(rate)
}

struct Shared {
    table: Mutex<NodeTable>,
    changed: Condvar,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, NodeTable> {
        // A poisoned table means the loop thread panicked mid-update; the data is still a
        // consistent snapshot of plain values, so keep serving it.
        self.table.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn apply(&self, event: RegistryEvent) {
        let mut t = self.lock();
        if t.apply(event) {
            self.changed.notify_all();
        }
    }
}

/// Work for the loop thread. Everything PipeWire-owned is created and touched there.
pub(crate) enum Command {
    Quit,
    /// Create a capture stream (not connected yet) under `key`.
    Open {
        key: u64,
        spec: OpenSpec,
        sink: FrameSink,
    },
    /// Create a microphone capture stream (not connected yet) under `key`. Connect, Disconnect
    /// and Close then reach it by the same key as any other capture stream.
    OpenMic {
        key: u64,
        spec: MicSpec,
        writer: MicWriter,
    },
    /// Retarget and (re)connect the stream under `key`.
    Connect {
        key: u64,
        target_object: Option<String>,
        capture_sink: bool,
    },
    /// Disconnect the stream under `key`; it can be connected again.
    Disconnect {
        key: u64,
    },
    /// Disconnect and destroy the stream under `key`.
    Close {
        key: u64,
    },
    /// Create and connect the virtual microphone's playback stream under `key` (#108), and
    /// the pass-through's capture stream when the spec has one (#114).
    OpenVmic {
        key: u64,
        spec: VmicSpec,
        mixer: Mixer,
    },
    /// Disconnect and destroy the virtual microphone under `key`; its node leaves the graph.
    CloseVmic {
        key: u64,
    },
    /// Create a playback stream (not connected yet) under `key`.
    OpenPlayback {
        key: u64,
        spec: PlaybackSpec,
        reader: PlaybackReader,
    },
    /// Retarget and (re)connect the playback stream under `key`; `None` is the default sink.
    ConnectPlayback {
        key: u64,
        target_object: Option<String>,
    },
    /// Disconnect the playback stream under `key`; it can be connected again.
    DisconnectPlayback {
        key: u64,
    },
    /// Disconnect and destroy the playback stream under `key`; its node leaves the graph.
    ClosePlayback {
        key: u64,
    },
    /// Create a per-application stream capture (not connected yet) under `key` (#8).
    OpenStream {
        key: u64,
        spec: StreamSpec,
        writer: StreamWriter,
    },
    /// Connect the stream capture under `key` to the app stream with this `object.serial`.
    /// There is no other kind of target: no name, no sink, no `stream.capture.sink`.
    ConnectStream {
        key: u64,
        serial: u64,
    },
    /// Unlink the stream capture under `key`: its target is gone. It is never connected again.
    DisconnectStream {
        key: u64,
    },
    /// Disconnect and destroy the stream capture under `key`; its node leaves the graph.
    CloseStream {
        key: u64,
    },
}

/// A bound `default` metadata proxy and its listener, kept alive for the loop's lifetime.
type BoundMetadata = (Metadata, Box<dyn Listener>);
/// A bound app-stream node and its info listener, kept until the global is removed.
type BoundNode = (Node, Box<dyn Listener>);
/// A bound client and its info listener, kept until the global is removed (#8).
type BoundClient = (Client, Box<dyn Listener>);

fn dict_to_vec(d: &pw::spa::utils::dict::DictRef) -> Vec<(String, String)> {
    d.iter()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect()
}

/// A connected PipeWire client running on its own thread. Dropping it disconnects.
pub struct PipeWireContext {
    shared: Arc<Shared>,
    quit: pw::channel::Sender<Command>,
    thread: Option<JoinHandle<()>>,
    next_key: AtomicU64,
}

impl PipeWireContext {
    /// Connect to the default server and start discovery. Returns once the core is connected
    /// (or has failed); the registry may still be enumerating, see [`Self::wait_for_sync`].
    pub fn connect() -> Result<Self, PipeWireError> {
        let shared = Arc::new(Shared {
            table: Mutex::new(NodeTable::default()),
            changed: Condvar::new(),
        });
        let (quit_tx, quit_rx) = pw::channel::channel::<Command>();
        let (ready_tx, ready_rx) = mpsc::channel::<Result<(), PipeWireError>>();
        let thread_shared = Arc::clone(&shared);
        let thread = std::thread::Builder::new()
            .name("dmn-pipewire".into())
            .spawn(move || loop_thread(thread_shared, quit_rx, ready_tx))
            .map_err(|e| PipeWireError::Init(e.to_string()))?;
        let mut ctx = Self {
            shared,
            quit: quit_tx,
            thread: Some(thread),
            next_key: AtomicU64::new(1),
        };
        match ready_rx.recv() {
            Ok(Ok(())) => Ok(ctx),
            Ok(Err(e)) => {
                ctx.join();
                Err(e)
            }
            Err(_) => {
                ctx.join();
                Err(PipeWireError::ThreadGone)
            }
        }
    }

    /// Snapshot of the table.
    pub fn nodes(&self) -> NodeTable {
        self.shared.lock().clone()
    }

    /// Block until the initial enumeration is done, the server disconnects, or `timeout`.
    /// Returns the snapshot either way; check `synced`.
    pub fn wait_for_sync(&self, timeout: Duration) -> NodeTable {
        self.wait_until(timeout, |t| t.synced || t.disconnected.is_some())
    }

    /// Block until `generation` moves past `seen`, or `timeout`.
    pub fn wait_for_change(&self, seen: u64, timeout: Duration) -> NodeTable {
        self.wait_until(timeout, |t| t.generation > seen || t.disconnected.is_some())
    }

    fn wait_until(&self, timeout: Duration, done: impl Fn(&NodeTable) -> bool) -> NodeTable {
        let deadline = Instant::now() + timeout;
        let mut guard = self.shared.lock();
        while !done(&guard) {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            guard = match self.shared.changed.wait_timeout(guard, deadline - now) {
                Ok((g, _)) => g,
                Err(e) => e.into_inner().0,
            };
        }
        guard.clone()
    }

    /// Hand work to the loop thread.
    pub(crate) fn send(&self, cmd: Command) -> Result<(), PipeWireError> {
        self.quit.send(cmd).map_err(|_| PipeWireError::LoopGone)
    }

    /// A fresh key for a capture stream.
    pub(crate) fn next_key(&self) -> u64 {
        self.next_key.fetch_add(1, Ordering::Relaxed)
    }

    fn join(&mut self) {
        let _ = self.quit.send(Command::Quit);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

impl Drop for PipeWireContext {
    fn drop(&mut self) {
        self.join();
    }
}

/// Everything below runs on the loop thread. Proxies and listeners must outlive the loop, so
/// they are parked in holders and dropped after `run` returns.
fn loop_thread(
    shared: Arc<Shared>,
    quit_rx: pw::channel::Receiver<Command>,
    ready: mpsc::Sender<Result<(), PipeWireError>>,
) {
    let setup = (|| -> Result<_, PipeWireError> {
        let main_loop =
            pw::main_loop::MainLoopRc::new(None).map_err(|e| PipeWireError::Init(e.to_string()))?;
        let context = pw::context::ContextRc::new(&main_loop, None)
            .map_err(|e| PipeWireError::Init(e.to_string()))?;
        let core = context
            .connect_rc(None)
            .map_err(|e| PipeWireError::Connect(e.to_string()))?;
        let registry = core
            .get_registry_rc()
            .map_err(|e| PipeWireError::Connect(e.to_string()))?;
        Ok((main_loop, context, core, registry))
    })();
    let (main_loop, _context, core, registry) = match setup {
        Ok(v) => v,
        Err(e) => {
            let _ = ready.send(Err(e));
            return;
        }
    };

    // Commands from other threads run here, on the loop. Drop sends Command::Quit; capture
    // sources open, retarget and close their streams through the rest, and the virtual
    // microphone opens and closes its playback stream the same way.
    let captures: Rc<RefCell<HashMap<u64, LoopCapture>>> = Rc::new(RefCell::new(HashMap::new()));
    let vmics: Rc<RefCell<HashMap<u64, LoopVmic>>> = Rc::new(RefCell::new(HashMap::new()));
    let mics: Rc<RefCell<HashMap<u64, LoopMic>>> = Rc::new(RefCell::new(HashMap::new()));
    let playbacks: Rc<RefCell<HashMap<u64, LoopPlayback>>> = Rc::new(RefCell::new(HashMap::new()));
    let app_streams: Rc<RefCell<HashMap<u64, LoopStream>>> = Rc::new(RefCell::new(HashMap::new()));
    let cmd_app_streams = Rc::clone(&app_streams);
    let rm_app_streams = Rc::clone(&app_streams);
    let cmd_shared = Arc::clone(&shared);
    let cmd_playbacks = Rc::clone(&playbacks);
    let cmd_captures = Rc::clone(&captures);
    let cmd_mics = Rc::clone(&mics);
    let cmd_vmics = Rc::clone(&vmics);
    let cmd_core = core.clone();
    let loop_weak = main_loop.downgrade();
    let _commands = quit_rx.attach(main_loop.loop_(), move |cmd| match cmd {
        Command::Quit => {
            if let Some(l) = loop_weak.upgrade() {
                l.quit();
            }
        }
        Command::Open { key, spec, sink } => {
            let status = Arc::clone(&spec.status);
            match LoopCapture::open(&cmd_core, spec, sink) {
                Ok(c) => {
                    cmd_captures.borrow_mut().insert(key, c);
                }
                Err(e) => status.set_error(e),
            }
        }
        Command::OpenMic { key, spec, writer } => {
            let status = Arc::clone(&spec.status);
            match LoopMic::open(&cmd_core, spec, writer) {
                Ok(m) => {
                    cmd_mics.borrow_mut().insert(key, m);
                }
                Err(e) => status.set_error(e),
            }
        }
        Command::Connect {
            key,
            target_object,
            capture_sink,
        } => {
            if let Some(c) = cmd_captures.borrow_mut().get_mut(&key) {
                c.connect(target_object.as_deref(), capture_sink);
            } else if let Some(m) = cmd_mics.borrow_mut().get_mut(&key) {
                m.connect(target_object.as_deref());
            }
        }
        Command::Disconnect { key } => {
            if let Some(c) = cmd_captures.borrow_mut().get_mut(&key) {
                c.disconnect();
            } else if let Some(m) = cmd_mics.borrow_mut().get_mut(&key) {
                m.disconnect();
            }
        }
        Command::Close { key } => {
            let closed = cmd_captures.borrow_mut().remove(&key);
            // Keys are unique across both maps; the borrow ends before the stream is dropped.
            let closed_mic = if closed.is_none() {
                cmd_mics.borrow_mut().remove(&key)
            } else {
                None
            };
            drop(closed);
            drop(closed_mic);
        }
        Command::OpenVmic { key, spec, mixer } => {
            let status = Arc::clone(&spec.status);
            match LoopVmic::open(&cmd_core, spec, mixer) {
                Ok(v) => {
                    cmd_vmics.borrow_mut().insert(key, v);
                }
                Err(e) => status.set_error(e),
            }
        }
        Command::CloseVmic { key } => {
            let closed = cmd_vmics.borrow_mut().remove(&key);
            drop(closed);
        }
        Command::OpenPlayback { key, spec, reader } => {
            let status = Arc::clone(&spec.status);
            match LoopPlayback::open(&cmd_core, spec, reader) {
                Ok(p) => {
                    cmd_playbacks.borrow_mut().insert(key, p);
                }
                Err(e) => status.set_error(e),
            }
        }
        Command::ConnectPlayback { key, target_object } => {
            if let Some(p) = cmd_playbacks.borrow_mut().get_mut(&key) {
                p.connect(target_object.as_deref());
            }
        }
        Command::DisconnectPlayback { key } => {
            if let Some(p) = cmd_playbacks.borrow_mut().get_mut(&key) {
                p.disconnect();
            }
        }
        Command::ClosePlayback { key } => {
            let closed = cmd_playbacks.borrow_mut().remove(&key);
            drop(closed);
        }
        Command::OpenStream { key, spec, writer } => {
            let status = Arc::clone(&spec.status);
            match LoopStream::open(&cmd_core, spec, writer) {
                Ok(s) => {
                    cmd_app_streams.borrow_mut().insert(key, s);
                }
                Err(e) => status.stream_error(e),
            }
        }
        Command::ConnectStream { key, serial } => {
            // Validated again, with the same rule as `StreamCapture::start`, on the thread that
            // connects and right before it does: the serial must still name exactly one app
            // stream and no node may be named like it. Anything else fails closed with its own
            // error, and nothing is connected.
            let checked = resolve_serial(&cmd_shared.lock(), serial).map(|_| ());
            if let Some(s) = cmd_app_streams.borrow_mut().get_mut(&key) {
                match checked {
                    Ok(()) => s.connect(serial),
                    Err(why) => s.refuse(why),
                }
            }
        }
        Command::DisconnectStream { key } => {
            if let Some(s) = cmd_app_streams.borrow_mut().get_mut(&key) {
                s.disconnect();
            }
        }
        Command::CloseStream { key } => {
            // The borrow ends before the stream is dropped.
            let closed = cmd_app_streams.borrow_mut().remove(&key);
            drop(closed);
        }
    });

    // Core: `done` completes the initial sync; `error` on id 0 means we lost the server.
    let sync_seq: Rc<Cell<Option<i32>>> = Rc::new(Cell::new(None));
    let done_seq = Rc::clone(&sync_seq);
    let done_shared = Arc::clone(&shared);
    let err_shared = Arc::clone(&shared);
    let loop_weak = main_loop.downgrade();
    let _core_listener = core
        .add_listener_local()
        .done(move |id, seq| {
            if id == pw::core::PW_ID_CORE && done_seq.get() == Some(seq.seq()) {
                done_shared.apply(RegistryEvent::Synced);
            }
        })
        .error(move |id, _seq, res, message| {
            if id == pw::core::PW_ID_CORE {
                err_shared.apply(RegistryEvent::Disconnected(format!("{message} ({res})")));
                if let Some(l) = loop_weak.upgrade() {
                    l.quit();
                }
            }
        })
        .register();

    // Registry: nodes go straight into the table; the `default` metadata object is bound so
    // its property events tell us the default source.
    let holders: Rc<RefCell<Vec<BoundMetadata>>> = Rc::new(RefCell::new(Vec::new()));
    let nodes: Rc<RefCell<HashMap<u32, BoundNode>>> = Rc::new(RefCell::new(HashMap::new()));
    let clients: Rc<RefCell<HashMap<u32, BoundClient>>> = Rc::new(RefCell::new(HashMap::new()));
    let add_holders = Rc::clone(&holders);
    let add_nodes = Rc::clone(&nodes);
    let rm_nodes = Rc::clone(&nodes);
    let add_clients = Rc::clone(&clients);
    let rm_clients = Rc::clone(&clients);
    let add_shared = Arc::clone(&shared);
    let rm_shared = Arc::clone(&shared);
    let registry_weak = registry.downgrade();
    let core_weak = core.downgrade();
    let resync_seq = Rc::clone(&sync_seq);
    let _registry_listener = registry
        .add_listener_local()
        .global(move |obj| {
            let Some(props) = obj.props.as_ref() else {
                return;
            };
            match obj.type_ {
                ObjectType::Node => {
                    let is_stream = props.get("media.class") == Some("Stream/Output/Audio");
                    add_shared.apply(RegistryEvent::Added {
                        id: obj.id,
                        props: dict_to_vec(props),
                    });
                    // App streams: the registry global lacks `media.name`, so bind the node and
                    // merge its full info when it arrives (and whenever it changes).
                    if is_stream
                        && let Some(registry) = registry_weak.upgrade()
                        && let Ok(node) = registry.bind::<Node, _>(obj)
                    {
                        let id = obj.id;
                        let info_shared = Arc::clone(&add_shared);
                        let listener = node
                            .add_listener_local()
                            .info(move |info| {
                                if let Some(p) = info.props() {
                                    info_shared.apply(RegistryEvent::Updated {
                                        id,
                                        props: dict_to_vec(p),
                                    });
                                }
                            })
                            .register();
                        add_nodes
                            .borrow_mut()
                            .insert(id, (node, Box::new(listener)));
                        if let Some(core) = core_weak.upgrade()
                            && let Ok(seq) = core.sync(0)
                        {
                            resync_seq.set(Some(seq.seq()));
                        }
                    }
                }
                // Clients (#8): `pipewire.sec.pid` is set on the client by the protocol, and an
                // app stream node reaches it through its `client.id`. The registry global
                // carries part of the client's properties; bind it for the rest
                // (`application.process.id`, which the client may set after connecting) and
                // for every later change.
                ObjectType::Client => {
                    let mut client_props = dict_to_vec(props);
                    client_props.push((REGISTRY_CLIENT_KEY.to_owned(), "true".to_owned()));
                    add_shared.apply(RegistryEvent::Added {
                        id: obj.id,
                        props: client_props,
                    });
                    if let Some(registry) = registry_weak.upgrade()
                        && let Ok(client) = registry.bind::<Client, _>(obj)
                    {
                        let id = obj.id;
                        let info_shared = Arc::clone(&add_shared);
                        let listener = client
                            .add_listener_local()
                            .info(move |info| {
                                if let Some(p) = info.props() {
                                    info_shared.apply(RegistryEvent::Updated {
                                        id,
                                        props: dict_to_vec(p),
                                    });
                                }
                            })
                            .register();
                        add_clients
                            .borrow_mut()
                            .insert(id, (client, Box::new(listener)));
                        if let Some(core) = core_weak.upgrade()
                            && let Ok(seq) = core.sync(0)
                        {
                            resync_seq.set(Some(seq.seq()));
                        }
                    }
                }
                // `default` carries the default source and sink; `settings` the graph's clock rate
                // (#108: the virtual microphone plays at that rate).
                ObjectType::Metadata
                    if matches!(props.get("metadata.name"), Some("default" | "settings")) =>
                {
                    let Some(registry) = registry_weak.upgrade() else {
                        return;
                    };
                    let Ok(metadata) = registry.bind::<Metadata, _>(obj) else {
                        return;
                    };
                    let meta_shared = Arc::clone(&add_shared);
                    let listener = metadata
                        .add_listener_local()
                        .property(move |_subject, key, _type, value| {
                            match key {
                                Some("default.audio.source") => {
                                    meta_shared.apply(RegistryEvent::DefaultSource(
                                        value.and_then(parse_default_name),
                                    ));
                                }
                                Some("default.audio.sink") => {
                                    meta_shared.apply(RegistryEvent::DefaultSink(
                                        value.and_then(parse_default_name),
                                    ));
                                }
                                Some(key @ ("clock.rate" | "clock.force-rate")) => {
                                    meta_shared.apply(RegistryEvent::ClockRate {
                                        rate: value.and_then(parse_rate),
                                        forced: key == "clock.force-rate",
                                    });
                                }
                                _ => {}
                            }
                            0
                        })
                        .register();
                    add_holders
                        .borrow_mut()
                        .push((metadata, Box::new(listener)));
                    // The bound proxy's property events arrive after another round trip, so
                    // push the "synced" barrier past them: only the newest sync counts.
                    if let Some(core) = core_weak.upgrade()
                        && let Ok(seq) = core.sync(0)
                    {
                        resync_seq.set(Some(seq.seq()));
                    }
                }
                _ => {}
            }
        })
        .global_remove(move |id| {
            rm_nodes.borrow_mut().remove(&id);
            rm_clients.borrow_mut().remove(&id);
            // A stream capture whose target this was is shut and unlinked here, on the loop
            // thread, as the removal arrives: before the table changes and before the session
            // manager could link the capture to anything else. Its supervisor sees the same
            // removal in the table and says so too; neither ever reconnects it.
            let gone_serial = rm_shared.lock().stream_metadata(id).and_then(|m| m.serial);
            if let Some(serial) = gone_serial {
                for s in rm_app_streams.borrow_mut().values_mut() {
                    s.target_removed(serial);
                }
            }
            rm_shared.apply(RegistryEvent::Removed(id));
        })
        .register();

    // Ask the server to tell us when everything enumerated so far has been delivered.
    match core.sync(0) {
        Ok(seq) => sync_seq.set(Some(seq.seq())),
        Err(e) => {
            let _ = ready.send(Err(PipeWireError::Connect(e.to_string())));
            return;
        }
    }
    let _ = ready.send(Ok(()));

    main_loop.run();

    // Streams first (they hang off the core; closing the virtual microphone's is what takes
    // its node out of the graph), then listeners before proxies before the core: drop in
    // reverse creation order.
    captures.borrow_mut().clear();
    mics.borrow_mut().clear();
    vmics.borrow_mut().clear();
    playbacks.borrow_mut().clear();
    app_streams.borrow_mut().clear();
    drop(_commands);
    drop(_registry_listener);
    drop(nodes);
    drop(clients);
    drop(holders);
    drop(_core_listener);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn added(id: u32, props: &[(&str, &str)]) -> RegistryEvent {
        RegistryEvent::Added {
            id,
            props: props
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    #[test]
    fn sources_and_streams_are_classified_from_media_class() {
        let mut t = NodeTable::default();
        assert!(t.apply(added(
            40,
            &[
                ("media.class", "Audio/Source"),
                ("node.name", "alsa_input.usb-mic"),
                ("node.description", "USB Condenser Mic"),
            ],
        )));
        assert!(t.apply(added(
            41,
            &[
                ("media.class", "Stream/Output/Audio"),
                ("application.name", "Firefox"),
                ("media.name", "Google Meet"),
                ("node.name", "firefox.out"),
            ],
        )));
        // Ports, links and other classes are ignored and do not bump the generation. (Sinks
        // were ignored here until `NodeTable` learned to list them; see the sink tests.)
        assert!(!t.apply(added(
            42,
            &[("media.class", "Audio/Duplex"), ("node.name", "speakers")]
        )));
        assert!(!t.apply(added(43, &[("object.serial", "7")])));

        let sources: Vec<_> = t.sources().collect();
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].name, "alsa_input.usb-mic");
        assert_eq!(sources[0].description, "USB Condenser Mic");
        let streams: Vec<_> = t.streams().collect();
        assert_eq!(streams.len(), 1);
        assert_eq!(streams[0].application, "Firefox");
        assert_eq!(streams[0].media_name, "Google Meet");
        assert_eq!(streams[0].node_name, "firefox.out");
        assert_eq!(t.generation, 2);
    }

    #[test]
    fn description_falls_back_to_nick_then_name() {
        let mut t = NodeTable::default();
        t.apply(added(
            1,
            &[
                ("media.class", "Audio/Source"),
                ("node.name", "n1"),
                ("node.nick", "Nick"),
            ],
        ));
        t.apply(added(
            2,
            &[("media.class", "Audio/Source"), ("node.name", "n2")],
        ));
        let d: Vec<_> = t.sources().map(|s| s.description.clone()).collect();
        assert_eq!(d, ["Nick", "n2"]);
    }

    #[test]
    fn node_info_fills_in_media_name_after_the_registry_global() {
        let mut t = NodeTable::default();
        // What the registry global carries: no media.name.
        t.apply(added(
            7,
            &[
                ("media.class", "Stream/Output/Audio"),
                ("application.name", "Firefox"),
                ("node.name", "firefox"),
            ],
        ));
        assert_eq!(t.streams().next().map(|s| s.media_name.as_str()), Some(""));
        let g = t.generation;
        // What the bound node's info delivers.
        let upd = |props: &[(&str, &str)]| RegistryEvent::Updated {
            id: 7,
            props: props
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        };
        assert!(t.apply(upd(&[
            ("media.name", "Google Meet"),
            ("application.name", "Firefox")
        ])));
        assert_eq!(
            t.streams().next().map(|s| s.media_name.as_str()),
            Some("Google Meet")
        );
        assert_eq!(t.generation, g + 1);
        // Same info again is not a change; an update for an unknown id is ignored.
        assert!(!t.apply(upd(&[("media.name", "Google Meet")])));
        assert!(!t.apply(RegistryEvent::Updated {
            id: 99,
            props: vec![("media.name".into(), "x".into())]
        }));
        assert_eq!(t.generation, g + 1);
    }

    #[test]
    fn removal_updates_the_table_and_generation_without_polling() {
        let mut t = NodeTable::default();
        t.apply(added(
            5,
            &[
                ("media.class", "Stream/Output/Audio"),
                ("application.name", "zoom"),
            ],
        ));
        let g = t.generation;
        assert!(t.apply(RegistryEvent::Removed(5)));
        assert_eq!(t.streams().count(), 0);
        assert_eq!(t.generation, g + 1);
        // Removing something we never tracked changes nothing.
        assert!(!t.apply(RegistryEvent::Removed(5)));
        assert_eq!(t.generation, g + 1);
    }

    #[test]
    fn default_source_resolves_to_a_tracked_node() {
        let mut t = NodeTable::default();
        t.apply(added(
            9,
            &[
                ("media.class", "Audio/Source"),
                ("node.name", "alsa_input.pci"),
            ],
        ));
        assert!(t.default_capture().is_none());
        assert!(t.apply(RegistryEvent::DefaultSource(Some("alsa_input.pci".into()))));
        assert_eq!(t.default_capture().map(|n| n.id), Some(9));
        assert!(!t.apply(RegistryEvent::DefaultSource(Some("alsa_input.pci".into()))));
        assert!(t.apply(RegistryEvent::DefaultSource(None)));
        assert!(t.default_capture().is_none());
    }

    #[test]
    fn sinks_are_listed_with_their_names_and_descriptions_and_apart_from_sources() {
        let mut t = NodeTable::default();
        assert!(t.apply(added(
            50,
            &[
                ("media.class", "Audio/Sink"),
                ("node.name", "alsa_output.pci.analog-stereo"),
                ("node.description", "Built-in Audio"),
            ],
        )));
        assert!(t.apply(added(
            51,
            &[("media.class", "Audio/Sink"), ("node.name", "bt-headset")],
        )));
        assert!(t.apply(added(
            52,
            &[("media.class", "Audio/Source"), ("node.name", "usb-mic")],
        )));
        let sinks: Vec<_> = t.sinks().map(|s| (s.id, s.name.as_str())).collect();
        assert_eq!(
            sinks,
            [(50, "alsa_output.pci.analog-stereo"), (51, "bt-headset")]
        );
        assert_eq!(
            t.sinks().next().map(|s| s.description.as_str()),
            Some("Built-in Audio")
        );
        assert_eq!(
            t.sinks().nth(1).map(|s| s.description.as_str()),
            Some("bt-headset"),
            "no description: the name"
        );
        assert_eq!(t.sources().count(), 1, "a sink is not a source");

        // The description arrives later with the bound node's info, as it can for a source.
        let g = t.generation;
        let upd = |d: &str| RegistryEvent::Updated {
            id: 51,
            props: vec![("node.description".into(), d.into())],
        };
        assert!(t.apply(upd("Headset")));
        assert!(!t.apply(upd("Headset")));
        assert_eq!(t.generation, g + 1);

        assert!(t.apply(RegistryEvent::Removed(51)));
        assert_eq!(t.sinks().count(), 1);
        assert!(!t.apply(RegistryEvent::Removed(51)));
    }

    #[test]
    fn default_sink_resolves_to_a_tracked_sink_and_is_independent_of_the_default_source() {
        let mut t = NodeTable::default();
        t.apply(added(
            60,
            &[("media.class", "Audio/Sink"), ("node.name", "speakers")],
        ));
        t.apply(added(
            61,
            &[("media.class", "Audio/Sink"), ("node.name", "headset")],
        ));
        assert!(t.default_playback().is_none());
        assert!(t.apply(RegistryEvent::DefaultSink(Some("speakers".into()))));
        assert_eq!(t.default_playback().map(|n| n.id), Some(60));
        assert!(!t.apply(RegistryEvent::DefaultSink(Some("speakers".into()))));
        // The default moves.
        assert!(t.apply(RegistryEvent::DefaultSink(Some("headset".into()))));
        assert_eq!(t.default_playback().map(|n| n.id), Some(61));
        assert!(
            t.default_capture().is_none(),
            "the source default is untouched"
        );
        // A default the table does not hold resolves to nothing; the name is still reported.
        assert!(t.apply(RegistryEvent::Removed(61)));
        assert!(t.default_playback().is_none());
        assert_eq!(t.default_sink.as_deref(), Some("headset"));
        assert!(t.apply(RegistryEvent::DefaultSink(None)));
        // The source default does not set the sink default.
        assert!(t.apply(RegistryEvent::DefaultSource(Some("speakers".into()))));
        assert!(t.default_sink.is_none());
    }

    #[test]
    fn synced_once_and_frozen_after_disconnect() {
        let mut t = NodeTable::default();
        assert!(t.apply(RegistryEvent::Synced));
        assert!(!t.apply(RegistryEvent::Synced));
        assert!(t.apply(RegistryEvent::Disconnected("gone".into())));
        let g = t.generation;
        assert!(!t.apply(added(1, &[("media.class", "Audio/Source")])));
        assert_eq!(t.generation, g);
        assert_eq!(t.disconnected.as_deref(), Some("gone"));
    }

    #[test]
    fn default_name_is_parsed_from_metadata_json() {
        assert_eq!(
            parse_default_name(r#"{ "name": "alsa_input.pci-0000_00_1f.3.analog-stereo" }"#)
                .as_deref(),
            Some("alsa_input.pci-0000_00_1f.3.analog-stereo")
        );
        assert_eq!(parse_default_name(r#"{"name":"x"}"#).as_deref(), Some("x"));
        assert_eq!(parse_default_name(r#"{"name":""}"#), None);
        assert_eq!(parse_default_name("{}"), None);
        assert_eq!(parse_default_name("not json"), None);
    }

    #[test]
    fn graph_rate_follows_settings_and_a_forced_rate_wins() {
        let mut t = NodeTable::default();
        assert_eq!(
            t.graph_rate(),
            DEFAULT_GRAPH_RATE,
            "nothing seen: PipeWire's default"
        );
        assert!(t.apply(RegistryEvent::ClockRate {
            rate: Some(44_100),
            forced: false
        }));
        assert_eq!(t.graph_rate(), 44_100);
        assert!(!t.apply(RegistryEvent::ClockRate {
            rate: Some(44_100),
            forced: false
        }));
        assert!(t.apply(RegistryEvent::ClockRate {
            rate: Some(96_000),
            forced: true
        }));
        assert_eq!(t.graph_rate(), 96_000, "clock.force-rate pins the graph");
        // force-rate back to 0 ("not forced") is reported as None and the clock rate shows again.
        assert!(t.apply(RegistryEvent::ClockRate {
            rate: parse_rate("0"),
            forced: true
        }));
        assert_eq!(t.graph_rate(), 44_100);
        assert_eq!(parse_rate("48000"), Some(48_000));
        assert_eq!(parse_rate(" \"48000\" "), Some(48_000));
        assert_eq!(parse_rate("fast"), None);
    }

    fn updated(id: u32, props: &[(&str, &str)]) -> RegistryEvent {
        RegistryEvent::Updated {
            id,
            props: props
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    /// A client global as the loop thread reports it: its registry properties plus the marker.
    fn client(id: u32, props: &[(&str, &str)]) -> RegistryEvent {
        let mut all = props.to_vec();
        all.push((REGISTRY_CLIENT_KEY, "true"));
        added(id, &all)
    }

    fn stream_node(id: u32, extra: &[(&str, &str)]) -> RegistryEvent {
        let mut all = vec![
            ("media.class", "Stream/Output/Audio"),
            ("node.name", "app.out"),
        ];
        all.extend_from_slice(extra);
        added(id, &all)
    }

    // Property keys the metadata tests use, short so each event fits on a line.
    const CID: &str = "client.id";
    const PID: &str = "application.process.id";
    const SEC: &str = "pipewire.sec.pid";
    const NAME: &str = "application.name";
    const SERIAL: &str = "object.serial";

    #[test]
    fn stream_metadata_reads_pids_from_the_node_and_from_its_client_in_either_order() {
        // (what, events, expected (process id, origin), expected (sec pid, origin)).
        type Case = (
            &'static str,
            Vec<RegistryEvent>,
            (Option<u32>, Option<PropertyOrigin>),
            Option<u32>,
        );
        use PropertyOrigin::{Client as C, Node as N};
        let cases: Vec<Case> = vec![
            (
                "neither on the node nor on a client",
                vec![stream_node(10, &[(SERIAL, "110")])],
                (None, None),
                None,
            ),
            (
                "native: node then client, sec pid only on the client",
                vec![
                    stream_node(10, &[(CID, "3"), (PID, "4242")]),
                    client(3, &[(SEC, "4242")]),
                ],
                (Some(4242), Some(N)),
                Some(4242),
            ),
            (
                "client first, then its node",
                vec![client(3, &[(SEC, "4242")]), stream_node(10, &[(CID, "3")])],
                (None, None),
                Some(4242),
            ),
            (
                "pulse: the app's pid differs from pipewire-pulse's sec pid",
                vec![
                    client(7, &[(SEC, "900"), (PID, "5150")]),
                    stream_node(11, &[(CID, "7")]),
                ],
                (Some(5150), Some(C)),
                Some(900),
            ),
            (
                "forged node sec pid never overrides the owning client's",
                vec![
                    client(7, &[(SEC, "900")]),
                    stream_node(11, &[(CID, "7"), (SEC, "42"), (PID, "43")]),
                ],
                (Some(43), Some(N)),
                Some(900),
            ),
            (
                "forged node sec pid via a later update still loses",
                vec![
                    client(7, &[(SEC, "900")]),
                    stream_node(11, &[(CID, "7")]),
                    updated(11, &[(SEC, "42")]),
                ],
                (None, None),
                Some(900),
            ),
            (
                "node-only sec pid with no client at all is not trusted",
                vec![stream_node(11, &[(SEC, "42")])],
                (None, None),
                None,
            ),
            (
                "node-only sec pid whose client is not known is not trusted",
                vec![stream_node(11, &[(CID, "7"), (SEC, "42")])],
                (None, None),
                None,
            ),
            (
                "pids arrive with the bound node's and client's info",
                vec![
                    stream_node(12, &[]),
                    updated(12, &[(CID, "8"), (PID, "77")]),
                    client(8, &[]),
                    updated(8, &[(SEC, "78")]),
                ],
                (Some(77), Some(N)),
                Some(78),
            ),
            (
                "unparseable and zero pids are none",
                vec![
                    stream_node(13, &[(PID, "abc"), (SEC, "0")]),
                    stream_node(14, &[(CID, "9")]),
                    client(9, &[(SEC, "0"), (PID, "x")]),
                ],
                (None, None),
                None,
            ),
        ];
        for (what, events, pid, sec) in cases {
            let mut t = NodeTable::default();
            for e in events {
                t.apply(e);
            }
            let id = t.streams().next().map(|s| s.id).expect(what);
            let m = t.stream_metadata(id).expect(what);
            assert_eq!(
                (m.application_process_id, m.application_process_id_origin),
                pid,
                "{what}"
            );
            assert_eq!(m.sec_pid, sec, "{what}");
        }
    }

    #[test]
    fn client_arrival_change_and_removal_bump_the_generation_only_when_a_stream_sees_it() {
        let mut t = NodeTable::default();
        // A client no stream points at: tracked, but nothing observable changed.
        assert!(!t.apply(client(3, &[(SEC, "10")])));
        assert!(t.apply(stream_node(20, &[(CID, "3")])));
        assert_eq!(t.stream_metadata(20).unwrap().sec_pid, Some(10));
        // The client's info arrives with the app's own pid: the stream reflects it.
        let g = t.generation;
        assert!(t.apply(updated(3, &[(SEC, "10"), (PID, "11")])));
        assert_eq!(t.generation, g + 1);
        let m = t.stream_metadata(20).unwrap();
        assert_eq!(m.application_process_id, Some(11));
        // The same info again is no change.
        assert!(!t.apply(updated(3, &[(PID, "11")])));
        assert_eq!(t.generation, g + 1);
        // A client added after its node makes the change visible too.
        assert!(t.apply(stream_node(21, &[(CID, "4")])));
        assert!(t.apply(client(4, &[(SEC, "12")])));
        assert_eq!(t.stream_metadata(21).unwrap().sec_pid, Some(12));
        // The client goes: what it gave goes with it, and that is a change.
        let g = t.generation;
        assert!(t.apply(RegistryEvent::Removed(3)));
        assert_eq!(t.generation, g + 1);
        let m = t.stream_metadata(20).unwrap();
        assert_eq!((m.sec_pid, m.application_process_id), (None, None));
        assert_eq!(m.client_id, Some(3), "the node still names it");
        assert!(!t.apply(RegistryEvent::Removed(3)), "already gone");
        // The node goes: no metadata for it any more, the other is untouched.
        assert!(t.apply(RegistryEvent::Removed(21)));
        assert!(t.stream_metadata(21).is_none());
        assert!(t.stream_metadata(20).is_some());
        // A client is never listed as a stream, source or sink.
        assert_eq!(t.streams().count(), 1);
        assert_eq!(t.sources().count() + t.sinks().count(), 0);
    }

    #[test]
    fn explicit_empty_invalid_or_zero_values_clear_and_notify_but_absent_keys_preserve() {
        use PropertyOrigin::{Client as C, Node as N};
        let mut t = NodeTable::default();
        t.apply(client(3, &[(SEC, "900"), (PID, "901")]));
        t.apply(stream_node(
            10,
            &[(CID, "3"), (PID, "43"), (NAME, "player"), (SERIAL, "110")],
        ));
        let m = t.stream_metadata(10).unwrap();
        assert_eq!(m.sec_pid, Some(900));
        assert_eq!(
            (m.application_process_id, m.application_process_id_origin),
            (Some(43), Some(N))
        );
        assert_eq!(m.application_name.as_deref(), Some("player"));
        assert_eq!(m.client_id, Some(3));

        // A partial update naming none of those keys preserves everything and is no change.
        let g = t.generation;
        assert!(!t.apply(updated(10, &[("node.latency", "1/48000")])));
        assert!(!t.apply(updated(3, &[("application.name", "c")])));
        assert_eq!(t.generation, g);
        let m = t.stream_metadata(10).unwrap();
        assert_eq!(
            (m.sec_pid, m.application_process_id, m.client_id),
            (Some(900), Some(43), Some(3))
        );

        // Node application.process.id cleared: falls back to the client's, with its origin.
        for bad in ["", "0", "junk", " "] {
            let mut t = t.clone();
            let g = t.generation;
            assert!(t.apply(updated(10, &[(PID, bad)])), "pid {bad:?}");
            assert_eq!(t.generation, g + 1);
            let m = t.stream_metadata(10).unwrap();
            assert_eq!(
                (m.application_process_id, m.application_process_id_origin),
                (Some(901), Some(C)),
                "pid {bad:?}"
            );
            // The same clear again is not a change.
            assert!(!t.apply(updated(10, &[(PID, bad)])));
        }
        // Name cleared.
        for bad in ["", "   "] {
            let mut t = t.clone();
            assert!(t.apply(updated(10, &[(NAME, bad)])), "name {bad:?}");
            assert_eq!(t.stream_metadata(10).unwrap().application_name, None);
        }
        // client.id cleared: the stream loses its client, and with it the security pid.
        for bad in ["", "0x", "-1"] {
            let mut t = t.clone();
            assert!(t.apply(updated(10, &[(CID, bad)])), "client {bad:?}");
            let m = t.stream_metadata(10).unwrap();
            assert_eq!((m.client_id, m.sec_pid), (None, None), "client {bad:?}");
        }
        // Client keys cleared: its sec pid and claimed pid go, observers are notified.
        for bad in ["", "0", "junk"] {
            let mut t = t.clone();
            let g = t.generation;
            assert!(t.apply(updated(3, &[(SEC, bad)])), "sec {bad:?}");
            assert_eq!(t.generation, g + 1);
            assert_eq!(t.stream_metadata(10).unwrap().sec_pid, None);
            assert!(t.apply(updated(3, &[(PID, bad)])), "client pid {bad:?}");
            let m = t.stream_metadata(10).unwrap();
            // Only the node's own claim is left, with its origin.
            assert_eq!(
                (m.application_process_id, m.application_process_id_origin),
                (Some(43), Some(N))
            );
        }
        // Clearing one key leaves the other keys of the same update untouched.
        let mut t2 = t.clone();
        assert!(t2.apply(updated(3, &[(SEC, "")])));
        assert_eq!(
            t2.stream_metadata(10).unwrap().application_process_id,
            Some(43)
        );
        assert_eq!(
            t2.stream_metadata(10).unwrap().application_name.as_deref(),
            Some("player")
        );
    }

    #[test]
    fn a_stream_says_whether_application_name_was_really_set() {
        let mut t = NodeTable::default();
        t.apply(stream_node(
            30,
            &[(NAME, "Firefox"), (SERIAL, "130"), (CID, "5")],
        ));
        t.apply(stream_node(31, &[("application.process.binary", "mpv")]));
        t.apply(stream_node(32, &[]));
        t.apply(stream_node(33, &[(NAME, "  ")]));
        fn by_id(t: &NodeTable, id: u32) -> StreamMetadata {
            t.stream_metadata(id).unwrap()
        }
        assert_eq!(by_id(&t, 30).application_name.as_deref(), Some("Firefox"));
        assert_eq!(by_id(&t, 30).serial, Some(130));
        assert_eq!(by_id(&t, 30).client_id, Some(5));
        // The fallbacks AppStream shows are not names.
        let apps: Vec<_> = t.streams().map(|s| s.application.as_str()).collect();
        assert_eq!(apps, ["Firefox", "mpv", "unknown", "  "]);
        assert_eq!(by_id(&t, 31).application_name, None);
        assert_eq!(by_id(&t, 32).application_name, None);
        assert_eq!(by_id(&t, 33).application_name, None, "blank is not a name");
        // The name arrives later with the node's info: now it is set.
        assert!(t.apply(updated(31, &[(NAME, "mpv")])));
        assert_eq!(by_id(&t, 31).application_name.as_deref(), Some("mpv"));
        // An unknown id has no metadata; the iterator pairs each stream with its own.
        assert!(t.stream_metadata(99).is_none());
        assert!(t.streams_with_metadata().all(|(s, m)| s.id == m.id));
        assert_eq!(t.streams_with_metadata().count(), 4);
    }

    #[test]
    fn stream_by_serial_finds_only_app_streams() {
        let mut t = NodeTable::default();
        t.apply(stream_node(40, &[(SERIAL, "140")]));
        t.apply(added(
            41,
            &[
                ("media.class", "Audio/Sink"),
                ("node.name", "speakers"),
                (SERIAL, "141"),
            ],
        ));
        assert_eq!(t.stream_by_serial(140).map(|s| s.id), Some(40));
        assert!(
            t.stream_by_serial(141).is_none(),
            "a sink is not an app stream"
        );
        assert!(t.stream_by_serial(142).is_none());
        assert!(t.node_named("speakers", 40));
        assert!(
            !t.node_named("app.out", 40),
            "the stream itself does not count"
        );
    }

    /// Needs a running PipeWire server: `just probe`.
    #[test]
    #[ignore]
    fn live_context_syncs_lists_a_source_and_stops_cleanly() {
        let ctx = PipeWireContext::connect().expect("PipeWire server running");
        let table = ctx.wait_for_sync(Duration::from_secs(2));
        assert!(table.synced, "registry did not sync: {table:?}");
        assert!(
            table.sources().count() >= 1,
            "expected at least one Audio/Source on the dev machine: {table:?}"
        );
        assert!(table.disconnected.is_none());
        let started = Instant::now();
        drop(ctx);
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "drop should join promptly"
        );
    }

    /// Needs a running PipeWire server: `just probe`. Second connect after a drop proves the
    /// loop thread and libpipewire state are reusable in-process.
    #[test]
    #[ignore]
    fn live_context_can_be_created_twice() {
        for _ in 0..2 {
            let ctx = PipeWireContext::connect().expect("PipeWire server running");
            assert!(ctx.wait_for_sync(Duration::from_secs(2)).synced);
        }
    }
}
