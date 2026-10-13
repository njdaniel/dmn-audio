//! PipeWire discovery: one OS thread runs the PipeWire main loop; a registry listener keeps a
//! live [`NodeTable`] of capture sources and app output streams (docs/SPEC.md § Audio capture on
//! PipeWire, Discovery). The same loop thread owns the capture streams of
//! [`crate::PipeWireSource`] (issue #5), which it creates, retargets and closes on
//! [`Command`]s, and the playback stream behind [`crate::VirtualMic`] (#108).
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
use pw::metadata::Metadata;
use pw::node::Node;
use pw::proxy::Listener;
use pw::types::ObjectType;

use crate::capture::{LoopCapture, OpenSpec};
use crate::frames::FrameSink;
use crate::mic::{LoopMic, MicSpec, MicWriter};
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

/// One registry event, decoupled from libpipewire so the table logic is testable without a
/// server. The loop thread produces these; [`NodeTable::apply`] consumes them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistryEvent {
    /// A global appeared with these properties (only nodes matter; others are ignored).
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
    /// `node.name` of PipeWire's default source, from the `default` metadata object.
    pub default_source: Option<String>,
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
                        true
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
                } else if let Some(n) = self.sources.get_mut(&id)
                    && let Some(d) = get("node.description")
                    && n.description != d
                {
                    n.description = d.to_owned();
                    changed = true;
                }
                changed
            }
            RegistryEvent::Removed(id) => {
                self.sources.remove(&id).is_some() | self.streams.remove(&id).is_some()
            }
            RegistryEvent::DefaultSource(name) => {
                let changed = self.default_source != name;
                self.default_source = name;
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
}

/// A bound `default` metadata proxy and its listener, kept alive for the loop's lifetime.
type BoundMetadata = (Metadata, Box<dyn Listener>);
/// A bound app-stream node and its info listener, kept until the global is removed.
type BoundNode = (Node, Box<dyn Listener>);

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
    let add_holders = Rc::clone(&holders);
    let add_nodes = Rc::clone(&nodes);
    let rm_nodes = Rc::clone(&nodes);
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
                // `default` carries the default source; `settings` the graph's clock rate
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
    drop(_commands);
    drop(_registry_listener);
    drop(nodes);
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
        // Sinks, ports, links and other classes are ignored and do not bump the generation.
        assert!(!t.apply(added(
            42,
            &[("media.class", "Audio/Sink"), ("node.name", "speakers")]
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
