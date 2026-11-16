# dmn-audio

PipeWire audio for Rust programs on Linux: capture from a microphone or from one application's output, with re-attach when the device or the application comes back; WAV replay behind the same trait; and a virtual microphone other programs can select. Samples cross threads through lock-free rings, and the PipeWire process callbacks allocate nothing, lock nothing and log nothing.

The crate's own documentation (`src/lib.rs`) describes the pieces and how they fit.

## Where this came from

This crate was written inside [`njdaniel/daemon`](https://github.com/njdaniel/daemon) and extracted from it at commit `360d613`, so that other projects can depend on it; [Conch](https://github.com/njdaniel/conch)'s voice client is the first. The code is unchanged from that commit. Only the manifest differs: it no longer inherits from `daemon`'s workspace.

Two things follow from that, until they are tidied up:

- Issue and pull-request numbers in comments (`#108`, `PR #25`) and references to `docs/SPEC.md` point at `daemon`, not at this repository.
- The API still speaks `daemon`'s language in places: sources are labelled `You` and `Interviewer`, `PipeWireSource` delivers 16 kHz mono in 20 ms frames (`MicCapture` delivers a microphone at a rate you choose, as plain samples in a ring), and there are two outputs: the virtual microphone, and `Playback`, which plays mono samples at a rate you choose to the default sink or a named one.

## Requirements

- Linux with PipeWire 0.3.49 or newer (Ubuntu 24.04 ships 1.0).
- To build: the PipeWire headers, clang and pkg-config. On Ubuntu:

  ```sh
  sudo apt-get install libpipewire-0.3-dev clang libclang-dev pkg-config
  ```

- Rust 1.85 or newer.

## Using it

```toml
[dependencies]
dmn-audio = { git = "https://github.com/njdaniel/dmn-audio", rev = "<commit>" }
```

Pin a commit. There are no releases yet and the API will change.

### Capturing one application's stream (`StreamCapture`)

`StreamCapture` captures one `Stream/Output/Audio` node, named by its `object.serial`, as f32 samples at a rate (8–192 kHz) and channel count (1 or 2, interleaved) you choose, into a lock-free ring. Several can run at once on one `PipeWireContext`. Its only target is a serial: it cannot capture a sink monitor, has no fallback, and never re-targets. When the stream goes away the capture stops delivering for good and its status says the target is gone. A stream that settles at another rate, channel count or sample format delivers nothing and says which.

Who a stream belongs to is in `NodeTable::stream_metadata`, kept apart from `AppStream`. **Only `StreamMetadata::sec_pid` is trustworthy**: it is the owning PipeWire client's `pipewire.sec.pid`, set by the protocol, and it is `None` when the client is unknown or has gone. A node's own `pipewire.sec.pid` property is application supplied (it can be forged), so it is never read. For a PulseAudio-API application the client is `pipewire-pulse`, so `sec_pid` is that process's pid, not the application's. `application_process_id` is a claim by the application, with the place it was read from; use it to find the application, never to authorise anything. `application_name` is `None` when the node did not set `application.name` (`AppStream::application` then holds a fallback).

```rust,no_run
use std::sync::Arc;
use std::time::Duration;
use dmn_audio::{PipeWireContext, StreamCapture};

let ctx = Arc::new(PipeWireContext::connect()?);
let table = ctx.wait_for_sync(Duration::from_secs(2));
// Use the serial selected from this table by your application's source picker.
let serial = 1234;
let mut capture = StreamCapture::new(Arc::clone(&ctx), serial, 48_000, 2, 2_000)?;
let mut reader = capture.start()?;
capture.wait_streaming(Duration::from_secs(3))?;
let mut block = [0.0f32; 1920]; // 20 ms of 48 kHz stereo: left, right, left, …
let _got = reader.pop(&mut block);
capture.stop()?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

`cargo run --example app_stream` lists the streams; `cargo run --example app_stream -- <serial> 5 out.wav` records one to a WAV file.

## Tests

```sh
cargo test                 # needs no PipeWire session and no audio device
cargo test -- --ignored    # the live tests: need a running PipeWire session
```

The live tests need `pw-play`, `pw-record` and `pw-loopback` on the PATH; the stream-capture cleanup checks also need `pw-link` and `pw-cli`. They create and remove their own nodes in the PipeWire graph. The older ones play a quiet 440 Hz tone through the default output for a couple of seconds, so you will hear them. The `StreamCapture` one plays only into a temporary silent virtual stereo sink it makes and removes, and changes no default. The `MicCapture` and `Playback` ones make their own virtual devices and are silent, but one of them, `live_playback_to_the_default_sink_follows_it_when_the_default_changes`, changes the machine's default sink while it runs and puts it back: run that one with nothing else playing. Run them on a development machine, not in CI.

`cargo run -p dmn-audio --example hotplug` is a manual check of the device table: start and stop `pw-play` in another terminal and watch it follow.

## Licence

MIT. See [LICENSE](LICENSE).
