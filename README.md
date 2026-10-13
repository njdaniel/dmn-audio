# dmn-audio

PipeWire audio for Rust programs on Linux: capture from a microphone or from one application's output, with re-attach when the device or the application comes back; WAV replay behind the same trait; and a virtual microphone other programs can select. Samples cross threads through lock-free rings, and the PipeWire process callbacks allocate nothing, lock nothing and log nothing.

The crate's own documentation (`src/lib.rs`) describes the pieces and how they fit.

## Where this came from

This crate was written inside [`njdaniel/daemon`](https://github.com/njdaniel/daemon) and extracted from it at commit `360d613`, so that other projects can depend on it; [Conch](https://github.com/njdaniel/conch)'s voice client is the first. The code is unchanged from that commit. Only the manifest differs: it no longer inherits from `daemon`'s workspace.

Two things follow from that, until they are tidied up:

- Issue and pull-request numbers in comments (`#108`, `PR #25`) and references to `docs/SPEC.md` point at `daemon`, not at this repository.
- The API still speaks `daemon`'s language in places: sources are labelled `You` and `Interviewer`, `PipeWireSource` delivers 16 kHz mono in 20 ms frames (`MicCapture` delivers a microphone at a rate you choose, as plain samples in a ring), and the only output is the virtual microphone.

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

## Tests

```sh
cargo test                 # needs no PipeWire session and no audio device
cargo test -- --ignored    # the live tests: need a running PipeWire session
```

The live tests need `pw-play` and `pw-record` on the PATH. They create and remove their own nodes in the PipeWire graph, and some play a quiet 440 Hz tone through the default output for a couple of seconds, so you will hear them. Run them on a development machine, not in CI.

`cargo run -p dmn-audio --example hotplug` is a manual check of the device table: start and stop `pw-play` in another terminal and watch it follow.

## Licence

MIT. See [LICENSE](LICENSE).
