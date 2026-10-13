//! Helpers the tests of `mic.rs` and `playback.rs` share: measuring a tone's pitch, and a silent
//! virtual device pair made with `pw-loopback`. Test builds only.

use std::process::Command as Proc;
use std::time::{Duration, Instant};

/// The pitch of a steady tone in `samples`, taken as played at `rate` Hz, from its upward
/// swings. This is what shows a rate is real: the same samples read at another rate come out
/// at another pitch. A swing counts only when it goes from below minus a quarter of the peak
/// to above plus a quarter, so the near-silence around the tone (a resampler leaves traces
/// there) adds nothing.
pub(crate) fn tone_hz(samples: &[f32], rate: u32) -> f32 {
    let level = samples.iter().fold(0.0f32, |p, s| p.max(s.abs())) / 4.0;
    if level == 0.0 {
        return 0.0;
    }
    let mut low = false;
    let mut ups: Vec<usize> = Vec::new();
    for (i, &x) in samples.iter().enumerate() {
        if x < -level {
            low = true;
        } else if x > level && low {
            low = false;
            ups.push(i);
        }
    }
    match (ups.first(), ups.last()) {
        (Some(first), Some(last)) if last > first => {
            (ups.len() - 1) as f32 * rate as f32 / (last - first) as f32
        }
        _ => 0.0,
    }
}

/// A virtual microphone made by `pw-loopback`: an `Audio/Source` called `name` that plays
/// whatever is played into the `Audio/Sink` called `name-sink`. Both are nodes of this
/// test's own making; the process is killed, and so the nodes removed, on every exit
/// path (drop), a panic included.
pub(crate) struct Loopback(std::process::Child);

impl Loopback {
    pub(crate) fn new(name: &str) -> Self {
        let child = Proc::new("pw-loopback")
            .args(["-n", name, "-m", "[ MONO ]"])
            .arg(format!(
                "--capture-props=media.class=Audio/Sink node.name={name}-sink"
            ))
            .arg(format!(
                "--playback-props=media.class=Audio/Source node.name={name}"
            ))
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("pw-loopback on PATH");
        Loopback(child)
    }
}

impl Drop for Loopback {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Poll `ok` every 20 ms until it holds; panic after `dur`.
pub(crate) fn wait_for(what: &str, dur: Duration, mut ok: impl FnMut() -> bool) {
    let t = Instant::now();
    while !ok() {
        assert!(t.elapsed() < dur, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// The loudest sample.
pub(crate) fn peak(samples: &[f32]) -> f32 {
    samples.iter().fold(0.0, |p, s| p.max(s.abs()))
}
