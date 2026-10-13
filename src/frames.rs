//! The SPSC ring between a source and its consumer. One producer (the capture thread) and one
//! consumer (the VAD thread); nothing here allocates or locks after construction.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use ringbuf::HeapRb;
use ringbuf::traits::{Consumer, Observer, Producer, Split};

use crate::{FRAME_LEN, Source};

type Prod = <HeapRb<f32> as Split>::Prod;
type Cons = <HeapRb<f32> as Split>::Cons;

/// Producer end. Held by the capture thread; `push_frame` is safe to call from a real-time
/// callback: no allocation, no lock, no logging, bounded time.
pub struct FrameSink {
    prod: Prod,
    overruns: Arc<AtomicU64>,
}

impl FrameSink {
    /// Push one 20 ms frame. Returns `false` and counts an overrun when the ring is full.
    ///
    /// docs/SPEC.md § Threading rules: a full buffer drops the *incoming* 20 ms frame and
    /// increments the overrun counter. The lock-free producer never touches the consumer's read
    /// index (SPSC gives each side one index) and the capture callback must not lock, so the
    /// consumer's backlog is never discarded from this side; one 20 ms frame is lost per overrun.
    /// Frames shorter than [`FRAME_LEN`] are zero-padded; longer ones are truncated.
    pub fn push_frame(&mut self, frame: &[f32]) -> bool {
        if self.prod.vacant_len() < FRAME_LEN {
            self.overruns.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        let n = frame.len().min(FRAME_LEN);
        self.prod.push_slice(&frame[..n]);
        for _ in n..FRAME_LEN {
            // Cannot fail: vacancy was checked above and this is the only producer.
            let _ = self.prod.try_push(0.0);
        }
        true
    }

    /// Total frames dropped since construction.
    pub fn overruns(&self) -> u64 {
        self.overruns.load(Ordering::Relaxed)
    }
}

/// Consumer end. Held by the VAD thread.
pub struct Frames {
    cons: Cons,
    overruns: Arc<AtomicU64>,
    source: Source,
    capacity_frames: usize,
}

impl Frames {
    pub fn source(&self) -> Source {
        self.source
    }

    /// Pop one frame into `out`. Returns `false` (and leaves `out` untouched) when fewer than
    /// [`FRAME_LEN`] samples are queued.
    pub fn pop_frame(&mut self, out: &mut [f32; FRAME_LEN]) -> bool {
        if self.cons.occupied_len() < FRAME_LEN {
            return false;
        }
        self.cons.pop_slice(out);
        true
    }

    /// Whole frames currently queued.
    pub fn queued_frames(&self) -> usize {
        self.cons.occupied_len() / FRAME_LEN
    }

    /// Frames the ring can hold.
    pub fn capacity_frames(&self) -> usize {
        self.capacity_frames
    }

    /// Total frames the producer dropped because the ring was full. The status bar reads this.
    pub fn overruns(&self) -> u64 {
        self.overruns.load(Ordering::Relaxed)
    }
}

/// Build a ring holding `capacity_frames` frames and split it into its two ends.
pub fn frame_channel(source: Source, capacity_frames: usize) -> (FrameSink, Frames) {
    let capacity_frames = capacity_frames.max(1);
    let (prod, cons) = HeapRb::<f32>::new(capacity_frames * FRAME_LEN).split();
    let overruns = Arc::new(AtomicU64::new(0));
    (
        FrameSink {
            prod,
            overruns: Arc::clone(&overruns),
        },
        Frames {
            cons,
            overruns,
            source,
            capacity_frames,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    fn frame(value: f32) -> Vec<f32> {
        vec![value; FRAME_LEN]
    }

    #[test]
    fn frames_flow_in_order() {
        let (mut sink, mut frames) = frame_channel(Source::You, 4);
        assert_eq!(frames.source(), Source::You);
        assert_eq!(frames.capacity_frames(), 4);
        for v in [1.0, 2.0, 3.0] {
            assert!(sink.push_frame(&frame(v)));
        }
        assert_eq!(frames.queued_frames(), 3);
        let mut out = [0.0; FRAME_LEN];
        for v in [1.0, 2.0, 3.0] {
            assert!(frames.pop_frame(&mut out));
            assert!(out.iter().all(|&s| s == v));
        }
        assert!(!frames.pop_frame(&mut out));
        assert_eq!(frames.overruns(), 0);
    }

    #[test]
    fn overrun_drops_one_frame_counts_and_never_blocks() {
        let (mut sink, mut frames) = frame_channel(Source::Interviewer, 2);
        assert!(sink.push_frame(&frame(1.0)));
        assert!(sink.push_frame(&frame(2.0)));
        let t = Instant::now();
        for v in [3.0, 4.0, 5.0] {
            assert!(
                !sink.push_frame(&frame(v)),
                "ring full: frame {v} is dropped"
            );
        }
        assert!(
            t.elapsed() < Duration::from_millis(5),
            "push on a full ring returned at once"
        );
        assert_eq!(sink.overruns(), 3);
        assert_eq!(frames.overruns(), 3);
        assert_eq!(
            frames.queued_frames(),
            2,
            "exactly one frame lost per overrun"
        );

        // Draining one frame frees exactly one slot.
        let mut out = [0.0; FRAME_LEN];
        assert!(frames.pop_frame(&mut out));
        assert!(sink.push_frame(&frame(6.0)));
        assert!(!sink.push_frame(&frame(7.0)));
        assert_eq!(frames.overruns(), 4);
    }

    #[test]
    fn short_frames_are_zero_padded() {
        let (mut sink, mut frames) = frame_channel(Source::Unknown, 1);
        assert!(sink.push_frame(&[0.5; 10]));
        let mut out = [1.0; FRAME_LEN];
        assert!(frames.pop_frame(&mut out));
        assert!(out[..10].iter().all(|&s| s == 0.5));
        assert!(out[10..].iter().all(|&s| s == 0.0));
    }
}
