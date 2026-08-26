//! Monotonic process-epoch clock for capture-time lineage.
//!
//! The process-wide epoch is owned by `pocketstation::timing` and shared across
//! capture, runtime, and external endpoint workers. All measurements are
//! nanoseconds relative to that epoch, never wall-clock time.
//!
//! Hot-path safety: the shared `OnceLock::get` is lock-free after initialisation;
//! `Instant::now()` is a vDSO call on Linux/macOS.  No allocation, no lock,
//! no blocking on any call after the first.

/// Returns nanoseconds elapsed since the process-epoch anchor.
///
/// The first call establishes the epoch; every subsequent call reads it
/// lock-free. The value is always nonzero because `0` is the unset timestamp
/// sentinel in graph accumulators.
///
/// # Hot-path requirements
/// No allocation · No lock · No blocking · No logging
#[inline]
pub(crate) fn monotonic_ns() -> u64 {
    pocketstation::timing::monotonic_timestamp_ns()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_two_calls_when_sequential_then_second_is_gte_first() {
        let t0 = monotonic_ns();
        let t1 = monotonic_ns();
        assert!(t1 >= t0, "monotonic_ns must be non-decreasing");
    }

    #[test]
    fn given_first_call_when_returned_then_nonzero() {
        assert!(monotonic_ns() > 0);
    }
}
