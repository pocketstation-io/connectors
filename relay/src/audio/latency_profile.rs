//! Latency profiles for the audio capture pipeline.
//!
//! Two profiles trade buffer depth (jitter tolerance) for end-to-end latency:
//!
//! | Profile  | Ring capacity | Capture channel depth | Target use case            |
//! |----------|--------------|----------------------|------------------------------|
//! | Standard | 16 frames    | 16 frames            | Glitch-resistant capture     |
//! | Low      | 2 frames     | 2 frames             | Live demos, latency-critical |
//!
//! Both profiles are deliberately shallow. Queue depth is scheduling headroom,
//! not a jitter buffer; receiver-side `NetEQ` owns network jitter adaptation.

/// Latency-vs-robustness tradeoff for the capture pipeline.
///
/// Passed to `start_graph_bridge` and `start_graph_bridge_profiled` to
/// control the ring and channel depths.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LatencyProfile {
    /// 2-frame ring + 2-frame channel.  Drop-oldest on overrun.
    /// Best for low-latency live demos where an occasional gap is acceptable.
    Low,
    /// 16-frame ring + 16-frame channels. Native capture callbacks are not
    /// required to contain one encoded 20 ms frame: WASAPI commonly delivers
    /// smaller packets. Sixteen slots preserve at least 160 ms of scheduling
    /// headroom for 10 ms callback packets. Empty capacity adds no steady-state
    /// latency.
    Standard,
}

impl LatencyProfile {
    /// Bounded channel depth for `AudioFrame` values between the capture callback
    /// and the graph bridge thread.
    pub(crate) fn capture_channel_depth(self) -> usize {
        match self {
            Self::Low => 2,
            Self::Standard => 16,
        }
    }

    /// Bounded encoded-frame depth between Opus and the RTP publisher.
    /// A low-latency profile must remain bounded after encoding too.
    pub(crate) fn encoded_channel_depth(self) -> usize {
        match self {
            Self::Low => 2,
            Self::Standard => 8,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Given the Low profile,
    // When capture_channel_depth is queried,
    // Then it returns 2 (40 ms at 20 ms/frame).
    #[test]
    fn given_low_profile_when_capture_depth_then_2() {
        assert_eq!(LatencyProfile::Low.capture_channel_depth(), 2);
    }

    // Given the Standard profile,
    // When capture_channel_depth is queried,
    // Then it returns 16 (at least 160 ms for 10 ms native packets).
    #[test]
    fn given_standard_profile_when_capture_depth_then_sixteen() {
        assert_eq!(LatencyProfile::Standard.capture_channel_depth(), 16);
    }

    #[test]
    fn given_low_profile_when_encoded_depth_then_two_frames() {
        assert_eq!(LatencyProfile::Low.encoded_channel_depth(), 2);
    }

    #[test]
    fn given_standard_profile_when_encoded_depth_then_eight_frames() {
        assert_eq!(LatencyProfile::Standard.encoded_channel_depth(), 8);
    }
}
