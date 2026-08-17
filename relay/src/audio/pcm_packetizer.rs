//! Fixed-capacity PCM packetization for the relay media worker.
//!
//! Capture APIs may deliver arbitrary callback frame sizes, while an Opus
//! encoder invocation requires one exact configured duration. This adapter
//! owns that transport concern without moving allocation or blocking into
//! `PocketStation` capture callbacks or realtime graph partitions.

use pocketstation::codec::OPUS_FRAME_SAMPLES;

const MAX_CHANNELS: usize = 2;
const MAX_INTERLEAVED_SAMPLES: usize = OPUS_FRAME_SAMPLES * MAX_CHANNELS;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PcmPacketizerError {
    UnsupportedChannelCount(u8),
    UnsupportedSampleRate(u32),
    MisalignedInput { samples: usize, channels: u8 },
}

/// Bounded assembler for one 20 ms, 48 kHz mono or stereo transport packet.
pub(crate) struct PcmPacketizer {
    samples: [f32; MAX_INTERLEAVED_SAMPLES],
    written: usize,
    packet_samples: usize,
    channels: u8,
    packet_timestamp_ns: u64,
    expected_sequence_number: Option<u64>,
}

impl PcmPacketizer {
    pub(crate) fn new(sample_rate_hz: u32, channels: u8) -> Result<Self, PcmPacketizerError> {
        if sample_rate_hz != 48_000 {
            return Err(PcmPacketizerError::UnsupportedSampleRate(sample_rate_hz));
        }
        if !matches!(channels, 1 | 2) {
            return Err(PcmPacketizerError::UnsupportedChannelCount(channels));
        }
        Ok(Self {
            samples: [0.0; MAX_INTERLEAVED_SAMPLES],
            written: 0,
            packet_samples: OPUS_FRAME_SAMPLES * usize::from(channels),
            channels,
            packet_timestamp_ns: 0,
            expected_sequence_number: None,
        })
    }

    /// Starts one source frame and returns the number of partial samples
    /// discarded at a sequence discontinuity.
    pub(crate) fn begin_frame(
        &mut self,
        sequence_number: u64,
        sample_count: usize,
    ) -> Result<usize, PcmPacketizerError> {
        if !sample_count.is_multiple_of(usize::from(self.channels)) {
            return Err(PcmPacketizerError::MisalignedInput {
                samples: sample_count,
                channels: self.channels,
            });
        }
        let discarded = if self
            .expected_sequence_number
            .is_some_and(|expected| expected != sequence_number)
        {
            self.discard_partial()
        } else {
            0
        };
        self.expected_sequence_number = Some(sequence_number.saturating_add(1));
        Ok(discarded)
    }

    /// Copies as much of `input` as fits and returns the number of interleaved
    /// samples consumed. Storage is embedded and this method never allocates.
    pub(crate) fn push(
        &mut self,
        input: &[f32],
        input_timestamp_ns: u64,
        input_offset: usize,
    ) -> usize {
        if self.written == 0 {
            let offset_frames = input_offset / usize::from(self.channels);
            let offset_ns = (offset_frames as u64)
                .saturating_mul(1_000_000_000)
                .checked_div(48_000)
                .unwrap_or(0);
            self.packet_timestamp_ns = input_timestamp_ns.saturating_add(offset_ns);
        }
        let copied = input.len().min(self.packet_samples - self.written);
        self.samples[self.written..self.written + copied].copy_from_slice(&input[..copied]);
        self.written += copied;
        copied
    }

    pub(crate) fn complete_packet(&self) -> Option<(&[f32], u64)> {
        (self.written == self.packet_samples).then_some((
            &self.samples[..self.packet_samples],
            self.packet_timestamp_ns,
        ))
    }

    pub(crate) fn finish_packet(&mut self) {
        debug_assert_eq!(self.written, self.packet_samples);
        self.written = 0;
        self.packet_timestamp_ns = 0;
    }

    /// Zero-pads at most one final packet during orderly shutdown.
    pub(crate) fn pad_tail(&mut self) -> Option<usize> {
        if self.written == 0 {
            return None;
        }
        let padding = self.packet_samples - self.written;
        self.samples[self.written..self.packet_samples].fill(0.0);
        self.written = self.packet_samples;
        Some(padding)
    }

    fn discard_partial(&mut self) -> usize {
        let discarded = self.written;
        self.written = 0;
        self.packet_timestamp_ns = 0;
        discarded
    }
}

impl std::fmt::Display for PcmPacketizerError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedChannelCount(channels) => {
                write!(formatter, "relay PCM input has unsupported channel count {channels}")
            }
            Self::UnsupportedSampleRate(sample_rate_hz) => write!(
                formatter,
                "relay PCM input has unsupported sample rate {sample_rate_hz} Hz; expected 48000 Hz"
            ),
            Self::MisalignedInput { samples, channels } => write!(
                formatter,
                "relay PCM input has {samples} interleaved samples, which is not aligned to {channels} channels"
            ),
        }
    }
}

impl std::error::Error for PcmPacketizerError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arbitrary_stereo_callback_widths_form_exact_twenty_millisecond_packets() {
        let mut packetizer = PcmPacketizer::new(48_000, 2).unwrap();
        assert_eq!(packetizer.begin_frame(1, 1_024).unwrap(), 0);
        assert_eq!(packetizer.push(&[0.5; 1_024], 10, 0), 1_024);
        assert_eq!(packetizer.begin_frame(2, 1_024).unwrap(), 0);
        assert_eq!(packetizer.push(&[0.25; 1_024], 20, 0), 896);
        assert_eq!(packetizer.complete_packet().unwrap().0.len(), 1_920);
    }

    #[test]
    fn discontinuity_discards_only_the_bounded_partial_packet() {
        let mut packetizer = PcmPacketizer::new(48_000, 1).unwrap();
        packetizer.begin_frame(7, 480).unwrap();
        packetizer.push(&[0.0; 480], 10, 0);
        assert_eq!(packetizer.begin_frame(9, 480).unwrap(), 480);
    }
}
