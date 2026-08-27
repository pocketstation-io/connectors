//! Opus encoder thread and the frame/counter types it produces.

use pocketstation::codec::{OpusConfig, OpusEncodeError, OpusEncoder, OPUS_MAX_PACKET_BYTES};
use pocketstation::OutputGeneration;
#[cfg(any(target_os = "macos", target_os = "windows", target_os = "linux"))]
use std::time::Instant;
use str0m::media::{Frequency, MediaTime};

#[cfg(any(target_os = "macos", target_os = "windows", target_os = "linux"))]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(any(target_os = "macos", target_os = "windows", target_os = "linux"))]
use std::sync::{mpsc, Arc};

use crate::audio::pcm_packetizer::PcmPacketizer;

/// A frame the encoder can consume, regardless of channel layout. The interleaved
/// sample slice is 960 (20 ms mono) or 1920 (20 ms stereo); `OpusEncoder` is
/// channel-aware and validates the length against its configured channel count.
/// This lets one encoder support content-aware profiles — true mono for voice /
/// conference (`source broadcast`) and true stereo for music (`source system`) —
/// with no dual-mono padding.
#[cfg(any(target_os = "macos", target_os = "windows", target_os = "linux"))]
pub(crate) trait EncodableFrame: Send + 'static {
    fn samples(&self) -> &[f32];
    fn sequence_number(&self) -> u64;
    fn sample_rate_hz(&self) -> u32;
    fn channels(&self) -> u8;
    /// Monotonic nanoseconds at which the first sample of this frame was captured.
    /// Returns 0 when the source has no capture timestamp (e.g. sine generator).
    fn capture_timestamp_ns(&self) -> u64;
    fn output_generation(&self) -> Option<OutputGeneration> {
        None
    }
}

/// Zero-copy bridge: `AudioFrame`'s pool-backed buffer stays alive until the
/// encoder finishes encoding. No intermediate Box<[f32]> allocation.
#[cfg(any(target_os = "macos", target_os = "windows", target_os = "linux"))]
impl EncodableFrame for pocketstation::AudioFrame {
    fn samples(&self) -> &[f32] {
        self.samples()
    }

    fn capture_timestamp_ns(&self) -> u64 {
        self.timestamp_ns()
    }

    fn sequence_number(&self) -> u64 {
        self.sequence_number()
    }

    fn sample_rate_hz(&self) -> u32 {
        self.sample_rate_hz()
    }

    fn channels(&self) -> u8 {
        self.channels()
    }
}

impl EncodableFrame for pocketstation::EndpointAudioFrame {
    fn samples(&self) -> &[f32] {
        self.samples()
    }

    fn capture_timestamp_ns(&self) -> u64 {
        self.timestamp_ns()
    }

    fn sequence_number(&self) -> u64 {
        self.sequence_number()
    }

    fn sample_rate_hz(&self) -> u32 {
        self.sample_rate_hz()
    }

    fn channels(&self) -> u8 {
        self.channels()
    }

    fn output_generation(&self) -> Option<OutputGeneration> {
        self.output_generation().cloned()
    }
}

/// Budget for the age of a frame at encode time: 2× the 20 ms frame period.
/// Frames older than this indicate upstream buffering beyond the design target.
#[cfg(any(target_os = "macos", target_os = "windows", target_os = "linux"))]
pub(crate) const CAPTURE_AGE_BUDGET_NS: u64 = 40_000_000; // 40 ms

/// Real-time counters shared between the frame-assembler thread, the encoder
/// thread, and the publisher loop status line.  All fields are monotonically
/// increasing totals.
#[cfg(any(target_os = "macos", target_os = "windows", target_os = "linux"))]
#[derive(Default)]
pub(crate) struct EncoderCounters {
    /// Partial interleaved samples discarded at an explicit normalization
    /// boundary. A non-zero value is always observable data loss.
    pub(crate) normalization_partial_samples_discarded: AtomicU64,
    /// Zero-valued interleaved samples appended to complete the final canonical
    /// frame during a clean capture shutdown. This is bounded to less than one
    /// canonical frame per source session and is not source-audio loss.
    pub(crate) normalization_tail_padding_samples_total: AtomicU64,
    /// Opus frames successfully encoded.
    pub(crate) opus_packets_out: AtomicU64,
    /// Opus encode errors (`OPUS_BAD_ARG` or similar).
    pub(crate) opus_encode_errors: AtomicU64,
    /// Frames dropped because the encoder→publisher channel was full.
    pub(crate) encoded_channel_drops: AtomicU64,
    /// Encoded frames discarded in favor of a fresher frame after an RTC-loop stall.
    pub(crate) publisher_stale_drops: AtomicU64,
    /// Output frames discarded after their application operation was cancelled.
    pub(crate) cancelled_output_frames: AtomicU64,
    /// Partial PCM samples removed when output ownership changes.
    pub(crate) cancelled_output_samples: AtomicU64,
    /// Cumulative capture age in nanoseconds (for mean computation).
    pub(crate) capture_age_sum_ns: AtomicU64,
    /// Maximum capture age observed, in nanoseconds.
    pub(crate) capture_age_max_ns: AtomicU64,
    /// Number of frames that contributed a non-zero capture age sample.
    pub(crate) capture_age_sample_count: AtomicU64,
    /// Frames whose capture age exceeded `CAPTURE_AGE_BUDGET_NS`.
    pub(crate) capture_age_over_budget: AtomicU64,
}

/// An Opus frame ready to inject into str0m via `Writer::write`.
pub(crate) struct EncodedAudioFrame {
    pub(crate) payload: Vec<u8>,
    pub(crate) wallclock: Instant,
    pub(crate) rtp_time: MediaTime,
    #[allow(dead_code)]
    pub(crate) duration_samples: u32,
    pub(crate) audio_level: Option<i8>,
    /// Monotonic nanoseconds at which the first sample of this frame was captured.
    /// 0 when the source has no capture timestamp (sine generator, test frames).
    #[allow(dead_code)]
    pub(crate) capture_timestamp_ns: u64,
    /// Nanoseconds between capture and the end of Opus encoding.
    /// 0 when `capture_timestamp_ns` is 0.
    #[allow(dead_code)]
    pub(crate) capture_age_ns: u64,
    pub(crate) output_generation: Option<OutputGeneration>,
}

/// Delivery behavior at the bounded encoder-to-publisher boundary.
///
/// This boundary runs on the encoder worker, never on a capture callback or a
/// realtime graph partition. Standard sessions may therefore apply bounded
/// backpressure without violating the hot-path contract. Low-latency sessions
/// retain an explicit lossy policy rather than accumulating stale audio.
#[cfg(any(target_os = "macos", target_os = "windows", target_os = "linux"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EncodedDeliveryPolicy {
    PreserveWithBackpressure,
    DropNewest,
}

#[cfg(any(target_os = "macos", target_os = "windows", target_os = "linux"))]
impl EncodedDeliveryPolicy {
    pub(crate) fn for_latency_profile(
        latency_profile: crate::audio::latency_profile::LatencyProfile,
    ) -> Self {
        match latency_profile {
            crate::audio::latency_profile::LatencyProfile::Low => Self::DropNewest,
            crate::audio::latency_profile::LatencyProfile::Standard => {
                Self::PreserveWithBackpressure
            }
        }
    }
}

/// Returns false only when the publisher receiver has disconnected.
#[cfg(any(target_os = "macos", target_os = "windows", target_os = "linux"))]
fn deliver_encoded_frame(
    encoded_tx: &mpsc::SyncSender<EncodedAudioFrame>,
    encoded_frame: EncodedAudioFrame,
    delivery_policy: EncodedDeliveryPolicy,
    counters: &EncoderCounters,
) -> bool {
    if encoded_frame
        .output_generation
        .as_ref()
        .is_some_and(|generation| !generation.is_active())
    {
        counters
            .cancelled_output_frames
            .fetch_add(1, Ordering::Relaxed);
        return true;
    }
    match delivery_policy {
        EncodedDeliveryPolicy::PreserveWithBackpressure => encoded_tx.send(encoded_frame).is_ok(),
        EncodedDeliveryPolicy::DropNewest => match encoded_tx.try_send(encoded_frame) {
            Ok(()) => true,
            Err(mpsc::TrySendError::Full(_)) => {
                counters
                    .encoded_channel_drops
                    .fetch_add(1, Ordering::Relaxed);
                true
            }
            Err(mpsc::TrySendError::Disconnected(_)) => false,
        },
    }
}

struct EncoderWorker {
    encoder: OpusEncoder,
    output: Vec<u8>,
    rtp_sample_count: u64,
    rtp_step_samples: u64,
}

impl EncoderWorker {
    fn new(config: &OpusConfig) -> Result<Self, OpusEncodeError> {
        Ok(Self {
            encoder: OpusEncoder::from_config(config)?,
            output: Vec::with_capacity(OPUS_MAX_PACKET_BYTES),
            rtp_sample_count: 0,
            rtp_step_samples: config.frame_duration.samples_at_48k() as u64,
        })
    }

    fn encode_and_deliver(
        &mut self,
        samples: &[f32],
        capture_timestamp_ns: u64,
        output_generation: Option<OutputGeneration>,
        encoded_tx: &mpsc::SyncSender<EncodedAudioFrame>,
        delivery_policy: EncodedDeliveryPolicy,
        counters: &EncoderCounters,
    ) -> bool {
        if output_generation
            .as_ref()
            .is_some_and(|generation| !generation.is_active())
        {
            counters
                .cancelled_output_frames
                .fetch_add(1, Ordering::Relaxed);
            return true;
        }
        let square_sum: f32 = samples.iter().map(|sample| sample * sample).sum();
        let sample_count = u16::try_from(samples.len()).unwrap_or(u16::MAX);
        let rms = if sample_count == 0 {
            0.0
        } else {
            (square_sum / f32::from(sample_count)).sqrt()
        };
        #[allow(clippy::cast_possible_truncation)]
        let audio_level = if rms > 1e-9 {
            Some((-20.0_f32 * rms.log10()).clamp(0.0, 127.0).round() as i8)
        } else {
            Some(127)
        };

        self.output.clear();
        if self.encoder.encode_into(samples, &mut self.output).is_err() {
            counters.opus_encode_errors.fetch_add(1, Ordering::Relaxed);
            return true;
        }
        counters.opus_packets_out.fetch_add(1, Ordering::Relaxed);

        let now_ns = crate::audio::clock::monotonic_ns();
        let capture_age_ns = if capture_timestamp_ns > 0 {
            now_ns.saturating_sub(capture_timestamp_ns)
        } else {
            0
        };
        observe_capture_age(counters, capture_age_ns);

        let encoded_frame = EncodedAudioFrame {
            payload: self.output.clone(),
            wallclock: Instant::now(),
            rtp_time: MediaTime::new(self.rtp_sample_count, Frequency::FORTY_EIGHT_KHZ),
            duration_samples: u32::try_from(self.rtp_step_samples).unwrap_or(u32::MAX),
            audio_level,
            capture_timestamp_ns,
            capture_age_ns,
            output_generation,
        };
        self.rtp_sample_count = self.rtp_sample_count.saturating_add(self.rtp_step_samples);
        deliver_encoded_frame(encoded_tx, encoded_frame, delivery_policy, counters)
    }
}

fn observe_capture_age(counters: &EncoderCounters, capture_age_ns: u64) {
    if capture_age_ns == 0 {
        return;
    }
    counters
        .capture_age_sum_ns
        .fetch_add(capture_age_ns, Ordering::Relaxed);
    counters
        .capture_age_sample_count
        .fetch_add(1, Ordering::Relaxed);
    let mut previous = counters.capture_age_max_ns.load(Ordering::Relaxed);
    while capture_age_ns > previous {
        match counters.capture_age_max_ns.compare_exchange_weak(
            previous,
            capture_age_ns,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => break,
            Err(actual) => previous = actual,
        }
    }
    if capture_age_ns > CAPTURE_AGE_BUDGET_NS {
        counters
            .capture_age_over_budget
            .fetch_add(1, Ordering::Relaxed);
    }
}

/// Spawns the bounded PCM packetizer and Opus encoder worker.
///
/// Source callback widths are normalized into exact transport packets on this
/// ordinary worker thread. Capture callbacks and realtime graph partitions do
/// not allocate, block, or invoke the codec.
#[cfg(any(target_os = "macos", target_os = "windows", target_os = "linux"))]
pub(crate) fn spawn_opus_encoder<F: EncodableFrame>(
    frame_rx: mpsc::Receiver<F>,
    encoded_tx: mpsc::SyncSender<EncodedAudioFrame>,
    counters: Arc<EncoderCounters>,
    config: OpusConfig,
    delivery_policy: EncodedDeliveryPolicy,
) -> Result<std::thread::JoinHandle<()>, Box<dyn std::error::Error>> {
    let handle = std::thread::Builder::new()
        .name("pks-encoder".into())
        .spawn(move || {
            let Ok(mut worker) = EncoderWorker::new(&config) else {
                counters.opus_encode_errors.fetch_add(1, Ordering::Relaxed);
                return;
            };
            let channels = config.channels.count();
            let Ok(mut packetizer) = PcmPacketizer::new(48_000, channels) else {
                counters.opus_encode_errors.fetch_add(1, Ordering::Relaxed);
                return;
            };
            let mut packet_output: Option<OutputGeneration> = None;

            while let Ok(frame) = frame_rx.recv() {
                if frame.sample_rate_hz() != 48_000 || frame.channels() != channels {
                    counters.opus_encode_errors.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                let output_generation = frame.output_generation();
                if output_generation
                    .as_ref()
                    .is_some_and(|generation| !generation.is_active())
                {
                    counters
                        .cancelled_output_frames
                        .fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                let output_generation_id = output_generation
                    .as_ref()
                    .map(|generation| generation.id().get());
                let samples = frame.samples();
                let Ok(boundary) = packetizer.begin_frame(
                    frame.sequence_number(),
                    samples.len(),
                    output_generation_id,
                ) else {
                    counters.opus_encode_errors.fetch_add(1, Ordering::Relaxed);
                    continue;
                };
                if packetizer.output_generation_id()
                    != packet_output.as_ref().map(|value| value.id().get())
                {
                    packet_output = output_generation;
                }
                counters.normalization_partial_samples_discarded.fetch_add(
                    boundary.discontinuity_discarded_samples as u64,
                    Ordering::Relaxed,
                );
                counters.cancelled_output_samples.fetch_add(
                    boundary.cancelled_output_discarded_samples as u64,
                    Ordering::Relaxed,
                );

                let mut offset = 0;
                while offset < samples.len() {
                    offset +=
                        packetizer.push(&samples[offset..], frame.capture_timestamp_ns(), offset);
                    if let Some((packet, timestamp_ns)) = packetizer.complete_packet() {
                        if !worker.encode_and_deliver(
                            packet,
                            timestamp_ns,
                            packet_output.clone(),
                            &encoded_tx,
                            delivery_policy,
                            counters.as_ref(),
                        ) {
                            return;
                        }
                        packetizer.finish_packet();
                    }
                }
            }

            if let Some(padding) = packetizer.pad_tail() {
                counters
                    .normalization_tail_padding_samples_total
                    .fetch_add(padding as u64, Ordering::Relaxed);
                if let Some((packet, timestamp_ns)) = packetizer.complete_packet() {
                    let _ = worker.encode_and_deliver(
                        packet,
                        timestamp_ns,
                        packet_output,
                        &encoded_tx,
                        delivery_policy,
                        counters.as_ref(),
                    );
                }
            }
        })?;
    Ok(handle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pocketstation::{AudioInputConfig, OutputCancelResult, SampleFormat, SampleSpec, Session};

    fn output_generation() -> OutputGeneration {
        let session = Session::builder()
            .sample_spec(SampleSpec::new(48_000, 1, SampleFormat::F32Interleaved))
            .build();
        let input = session
            .audio_input(
                AudioInputConfig::new(
                    SampleSpec::new(48_000, 1, SampleFormat::F32Interleaved),
                    2,
                    960,
                )
                .expect("valid output input"),
            )
            .expect("output input");
        input.begin_output_generation().expect("output generation")
    }

    fn encoded_frame(sequence: u8) -> EncodedAudioFrame {
        EncodedAudioFrame {
            payload: vec![sequence],
            wallclock: Instant::now(),
            rtp_time: MediaTime::new(u64::from(sequence) * 960, Frequency::FORTY_EIGHT_KHZ),
            duration_samples: 960,
            audio_level: None,
            capture_timestamp_ns: 0,
            capture_age_ns: 0,
            output_generation: None,
        }
    }

    // Given a temporarily saturated one-frame publisher channel,
    // When Standard delivery sends a burst from the encoder worker,
    // Then backpressure preserves every frame in sequence without a drop.
    #[test]
    fn given_saturated_channel_when_standard_delivery_bursts_then_all_frames_are_preserved() {
        const FRAME_COUNT: u8 = 16;
        let counters = Arc::new(EncoderCounters::default());
        let sender_counters = Arc::clone(&counters);
        let (encoded_tx, encoded_rx) = mpsc::sync_channel(1);

        let sender = std::thread::spawn(move || {
            for sequence in 0..FRAME_COUNT {
                assert!(deliver_encoded_frame(
                    &encoded_tx,
                    encoded_frame(sequence),
                    EncodedDeliveryPolicy::PreserveWithBackpressure,
                    sender_counters.as_ref(),
                ));
            }
        });

        std::thread::sleep(std::time::Duration::from_millis(5));
        let received: Vec<u8> = (0..FRAME_COUNT)
            .map(|_| encoded_rx.recv().expect("sender remains connected").payload[0])
            .collect();
        sender.join().expect("sender thread must complete");

        assert_eq!(received, (0..FRAME_COUNT).collect::<Vec<_>>());
        assert_eq!(counters.encoded_channel_drops.load(Ordering::Relaxed), 0);
    }

    // Given a saturated one-frame publisher channel,
    // When Low delivery sends another frame,
    // Then it drops the newest frame and reports exactly one quality event.
    #[test]
    fn given_saturated_channel_when_low_delivery_sends_then_newest_frame_is_dropped() {
        let counters = EncoderCounters::default();
        let (encoded_tx, encoded_rx) = mpsc::sync_channel(1);

        assert!(deliver_encoded_frame(
            &encoded_tx,
            encoded_frame(1),
            EncodedDeliveryPolicy::DropNewest,
            &counters,
        ));
        assert!(deliver_encoded_frame(
            &encoded_tx,
            encoded_frame(2),
            EncodedDeliveryPolicy::DropNewest,
            &counters,
        ));

        assert_eq!(
            encoded_rx.recv().expect("first frame remains").payload,
            vec![1]
        );
        assert_eq!(counters.encoded_channel_drops.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn given_cancelled_output_when_encoded_delivery_runs_then_frame_is_discarded() {
        let counters = EncoderCounters::default();
        let (encoded_tx, encoded_rx) = mpsc::sync_channel(1);
        let generation = output_generation();
        let mut frame = encoded_frame(1);
        frame.output_generation = Some(generation.clone());

        assert_eq!(generation.cancel(), OutputCancelResult::Cancelled);
        assert!(deliver_encoded_frame(
            &encoded_tx,
            frame,
            EncodedDeliveryPolicy::PreserveWithBackpressure,
            &counters,
        ));

        assert!(matches!(
            encoded_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        assert_eq!(counters.cancelled_output_frames.load(Ordering::Relaxed), 1);
    }

    // Given an EncodedAudioFrame with capture_timestamp_ns == 0,
    // When capture_age_ns is read,
    // Then it is also 0 (unknown-source frames carry no age).
    #[test]
    fn given_encoded_frame_when_capture_timestamp_zero_then_age_is_zero() {
        let frame = EncodedAudioFrame {
            payload: vec![],
            wallclock: Instant::now(),
            rtp_time: str0m::media::MediaTime::new(0, str0m::media::Frequency::FORTY_EIGHT_KHZ),
            duration_samples: 960,
            audio_level: None,
            capture_timestamp_ns: 0,
            capture_age_ns: 0,
            output_generation: None,
        };
        assert_eq!(frame.capture_timestamp_ns, 0);
        assert_eq!(frame.capture_age_ns, 0);
    }

    // Given an EncodedAudioFrame with a known capture_timestamp_ns,
    // When capture_age_ns is computed as now - capture_ts,
    // Then age is a positive number of nanoseconds.
    #[test]
    fn given_encoded_frame_when_capture_timestamp_present_then_age_computed() {
        use crate::audio::clock::monotonic_ns;
        let capture_ts = monotonic_ns();
        // Simulate a small delay between capture and encode.
        std::thread::sleep(std::time::Duration::from_millis(1));
        let age_ns = monotonic_ns().saturating_sub(capture_ts);
        let frame = EncodedAudioFrame {
            payload: vec![],
            wallclock: Instant::now(),
            rtp_time: str0m::media::MediaTime::new(0, str0m::media::Frequency::FORTY_EIGHT_KHZ),
            duration_samples: 960,
            audio_level: None,
            capture_timestamp_ns: capture_ts,
            capture_age_ns: age_ns,
            output_generation: None,
        };
        assert!(
            frame.capture_age_ns > 0,
            "capture_age_ns must be > 0 when timestamp is present"
        );
        assert_eq!(frame.capture_timestamp_ns, capture_ts);
    }

    // Given CAPTURE_AGE_BUDGET_NS,
    // When compared to the 20 ms frame period in nanoseconds,
    // Then it equals exactly 2× the frame period (40 ms).
    #[test]
    fn given_capture_age_budget_when_checked_then_equals_40ms() {
        const FRAME_PERIOD_NS: u64 = 20_000_000; // 20 ms
        assert_eq!(
            CAPTURE_AGE_BUDGET_NS,
            2 * FRAME_PERIOD_NS,
            "CAPTURE_AGE_BUDGET_NS must equal 2× the 20 ms frame period"
        );
    }
}
