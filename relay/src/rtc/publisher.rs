//! Invariant-correct str0m RTP publisher loop.

use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::Ordering;
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};
use str0m::media::Mid;
use str0m::net::{Protocol, Receive};
use str0m::{Input, Rtc};

use crate::audio::opus_worker::{EncodedAudioFrame, EncoderCounters};
use crate::rtc::signaling::{
    send_leave, ws_set_read_timeout, ws_set_write_timeout, Ws, WS_POLL_TIMEOUT, WS_WRITE_TIMEOUT,
};
use crate::rtc::types::drain_all_outputs;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PublisherBacklogPolicy {
    /// Preserve every bounded queued frame. This is the production policy for
    /// music because timestamp holes force receiver-side concealment.
    Preserve,
    /// Keep only the newest queued frame. This is opt-in for interactive paths
    /// where freshness is more important than an audible gap.
    DropStale,
}

/// Return the next frame according to the selected bounded-backlog policy.
fn try_recv_frame(
    encoded_rx: &mpsc::Receiver<EncodedAudioFrame>,
    backlog_policy: PublisherBacklogPolicy,
) -> Result<(EncodedAudioFrame, u64), mpsc::TryRecvError> {
    let mut freshest = encoded_rx.try_recv()?;
    if backlog_policy == PublisherBacklogPolicy::Preserve {
        return Ok((freshest, 0));
    }
    let mut stale_drops = 0u64;
    loop {
        match encoded_rx.try_recv() {
            Ok(next) => {
                freshest = next;
                stale_drops += 1;
            }
            Err(mpsc::TryRecvError::Empty | mpsc::TryRecvError::Disconnected) => {
                return Ok((freshest, stale_drops));
            }
        }
    }
}

fn classify_signaling_read(
    result: tungstenite::Result<tungstenite::Message>,
) -> Result<(), Box<dyn std::error::Error>> {
    match result {
        Ok(tungstenite::Message::Close(_)) | Err(tungstenite::Error::ConnectionClosed) => {
            Err("relay signaling connection closed during publication".into())
        }
        Ok(_) => Ok(()),
        Err(tungstenite::Error::Io(error))
            if matches!(
                error.kind(),
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
            ) =>
        {
            Ok(())
        }
        Err(error) => Err(format!("relay signaling connection failed: {error}").into()),
    }
}

/// How often the source sends a WebSocket ping to the relay.
/// The relay kills connections that are silent for 90 s; 45 s keeps us well within that window.
const WS_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(45);

// RTP publisher loop

/// Statistics returned by [`run_publish_loop`] when it exits cleanly.
pub(crate) struct PublishStats {
    pub(crate) streams: Vec<PublishStreamStats>,
    pub(crate) elapsed: Duration,
    pub(crate) drains: u64,
    pub(crate) writes: u64,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct PublishStreamStats {
    pub(crate) rtp_sent: u64,
    pub(crate) bytes_sent: u64,
}

pub(crate) struct PublishStream {
    pub(crate) mid: Mid,
    pub(crate) encoded_rx: mpsc::Receiver<EncodedAudioFrame>,
    pub(crate) backlog_policy: PublisherBacklogPolicy,
    pub(crate) counters: Arc<EncoderCounters>,
}

struct ActivePublishStream {
    stream: PublishStream,
    payload_type: str0m::media::Pt,
    disconnected: bool,
    statistics: PublishStreamStats,
}

/// Invariant-correct str0m RTP publisher loop.
///
/// **Only this function mutates `Rtc`.**  After every mutation it drains
/// `poll_output()` to `Output::Timeout` before handling the next event,
/// satisfying str0m's single-mutation invariant and preventing
/// `WriteWithoutPoll`.
///
/// Loop priority (highest first):
///   1. RTC timeout tick — `handle_input(Timeout)`
///   2. Incoming UDP datagram — `handle_input(Receive)`
///   3. Next encoded Opus frame — `writer.write(...)`
///
/// After each mutation, `drain_all_outputs` is called before the next one.
#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines, clippy::needless_pass_by_value)]
pub(crate) fn run_publish_loop(
    mut rtc: Rtc,
    udp: UdpSocket,
    mut ws: Ws,
    bound_addr: SocketAddr,
    streams: Vec<PublishStream>,
    room: &str,
) -> Result<PublishStats, Box<dyn std::error::Error>> {
    if streams.is_empty() {
        return Err("relay publisher requires at least one encoded AudioBus".into());
    }
    let mut streams = streams
        .into_iter()
        .map(|stream| {
            let payload_type = rtc
                .writer(stream.mid)
                .and_then(|writer| {
                    writer
                        .payload_params()
                        .next()
                        .map(str0m::format::PayloadParams::pt)
                })
                .ok_or("relay AudioBus has no negotiated Opus payload type")?;
            Ok(ActivePublishStream {
                stream,
                payload_type,
                disconnected: false,
                statistics: PublishStreamStats::default(),
            })
        })
        .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;

    // handshake.rs leaves the WS in blocking mode (no read timeout).
    // Re-enable the short poll timeout so we can call ws.read() in the idle
    // section without stalling the RTP loop, and so tungstenite can auto-reply
    // to relay Pings with Pong (it only does so when read() is called).
    ws_set_read_timeout(&ws, Some(WS_POLL_TIMEOUT))?;
    ws_set_write_timeout(&ws, Some(WS_WRITE_TIMEOUT))?;

    udp.set_nonblocking(true)
        .map_err(|e| format!("UDP set_nonblocking: {e}"))?;

    let mut next_rtc_timeout = Instant::now();
    let run_start = Instant::now();

    // ── Diagnostics counters ──────────────────────────────────────────────
    let mut rtc_writer_writes_total: u64 = 0;
    let mut rtc_poll_drains_total: u64 = 0;
    let mut round_robin_start = 0usize;

    // WebSocket keepalive: relay closes the connection after 90 s of silence;
    // send a ping every 45 s to keep the session alive indefinitely.
    let mut last_ws_ping = Instant::now();

    'publish: loop {
        let now = Instant::now();

        // ── Priority 1: RTC timeout tick ─────────────────────────────────
        if now >= next_rtc_timeout {
            rtc.handle_input(Input::Timeout(now))
                .map_err(|e| format!("handle_input Timeout: {e:?}"))?;
            next_rtc_timeout = drain_all_outputs(&mut rtc, &udp)?;
            rtc_poll_drains_total += 1;
            continue 'publish;
        }

        // ── Priority 2: Incoming UDP datagram (non-blocking) ─────────────
        let mut udp_buf = [0u8; 2048];
        match udp.recv_from(&mut udp_buf) {
            Ok((n, src)) => {
                let recv = Receive::new(Protocol::Udp, src, bound_addr, &udp_buf[..n])
                    .map_err(|e| format!("Receive::new: {e:?}"))?;
                rtc.handle_input(Input::Receive(Instant::now(), recv))
                    .map_err(|e| format!("handle_input Receive: {e:?}"))?;
                next_rtc_timeout = drain_all_outputs(&mut rtc, &udp)?;
                rtc_poll_drains_total += 1;
                continue 'publish;
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(e) => return Err(e.into()),
        }

        // ── Priority 3: Publish the next encoded frame ───────────────────
        //
        // Live capture cadence and RTP timestamps are already established by
        // the platform callback and Opus frame assembler. Publish those frames
        // directly; receiver/downlink pacing belongs to the RTP media plane.
        let mut wrote_frame = false;
        for offset in 0..streams.len() {
            let index = (round_robin_start + offset) % streams.len();
            if streams[index].disconnected {
                continue;
            }
            let received = try_recv_frame(
                &streams[index].stream.encoded_rx,
                streams[index].stream.backlog_policy,
            );
            match received {
                Ok((frame, stale_drops)) => {
                    if stale_drops > 0 {
                        streams[index]
                            .stream
                            .counters
                            .publisher_stale_drops
                            .fetch_add(stale_drops, Ordering::Relaxed);
                    }
                    let mid = streams[index].stream.mid;
                    let payload_type = streams[index].payload_type;
                    let payload_bytes = frame.payload.len() as u64;
                    let writer = rtc
                        .writer(mid)
                        .ok_or("negotiated Relay AudioBus writer disappeared")?;
                    let writer = if let Some(level) = frame.audio_level {
                        writer.audio_level(-level, level < 40)
                    } else {
                        writer
                    };
                    writer
                        .write(
                            payload_type,
                            frame.wallclock,
                            frame.rtp_time,
                            frame.payload.as_slice(),
                        )
                        .map_err(|error| format!("Relay AudioBus RTP write failed: {error:?}"))?;
                    streams[index].statistics.rtp_sent =
                        streams[index].statistics.rtp_sent.saturating_add(1);
                    streams[index].statistics.bytes_sent = streams[index]
                        .statistics
                        .bytes_sent
                        .saturating_add(payload_bytes);
                    rtc_writer_writes_total = rtc_writer_writes_total.saturating_add(1);
                    next_rtc_timeout = drain_all_outputs(&mut rtc, &udp)?;
                    rtc_poll_drains_total = rtc_poll_drains_total.saturating_add(1);
                    round_robin_start = (index + 1) % streams.len();
                    wrote_frame = true;
                    break;
                }
                Err(mpsc::TryRecvError::Empty) => {}
                Err(mpsc::TryRecvError::Disconnected) => {
                    streams[index].disconnected = true;
                }
            }
        }
        if wrote_frame {
            continue 'publish;
        }
        if streams.iter().all(|stream| stream.disconnected) {
            break 'publish;
        }

        // ── WebSocket keepalive ping ──────────────────────────────────────
        if last_ws_ping.elapsed() >= WS_KEEPALIVE_INTERVAL {
            // Any message resets the relay's 90-second read deadline.
            // Ping is the cleanest option — it carries no payload and
            // the relay doesn't need to do anything with the pong.
            ws.send(tungstenite::Message::Ping(tungstenite::Bytes::new()))?;
            last_ws_ping = Instant::now();
        }

        // ── Poll WebSocket (replaces sleep) ───────────────────────────────
        // Calling ws.read() serves two purposes:
        //   1. tungstenite auto-replies to relay Pings with Pong — without
        //      this the relay's read deadline fires and closes the session.
        //   2. It blocks for at most WS_POLL_TIMEOUT_MS ms, which throttles
        //      this idle branch the same way a 1 ms sleep would.
        classify_signaling_read(ws.read())?;
    }

    let _ = send_leave(&mut ws, room);
    let _ = ws.close(None);

    Ok(PublishStats {
        streams: streams
            .into_iter()
            .map(|stream| stream.statistics)
            .collect(),
        elapsed: run_start.elapsed(),
        drains: rtc_poll_drains_total,
        writes: rtc_writer_writes_total,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use str0m::media::{Frequency, MediaTime};

    fn encoded_frame(rtp_samples: u64) -> EncodedAudioFrame {
        EncodedAudioFrame {
            payload: vec![rtp_samples.to_le_bytes()[0]],
            wallclock: Instant::now(),
            rtp_time: MediaTime::new(rtp_samples, Frequency::FORTY_EIGHT_KHZ),
            duration_samples: 960,
            audio_level: None,
            capture_timestamp_ns: 0,
            capture_age_ns: 0,
        }
    }

    #[test]
    fn given_drop_stale_policy_when_encoded_backlog_then_freshest_frame_wins() {
        let (tx, rx) = mpsc::sync_channel(4);
        tx.try_send(encoded_frame(0)).unwrap();
        tx.try_send(encoded_frame(960)).unwrap();
        tx.try_send(encoded_frame(1920)).unwrap();

        let (freshest, stale_drops) =
            try_recv_frame(&rx, PublisherBacklogPolicy::DropStale).unwrap();

        assert_eq!(freshest.payload, vec![1920_u64.to_le_bytes()[0]]);
        assert_eq!(
            freshest.rtp_time,
            MediaTime::new(1920, Frequency::FORTY_EIGHT_KHZ)
        );
        assert_eq!(stale_drops, 2);
    }

    #[test]
    fn given_preserve_policy_when_encoded_backlog_then_oldest_frame_wins() {
        let (tx, rx) = mpsc::sync_channel(4);
        tx.try_send(encoded_frame(0)).unwrap();
        tx.try_send(encoded_frame(960)).unwrap();
        tx.try_send(encoded_frame(1920)).unwrap();

        let (oldest, stale_drops) = try_recv_frame(&rx, PublisherBacklogPolicy::Preserve).unwrap();

        assert_eq!(
            oldest.rtp_time,
            MediaTime::new(0, Frequency::FORTY_EIGHT_KHZ)
        );
        assert_eq!(stale_drops, 0);
        assert_eq!(
            rx.try_recv().unwrap().rtp_time,
            MediaTime::new(960, Frequency::FORTY_EIGHT_KHZ)
        );
        assert_eq!(
            rx.try_recv().unwrap().rtp_time,
            MediaTime::new(1920, Frequency::FORTY_EIGHT_KHZ)
        );
    }

    #[test]
    fn given_remote_signaling_close_when_classified_then_publication_fails() {
        let error = classify_signaling_read(Ok(tungstenite::Message::Close(None)))
            .expect_err("remote close must not look like a clean local stop");

        assert!(error.to_string().contains("closed during publication"));
    }

    #[test]
    fn given_signaling_read_timeout_when_classified_then_publication_continues() {
        let timeout = std::io::Error::new(std::io::ErrorKind::TimedOut, "bounded poll");

        classify_signaling_read(Err(tungstenite::Error::Io(timeout)))
            .expect("bounded idle timeout is not a transport failure");
    }
}
