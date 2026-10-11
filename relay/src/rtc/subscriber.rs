//! Receive-only WebRTC transport for one authorized Relay `AudioBus`.

use std::collections::VecDeque;
use std::net::UdpSocket;
use std::sync::Arc;
use std::time::{Duration, Instant};

use str0m::media::Mid;
use str0m::net::{Protocol, Receive};
use str0m::{Candidate, Event, IceConnectionState, Input, Output, Rtc};

use crate::rtc::handshake::HandshakeResult;
use crate::rtc::signaling::{
    send_leave, ws_set_read_timeout, ws_set_write_timeout, ServerMsg, Ws, WS_POLL_TIMEOUT,
    WS_WRITE_TIMEOUT,
};

const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(45);

pub(crate) struct ReceivedOpusFrame {
    pub(crate) payload: Arc<[u8]>,
    pub(crate) rtp_timestamp: u64,
    pub(crate) rtp_clock_hz: u32,
    pub(crate) contiguous: bool,
}

pub(crate) struct SubscriberConnection {
    rtc: Rtc,
    ws: Ws,
    mid: Mid,
    bound_addr: std::net::SocketAddr,
    udp: UdpSocket,
    session_id: String,
    pending_events: VecDeque<Event>,
    next_rtc_timeout: Instant,
    last_keepalive: Instant,
    closed: bool,
}

impl SubscriberConnection {
    pub(crate) fn new(
        mut handshake: HandshakeResult,
        session_id: String,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mid = handshake
            .mids
            .pop()
            .ok_or("Relay subscriber negotiated no audio media")?;
        handshake
            .udp
            .set_nonblocking(true)
            .map_err(|error| format!("subscriber UDP set_nonblocking failed: {error}"))?;
        ws_set_read_timeout(&handshake.ws, Some(WS_POLL_TIMEOUT))?;
        ws_set_write_timeout(&handshake.ws, Some(WS_WRITE_TIMEOUT))?;
        let mut connection = Self {
            rtc: handshake.rtc,
            ws: handshake.ws,
            mid,
            bound_addr: handshake.bound_addr,
            udp: handshake.udp,
            session_id,
            pending_events: VecDeque::new(),
            next_rtc_timeout: Instant::now(),
            last_keepalive: Instant::now(),
            closed: false,
        };
        connection.drain_outputs()?;
        Ok(connection)
    }

    /// Advances the WebRTC state machine once and returns at most one complete
    /// depayloaded Opus frame. The caller repeats this bounded poll so Core
    /// cancellation is observed promptly.
    pub(crate) fn poll_frame(
        &mut self,
    ) -> Result<Option<ReceivedOpusFrame>, Box<dyn std::error::Error>> {
        if let Some(frame) = self.take_pending_frame()? {
            return Ok(Some(frame));
        }

        let now = Instant::now();
        if now >= self.next_rtc_timeout {
            self.rtc
                .handle_input(Input::Timeout(now))
                .map_err(|error| format!("subscriber RTC timeout failed: {error:?}"))?;
            self.drain_outputs()?;
            if let Some(frame) = self.take_pending_frame()? {
                return Ok(Some(frame));
            }
        }

        let mut udp_buffer = [0_u8; 2048];
        match self.udp.recv_from(&mut udp_buffer) {
            Ok((size, source)) => {
                let receive =
                    Receive::new(Protocol::Udp, source, self.bound_addr, &udp_buffer[..size])
                        .map_err(|error| format!("subscriber Receive::new failed: {error:?}"))?;
                self.rtc
                    .handle_input(Input::Receive(Instant::now(), receive))
                    .map_err(|error| format!("subscriber RTC receive failed: {error:?}"))?;
                self.drain_outputs()?;
                if let Some(frame) = self.take_pending_frame()? {
                    return Ok(Some(frame));
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => return Err(error.into()),
        }

        self.poll_signaling()?;
        if self.last_keepalive.elapsed() >= KEEPALIVE_INTERVAL {
            self.ws
                .send(tungstenite::Message::Ping(Vec::new().into()))?;
            self.last_keepalive = Instant::now();
        }
        self.take_pending_frame()
    }

    pub(crate) fn close(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        let leave_result = send_leave(&mut self.ws, &self.session_id);
        let close_result = self.ws.close(None);
        leave_result?;
        close_result?;
        Ok(())
    }

    fn drain_outputs(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        loop {
            match self
                .rtc
                .poll_output()
                .map_err(|error| format!("subscriber poll_output failed: {error:?}"))?
            {
                Output::Transmit(transmit) => {
                    if let Err(error) = self.udp.send_to(&transmit.contents, transmit.destination) {
                        if error.kind() != std::io::ErrorKind::WouldBlock {
                            return Err(error.into());
                        }
                    }
                }
                Output::Event(event) => self.pending_events.push_back(event),
                Output::Timeout(timeout) => {
                    self.next_rtc_timeout = timeout;
                    return Ok(());
                }
            }
        }
    }

    fn take_pending_frame(
        &mut self,
    ) -> Result<Option<ReceivedOpusFrame>, Box<dyn std::error::Error>> {
        while let Some(event) = self.pending_events.pop_front() {
            match event {
                Event::MediaData(media) if media.mid == self.mid => {
                    return Ok(Some(ReceivedOpusFrame {
                        payload: media.data,
                        rtp_timestamp: media.time.numer(),
                        rtp_clock_hz: media.time.denom(),
                        contiguous: media.contiguous,
                    }));
                }
                Event::IceConnectionStateChange(IceConnectionState::Disconnected) => {
                    return Err("Relay subscriber ICE path disconnected".into());
                }
                _ => {}
            }
        }
        Ok(None)
    }

    fn poll_signaling(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        match self.ws.read() {
            Ok(tungstenite::Message::Text(text)) => {
                if let Ok(message) = serde_json::from_str::<ServerMsg>(&text) {
                    if message.msg_type == "ICE" && !message.candidate.is_empty() {
                        let candidate = Candidate::from_sdp_string(&message.candidate)
                            .map_err(|error| format!("invalid Relay ICE candidate: {error:?}"))?;
                        self.rtc.add_remote_candidate(candidate);
                        self.drain_outputs()?;
                    } else if message.msg_type == "ERROR" {
                        return Err(
                            format!("relay error {}: {}", message.code, message.message).into()
                        );
                    }
                }
                Ok(())
            }
            Ok(tungstenite::Message::Close(_)) => Err("Relay subscriber signaling closed".into()),
            Ok(_) => Ok(()),
            Err(tungstenite::Error::Io(error))
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                Ok(())
            }
            Err(tungstenite::Error::ConnectionClosed) => {
                Err("Relay subscriber signaling closed".into())
            }
            Err(error) => Err(format!("Relay subscriber signaling failed: {error}").into()),
        }
    }
}

impl Drop for SubscriberConnection {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
