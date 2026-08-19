//! WebSocket signaling message types and helpers.
//!
//! Mirrors `relay/internal/signaling/messages.go`.

use serde::{Deserialize, Serialize};
use std::io;
use std::time::Duration;
use tungstenite::stream::MaybeTlsStream;

// Signaling message types  (mirrors relay/internal/signaling/messages.go)

#[derive(Serialize)]
pub(crate) struct PublishBusBinding<'a> {
    pub(crate) stream_id: &'a str,
    pub(crate) bus_id: &'a str,
}

fn publish_buses_are_empty(bindings: &&[PublishBusBinding<'_>]) -> bool {
    bindings.is_empty()
}

#[derive(Serialize)]
pub(crate) struct PublishMsg<'a> {
    #[serde(rename = "type")]
    pub(crate) msg_type: &'a str,
    pub(crate) room_id: &'a str,
    pub(crate) token: &'a str,
    pub(crate) sdp_offer: &'a str,
    #[serde(rename = "bus_id", skip_serializing_if = "Option::is_none")]
    pub(crate) bus_id: Option<&'a str>,
    #[serde(
        rename = "publish_buses",
        skip_serializing_if = "publish_buses_are_empty"
    )]
    pub(crate) publish_buses: &'a [PublishBusBinding<'a>],
}

#[derive(Serialize)]
pub(crate) struct LeaveMsg<'a> {
    #[serde(rename = "type")]
    pub(crate) msg_type: &'a str,
    pub(crate) room_id: &'a str,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ServerMsg {
    #[serde(rename = "type")]
    pub(crate) msg_type: String,
    #[serde(default)]
    pub(crate) sdp_answer: String,
    #[serde(default)]
    pub(crate) candidate: String,
    #[serde(default)]
    pub(crate) code: String,
    #[serde(default)]
    pub(crate) message: String,
}

// Convenience type alias

pub(crate) type Ws = tungstenite::WebSocket<MaybeTlsStream<std::net::TcpStream>>;

pub(crate) const WS_POLL_TIMEOUT: Duration = Duration::from_millis(1);
pub(crate) const WS_WRITE_TIMEOUT: Duration = Duration::from_secs(1);

pub(crate) struct SignalingAnswer {
    pub(crate) sdp_answer: String,
    /// Relay candidates may race ahead of the SDP answer on a local or very
    /// fast connection. They must be retained until str0m has accepted the
    /// answer; discarding them leaves ICE permanently in Checking.
    pub(crate) early_candidates: Vec<String>,
}

// Set a read timeout on the underlying TCP stream inside a tungstenite WebSocket.

pub(crate) fn ws_set_read_timeout(ws: &Ws, timeout: Option<Duration>) -> io::Result<()> {
    // MaybeTlsStream exposes the inner stream reference.  We need to reach
    // the TcpStream to call set_read_timeout.  Match on all known variants;
    // use a wildcard for any future variant added by tungstenite.
    match ws.get_ref() {
        MaybeTlsStream::Plain(tcp) => tcp.set_read_timeout(timeout),
        MaybeTlsStream::NativeTls(tls) => tls.get_ref().set_read_timeout(timeout),
        _ => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "relay WebSocket transport does not expose a readable TCP timeout",
        )),
    }
}

pub(crate) fn ws_set_write_timeout(ws: &Ws, timeout: Option<Duration>) -> io::Result<()> {
    match ws.get_ref() {
        MaybeTlsStream::Plain(tcp) => tcp.set_write_timeout(timeout),
        MaybeTlsStream::NativeTls(tls) => tls.get_ref().set_write_timeout(timeout),
        _ => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "relay WebSocket transport does not expose a writable TCP timeout",
        )),
    }
}

/// Block on the WebSocket until we receive an `SDP_ANSWER`; return the SDP string.
pub(crate) fn wait_for_sdp_answer(
    ws: &mut Ws,
    _room_id: &str,
) -> Result<SignalingAnswer, Box<dyn std::error::Error>> {
    let mut early_candidates = Vec::new();
    loop {
        let msg = ws.read()?;
        if let tungstenite::Message::Text(txt) = msg {
            let server_msg: ServerMsg = match serde_json::from_str(&txt) {
                Ok(m) => m,
                Err(_) => continue,
            };
            match server_msg.msg_type.as_str() {
                "SDP_ANSWER" => {
                    if server_msg.sdp_answer.is_empty() {
                        return Err("SDP_ANSWER received but sdp_answer field is empty".into());
                    }
                    return Ok(SignalingAnswer {
                        sdp_answer: server_msg.sdp_answer,
                        early_candidates,
                    });
                }
                "ICE" if !server_msg.candidate.is_empty() => {
                    early_candidates.push(server_msg.candidate);
                }
                "ERROR" => {
                    return Err(
                        format!("relay error {}: {}", server_msg.code, server_msg.message).into(),
                    );
                }
                _ => {}
            }
        }
    }
}

/// Send a `LEAVE` message to the relay.
pub(crate) fn send_leave(ws: &mut Ws, room_id: &str) -> Result<(), Box<dyn std::error::Error>> {
    let leave = LeaveMsg {
        msg_type: "LEAVE",
        room_id,
    };
    ws.send(tungstenite::Message::Text(
        serde_json::to_string(&leave)?.into(),
    ))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{PublishBusBinding, PublishMsg};

    #[test]
    fn grouped_publish_uses_canonical_wire_names_without_legacy_bus() {
        let buses = [
            PublishBusBinding {
                stream_id: "application",
                bus_id: "application",
            },
            PublishBusBinding {
                stream_id: "microphone",
                bus_id: "microphone",
            },
        ];
        let encoded = serde_json::to_value(PublishMsg {
            msg_type: "PUBLISH",
            room_id: "session",
            token: "secret",
            sdp_offer: "offer",
            bus_id: None,
            publish_buses: &buses,
        })
        .unwrap();

        assert!(encoded.get("bus_id").is_none());
        assert_eq!(encoded["publish_buses"][0]["stream_id"], "application");
        assert_eq!(encoded["publish_buses"][1]["bus_id"], "microphone");
    }
}
