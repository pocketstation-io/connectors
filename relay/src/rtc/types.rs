//! Shared str0m I/O helpers used by the handshake and publisher loops.

use std::net::UdpSocket;
use std::time::Instant;
use str0m::{Output, Rtc};

// str0m I/O helpers

/// Drain **all** str0m outputs until `Output::Timeout` and return the next
/// deadline.
///
/// str0m's poll invariant: after any `Rtc` mutation (`handle_input`,
/// `writer.write`, `add_remote_candidate`) `poll_output()` MUST be called
/// until it returns `Output::Timeout` before the next mutation.  Stopping
/// early on `Output::Event` leaves `needs_poll = true` inside str0m, so the
/// very next mutation fires `WriteWithoutPoll`.
pub(crate) fn drain_all_outputs(
    rtc: &mut Rtc,
    udp: &UdpSocket,
) -> Result<Instant, Box<dyn std::error::Error>> {
    loop {
        match rtc.poll_output() {
            Err(e) => {
                let msg = format!("{e:?}");
                if msg.contains("CloseNotify") {
                    // Relay sent DTLS CloseNotify — graceful shutdown, not a bug.
                    return Err("relay closed DTLS connection".into());
                }
                return Err(format!("poll_output: {msg}").into());
            }
            Ok(Output::Timeout(t)) => return Ok(t),
            Ok(Output::Transmit(t)) => {
                if let Err(error) = udp.send_to(&t.contents, t.destination) {
                    if error.kind() != std::io::ErrorKind::WouldBlock {
                        return Err(error.into());
                    }
                }
            }
            Ok(Output::Event(_)) => {}
        }
    }
}
