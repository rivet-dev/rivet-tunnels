mod actor;
mod protocol;

pub use actor::{ACTOR_NAME, TunnelActor};
pub use protocol::{ActorMessage, AgentMessage, Header, TunnelRequest, TunnelResponse};

pub const DEFAULT_MAX_BODY_BYTES: usize = 8 * 1024 * 1024;

pub async fn read_response_body_bounded(
	mut response: reqwest::Response,
	max_bytes: usize,
) -> anyhow::Result<Vec<u8>> {
	if response
		.content_length()
		.is_some_and(|length| length > max_bytes as u64)
	{
		anyhow::bail!("HTTP response body exceeds {max_bytes} bytes");
	}

	let mut body = Vec::new();
	while let Some(chunk) = response.chunk().await? {
		if chunk.len() > max_bytes.saturating_sub(body.len()) {
			anyhow::bail!("HTTP response body exceeds {max_bytes} bytes");
		}
		body.extend_from_slice(&chunk);
	}
	Ok(body)
}

pub fn is_hop_by_hop_header(name: &str) -> bool {
	matches!(
		name.to_ascii_lowercase().as_str(),
		"connection"
			| "keep-alive"
			| "proxy-authenticate"
			| "proxy-authorization"
			| "te" | "trailer"
			| "transfer-encoding"
			| "upgrade"
	)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn classifies_hop_by_hop_headers_case_insensitively() {
		assert!(is_hop_by_hop_header("Connection"));
		assert!(is_hop_by_hop_header("transfer-encoding"));
		assert!(!is_hop_by_hop_header("content-type"));
	}

	#[test]
	fn protocol_round_trips_through_cbor() {
		let message = ActorMessage::Request(TunnelRequest {
			id: 7,
			method: "POST".into(),
			path: "/hello?name=rivet".into(),
			headers: vec![Header {
				name: "content-type".into(),
				value: "text/plain".into(),
			}],
			body: b"hello".to_vec(),
		});
		let bytes = serde_cbor::to_vec(&message).unwrap();
		let decoded: ActorMessage = serde_cbor::from_slice(&bytes).unwrap();
		assert_eq!(decoded, message);
	}
}
