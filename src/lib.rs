mod actor;
mod protocol;

use std::str::FromStr;

use anyhow::{Context, Result, bail};
use percent_encoding::percent_decode_str;
use url::Url;

pub use actor::{ACTOR_NAME, TunnelActor};
pub use protocol::{ActorMessage, AgentMessage, Header, TunnelRequest, TunnelResponse};

pub const DEFAULT_MAX_BODY_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RivetEndpoint {
	pub url: Url,
	pub namespace: Option<String>,
	pub token: Option<String>,
}

impl FromStr for RivetEndpoint {
	type Err = anyhow::Error;

	fn from_str(value: &str) -> Result<Self> {
		let mut url = Url::parse(value).context("parse Rivet endpoint")?;
		if !matches!(url.scheme(), "http" | "https") {
			bail!("Rivet endpoint must use http or https");
		}

		let namespace = (!url.username().is_empty())
			.then(|| decode_url_auth(url.username(), "namespace"))
			.transpose()?;
		let token = url
			.password()
			.map(|value| decode_url_auth(value, "token"))
			.transpose()?;
		if namespace.is_none() && token.is_some() {
			bail!("Rivet endpoint token requires a namespace");
		}
		if namespace.is_some() {
			url.set_username("")
				.map_err(|_| anyhow::anyhow!("remove Rivet endpoint namespace"))?;
			url.set_password(None)
				.map_err(|_| anyhow::anyhow!("remove Rivet endpoint token"))?;
		}

		Ok(Self {
			url,
			namespace,
			token,
		})
	}
}

fn decode_url_auth(value: &str, field: &str) -> Result<String> {
	let decoded = percent_decode_str(value)
		.decode_utf8()
		.with_context(|| format!("decode Rivet endpoint {field}"))?
		.into_owned();
	if decoded.is_empty() {
		bail!("Rivet endpoint {field} cannot be empty");
	}
	Ok(decoded)
}

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

pub fn is_rivet_control_header(name: &str) -> bool {
	name.get(..8)
		.is_some_and(|prefix| prefix.eq_ignore_ascii_case("x-rivet-"))
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
	fn classifies_rivet_control_headers_case_insensitively() {
		assert!(is_rivet_control_header("X-Rivet-Token"));
		assert!(is_rivet_control_header("x-rivet-namespace"));
		assert!(!is_rivet_control_header("x-river-token"));
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

	#[test]
	fn parses_credentials_from_rivet_endpoint() {
		let endpoint: RivetEndpoint = "https://my%2Dnamespace:secret%2Ftoken@api.rivet.dev/"
			.parse()
			.unwrap();
		assert_eq!(endpoint.url.as_str(), "https://api.rivet.dev/");
		assert_eq!(endpoint.namespace.as_deref(), Some("my-namespace"));
		assert_eq!(endpoint.token.as_deref(), Some("secret/token"));
	}

	#[test]
	fn rejects_unsupported_rivet_endpoint_scheme() {
		assert!("ftp://api.rivet.dev".parse::<RivetEndpoint>().is_err());
	}
}
