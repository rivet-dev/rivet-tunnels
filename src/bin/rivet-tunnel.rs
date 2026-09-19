use std::{io::Write, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use rand::{Rng, distr::Alphanumeric};
use reqwest::{Client as HttpClient, Method, redirect::Policy};
use rivet_tunnel::{
	ACTOR_NAME, ActorMessage, AgentMessage, DEFAULT_MAX_BODY_BYTES, Header, TunnelActor,
	TunnelRequest, TunnelResponse, is_hop_by_hop_header, read_response_body_bounded,
};
use rivetkit::{
	ServeConfig, TypedClientExt,
	client::{Client, ClientConfig},
};
use tokio::sync::Mutex;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};
use tokio_util::sync::CancellationToken;
use url::Url;

#[derive(Parser, Debug)]
#[command(
	name = "rivet-tunnel",
	version,
	about = "Expose a local HTTP endpoint through a Rivet Actor"
)]
struct Args {
	/// Local HTTP endpoint to expose, for example http://127.0.0.1:3000.
	#[arg(long)]
	endpoint: Url,

	/// Rivet API endpoint hosting the tunnel actor.
	#[arg(long)]
	rivet_endpoint: Option<String>,

	/// Rivet namespace containing the tunnel actor.
	#[arg(long)]
	namespace: Option<String>,

	/// Rivet API token. Not required when local authentication is disabled.
	#[arg(long)]
	token: Option<String>,

	/// Rivet pool hosting the tunnel actor.
	#[arg(long)]
	pool: Option<String>,

	/// Base URL served by the tunnel gateway.
	#[arg(
		long,
		env = "RIVET_TUNNEL_PUBLIC_BASE_URL",
		default_value = "http://localhost:8080"
	)]
	public_base_url: Url,
}

type AgentSocket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

#[tokio::main]
async fn main() -> Result<()> {
	tracing_subscriber::fmt()
		.with_env_filter(
			tracing_subscriber::EnvFilter::try_from_default_env()
				.unwrap_or_else(|_| "rivet_tunnel=info".into()),
		)
		.with_writer(std::io::stderr)
		.init();

	let args = Args::parse();
	let tunnel_name = random_tunnel_name();
	let public_url = tunnel_url(&args.public_base_url, &tunnel_name)?;
	let defaults = ServeConfig::from_env();
	let client = Client::new(
		ClientConfig::new(args.rivet_endpoint.unwrap_or(defaults.endpoint))
			.namespace(args.namespace.unwrap_or(defaults.namespace))
			.token_opt(args.token.or(defaults.token))
			.pool_name(args.pool.unwrap_or(defaults.pool_name)),
	);
	let handle =
		client.get_or_create_typed_default::<TunnelActor>(ACTOR_NAME, [tunnel_name.as_str()])?;
	let http = HttpClient::builder()
		.redirect(Policy::none())
		.build()
		.context("build local HTTP client")?;
	let stop = CancellationToken::new();
	let signal = stop.clone();
	tokio::spawn(async move {
		let _ = tokio::signal::ctrl_c().await;
		signal.cancel();
	});

	let mut announced = false;
	loop {
		let socket = tokio::select! {
			result = handle.inner().web_socket("agent", Some(vec!["rivet-tunnel.v1".into()])) => result,
			_ = stop.cancelled() => break,
		};
		match socket {
			Ok(socket) => {
				if let Err(error) = run_agent_session(
					socket,
					&args.endpoint,
					&public_url,
					&mut announced,
					http.clone(),
					stop.clone(),
				)
				.await
				{
					tracing::warn!(%error, "agent connection closed; reconnecting");
				}
			}
			Err(error) => tracing::warn!(%error, "failed to connect tunnel; retrying"),
		}

		tokio::select! {
			_ = tokio::time::sleep(Duration::from_secs(1)) => {}
			_ = stop.cancelled() => break,
		}
	}

	Ok(())
}

async fn run_agent_session(
	mut socket: AgentSocket,
	local_endpoint: &Url,
	public_url: &Url,
	announced: &mut bool,
	http: HttpClient,
	stop: CancellationToken,
) -> Result<()> {
	let ready = tokio::select! {
		message = socket.next() => message.context("tunnel closed before ready")??,
		_ = stop.cancelled() => return Ok(()),
	};
	let ready = decode_actor_message(ready)?;
	if !matches!(ready, ActorMessage::Ready { .. }) {
		bail!("tunnel actor did not send a ready message");
	}
	if !*announced {
		println!("{public_url}");
		std::io::stdout()
			.flush()
			.context("flush public tunnel URL")?;
		*announced = true;
	}

	let (writer, mut reader) = socket.split();
	let writer = Arc::new(Mutex::new(writer));
	loop {
		let message = tokio::select! {
			message = reader.next() => message.context("tunnel websocket closed")??,
			_ = stop.cancelled() => return Ok(()),
		};
		match decode_actor_message(message)? {
			ActorMessage::Ready { .. } => {}
			ActorMessage::Request(request) => {
				let writer = writer.clone();
				let local_endpoint = local_endpoint.clone();
				let http = http.clone();
				tokio::spawn(async move {
					let response = forward_request(&http, &local_endpoint, request).await;
					let payload = match serde_cbor::to_vec(&AgentMessage::Response(response)) {
						Ok(payload) => payload,
						Err(error) => {
							tracing::error!(%error, "failed to encode tunnel response");
							return;
						}
					};
					if let Err(error) = writer
						.lock()
						.await
						.send(Message::Binary(payload.into()))
						.await
					{
						tracing::warn!(%error, "failed to send tunnel response");
					}
				});
			}
		}
	}
}

fn decode_actor_message(message: Message) -> Result<ActorMessage> {
	match message {
		Message::Binary(bytes) => serde_cbor::from_slice(&bytes).context("decode tunnel message"),
		Message::Close(frame) => bail!("tunnel websocket closed: {frame:?}"),
		_ => bail!("unexpected non-binary tunnel message"),
	}
}

async fn forward_request(
	http: &HttpClient,
	local_endpoint: &Url,
	request: TunnelRequest,
) -> TunnelResponse {
	let request_id = request.id;
	match forward_request_inner(http, local_endpoint, request).await {
		Ok(response) => response,
		Err(error) => TunnelResponse {
			id: request_id,
			status: 502,
			headers: vec![Header {
				name: "content-type".into(),
				value: "text/plain; charset=utf-8".into(),
			}],
			body: format!("failed to reach local endpoint: {error}").into_bytes(),
		},
	}
}

async fn forward_request_inner(
	http: &HttpClient,
	local_endpoint: &Url,
	request: TunnelRequest,
) -> Result<TunnelResponse> {
	let url = local_request_url(local_endpoint, &request.path)?;
	let method = Method::from_bytes(request.method.as_bytes()).context("invalid HTTP method")?;
	let mut builder = http.request(method, url).body(request.body);
	for header in request.headers {
		if !is_hop_by_hop_header(&header.name)
			&& !header.name.eq_ignore_ascii_case("host")
			&& !header.name.eq_ignore_ascii_case("content-length")
		{
			builder = builder.header(&header.name, &header.value);
		}
	}
	let response = builder.send().await.context("send local request")?;
	let status = response.status().as_u16();
	let headers = response
		.headers()
		.iter()
		.filter(|(name, _)| !is_hop_by_hop_header(name.as_str()))
		.map(|(name, value)| Header {
			name: name.to_string(),
			value: String::from_utf8_lossy(value.as_bytes()).into_owned(),
		})
		.collect();
	let body = read_response_body_bounded(response, DEFAULT_MAX_BODY_BYTES)
		.await
		.context("read local response")?;
	Ok(TunnelResponse {
		id: request.id,
		status,
		headers,
		body,
	})
}

fn local_request_url(endpoint: &Url, request_path: &str) -> Result<Url> {
	let request_uri = Url::parse(&format!("http://tunnel.invalid{request_path}"))
		.context("parse tunneled request path")?;
	let mut url = endpoint.clone();
	let base_path = endpoint.path().trim_end_matches('/');
	let request_path = request_uri.path().trim_start_matches('/');
	url.set_path(&format!("{base_path}/{request_path}"));
	url.set_query(request_uri.query());
	Ok(url)
}

fn random_tunnel_name() -> String {
	rand::rng()
		.sample_iter(Alphanumeric)
		.filter(u8::is_ascii_alphabetic)
		.map(|byte| (byte as char).to_ascii_lowercase())
		.take(12)
		.collect()
}

fn tunnel_url(base: &Url, tunnel_name: &str) -> Result<Url> {
	let host = base
		.host_str()
		.context("public base URL must have a host")?;
	let mut url = base.clone();
	url.set_host(Some(&format!("{tunnel_name}.{host}")))
		.map_err(|_| anyhow::anyhow!("invalid tunnel hostname"))?;
	Ok(url)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn builds_public_subdomain_url() {
		let base = Url::parse("https://tunnels.gameinc.io").unwrap();
		assert_eq!(
			tunnel_url(&base, "quiet-river").unwrap().as_str(),
			"https://quiet-river.tunnels.gameinc.io/"
		);
	}

	#[test]
	fn preserves_endpoint_base_path_and_query() {
		let base = Url::parse("http://127.0.0.1:3000/api").unwrap();
		assert_eq!(
			local_request_url(&base, "/users?id=7").unwrap().as_str(),
			"http://127.0.0.1:3000/api/users?id=7"
		);
	}
}
