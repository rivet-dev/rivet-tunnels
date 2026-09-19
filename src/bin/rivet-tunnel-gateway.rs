use std::{net::SocketAddr, sync::Arc};

use anyhow::{Context, Result, bail};
use axum::{
	Router,
	body::{Body, to_bytes},
	extract::{Request, State},
	http::{HeaderMap, HeaderName, HeaderValue, Response, StatusCode, header::HOST},
	routing::any,
};
use clap::Parser;
use reqwest::Client;
use rivet_tunnel::{
	ACTOR_NAME, DEFAULT_MAX_BODY_BYTES, is_hop_by_hop_header, read_response_body_bounded,
};
use rivetkit::ServeConfig;
use url::Url;

#[derive(Parser, Debug)]
#[command(
	name = "rivet-tunnel-gateway",
	version,
	about = "Public gateway for Rivet tunnels"
)]
struct Args {
	#[arg(long, env = "LISTEN_ADDR", default_value = "127.0.0.1:8080")]
	listen: SocketAddr,

	#[arg(long)]
	rivet_endpoint: Option<Url>,

	#[arg(long)]
	namespace: Option<String>,

	#[arg(long)]
	token: Option<String>,

	#[arg(long, env = "RIVET_TUNNEL_BASE_DOMAIN", default_value = "localhost")]
	base_domain: String,
}

struct AppState {
	http: Client,
	rivet_endpoint: Url,
	namespace: String,
	token: Option<String>,
	base_domain: String,
}

#[tokio::main]
async fn main() -> Result<()> {
	tracing_subscriber::fmt()
		.with_env_filter(
			tracing_subscriber::EnvFilter::try_from_default_env()
				.unwrap_or_else(|_| "rivet_tunnel=info".into()),
		)
		.init();
	let args = Args::parse();
	let defaults = ServeConfig::from_env();
	let state = Arc::new(AppState {
		http: Client::builder()
			.redirect(reqwest::redirect::Policy::none())
			.build()?,
		rivet_endpoint: args
			.rivet_endpoint
			.unwrap_or(Url::parse(&defaults.endpoint).context("parse Rivet endpoint")?),
		namespace: args.namespace.unwrap_or(defaults.namespace),
		token: args.token.or(defaults.token),
		base_domain: args.base_domain.trim_matches('.').to_ascii_lowercase(),
	});
	let app = Router::new().fallback(any(proxy)).with_state(state);
	let listener = tokio::net::TcpListener::bind(args.listen).await?;
	tracing::info!(address = %args.listen, "tunnel gateway listening");
	axum::serve(listener, app).await?;
	Ok(())
}

async fn proxy(State(state): State<Arc<AppState>>, request: Request) -> Response<Body> {
	match proxy_inner(&state, request).await {
		Ok(response) => response,
		Err(error) => {
			tracing::warn!(%error, "tunnel gateway request failed");
			plain_response(StatusCode::BAD_GATEWAY, &error.to_string())
		}
	}
}

async fn proxy_inner(state: &AppState, request: Request) -> Result<Response<Body>> {
	let host = request
		.headers()
		.get(HOST)
		.and_then(|value| value.to_str().ok())
		.context("missing Host header")?
		.split(':')
		.next()
		.context("invalid Host header")?;
	let tunnel_name = tunnel_name_from_host(host, &state.base_domain)?;
	let (parts, body) = request.into_parts();
	let body = to_bytes(body, DEFAULT_MAX_BODY_BYTES)
		.await
		.context("read public request body")?;
	let upstream = actor_url(
		&state.rivet_endpoint,
		&state.namespace,
		&tunnel_name,
		parts
			.uri
			.path_and_query()
			.map(|value| value.as_str())
			.unwrap_or("/"),
	)?;

	let mut builder = state.http.request(parts.method, upstream).body(body);
	for (name, value) in &parts.headers {
		if !is_hop_by_hop_header(name.as_str())
			&& name != HOST
			&& name.as_str() != "content-length"
			&& name.as_str() != "x-rivet-token"
		{
			builder = builder.header(name, value);
		}
	}
	if let Some(token) = &state.token {
		builder = builder.header("x-rivet-token", token);
	}
	let upstream = builder.send().await.context("send request to Rivet")?;
	let status = upstream.status();
	let response_headers = upstream.headers().clone();
	let body = read_response_body_bounded(upstream, DEFAULT_MAX_BODY_BYTES)
		.await
		.context("read Rivet response")?;

	let mut response = Response::builder().status(status);
	copy_response_headers(response.headers_mut().unwrap(), &response_headers)?;
	response
		.body(Body::from(body))
		.context("build gateway response")
}

fn copy_response_headers(target: &mut HeaderMap, source: &HeaderMap) -> Result<()> {
	for (name, value) in source {
		if !is_hop_by_hop_header(name.as_str()) && name.as_str() != "content-length" {
			target.append(
				HeaderName::from_bytes(name.as_str().as_bytes())?,
				HeaderValue::from_bytes(value.as_bytes())?,
			);
		}
	}
	Ok(())
}

fn tunnel_name_from_host(host: &str, base_domain: &str) -> Result<String> {
	let suffix = format!(".{base_domain}");
	let name = host
		.to_ascii_lowercase()
		.strip_suffix(&suffix)
		.map(ToOwned::to_owned)
		.context("host is outside the configured tunnel domain")?;
	if name.is_empty()
		|| name.contains('.')
		|| !name.chars().all(|character| {
			character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-'
		}) {
		bail!("invalid tunnel name");
	}
	Ok(name)
}

fn actor_url(
	rivet_endpoint: &Url,
	namespace: &str,
	tunnel_name: &str,
	path_and_query: &str,
) -> Result<Url> {
	let incoming = Url::parse(&format!("http://tunnel.invalid{path_and_query}"))?;
	let mut url = rivet_endpoint.clone();
	let path = incoming.path().trim_start_matches('/');
	url.set_path(&format!("/gateway/{ACTOR_NAME}/request/{path}"));
	{
		let mut query = url.query_pairs_mut();
		for (name, value) in incoming.query_pairs() {
			if !name.starts_with("rvt-") {
				query.append_pair(&name, &value);
			}
		}
		query
			.append_pair("rvt-namespace", namespace)
			.append_pair("rvt-method", "get")
			.append_pair("rvt-key", tunnel_name);
	}
	Ok(url)
}

fn plain_response(status: StatusCode, message: &str) -> Response<Body> {
	Response::builder()
		.status(status)
		.header("content-type", "text/plain; charset=utf-8")
		.body(Body::from(message.to_owned()))
		.expect("valid plain response")
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn extracts_single_tunnel_label() {
		assert_eq!(
			tunnel_name_from_host("quiet-river.tunnels.gameinc.io", "tunnels.gameinc.io").unwrap(),
			"quiet-river".to_owned()
		);
		assert!(tunnel_name_from_host("a.b.tunnels.gameinc.io", "tunnels.gameinc.io").is_err());
	}

	#[test]
	fn rewrites_public_url_to_actor_query() {
		let endpoint = Url::parse("https://api.rivet.dev").unwrap();
		let url = actor_url(
			&endpoint,
			"tunnels",
			"quiet-river",
			"/hello?x=1&rvt-key=bad",
		)
		.unwrap();
		assert_eq!(url.path(), "/gateway/tunnel/request/hello");
		let query: Vec<_> = url.query_pairs().collect();
		assert!(query.contains(&("x".into(), "1".into())));
		assert!(query.contains(&("rvt-key".into(), "quiet-river".into())));
		assert!(!query.contains(&("rvt-key".into(), "bad".into())));
	}
}
