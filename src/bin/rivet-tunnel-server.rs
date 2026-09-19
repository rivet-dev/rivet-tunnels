use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use clap::{Parser, ValueEnum};
use reqwest::{Client, Method};
use rivet_tunnel::{
	ACTOR_NAME, DEFAULT_MAX_BODY_BYTES, RivetEndpoint, TunnelActor, is_hop_by_hop_header,
	is_rivet_control_header, read_response_body_bounded,
};
use rivetkit::{
	EngineSpawnMode, Registry, ServeConfig,
	serverless_http::{
		self, ApplicationFetch, ApplicationRequest, ApplicationResponse, ApplicationResponseBody,
		ListenerConfig,
	},
};
use tokio_util::sync::CancellationToken;
use url::Url;

const UPSTREAM_TIMEOUT: Duration = Duration::from_secs(65);

#[derive(Parser, Debug)]
#[command(name = "rivet-tunnel-server", version, about = "Rivet tunnel server")]
struct Args {
	#[arg(long, default_value = "0.0.0.0:8080")]
	listen: SocketAddr,

	#[arg(long, value_enum, default_value_t = RunMode::Envoy)]
	runtime_mode: RunMode,

	#[arg(long)]
	engine_auto_download: bool,

	#[arg(long, value_enum, default_value_t = EngineSpawn::Auto)]
	engine_spawn: EngineSpawn,

	#[arg(long)]
	rivet: Option<RivetEndpoint>,

	#[arg(long)]
	namespace: Option<String>,

	#[arg(long)]
	token: Option<String>,

	#[arg(long, default_value = "localhost")]
	base_domain: String,

	#[arg(long)]
	engine_host: Option<String>,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum RunMode {
	Envoy,
	Serverless,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum EngineSpawn {
	Auto,
	Always,
	Never,
}

struct GatewayState {
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
	let runtime_mode = args.runtime_mode;
	let mut config = ServeConfig::from_env();
	if let Some(endpoint) = args.rivet {
		config.endpoint = endpoint.url.to_string();
		if args.namespace.is_none()
			&& let Some(namespace) = endpoint.namespace
		{
			config.namespace = namespace;
		}
		if args.token.is_none()
			&& let Some(token) = endpoint.token
		{
			config.token = Some(token);
		}
	}
	if let Some(namespace) = args.namespace {
		config.namespace = namespace;
	}
	if let Some(token) = args.token {
		config.token = Some(token);
	}
	config.engine_host = args.engine_host;
	config.engine_auto_download = args.engine_auto_download;
	config.engine_spawn = match args.engine_spawn {
		EngineSpawn::Auto => EngineSpawnMode::Auto,
		EngineSpawn::Always => EngineSpawnMode::Always,
		EngineSpawn::Never => EngineSpawnMode::Never,
	};
	let base_domain = normalize_base_domain(&args.base_domain)?;

	let state = Arc::new(GatewayState {
		http: Client::builder()
			.redirect(reqwest::redirect::Policy::none())
			.timeout(UPSTREAM_TIMEOUT)
			.build()?,
		rivet_endpoint: Url::parse(&config.endpoint).context("parse Rivet endpoint")?,
		namespace: config.namespace.clone(),
		token: config.token.clone(),
		base_domain,
	});
	let application: ApplicationFetch = Arc::new(move |request| {
		let state = Arc::clone(&state);
		Box::pin(async move {
			Ok(match proxy(&state, request).await {
				Ok(response) => response,
				Err(error) => {
					tracing::warn!(%error, "tunnel gateway request failed");
					plain_response(502, "tunnel gateway unavailable")
				}
			})
		})
	});
	let listener = ListenerConfig {
		host: Some(args.listen.ip().to_string()),
		port: args.listen.port(),
		public_dir: None,
		application: Some(Arc::clone(&application)),
	};

	let mut registry = Registry::new();
	registry.register_actor::<TunnelActor>(ACTOR_NAME);
	let shutdown = CancellationToken::new();

	match runtime_mode {
		RunMode::Serverless => {
			let runtime = registry.into_serverless_runtime(config).await?;
			let runtime_for_shutdown = runtime.clone();
			let mut server =
				tokio::spawn(serverless_http::serve(runtime, listener, shutdown.clone()));
			let result = tokio::select! {
				result = &mut server => result?,
				_ = shutdown_signal() => {
					shutdown.cancel();
					server.await?
				}
			};
			runtime_for_shutdown.shutdown().await;
			result
		}
		RunMode::Envoy => {
			let mut actors = tokio::spawn(registry.serve_with_config(config, shutdown.clone()));
			let mut gateway = tokio::spawn(serverless_http::serve_application(
				listener,
				application,
				DEFAULT_MAX_BODY_BYTES,
				shutdown.clone(),
			));
			tokio::select! {
				result = &mut actors => {
					shutdown.cancel();
					gateway.await??;
					result?
				}
				result = &mut gateway => {
					shutdown.cancel();
					actors.await??;
					result?
				}
				_ = shutdown_signal() => {
					shutdown.cancel();
					actors.await??;
					gateway.await??;
					Ok(())
				}
			}
		}
	}
}

async fn proxy(state: &GatewayState, request: ApplicationRequest) -> Result<ApplicationResponse> {
	let incoming = Url::parse(&request.url).context("parse public request URL")?;
	if incoming.path() == "/healthz"
		&& incoming
			.host_str()
			.is_none_or(|host| tunnel_name_from_host(host, &state.base_domain).is_err())
	{
		return Ok(plain_response(200, "ok"));
	}
	let host = incoming.host_str().context("missing Host header")?;
	let tunnel_name = tunnel_name_from_host(host, &state.base_domain)?;
	let path_and_query = match incoming.query() {
		Some(query) => format!("{}?{query}", incoming.path()),
		None => incoming.path().to_owned(),
	};
	let upstream = actor_url(
		&state.rivet_endpoint,
		&state.namespace,
		&tunnel_name,
		&path_and_query,
	)?;
	let method = Method::from_bytes(request.method.as_bytes()).context("parse request method")?;
	let mut builder = state.http.request(method, upstream).body(request.body);
	let connection_headers =
		connection_header_names(request.headers.get("connection").map(String::as_str));
	for (name, value) in request.headers {
		if !is_hop_by_hop_header(&name)
			&& !name.eq_ignore_ascii_case("host")
			&& !name.eq_ignore_ascii_case("content-length")
			&& !is_rivet_control_header(&name)
			&& !connection_headers.iter().any(|header| header == &name)
		{
			builder = builder.header(name, value);
		}
	}
	if let Some(token) = &state.token {
		builder = builder.header("x-rivet-token", token);
	}
	let upstream = builder.send().await.context("send request to Rivet")?;
	let status = upstream.status().as_u16();
	let response_headers = upstream.headers().clone();
	let body = read_response_body_bounded(upstream, DEFAULT_MAX_BODY_BYTES)
		.await
		.context("read Rivet response")?;
	let mut headers = HashMap::new();
	let response_connection_headers = connection_header_names(
		response_headers
			.get("connection")
			.and_then(|value| value.to_str().ok()),
	);
	for (name, value) in response_headers {
		if let Some(name) = name
			&& !is_hop_by_hop_header(name.as_str())
			&& name.as_str() != "content-length"
			&& !response_connection_headers
				.iter()
				.any(|header| header == name.as_str())
			&& let Ok(value) = value.to_str()
		{
			headers.insert(name.to_string(), value.to_owned());
		}
	}
	Ok(ApplicationResponse {
		status,
		headers,
		body: ApplicationResponseBody::Buffered(body.to_vec()),
	})
}

fn connection_header_names(value: Option<&str>) -> Vec<String> {
	value
		.into_iter()
		.flat_map(|value| value.split(','))
		.map(str::trim)
		.filter(|value| !value.is_empty())
		.map(str::to_ascii_lowercase)
		.collect()
}

fn normalize_base_domain(value: &str) -> Result<String> {
	let domain = value.trim_end_matches('.').to_ascii_lowercase();
	if domain.is_empty() || domain.len() > 253 {
		bail!("base domain must be between 1 and 253 characters");
	}
	for label in domain.split('.') {
		if label.is_empty()
			|| label.len() > 63
			|| label.starts_with('-')
			|| label.ends_with('-')
			|| !label
				.chars()
				.all(|character| character.is_ascii_alphanumeric() || character == '-')
		{
			bail!("base domain contains an invalid DNS label");
		}
	}
	Ok(domain)
}

fn tunnel_name_from_host(host: &str, base_domain: &str) -> Result<String> {
	let suffix = format!(".{base_domain}");
	let name = host
		.to_ascii_lowercase()
		.strip_suffix(&suffix)
		.map(ToOwned::to_owned)
		.context("host is outside the configured tunnel domain")?;
	if name.is_empty()
		|| name.len() > 63
		|| name.contains('.')
		|| name.starts_with('-')
		|| name.ends_with('-')
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
	drop(query);
	Ok(url)
}

fn plain_response(status: u16, message: &str) -> ApplicationResponse {
	ApplicationResponse {
		status,
		headers: HashMap::from([(
			"content-type".to_owned(),
			"text/plain; charset=utf-8".to_owned(),
		)]),
		body: ApplicationResponseBody::Buffered(message.as_bytes().to_vec()),
	}
}

async fn shutdown_signal() {
	#[cfg(unix)]
	{
		use tokio::signal::unix::{SignalKind, signal};
		let Ok(mut terminate) = signal(SignalKind::terminate()) else {
			let _ = tokio::signal::ctrl_c().await;
			return;
		};
		tokio::select! {
			_ = tokio::signal::ctrl_c() => {}
			_ = terminate.recv() => {}
		}
	}
	#[cfg(not(unix))]
	{
		let _ = tokio::signal::ctrl_c().await;
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use axum::{
		Router,
		body::Body,
		extract::Request,
		http::{Response, StatusCode},
		routing::any,
	};

	#[test]
	fn normalizes_base_domain() {
		assert_eq!(
			normalize_base_domain("Example.COM.").unwrap(),
			"example.com"
		);
		assert!(normalize_base_domain("bad..example.com").is_err());
		assert!(normalize_base_domain("-bad.example.com").is_err());
	}

	#[test]
	fn extracts_single_tunnel_label() {
		assert_eq!(
			tunnel_name_from_host("quiet-river.example.com", "example.com").unwrap(),
			"quiet-river"
		);
		assert!(tunnel_name_from_host("a.b.example.com", "example.com").is_err());
		assert!(tunnel_name_from_host("-bad.example.com", "example.com").is_err());
	}

	#[test]
	fn rewrites_public_url_to_actor_query() {
		let endpoint = Url::parse("https://api.rivet.dev/base?existing=1").unwrap();
		let url = actor_url(
			&endpoint,
			"tunnels",
			"quiet-river",
			"/hello?x=1&rvt-key=bad",
		)
		.unwrap();
		assert_eq!(url.path(), "/gateway/tunnel/request/hello");
		let query: Vec<_> = url.query_pairs().collect();
		assert!(query.contains(&("existing".into(), "1".into())));
		assert!(query.contains(&("x".into(), "1".into())));
		assert!(query.contains(&("rvt-key".into(), "quiet-river".into())));
		assert!(!query.contains(&("rvt-key".into(), "bad".into())));
	}

	#[test]
	fn parses_connection_header_names() {
		assert_eq!(
			connection_header_names(Some("Keep-Alive, X-Remove")),
			vec!["keep-alive", "x-remove"]
		);
	}

	#[tokio::test]
	async fn proxy_routes_request_and_filters_control_headers() {
		let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
		let address = listener.local_addr().unwrap();
		let app = Router::new().fallback(any(|request: Request| async move {
			let uri = request.uri().to_string();
			let token = request
				.headers()
				.get("x-rivet-token")
				.and_then(|value| value.to_str().ok())
				.unwrap_or("missing");
			let kept = request
				.headers()
				.get("x-kept")
				.and_then(|value| value.to_str().ok())
				.unwrap_or("missing");
			let removed = request.headers().contains_key("x-remove")
				|| request.headers().contains_key("x-rivet-namespace");
			Response::builder()
				.status(StatusCode::CREATED)
				.header("x-kept-response", "yes")
				.header("connection", "x-remove-response")
				.header("x-remove-response", "secret")
				.body(Body::from(format!(
					"{uri}|token={token}|kept={kept}|removed={removed}"
				)))
				.unwrap()
		}));
		let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

		let state = GatewayState {
			http: Client::builder().timeout(UPSTREAM_TIMEOUT).build().unwrap(),
			rivet_endpoint: Url::parse(&format!("http://{address}")).unwrap(),
			namespace: "production".into(),
			token: Some("trusted".into()),
			base_domain: "example.com".into(),
		};
		let response = proxy(
			&state,
			ApplicationRequest {
				method: "POST".into(),
				url: "https://quiet-river.example.com/hello?x=1&rvt-key=bad".into(),
				headers: HashMap::from([
					("connection".into(), "x-remove".into()),
					("x-remove".into(), "secret".into()),
					("x-rivet-token".into(), "untrusted".into()),
					("x-rivet-namespace".into(), "untrusted".into()),
					("x-kept".into(), "yes".into()),
				]),
				body: b"hello".to_vec(),
				cancel_token: CancellationToken::new(),
			},
		)
		.await
		.unwrap();

		assert_eq!(response.status, 201);
		assert_eq!(response.headers.get("x-kept-response").unwrap(), "yes");
		assert!(!response.headers.contains_key("x-remove-response"));
		let ApplicationResponseBody::Buffered(body) = response.body else {
			panic!("expected buffered response");
		};
		let body = String::from_utf8(body).unwrap();
		assert!(body.starts_with(
			"/gateway/tunnel/request/hello?x=1&rvt-namespace=production&rvt-method=get&rvt-key=quiet-river"
		));
		assert!(body.contains("token=trusted"));
		assert!(body.contains("kept=yes"));
		assert!(body.contains("removed=false"));

		server.abort();
	}

	#[tokio::test]
	async fn health_check_does_not_require_tunnel_hostname() {
		let state = GatewayState {
			http: Client::new(),
			rivet_endpoint: Url::parse("http://127.0.0.1:1").unwrap(),
			namespace: "default".into(),
			token: None,
			base_domain: "example.com".into(),
		};
		let response = proxy(
			&state,
			ApplicationRequest {
				method: "GET".into(),
				url: "http://deployment.internal/healthz".into(),
				headers: HashMap::new(),
				body: Vec::new(),
				cancel_token: CancellationToken::new(),
			},
		)
		.await
		.unwrap();
		assert_eq!(response.status, 200);
	}
}
