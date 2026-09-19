use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use anyhow::{Context, Result, bail};
use clap::Parser;
use reqwest::{Client, Method};
use rivet_tunnel::{
	ACTOR_NAME, DEFAULT_MAX_BODY_BYTES, TunnelActor, is_hop_by_hop_header,
	read_response_body_bounded,
};
use rivetkit::{
	Registry, RuntimeMode, ServeConfig,
	serverless_http::{
		self, ApplicationFetch, ApplicationRequest, ApplicationResponse, ApplicationResponseBody,
		ListenerConfig,
	},
};
use tokio_util::sync::CancellationToken;
use url::Url;

#[derive(Parser, Debug)]
#[command(name = "rivet-tunnel-server", version, about = "Rivet tunnel server")]
struct Args {
	#[arg(long, env = "LISTEN_ADDR", default_value = "0.0.0.0:8080")]
	listen: SocketAddr,

	#[arg(long)]
	rivet_endpoint: Option<Url>,

	#[arg(long)]
	namespace: Option<String>,

	#[arg(long)]
	token: Option<String>,

	#[arg(long, env = "RIVET_TUNNEL_BASE_DOMAIN", default_value = "localhost")]
	base_domain: String,

	#[arg(long)]
	host: Option<String>,
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
	let mut config = ServeConfig::from_env();
	if let Some(endpoint) = args.rivet_endpoint {
		config.endpoint = endpoint.to_string();
	}
	if let Some(namespace) = args.namespace {
		config.namespace = namespace;
	}
	if let Some(token) = args.token {
		config.token = Some(token);
	}
	config.engine_host = args.host;

	let state = Arc::new(GatewayState {
		http: Client::builder()
			.redirect(reqwest::redirect::Policy::none())
			.build()?,
		rivet_endpoint: Url::parse(&config.endpoint).context("parse Rivet endpoint")?,
		namespace: config.namespace.clone(),
		token: config.token.clone(),
		base_domain: args.base_domain.trim_matches('.').to_ascii_lowercase(),
	});
	let application: ApplicationFetch = Arc::new(move |request| {
		let state = Arc::clone(&state);
		Box::pin(async move {
			Ok(match proxy(&state, request).await {
				Ok(response) => response,
				Err(error) => {
					tracing::warn!(%error, "tunnel gateway request failed");
					plain_response(502, &error.to_string())
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

	match RuntimeMode::from_env() {
		RuntimeMode::Serverless => {
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
		RuntimeMode::Envoy => {
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
	for (name, value) in request.headers {
		if !is_hop_by_hop_header(&name)
			&& name != "host"
			&& name != "content-length"
			&& name != "x-rivet-token"
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
	for (name, value) in response_headers {
		if let Some(name) = name
			&& !is_hop_by_hop_header(name.as_str())
			&& name.as_str() != "content-length"
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
