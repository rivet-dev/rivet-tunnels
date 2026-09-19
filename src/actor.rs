use std::{
	collections::HashMap,
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
	time::Duration,
};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use parking_lot::Mutex;
use rivetkit::prelude::*;
use rivetkit::{ActorHttpResponse, Request, Response, WebSocket, WsMessage, action};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::{
	ConnectorMessage, DEFAULT_MAX_BODY_BYTES, Header, TunnelRequest, TunnelResponse,
	TunnelServerMessage, is_hop_by_hop_header,
};

pub const ACTOR_NAME: &str = "tunnel";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Default, Serialize, Deserialize)]
pub struct TunnelState;

pub struct TunnelActor {
	runtime: Arc<TunnelRuntime>,
}

struct TunnelRuntime {
	connector: Mutex<Option<Connector>>,
	pending: Mutex<HashMap<u64, PendingRequest>>,
	next_connector_id: AtomicU64,
	next_request_id: AtomicU64,
}

struct PendingRequest {
	connector_id: u64,
	sender: oneshot::Sender<Result<TunnelResponse, String>>,
}

#[derive(Clone)]
struct Connector {
	id: u64,
	websocket: WebSocket,
}

impl TunnelRuntime {
	fn new() -> Self {
		Self {
			connector: Mutex::new(None),
			pending: Mutex::new(HashMap::new()),
			next_connector_id: AtomicU64::new(1),
			next_request_id: AtomicU64::new(1),
		}
	}

	fn begin_request(
		&self,
		request_id: u64,
		sender: oneshot::Sender<Result<TunnelResponse, String>>,
	) -> Option<Connector> {
		let connector = self.connector.lock();
		let connector = connector.as_ref()?.clone();
		self.pending.lock().insert(
			request_id,
			PendingRequest {
				connector_id: connector.id,
				sender,
			},
		);
		Some(connector)
	}

	fn handle_connector_message(&self, connector_id: u64, message: ConnectorMessage) {
		let is_current = self
			.connector
			.lock()
			.as_ref()
			.is_some_and(|connector| connector.id == connector_id);
		if !is_current {
			return;
		}

		match message {
			ConnectorMessage::Response(response) => {
				if let Some(pending) = self.pending.lock().remove(&response.id)
					&& pending.connector_id == connector_id
				{
					let _ = pending.sender.send(Ok(response));
				}
			}
		}
	}

	fn detach_connector(&self, connector_id: u64) {
		{
			let mut connector = self.connector.lock();
			if !connector
				.as_ref()
				.is_some_and(|connector| connector.id == connector_id)
			{
				return;
			}
			*connector = None;
		}
		self.fail_pending(connector_id);
	}

	fn fail_pending(&self, connector_id: u64) {
		let mut pending = self.pending.lock();
		let request_ids: Vec<_> = pending
			.iter()
			.filter_map(|(request_id, request)| {
				(request.connector_id == connector_id).then_some(*request_id)
			})
			.collect();
		for request_id in request_ids {
			if let Some(request) = pending.remove(&request_id) {
				let _ = request
					.sender
					.send(Err("tunnel connector disconnected".into()));
			}
		}
	}
}

#[async_trait]
impl Actor for TunnelActor {
	type State = TunnelState;
	type Input = ();
	type Actions = ();
	type Events = ();
	type Queue = ();
	type ConnParams = ();
	type ConnState = ();
	type Action = action::Raw;

	const CONCURRENT_HTTP_CALLBACKS: bool = true;
	const MAX_CONCURRENT_HTTP_CALLBACKS: usize = 64;

	async fn create_state(_ctx: &Ctx<Self>, _input: Self::Input) -> Result<Self::State> {
		Ok(TunnelState)
	}

	async fn create(_ctx: &Ctx<Self>) -> Result<Self> {
		Ok(Self {
			runtime: Arc::new(TunnelRuntime::new()),
		})
	}

	async fn on_fetch_response(
		self: Arc<Self>,
		_ctx: Ctx<Self>,
		req: Request,
	) -> Result<ActorHttpResponse> {
		let req = req.into_buffered().await.context("buffer request body")?;
		if req.body().len() > DEFAULT_MAX_BODY_BYTES {
			return Ok(text_response(413, "request body exceeds 8 MiB")?.into());
		}

		let request_id = self.runtime.next_request_id.fetch_add(1, Ordering::Relaxed);
		let request = TunnelRequest {
			id: request_id,
			method: req.method().to_string(),
			path: req.uri().to_string(),
			headers: req
				.headers()
				.iter()
				.filter(|(name, _)| !is_hop_by_hop_header(name.as_str()))
				.map(|(name, value)| Header {
					name: name.to_string(),
					value: String::from_utf8_lossy(value.as_bytes()).into_owned(),
				})
				.collect(),
			body: req.body().clone(),
		};

		let payload = serde_cbor::to_vec(&TunnelServerMessage::Request(request))
			.context("encode tunnel request")?;
		let (response_tx, response_rx) = oneshot::channel();
		let Some(connector) = self.runtime.begin_request(request_id, response_tx) else {
			return Ok(text_response(503, "tunnel connector is offline")?.into());
		};
		connector.websocket.send(WsMessage::Binary(payload));
		let cancellation = req.cancellation_token();

		let result = tokio::select! {
			response = response_rx => response.context("connector dropped response")?,
			_ = cancellation.cancelled() => {
			self.runtime.pending.lock().remove(&request_id);
				return Ok(text_response(499, "client closed request")?.into());
			}
			_ = tokio::time::sleep(REQUEST_TIMEOUT) => {
			self.runtime.pending.lock().remove(&request_id);
				return Ok(text_response(504, "tunnel response timed out")?.into());
			}
		};
		let response = match result {
			Ok(response) => response,
			Err(message) => return Ok(text_response(502, &message)?.into()),
		};
		if response.body.len() > DEFAULT_MAX_BODY_BYTES {
			return Ok(text_response(502, "response body exceeds 8 MiB")?.into());
		}
		let headers = response
			.headers
			.into_iter()
			.filter(|header| !is_hop_by_hop_header(&header.name))
			.map(|header| (header.name, header.value))
			.collect();
		Ok(Response::from_parts(response.status, headers, response.body)?.into())
	}

	async fn on_websocket(
		self: Arc<Self>,
		ctx: Ctx<Self>,
		websocket: WebSocket,
		req: Request,
	) -> Result<()> {
		if !is_connector_path(req.uri().path()) {
			bail!("unknown tunnel websocket path");
		}

		let connector_id = self
			.runtime
			.next_connector_id
			.fetch_add(1, Ordering::Relaxed);
		let runtime = self.runtime.clone();
		websocket.configure_message_event_callback(Some(Arc::new(move |message, _| {
			let bytes = match message {
				WsMessage::Binary(bytes) => bytes,
				WsMessage::Text(_) => bail!("tunnel protocol requires binary CBOR messages"),
			};
			let message: ConnectorMessage =
				serde_cbor::from_slice(&bytes).context("decode connector message")?;
			runtime.handle_connector_message(connector_id, message);
			Ok(())
		})));

		let runtime = self.runtime.clone();
		websocket.configure_close_event_callback(Some(Arc::new(move |_, _, _| {
			let runtime = runtime.clone();
			Box::pin(async move {
				runtime.detach_connector(connector_id);
				Ok(())
			})
		})));

		let previous = self.runtime.connector.lock().replace(Connector {
			id: connector_id,
			websocket: websocket.clone(),
		});
		if let Some(previous) = previous {
			self.runtime.fail_pending(previous.id);
			previous
				.websocket
				.close(Some(1012), Some("replaced by a new connector".into()))
				.await;
		}

		let ready = serde_cbor::to_vec(&TunnelServerMessage::Ready {
			actor_id: ctx.actor_id().to_owned(),
		})?;
		websocket.send(WsMessage::Binary(ready));
		Ok(())
	}
}

fn text_response(status: u16, message: &str) -> Result<Response> {
	Response::from_parts(
		status,
		HashMap::from([("content-type".into(), "text/plain; charset=utf-8".into())]),
		message.as_bytes().to_vec(),
	)
}

fn is_connector_path(path: &str) -> bool {
	matches!(path, "/websocket/connect" | "/connect")
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn recognizes_rivetkit_raw_websocket_path() {
		assert!(is_connector_path("/websocket/connect"));
		assert!(is_connector_path("/connect"));
		assert!(!is_connector_path("/websocket/other"));
	}
}
