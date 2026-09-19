use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Header {
	pub name: String,
	pub value: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TunnelRequest {
	pub id: u64,
	pub method: String,
	pub path: String,
	pub headers: Vec<Header>,
	pub body: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TunnelResponse {
	pub id: u64,
	pub status: u16,
	pub headers: Vec<Header>,
	pub body: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ActorMessage {
	Ready { actor_id: String },
	Request(TunnelRequest),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum AgentMessage {
	Response(TunnelResponse),
}
