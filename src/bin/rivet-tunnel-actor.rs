use clap::Parser;
use rivet_tunnel::{ACTOR_NAME, TunnelActor};
use rivetkit::{Registry, ServeConfig};

#[derive(Parser)]
#[command(
	name = "rivet-tunnel-actor",
	version,
	about = "Run the Rivet tunnel actor"
)]
struct Args {
	/// Override the host used by a locally managed Rivet engine.
	#[arg(long)]
	host: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
	let args = Args::parse();
	tracing_subscriber::fmt()
		.with_env_filter(
			tracing_subscriber::EnvFilter::try_from_default_env()
				.unwrap_or_else(|_| "rivet_tunnel=info".into()),
		)
		.init();

	let mut registry = Registry::new();
	registry.register_actor::<TunnelActor>(ACTOR_NAME);
	let mut config = ServeConfig::from_env();
	config.engine_host = args.host;
	registry.start_with_config(config).await
}
