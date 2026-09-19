FROM rust:1.90-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --locked --release --bin rivet-tunnel-actor

FROM debian:bookworm-slim

RUN apt-get update \
	&& apt-get install --yes --no-install-recommends ca-certificates \
	&& rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/rivet-tunnel-actor /usr/local/bin/rivet-tunnel-actor

ENV RIVETKIT_RUNTIME_MODE=serverless \
	RIVETKIT_ENGINE_SPAWN=never \
	RIVET_PORT=8080

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/rivet-tunnel-actor"]
