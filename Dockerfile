FROM rust:1.90-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --locked --release --bin rivet-tunnel-server

FROM debian:bookworm-slim

RUN apt-get update \
	&& apt-get install --yes --no-install-recommends ca-certificates \
	&& groupadd --system rivet \
	&& useradd --system --gid rivet --no-create-home rivet \
	&& rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/rivet-tunnel-server /usr/local/bin/rivet-tunnel-server

EXPOSE 8080

USER rivet

ENTRYPOINT ["/usr/local/bin/rivet-tunnel-server", "--runtime-mode", "serverless", "--engine-spawn", "never"]
