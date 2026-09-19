# Rivet Tunnels

> **Experimental:** This is an early prototype. It buffers HTTP bodies in memory, has no availability guarantees, and is not ready for production traffic.

Expose a local HTTP server through a public URL backed by a Rivet Actor:

```bash
npx @rivet-labs/tunnel --endpoint http://127.0.0.1:3000
# https://quietriver.tunnels.example.com/
```

## Architecture

```text
Browser → *.tunnels.example.com → wildcard ingress → tunnel actor ⇄ CLI → localhost
```

Each CLI invocation creates a random actor key and holds a WebSocket open to that actor. The wildcard ingress extracts the subdomain and routes the request to the matching actor, which relays it to the CLI and returns the local response.

## Run locally

```bash
# Terminal 1: actor server + local Rivet engine
RIVETKIT_ENGINE_AUTO_DOWNLOAD=1 cargo run --bin rivet-tunnel-server -- --host 0.0.0.0

# Terminal 2: wildcard ingress
cargo run --bin rivet-tunnel-ingress -- --listen 0.0.0.0:8080

# Terminal 3: expose a local server
cargo run --bin rivet-tunnel -- --endpoint http://127.0.0.1:3000
```

For a hosted deployment, point `*.tunnels.gameinc.io` at the ingress and configure `RIVET_ENDPOINT`, `RIVET_NAMESPACE`, `RIVET_TOKEN`, `RIVET_POOL_NAME`, `RIVET_TUNNEL_BASE_DOMAIN`, and `RIVET_TUNNEL_PUBLIC_BASE_URL`. The wildcard only selects the actor; the ingress keeps the Rivet token private and performs the host-to-actor rewrite.

## Current limits

HTTP only, one connector per tunnel, 8 MiB buffered request and response bodies, and a 60-second response timeout.
