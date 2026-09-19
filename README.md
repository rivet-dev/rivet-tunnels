# Rivet Tunnels

> **Experimental:** This is an early prototype with no availability guarantees. Do not use it for production traffic.

Expose a local HTTP server through a public URL backed by a Rivet Actor:

```bash
npx @rivet-labs/tunnel --endpoint http://127.0.0.1:3000
# https://quietriver.example.com/
```

The npm package and hosted gateway are not available yet. Use the local setup below to try it today.

## Architecture

```mermaid
flowchart LR
    Client
    subgraph Server["rivet-tunnel-server"]
        Gateway
        Actor
        Gateway --> Actor
    end
    subgraph CLI["rivet-tunnel"]
        Agent
    end
    App
    Client --> Gateway
    Actor <--> Agent
    Agent --> App
```

- **Client:** Browser, webhook, or API caller using the public URL.
- **Gateway:** Public wildcard endpoint that routes a hostname to its actor.
- **Actor:** Rivet Actor coordinating one tunnel.
- **Agent:** Local tunnel CLI connected to the actor over WebSocket.
- **App:** Local HTTP server being exposed.

The agent creates an actor with a random tunnel name. The gateway reads that name from the request hostname and sends the request through the actor to the agent and local app.

## Local setup

**Step 1: Start a local app**

```bash
python3 -m http.server 3000 --bind 0.0.0.0
```

**Step 2: Start the tunnel server**


The server runs both the gateway and the Rivet Actor. It also downloads and starts a local Rivet engine.

```bash
RIVETKIT_ENGINE_AUTO_DOWNLOAD=1 cargo run --bin rivet-tunnel-server -- --host 0.0.0.0
```

**Step 3: Open a tunnel**

```bash
cargo run --bin rivet-tunnel -- --endpoint http://127.0.0.1:3000
```

Open the printed `http://<tunnel-name>.localhost:8080` URL.

## Production deployment

**Step 1: Get a Rivet endpoint**

Get an endpoint from [Rivet Cloud](https://rivet.dev) or a self-hosted Rivet deployment.

**Step 2: Deploy the tunnel server**

Deploy the included `Dockerfile`. The single server handles both Rivet Actor requests and public tunnel traffic.

```bash
RIVETKIT_RUNTIME_MODE=serverless \
RIVET_ENDPOINT="<endpoint>" \
RIVET_TUNNEL_BASE_DOMAIN="example.com" \
rivet-tunnel-server
```

Register `https://<server>/api/rivet` as the serverless actor runner for the endpoint.

**Step 3: Configure wildcard DNS**

Point `*.example.com` at the server. Your proxy or load balancer must preserve the original `Host` header.

**Step 4: Open a tunnel**

Run the agent with the same Rivet endpoint and the public base URL.

```bash
RIVET_ENDPOINT="<endpoint>" \
RIVET_TUNNEL_PUBLIC_BASE_URL="https://example.com" \
rivet-tunnel --endpoint http://127.0.0.1:3000
```

## Limits

HTTP only, one active agent per tunnel, 8 MiB buffered request and response bodies, and a 60-second response timeout.

## License

Copyright Rivet Gaming, LLC. All rights reserved.
