#!/bin/bash
podman run \
--name caddy-server \
-v $(pwd)/../server_data/caddy/Caddyfile:/etc/caddy/Caddyfile:Z \
-p 4430:443 \
-it \
--rm \
docker.io/caddy \
caddy run --config /etc/caddy/Caddyfile
