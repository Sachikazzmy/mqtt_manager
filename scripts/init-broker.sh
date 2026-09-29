#!/bin/sh
set -eu

cd "$(dirname "$0")/.."
./scripts/create-local-secrets.sh
openssl verify -CAfile .secrets/mosquitto/ca.crt \
  -verify_hostname broker .secrets/mosquitto/server.crt
docker compose up -d --build backend
printf '%s\n' 'Containers started. Check: docker compose ps'
printf '%s\n' 'Open the running CLI: docker compose attach backend'
printf '%s\n' 'Detach without stopping it: Ctrl-P, then Ctrl-Q. Do not start a second server with docker compose exec.'
