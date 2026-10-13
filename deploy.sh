#!/usr/bin/env bash
# =============================================================
# Author : Aaditya Malla (CSCI 312 - Group 4)
# File   : deploy.sh
# Purpose: One-command automated deployment. Builds the images,
#          starts the containers, waits until the whole chain
#          (NGINX -> Rocket -> Spark -> Delta Lake on S3) answers,
#          then prints the container status.
# Usage  : ./deploy.sh
# =============================================================
set -euo pipefail
cd "$(dirname "$0")"

echo "==> Building images and starting containers..."
docker compose up -d --build

echo "==> Waiting for the full chain to respond (Spark needs about a minute)..."
for i in $(seq 1 60); do
  if curl -sf "http://localhost/api/students" > /dev/null; then
    echo "==> Ready."
    docker compose ps
    echo
    echo "Open http://<your EC2 public IP>/ in a browser."
    exit 0
  fi
  sleep 5
done

echo "!! Timed out. Check logs with: docker compose logs spark rocket nginx"
exit 1
