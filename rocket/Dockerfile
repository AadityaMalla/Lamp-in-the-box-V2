# =============================================================
# Author : Aaditya Malla (CSCI 312 - Group 4)
# File   : rocket/Dockerfile
# Purpose: Multi-stage build. Compile the Rust app, then ship only
#          the binary on a small Debian image.
# =============================================================
FROM rust:1-slim-bookworm AS build
WORKDIR /app
COPY Cargo.toml .
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
COPY --from=build /app/target/release/rocket-app /usr/local/bin/rocket-app
ENV ROCKET_ADDRESS=0.0.0.0 ROCKET_PORT=8000
EXPOSE 8000
CMD ["rocket-app"]
