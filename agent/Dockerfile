# Stage 1: Plan dependencies
FROM rust:1.88.0-slim AS planner

WORKDIR /usr/src/app

RUN cargo install cargo-chef --locked

COPY . .

RUN cargo chef prepare --recipe-path recipe.json


# Stage 2: Build dependencies (cached layer)
FROM rust:1.88.0-slim AS builder

WORKDIR /usr/src/app

RUN apt update && apt upgrade -y && apt-get install -y musl-tools curl

RUN cargo install cargo-chef --locked

RUN rustup target add x86_64-unknown-linux-musl

COPY --from=planner /usr/src/app/recipe.json recipe.json

RUN cargo chef cook --release --target x86_64-unknown-linux-musl --recipe-path recipe.json

COPY . .

RUN cargo build --target x86_64-unknown-linux-musl --release


# Stage 3: Runtime — fully static, no runtime deps
FROM alpine:3.17

COPY --from=builder /usr/src/app/target/x86_64-unknown-linux-musl/release/red-eye-agent /usr/local/bin/red-eye-agent

CMD ["red-eye-agent"]
