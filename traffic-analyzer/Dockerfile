# Stage 1: Plan dependencies
FROM rust:1.88.0-slim AS planner

WORKDIR /usr/src/app

RUN cargo install cargo-chef --locked

COPY . .

RUN cargo chef prepare --recipe-path recipe.json


# Stage 2: Build dependencies (cached layer)
FROM rust:1.88.0-slim AS builder

WORKDIR /usr/src/app

RUN apt update && apt upgrade -y && apt-get install -y musl-tools curl libpcap-dev

RUN cargo install cargo-chef --locked

RUN rustup target add x86_64-unknown-linux-musl

# Copy only the dependency specification
COPY --from=planner /usr/src/app/recipe.json recipe.json

# Build dependencies - this layer will be cached
RUN cargo chef cook --release --target x86_64-unknown-linux-musl --recipe-path recipe.json

# Copy source code and build application
COPY . .

RUN cargo build --target x86_64-unknown-linux-musl --release


# Stage 3: Runtime
FROM alpine:3.17

RUN apk add --no-cache libpcap

COPY --from=builder /usr/src/app/target/x86_64-unknown-linux-musl/release/traffic-analyzer /usr/local/bin/traffic-analyzer

CMD ["traffic-analyzer"]
