# Stage 1: Plan dependencies
FROM rust:1.88.0-slim AS planner

WORKDIR /usr/src/app

RUN cargo install cargo-chef --locked

# proto/ is the shared wire contract (symlinked into this crate as
# ./proto); the crate itself comes in below. Copy proto/ explicitly — a
# symlinked path cannot cross the build context boundary.
COPY proto/ ./proto/
COPY traffic-analyzer/ .

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

# Stamp the built binary with the building commit and release version
# (no .git in the context). Below the cook layer so a new sha doesn't
# invalidate cached deps.
ARG RED_EYE_GIT_SHA=""
ENV RED_EYE_GIT_SHA=${RED_EYE_GIT_SHA}
ARG RED_EYE_VERSION=""
ENV RED_EYE_VERSION=${RED_EYE_VERSION}

# Copy source code and build application
COPY proto/ ./proto/
COPY traffic-analyzer/ .

RUN cargo build --target x86_64-unknown-linux-musl --release


# Stage 3: Runtime
FROM alpine:3.17

RUN apk add --no-cache libpcap

COPY --from=builder /usr/src/app/target/x86_64-unknown-linux-musl/release/traffic-analyzer /usr/local/bin/traffic-analyzer

CMD ["traffic-analyzer"]
