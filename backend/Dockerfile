# Stage 1: Plan dependencies
FROM rust:1.88.0-slim AS planner

WORKDIR /usr/src/app

RUN cargo install cargo-chef --locked

# proto/ is the shared wire contract
COPY proto/ ./proto/
COPY backend/ .

RUN cargo chef prepare --recipe-path recipe.json


# Stage 2: Build dependencies (cached layer)
FROM rust:1.88.0-slim AS builder

WORKDIR /usr/src/app

RUN apt update && apt upgrade -y && apt-get install -y musl-tools curl

RUN cargo install cargo-chef --locked

RUN rustup target add x86_64-unknown-linux-musl

# Copy only the dependency specification
COPY --from=planner /usr/src/app/recipe.json recipe.json

# Build dependencies - this layer will be cached
RUN cargo chef cook --release --target x86_64-unknown-linux-musl --recipe-path recipe.json

# Stamp the built binary with the building commit and release version
# (no .git in the context). Kept BELOW the chef-cook layer on purpose:
# the sha changes with every commit and must not invalidate the cached
# dependency build. Passed by the Makefile via compose build args; empty
# -> build.rs falls back to git / the VERSION file -> "unknown".
ARG RED_EYE_GIT_SHA=""
ENV RED_EYE_GIT_SHA=${RED_EYE_GIT_SHA}
ARG RED_EYE_VERSION=""
ENV RED_EYE_VERSION=${RED_EYE_VERSION}

# Copy source code and build application
COPY proto/ ./proto/
COPY backend/ .

RUN cargo build --target x86_64-unknown-linux-musl --release


# Stage 2b: the red-eye-agent binary served by GET /api/agent/download
# (backend/src/api/download.rs). Built from the SAME commit as the backend
# so the served agent can never drift from the stack.
FROM rust:1.88.0-slim AS agent-planner

WORKDIR /usr/src/agent
RUN cargo install cargo-chef --locked
# proto/ explicitly (the agent/proto symlink is .dockerignored — same
# pattern as the backend stages below).
COPY proto/ ./proto/
COPY agent/ .
RUN cargo chef prepare --recipe-path recipe.json


FROM rust:1.88.0-slim AS agent-builder

WORKDIR /usr/src/agent
RUN apt update && apt-get install -y musl-tools curl
RUN cargo install cargo-chef --locked
RUN rustup target add x86_64-unknown-linux-musl

COPY --from=agent-planner /usr/src/agent/recipe.json recipe.json
RUN cargo chef cook --release --target x86_64-unknown-linux-musl --recipe-path recipe.json

# Below the cook layer on purpose — see the builder stage note above.
ARG RED_EYE_GIT_SHA=""
ENV RED_EYE_GIT_SHA=${RED_EYE_GIT_SHA}
ARG RED_EYE_VERSION=""
ENV RED_EYE_VERSION=${RED_EYE_VERSION}

COPY proto/ ./proto/
COPY agent/ .
RUN cargo build --release --target x86_64-unknown-linux-musl


# Stage 3: Runtime
FROM alpine:3.17

COPY --from=builder /usr/src/app/target/x86_64-unknown-linux-musl/release/backend /usr/local/bin/backend
COPY --from=agent-builder /usr/src/agent/target/x86_64-unknown-linux-musl/release/red-eye-agent /usr/local/share/red-eye/red-eye-agent

CMD ["backend"]
