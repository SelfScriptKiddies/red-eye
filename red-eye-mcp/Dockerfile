FROM rust:1.93-slim AS build
WORKDIR /build
# Red Eye proto/ is not needed here: red-eye-mcp is a standalone crate.
COPY red-eye-mcp/Cargo.toml red-eye-mcp/Cargo.lock* ./red-eye-mcp/
COPY red-eye-mcp/src ./red-eye-mcp/src
WORKDIR /build/red-eye-mcp
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /build/red-eye-mcp/target/release/red-eye-mcp /usr/local/bin/red-eye-mcp
ENV MCP_BIND=0.0.0.0:8081
EXPOSE 8081
CMD ["red-eye-mcp"]
