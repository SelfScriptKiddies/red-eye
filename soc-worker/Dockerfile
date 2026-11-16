FROM rust:1.93-slim AS build
WORKDIR /build
COPY soc-worker/Cargo.toml soc-worker/Cargo.lock* ./soc-worker/
COPY soc-worker/src ./soc-worker/src
WORKDIR /build/soc-worker
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /build/soc-worker/target/release/soc-worker /usr/local/bin/soc-worker
CMD ["soc-worker"]
