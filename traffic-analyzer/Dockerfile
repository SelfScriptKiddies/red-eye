FROM rust:1.88.0-slim as builder

RUN apt update && apt upgrade -y && apt-get install -y musl-tools curl libpcap-dev

WORKDIR /usr/src/app

COPY Cargo.toml Cargo.lock ./
COPY src src
RUN rustup target add x86_64-unknown-linux-musl
RUN cargo build --target x86_64-unknown-linux-musl --release


FROM alpine:3.17

COPY --from=builder /usr/src/app/target/x86_64-unknown-linux-musl/release/traffic-analyzer /usr/local/bin/traffic-analyzer

CMD ["traffic-analyzer"]
