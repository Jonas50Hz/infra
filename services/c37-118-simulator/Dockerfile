FROM rust:1.85-bookworm AS build

WORKDIR /build

COPY services/c37-118-simulator/Cargo.toml /build/Cargo.toml
COPY services/c37-118-simulator/Cargo.lock /build/Cargo.lock
COPY services/c37-118-simulator/src /build/src
COPY services/c37-118-simulator/profiles /build/profiles

RUN cargo build --release --locked --bins

FROM build AS test

RUN cargo test --locked

FROM debian:bookworm-slim AS runtime

RUN useradd --system --uid 10001 --create-home simulator

COPY --from=build /build/target/release/c37-118-simulator /usr/local/bin/c37-118-simulator
COPY --from=build /build/target/release/c37-118-probe /usr/local/bin/c37-118-probe
COPY services/c37-118-simulator/profiles /etc/c37-118/profiles

USER simulator

ENTRYPOINT ["/usr/local/bin/c37-118-simulator"]