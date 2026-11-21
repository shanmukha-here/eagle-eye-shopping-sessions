FROM rust:1.85.0-slim-bookworm@sha256:1829c432be4a592f3021501334d3fcca24f238432b13306a4e62669dec538e52 AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
ENV RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+crt-static"
ENV CARGO_PROFILE_RELEASE_LTO=fat CARGO_PROFILE_RELEASE_PANIC=abort
RUN cargo build -j 4 --release --locked --target x86_64-unknown-linux-gnu && strip target/x86_64-unknown-linux-gnu/release/shopping-sessions
FROM scratch
COPY --from=build /src/target/x86_64-unknown-linux-gnu/release/shopping-sessions /shopping-sessions
ENV SHOPPING_RAW_EVENTS=1 SHOPPING_MASKED_PREFIX=1 SHOPPING_FORCE_MMAP=1
USER 1000:1000
ENTRYPOINT ["/shopping-sessions"]
