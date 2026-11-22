FROM rust:1.85.0-slim-bookworm@sha256:1829c432be4a592f3021501334d3fcca24f238432b13306a4e62669dec538e52 AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
ENV RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+crt-static"
ENV CARGO_PROFILE_RELEASE_LTO=fat CARGO_PROFILE_RELEASE_PANIC=abort
RUN cargo build -j 4 --release --locked --target x86_64-unknown-linux-gnu && strip target/x86_64-unknown-linux-gnu/release/shopping-sessions
FROM scratch
COPY --from=build /src/target/x86_64-unknown-linux-gnu/release/shopping-sessions /shopping-sessions
ENV SHOPPING_RAW_EVENTS=1 SHOPPING_MASKED_PREFIX=1 SHOPPING_HEAVIEST_FIRST=1
ENV SHOPPING_REUSE_THREADS=1 SHOPPING_ALIAS_FILTER=1 SHOPPING_EARLY_CLICKS=1 SHOPPING_LF_IN_TAIL=1 SHOPPING_MAP_POLICY=huge-cold
ENV SHOPPING_BORROW_EVENT_IDS=1 SHOPPING_DEFER_CLICK_IDS=1 SHOPPING_BUFFER_MIB=8
USER 1000:1000
ENTRYPOINT ["/shopping-sessions"]
