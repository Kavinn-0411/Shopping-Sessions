# Shopping Sessions: native Rust pipeline.
FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
ENV RUSTFLAGS="-C target-cpu=x86-64-v3"
RUN cargo build --release --locked && strip target/release/sessions

FROM debian:bookworm-slim
COPY --from=build /src/target/release/sessions /app/sessions
WORKDIR /app
CMD ["/app/sessions"]
