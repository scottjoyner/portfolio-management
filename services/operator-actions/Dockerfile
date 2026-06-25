# syntax=docker/dockerfile:1
FROM rust:1-bookworm AS build
WORKDIR /src
COPY services/operator-actions/Cargo.toml ./Cargo.toml
COPY services/operator-actions/src ./src
RUN cargo build --release

FROM debian:bookworm-slim AS runtime
WORKDIR /app
COPY --from=build /src/target/release/operator-actions /usr/local/bin/operator-actions
RUN mkdir -p /app/data
ENV OPERATOR_ACTIONS_BIND=0.0.0.0:8098
ENV OPERATOR_ACTIONS_PATH=/app/data/operator-actions.json
EXPOSE 8098
CMD ["operator-actions"]
