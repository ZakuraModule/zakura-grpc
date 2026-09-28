# syntax=docker/dockerfile:1

# Build with the directory containing the sibling `zakura` and `zakura-grpc`
# repositories as the context. docker-compose.yml configures that layout.
FROM rust:1.97.1-trixie AS builder

RUN apt-get -qq update && \
    apt-get -qq install -y --no-install-recommends libclang-dev protobuf-compiler && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY zakura /src/zakura
COPY zakura-grpc /src/zakura-grpc
WORKDIR /src/zakura
# The sibling workspaces do not share a lockfile until the gRPC crates are
# published, so let Cargo reconcile Zakura's lockfile inside the build stage.
RUN cargo build --release --package zakura --bin zakurad

FROM debian:trixie-slim AS runtime

ARG UID=10001
ARG GID=10001
ARG USER=zakura
ENV UID=${UID} GID=${GID} USER=${USER} HOME=/home/${USER}

RUN apt-get -qq update && \
    apt-get -qq install -y --no-install-recommends adduser ca-certificates curl util-linux && \
    rm -rf /var/lib/apt/lists/* && \
    addgroup --quiet --gid ${GID} ${USER} && \
    adduser --quiet --gid ${GID} --uid ${UID} --home ${HOME} \
      ${USER} --disabled-password --gecos ""

COPY --from=builder /src/zakura/target/release/zakurad /usr/local/bin/zakurad
COPY zakura/docker/entrypoint.sh /usr/local/bin/entrypoint.sh

EXPOSE 10000
ENTRYPOINT ["entrypoint.sh"]
CMD ["zakurad"]
