FROM alpine:3.19 as builder

# install Zig 0.14 from Alpine edge community repo: https://pkgs.alpinelinux.org/package/edge/community/x86_64/zig
RUN echo "@edge-community https://dl-cdn.alpinelinux.org/alpine/edge/community" >> /etc/apk/repositories
RUN apk add --no-cache zig@edge-community~=0.14.0

# install dependencies
RUN apk install --no-cache zeromq

# install duckdb
RUN wget https://github.com/duckdb/duckdb/releases/download/v1.0.0/libduckdb-linux-amd64.zip \
    && unzip duckdb_cli-linux-amd64.zip -d /usr/local/bin \
    && rm duckdb_cli-linux-amd64.zip

COPY . /build/

WORKDIR /build

RUN zig build test-all -Doptimize=ReleaseFast --summary all -Dduckdb_prefix=/usr/local -Dzmq_prefix=/usr/local

RUN touch /var/touched # dummy build output

# empty result image
FROM scratch

COPY --from=builder /var/touched /tmp/touched
