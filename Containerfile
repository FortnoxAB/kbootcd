FROM docker.io/golang:1.27.4 AS builder

WORKDIR /workspace

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build

FROM docker.io/alpine:3.24@sha256:f59abe656b692d901e184be3445e084786602108599d2a51d229c0f5a48c71bf

COPY --from=builder /workspace/kbootcd /usr/local/bin/kbootcd

USER root
ENTRYPOINT ["/usr/local/bin/kbootcd"]
