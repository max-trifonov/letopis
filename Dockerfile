# syntax=docker/dockerfile:1

FROM golang:1.26-alpine AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
ARG VERSION=dev
ARG COMMIT=none
ARG DATE=unknown
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath \
    -ldflags "-s -w \
      -X github.com/max-trifonov/letopis/internal/version.Version=${VERSION} \
      -X github.com/max-trifonov/letopis/internal/version.Commit=${COMMIT} \
      -X github.com/max-trifonov/letopis/internal/version.Date=${DATE}" \
    -o /out/letopis ./cmd/letopis

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/letopis /letopis

EXPOSE 8080 9090
ENTRYPOINT ["/letopis"]
# The config is mounted by the operator; see docker-compose.yml.
CMD ["serve", "--config", "/etc/letopis/config.yaml"]
