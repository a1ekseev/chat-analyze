FROM mirror.gcr.io/library/golang:1.27.3-bookworm AS test
# Debian image: gcc is preinstalled, which -race (cgo) needs.
WORKDIR /src
ENV CGO_ENABLED=1
COPY go.mod go.sum ./
RUN go mod download
COPY . .
CMD ["go", "test", "-tags=integration", "-race", "-count=1", "./..."]

FROM mirror.gcr.io/library/golang:1.27.3-alpine3.24 AS build
WORKDIR /src
ARG VERSION=dev
ARG COMMIT=none
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath \
    -ldflags "-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" \
    -o /out/chat-analyze ./cmd/chat-analyze

FROM gcr.io/distroless/static-debian13:nonroot AS runtime
COPY --from=build /out/chat-analyze /chat-analyze
USER nonroot:nonroot
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s CMD ["/chat-analyze","healthcheck"]
ENTRYPOINT ["/chat-analyze"]
CMD ["run"]
