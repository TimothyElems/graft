# Build context is the repo root: the service imports the kernel module from ../kernel.
FROM golang:1.26-bookworm AS build
WORKDIR /src
COPY kernel/ kernel/
COPY service/ service/
WORKDIR /src/service
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /graftd ./cmd/graftd

# No shell, no package manager, non-root. `graftd -healthcheck` replaces curl.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /graftd /graftd
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/graftd"]
