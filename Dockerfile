FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/ratelimiter .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ratelimiter /ratelimiter
USER nonroot:nonroot
EXPOSE 8080 9090
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 CMD ["/ratelimiter", "healthcheck"]
ENTRYPOINT ["/ratelimiter"]
