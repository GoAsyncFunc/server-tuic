# Build go
FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS builder

WORKDIR /app
COPY . .

ARG TARGETOS
ARG TARGETARCH

ENV CGO_ENABLED=0
RUN go mod download
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH go build -v -o tuic-node -trimpath -ldflags "-s -w" ./cmd/server

# Release
FROM alpine
# Install dependencies
RUN apk --update --no-cache add tzdata ca-certificates \
    && cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime

COPY --from=builder /app/tuic-node /usr/local/bin
ENTRYPOINT ["tuic-node"]
