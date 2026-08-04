FROM golang:1.24 AS builder

ARG TARGETARCH=amd64
ARG TARGETOS=linux

WORKDIR /workspace

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG GIT_COMMIT=unknown
ARG BUILD_DATE=unknown
ARG VERSION=0.0.1
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -a \
    -ldflags "-X main.version=${VERSION} -X main.gitCommit=${GIT_COMMIT} -X main.buildDate=${BUILD_DATE}" \
    -o manager cmd/main.go

FROM registry.access.redhat.com/ubi9/ubi-minimal:latest

RUN microdnf install -y ca-certificates && microdnf clean all

COPY --from=builder /workspace/manager /manager

USER 65532:65532

ENTRYPOINT ["/manager"]
