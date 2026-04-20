FROM golang:1.26-alpine AS build

WORKDIR /src

ARG SERVICE

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/service ./cmd/${SERVICE}

FROM alpine:3.21

RUN adduser -D -g '' demo

WORKDIR /app
COPY --from=build /out/service /app/service

USER demo

ENTRYPOINT ["/app/service"]
