FROM golang:1.21-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/edge-server ./edge-server

FROM alpine:3.20
RUN apk add --no-cache ca-certificates

WORKDIR /app
COPY --from=build /out/edge-server /app/edge-server

EXPOSE 8000 8080 8081
ENTRYPOINT ["/app/edge-server"]

