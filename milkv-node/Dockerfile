FROM golang:1.21-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/milkv-node ./milkv-node

FROM alpine:3.20
RUN apk add --no-cache ca-certificates

WORKDIR /app
COPY --from=build /out/milkv-node /app/milkv-node

ENTRYPOINT ["/app/milkv-node"]

