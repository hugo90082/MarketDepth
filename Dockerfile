FROM golang:1.23-bookworm AS build
WORKDIR /src
COPY . .
RUN go mod tidy \
 && go test ./... \
 && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/marketdepth .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
WORKDIR /app
COPY --from=build /out/marketdepth /app/marketdepth
ENV DATA_DIR=/data PORT=8080
EXPOSE 8080
ENTRYPOINT ["/app/marketdepth"]
