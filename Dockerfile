FROM golang:1.25-alpine3.22 AS builder
WORKDIR /sources
COPY . .
RUN apk add --no-cache make
RUN make build

FROM alpine:3.22
RUN addgroup -S tenderseed && adduser -S -G tenderseed tenderseed -h /data
COPY --from=builder /sources/build/tenderseed /usr/local/bin/tenderseed
USER tenderseed
WORKDIR /data
EXPOSE 26656
ENTRYPOINT ["tenderseed", "-home", "/data"]
CMD ["start"]
