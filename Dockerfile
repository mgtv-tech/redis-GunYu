FROM golang:latest AS builder

#ENV GOPROXY=https://goproxy.cn,direct
ENV GO111MODULE=on

WORKDIR /app/redisgunyu/
COPY . .
RUN go mod download && go mod tidy
RUN make

FROM busybox

WORKDIR /app/
COPY --from=builder /app/redisgunyu/redisGunYu ./

ENTRYPOINT ["/app/redisGunYu"]
