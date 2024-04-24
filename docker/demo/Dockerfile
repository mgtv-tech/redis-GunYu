FROM golang:latest AS builder

#ENV GOPROXY=https://goproxy.cn,direct
ENV GO111MODULE=on

WORKDIR /app/redisgunyu/
COPY . .
RUN go mod download && go mod tidy
RUN make


FROM ubuntu:22.04
RUN apt-get update && apt-get install -y redis-server

WORKDIR /app/
COPY --from=builder /app/redisgunyu/redisGunYu /app/redisgunyu/docker/demo/ ./

CMD ["bash", "/app/start.sh"]
