FROM golang:1.18

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y --no-install-recommends --no-install-suggests netcat

RUN go get -u github.com/rakyll/gotest