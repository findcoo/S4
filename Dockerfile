FROM golang:1.8.3-alpine
MAINTAINER findcoo <thirdlif2@gmail.com>

RUN apk update && apk add curl git
RUN curl https://glide.sh/get | sh
WORKDIR /go/src/github.com/findcoo/s4/
COPY glide.yaml ./
COPY glide.lock ./
RUN glide install
COPY ./ ./
RUN go build
ENTRYPOINT ["./S4"]
