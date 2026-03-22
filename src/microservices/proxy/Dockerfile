FROM golang:1.25.6-alpine as builder

WORKDIR /app

COPY go.mod ./
COPY main.go ./

RUN go build -o proxy-service

FROM alpine:3.22

WORKDIR /app

COPY --from=builder /app/proxy-service ./proxy-service

EXPOSE 8000

CMD [ "./proxy-service" ]