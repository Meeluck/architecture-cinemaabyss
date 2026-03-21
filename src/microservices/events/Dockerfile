FROM golang:1.25.6-alpine as builder

WORKDIR /app

COPY go.mod ./
RUN go mod download

COPY . ./
RUN CGO_ENABLED=0 GOOS=linux go build -o events-service .

FROM alpine:3.22

WORKDIR /app

COPY --from=builder /app/events-service ./events-service

EXPOSE 8082

CMD ["./events-service"]