# Сборка
FROM golang:latest AS builder
WORKDIR /app
COPY . .
RUN go mod download
RUN CGO_ENABLED=0 go build -o /app/main .   # статическая сборка

# Финальный образ
FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/main .
ENTRYPOINT ["./main"]
EXPOSE 8888
