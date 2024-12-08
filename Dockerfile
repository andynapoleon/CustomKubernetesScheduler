FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o scheduler

FROM alpine:3.18
WORKDIR /app
COPY --from=builder /app/scheduler .
CMD ["./scheduler"]