FROM golang:1.24

WORKDIR /app

CMD ["sh", "-c", "rm -f ./tmp/main && go build -buildvcs=false -o ./tmp/main ./cmd/agent && exec ./tmp/main dev"]
