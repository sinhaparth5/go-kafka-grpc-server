# Golang + Kafka + gRPC
Create a Kafka producer and consumer with Golang using grpc


## Generate proto gRPC file 
```Bash
protoc -Iproto --go_out=. --go_opt=module=github.com/sinhaparth5/go-kafka-grpc-server --go-grpc_out=. --go-grpc_opt=module=github.com/sinhaparth5/go-kafka-grpc-server .\proto\service.proto
```

## Run Go files
```Bash
# Run main file
go run main.go

# Run client file
go run client.go
```
