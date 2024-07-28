package main

import (
	"context"
	"log"
	"time"

	pb "github.com/sinhaparth5/go-kafka-grpc-server/proto"
	"google.golang.org/grpc"
)

func main() {
	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	client := pb.NewKafkaServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	createTopicResp, err := client.CreateTopic(ctx, &pb.CreateTopicRequest{
		Topic:             "test_topic",
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
	if err != nil {
		log.Fatalf("could not create topic: %v", err)
	}
	log.Printf("CreateTopic response: success=%v, error=%s", createTopicResp.Success, createTopicResp.Error)

	produceResp, err := client.Produce(ctx, &pb.ProduceRequest{Topic: "test_topic", Message: "Hello Kafka Bro"})
	if err != nil {
		log.Fatalf("could not produce: %v", err)
	}
	log.Printf("Produce response: %v", produceResp.Success)

	consumeResp, err := client.Consume(ctx, &pb.ConsumeRequest{Topic: "test_topic"})
	if err != nil {
		log.Fatalf("could not consume message: %v", err)
	}
	log.Printf("Consume response: %v", consumeResp.Messages)
}
