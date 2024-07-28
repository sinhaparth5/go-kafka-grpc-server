package main

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	pb "github.com/sinhaparth5/go-kafka-grpc-server/proto"
	"google.golang.org/grpc"
	"log"
	"net"
	"time"
)

const (
	kafkaBroker = "localhost:9092"
)

type server struct {
	pb.UnimplementedKafkaServiceServer
}

func (s *server) Produce(ctx context.Context, req *pb.ProduceRequest) (*pb.ProduceResponse, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaBroker})
	if err != nil {
		return nil, err
	}
	defer p.Close()

	deliveryChan := make(chan kafka.Event)
	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &req.Topic, Partition: kafka.PartitionAny},
		Value:          []byte(req.Message),
	}, deliveryChan)

	e := <-deliveryChan
	m := e.(*kafka.Message)
	if m.TopicPartition.Error != nil {
		return nil, m.TopicPartition.Error
	}

	return &pb.ProduceResponse{Success: true}, nil
}

func (s *server) Consume(ctx context.Context, req *pb.ConsumeRequest) (*pb.ConsumeResponse, error) {
	log.Printf("Received Consume request: topic=%s", req.Topic)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBroker,
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatalf("Failed to create Kafka consumer: %v", err)
		return nil, err
	}
	defer c.Close()

	err = c.SubscribeTopics([]string{req.Topic}, nil)
	if err != nil {
		log.Fatalf("Failed to subscribe to topics: %v", err)
		return nil, err
	}

	var messages []string
	run := true
	for run {
		select {
		case <-ctx.Done():
			run = false
		default:
			ev := c.Poll(100)
			switch e := ev.(type) {
			case *kafka.Message:
				log.Printf("Received message: %s", string(e.Value))
				messages = append(messages, string(e.Value))
				run = false // exit loop after receiving the first message
			case kafka.Error:
				if e.Code() == kafka.ErrAllBrokersDown {
					log.Fatalf("All brokers are down: %v", e)
					return nil, fmt.Errorf("all brokers are down")
				}
				log.Printf("Kafka error: %v", e)
			default:
				log.Printf("Ignored event: %v", e)
			}
		}
	}

	if len(messages) == 0 {
		return nil, fmt.Errorf("no messages consumed")
	}

	return &pb.ConsumeResponse{Messages: messages}, nil
}

func (s *server) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	log.Printf("Received CreateTopic request: %v", req)
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": kafkaBroker})
	if err != nil {
		return &pb.CreateTopicResponse{Success: false, Error: err.Error()}, err
	}
	defer adminClient.Close()

	// Check if topic exists
	topics, err := adminClient.GetMetadata(&req.Topic, false, int(10*time.Second))
	if err != nil {
		return &pb.CreateTopicResponse{Success: false, Error: err.Error()}, err
	}

	for topic := range topics.Topics {
		if topic == req.Topic {
			log.Printf("Topic %s already exists", req.Topic)
			return &pb.CreateTopicResponse{Success: true}, nil
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	results, err := adminClient.CreateTopics(
		ctx,
		[]kafka.TopicSpecification{{
			Topic:             req.Topic,
			NumPartitions:     int(req.NumPartitions),
			ReplicationFactor: int(req.ReplicationFactor)}},
	)
	if err != nil {
		return &pb.CreateTopicResponse{Success: false, Error: err.Error()}, err
	}

	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError {
			return &pb.CreateTopicResponse{Success: false, Error: result.Error.String()}, result.Error
		}
	}
	return &pb.CreateTopicResponse{Success: true}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterKafkaServiceServer(s, &server{})

	fmt.Println("Server is running on port :50051")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
