package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/aws/aws-sdk-go-v2/credentials"

	"github.com/IBM/sarama"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// here's my new comment
// another cool commit
const DDB_CONNECT_URL = "http://helm-ddb-dynamodb.default.svc.cluster.local:8000"
const KAFKA_CONNECT_URL = "kafka-service.kafka.svc.cluster.local:9092"

// Message represents the structure of the JSON message consumed from Kafka.
type Message struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

var dynamoClient *dynamodb.Client
var tableName = "KafkaMessages" // Update with your DynamoDB table name

// initDynamoDB initializes the DynamoDB client.
func initDynamoDB() {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-east-1"), // Set your AWS region
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("key", "key", "")), // Replace with your AWS credentials
		config.WithEndpointResolver(aws.EndpointResolverFunc(
			func(service, region string) (aws.Endpoint, error) {
				if service == dynamodb.ServiceID && region == "us-east-1" {
					return aws.Endpoint{
						URL: DDB_CONNECT_URL,
					}, nil
				}
				return aws.Endpoint{}, fmt.Errorf("unknown endpoint requested")
			}),
		),
	)
	if err != nil {
		log.Fatalf("Unable to load AWS SDK config: %v", err)
	}
	dynamoClient = dynamodb.NewFromConfig(cfg)
}

// writeToDynamoDB writes a message to DynamoDB.
func writeToDynamoDB(msg Message) error {
	log.Println("About to write message to DynamoDB: ", msg.Value)
	_, err := dynamoClient.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"Key":   &types.AttributeValueMemberS{Value: msg.Key},
			"Value": &types.AttributeValueMemberS{Value: msg.Value},
		},
	})
	return err
}

func main() {
	// Initialize DynamoDB client
	initDynamoDB()

	// Kafka consumer configuration
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// Define Kafka broker and topic
	brokers := []string{KAFKA_CONNECT_URL}
	topic := "my-topic"

	// Create a new consumer group
	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Fatalf("Error creating Kafka consumer: %v", err)
	}
	defer consumer.Close()

	// Start consuming the specified topic
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("Error creating Kafka partition consumer: %v", err)
	}
	defer partitionConsumer.Close()

	// Handle graceful shutdown
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Consume messages from the topic
	log.Println("Consuming messages from Kafka topic and writing to DynamoDB...")
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			var message Message
			log.Printf("Raw Message value: %s", string(msg.Value))
			log.Printf("Raw Message key: %s", string(msg.Key))
			message.Key = string(msg.Key)
			message.Value = string(msg.Value)
			// Write the consumed message to DynamoDB
			if err := writeToDynamoDB(message); err != nil {
				log.Printf("Error writing message to DynamoDB: %v", err)
			} else {
				log.Printf("Message written to DynamoDB: Key=%s, Value=%s\n", message.Key, message.Value)
			}

		case err := <-partitionConsumer.Errors():
			log.Printf("Error consuming message: %v", err)

		case <-signals:
			log.Println("Interrupt signal received, shutting down consumer...")
			return
		}
	}
}
