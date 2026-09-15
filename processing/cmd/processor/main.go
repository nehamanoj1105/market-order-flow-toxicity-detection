package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/nehamanoj1105/market-order-flow-toxicity-detection/processing/internal/consumer"
	"github.com/nehamanoj1105/market-order-flow-toxicity-detection/processing/internal/forwarder"
)

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func main() {
	rawBrokers := getEnv("KAFKA_BROKERS", "localhost:9092")
	brokers := strings.Split(rawBrokers, ",")
	groupID := getEnv("KAFKA_GROUP_ID", "processing-group")
	inputTopic := getEnv("INPUT_TOPIC", "trades.raw")
	outputTopic := getEnv("OUTPUT_TOPIC", "trades.normalized")

	workersStr := getEnv("NUM_WORKERS", "4")
	numWorkers, err := strconv.Atoi(workersStr)
	if err != nil || numWorkers <= 0 {
		numWorkers = 4
	}

	log.Printf("Starting processing service with %d workers...", numWorkers)
	log.Printf("Kafka brokers: %v | Input topic: %s | Output topic: %s", brokers, inputTopic, outputTopic)

	c, err := consumer.New(brokers, groupID, inputTopic)
	if err != nil {
		log.Fatalf("Failed to initialize consumer: %v", err)
	}
	defer c.Close()

	fw, err := forwarder.New(brokers, outputTopic)
	if err != nil {
		log.Fatalf("Failed to initialize forwarder: %v", err)
	}
	defer fw.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			log.Printf("Worker %d started", workerID)
			for {
				select {
				case <-ctx.Done():
					log.Printf("Worker %d shutting down...", workerID)
					return
				default:
					msg, err := c.Poll(ctx)
					if err != nil {
						if ctx.Err() != nil {
							return
						}
						log.Printf("Worker %d poll error: %v", workerID, err)
						continue
					}
					if len(msg.Value) == 0 {
						continue
					}
					if err := fw.Forward(ctx, msg.Key, msg.Value); err != nil {
						log.Printf("Worker %d forward error: %v", workerID, err)
					}
				}
			}
		}(i)
	}

	<-ctx.Done()
	log.Println("Shutdown signal received, waiting for workers to complete...")
	wg.Wait()
	log.Println("Processing service shutdown complete.")
}
