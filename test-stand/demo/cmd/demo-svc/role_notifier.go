package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/mydecisive/mdai-tracealyzer/test-stand/demo/internal/common"
)

type checkoutEvent struct {
	RequestID string `json:"request_id"`
	Scenario  string `json:"scenario"`
	SKU       string `json:"sku"`
	Quantity  int    `json:"quantity"`
}

func runNotifier(service string, logger *common.Logger) error {
	httpClient := common.NewTracedHTTPClient(service)
	brokers := common.Getenv("KAFKA_BROKERS", "kafka:9092")
	catalogURL := common.Getenv("CATALOG_URL", "http://catalog-api:8080")
	joinedTopic := common.Getenv("KAFKA_TOPIC_JOINED", "checkout-completed-joined")
	detachedTopic := common.Getenv("KAFKA_TOPIC_DETACHED", "checkout-completed-detached")
	groupID := common.Getenv("KAFKA_GROUP_ID", "notifier")

	rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	handler := func(ctx context.Context, msg common.KafkaMessage) error {
		var event checkoutEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			return err
		}
		meta := common.RequestMeta{
			RequestID: event.RequestID,
			Scenario:  event.Scenario,
			Method:    http.MethodGet,
			Route:     "/catalog",
		}
		_, err := common.JSONRequest(ctx, httpClient, logger, http.MethodGet, catalogURL+"/catalog", "notifier.fetch_catalog", meta, nil, nil)
		return err
	}

	consumers := []*common.KafkaConsumer{
		common.NewKafkaConsumer(common.KafkaConsumerConfig{
			Service:  service,
			Brokers:  brokers,
			Topic:    joinedTopic,
			GroupID:  groupID + "-joined",
			Detached: false,
			Handler:  handler,
		}),
		common.NewKafkaConsumer(common.KafkaConsumerConfig{
			Service:  service,
			Brokers:  brokers,
			Topic:    detachedTopic,
			GroupID:  groupID + "-detached",
			Detached: true,
			Handler:  handler,
		}),
	}

	logger.Info(rootCtx, "notifier starting", map[string]any{
		"event":          "notifier_started",
		"brokers":        brokers,
		"joined_topic":   joinedTopic,
		"detached_topic": detachedTopic,
	})

	var wg sync.WaitGroup
	for _, consumer := range consumers {
		wg.Add(1)
		c := consumer
		go func() {
			defer wg.Done()
			if err := c.Run(rootCtx, logger); err != nil && !errors.Is(err, context.Canceled) {
				logger.Info(context.Background(), "consumer stopped with error", map[string]any{
					"event": "consumer_stopped",
					"error": err.Error(),
				})
			}
		}()
	}

	<-rootCtx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-shutdownCtx.Done():
	}

	logger.Info(context.Background(), "notifier stopped", map[string]any{
		"event": "notifier_stopped",
	})
	return nil
}
