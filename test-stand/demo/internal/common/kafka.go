package common

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/segmentio/kafka-go"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	ddtracer "gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

type kafkaHeadersCarrier struct {
	headers *[]kafka.Header
}

func (c kafkaHeadersCarrier) Set(key, val string) {
	*c.headers = append(*c.headers, kafka.Header{Key: key, Value: []byte(val)})
}

func (c kafkaHeadersCarrier) ForeachKey(handler func(key, val string) error) error {
	if c.headers == nil {
		return nil
	}
	for _, h := range *c.headers {
		if err := handler(h.Key, string(h.Value)); err != nil {
			return err
		}
	}
	return nil
}

type KafkaProducer struct {
	writer  *kafka.Writer
	service string
	topic   string
}

func NewKafkaProducer(service, brokers, topic string) *KafkaProducer {
	return &KafkaProducer{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokers),
			Topic:                  topic,
			Balancer:               &kafka.LeastBytes{},
			RequiredAcks:           kafka.RequireOne,
			AllowAutoTopicCreation: true,
			WriteTimeout:           10 * time.Second,
			ReadTimeout:            10 * time.Second,
		},
		service: service,
		topic:   topic,
	}
}

func (p *KafkaProducer) Publish(ctx context.Context, key string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	span, _ := ddtracer.StartSpanFromContext(ctx, "kafka.publish",
		ddtracer.ServiceName(p.service),
		ddtracer.ResourceName("Publish "+p.topic),
		ddtracer.Tag(ext.SpanKind, ext.SpanKindProducer),
	)
	span.SetTag("messaging.system", "kafka")
	span.SetTag("messaging.destination.name", p.topic)
	span.SetTag("messaging.operation.type", "publish")
	defer span.Finish()

	headers := make([]kafka.Header, 0, 4)
	carrier := kafkaHeadersCarrier{headers: &headers}
	if err := ddtracer.Inject(span.Context(), carrier); err != nil {
		span.SetTag(ext.Error, err)
	}

	msg := kafka.Message{
		Key:     []byte(key),
		Value:   body,
		Time:    time.Now(),
		Headers: headers,
	}
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		span.SetTag(ext.Error, err)
		return err
	}
	return nil
}

func (p *KafkaProducer) Close() error {
	return p.writer.Close()
}

type KafkaMessage struct {
	Key     string
	Value   []byte
	Headers []kafka.Header
}

type KafkaHandler func(ctx context.Context, msg KafkaMessage) error

type KafkaConsumerConfig struct {
	Service  string
	Brokers  string
	Topic    string
	GroupID  string
	Detached bool
	Handler  KafkaHandler
}

type KafkaConsumer struct {
	cfg    KafkaConsumerConfig
	reader *kafka.Reader
}

func NewKafkaConsumer(cfg KafkaConsumerConfig) *KafkaConsumer {
	return &KafkaConsumer{
		cfg: cfg,
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:     []string{cfg.Brokers},
			GroupTopics: []string{cfg.Topic},
			GroupID:     cfg.GroupID,
			MinBytes:    1,
			MaxBytes:    10e6,
			StartOffset: kafka.FirstOffset,
		}),
	}
}

func (c *KafkaConsumer) Run(ctx context.Context, logger *Logger) error {
	defer c.reader.Close()
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			logger.Info(ctx, "kafka fetch failed", map[string]any{
				"event": "kafka_fetch_failed",
				"topic": c.cfg.Topic,
				"error": err.Error(),
			})
			time.Sleep(time.Second)
			continue
		}

		c.handleMessage(ctx, logger, msg)

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			logger.Info(ctx, "kafka commit failed", map[string]any{
				"event": "kafka_commit_failed",
				"topic": c.cfg.Topic,
				"error": err.Error(),
			})
		}
	}
}

func (c *KafkaConsumer) handleMessage(ctx context.Context, logger *Logger, msg kafka.Message) {
	spanOpts := []ddtracer.StartSpanOption{
		ddtracer.ServiceName(c.cfg.Service),
		ddtracer.ResourceName("Process " + c.cfg.Topic),
		ddtracer.Tag(ext.SpanKind, ext.SpanKindConsumer),
	}
	if !c.cfg.Detached {
		headers := msg.Headers
		carrier := kafkaHeadersCarrier{headers: &headers}
		if parent, err := ddtracer.Extract(carrier); err == nil && parent != nil {
			spanOpts = append(spanOpts, ddtracer.ChildOf(parent))
		}
	}

	span, spanCtx := ddtracer.StartSpanFromContext(ctx, "kafka.process", spanOpts...)
	span.SetTag("messaging.system", "kafka")
	span.SetTag("messaging.destination.name", c.cfg.Topic)
	span.SetTag("messaging.operation.type", "process")
	span.SetTag("messaging.kafka.consumer_group", c.cfg.GroupID)
	span.SetTag("messaging.kafka.partition", msg.Partition)
	span.SetTag("messaging.kafka.offset", msg.Offset)
	if c.cfg.Detached {
		span.SetTag("messaging.detached", true)
	}
	defer span.Finish()

	payload := KafkaMessage{Key: string(msg.Key), Value: msg.Value, Headers: msg.Headers}
	if err := c.cfg.Handler(spanCtx, payload); err != nil {
		span.SetTag(ext.Error, err)
		logger.Info(spanCtx, "kafka handler failed", map[string]any{
			"event": "kafka_handler_failed",
			"topic": c.cfg.Topic,
			"error": err.Error(),
		})
	}
}
