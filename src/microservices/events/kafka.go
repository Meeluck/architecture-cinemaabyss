package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

type KafkaClient struct {
	producer sarama.SyncProducer
	consumer sarama.Consumer
}

func NewKafkaClient(brokers []string) (*KafkaClient, error) {
	producer, err := newSyncProducerWithRetry(brokers, 20, 3*time.Second)
	if err != nil {
		return nil, fmt.Errorf("create producer: %w", err)
	}

	consumer, err := newConsumerWithRetry(brokers, 20, 3*time.Second)
	if err != nil {
		_ = producer.Close()
		return nil, fmt.Errorf("create consumer: %w", err)
	}

	return &KafkaClient{
		producer: producer,
		consumer: consumer,
	}, nil
}

func (k *KafkaClient) Close() {
	if k.producer != nil {
		if err := k.producer.Close(); err != nil {
			log.Printf("producer close error: %v", err)
		}
	}
	if k.consumer != nil {
		if err := k.consumer.Close(); err != nil {
			log.Printf("consumer close error: %v", err)
		}
	}
}

func (k *KafkaClient) Publish(topic string, key string, event Event) (EventResponse, error) {
	body, err := json.Marshal(event)
	if err != nil {
		return EventResponse{}, fmt.Errorf("marshal event: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(body),
	}

	partition, offset, err := k.producer.SendMessage(msg)
	if err != nil {
		return EventResponse{}, fmt.Errorf("send message: %w", err)
	}

	log.Printf(
		"event published topic=%s partition=%d offset=%d key=%s event_id=%s type=%s",
		topic, partition, offset, key, event.ID, event.Type,
	)

	return EventResponse{
		Status:    "success",
		Partition: partition,
		Offset:    offset,
		Event:     event,
	}, nil
}

func (k *KafkaClient) StartConsumers() error {
	topics := []string{TopicMovie, TopicUser, TopicPayment}

	for _, topic := range topics {
		pc, err := k.consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
		if err != nil {
			return fmt.Errorf("consume topic=%s partition=0: %w", topic, err)
		}

		go func(topic string, partitionConsumer sarama.PartitionConsumer) {
			defer func() {
				if err := partitionConsumer.Close(); err != nil {
					log.Printf("partition consumer close error topic=%s err=%v", topic, err)
				}
			}()

			for msg := range partitionConsumer.Messages() {
				var event Event
				if err := json.Unmarshal(msg.Value, &event); err != nil {
					log.Printf(
						"event unmarshal error topic=%s partition=%d offset=%d err=%v raw=%s",
						msg.Topic, msg.Partition, msg.Offset, err, string(msg.Value),
					)
					continue
				}

				log.Printf(
					"event processed topic=%s partition=%d offset=%d key=%s event_id=%s type=%s timestamp=%s payload=%s",
					msg.Topic,
					msg.Partition,
					msg.Offset,
					string(msg.Key),
					event.ID,
					event.Type,
					event.Timestamp,
					compactJSON(event.Payload),
				)
			}
		}(topic, pc)
	}

	return nil
}

func newSyncProducerWithRetry(brokers []string, attempts int, delay time.Duration) (sarama.SyncProducer, error) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_7_0_0
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 5
	cfg.Producer.Return.Successes = true

	var lastErr error
	for i := 1; i <= attempts; i++ {
		producer, err := sarama.NewSyncProducer(brokers, cfg)
		if err == nil {
			return producer, nil
		}
		lastErr = err
		log.Printf("waiting for kafka producer, attempt=%d err=%v", i, err)
		time.Sleep(delay)
	}

	return nil, lastErr
}

func newConsumerWithRetry(brokers []string, attempts int, delay time.Duration) (sarama.Consumer, error) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_7_0_0

	var lastErr error
	for i := 1; i <= attempts; i++ {
		consumer, err := sarama.NewConsumer(brokers, cfg)
		if err == nil {
			return consumer, nil
		}
		lastErr = err
		log.Printf("waiting for kafka consumer, attempt=%d err=%v", i, err)
		time.Sleep(delay)
	}

	return nil, lastErr
}

func compactJSON(v any) string {
	raw, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(raw)
}
