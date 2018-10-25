package kafka

import (
	"encoding/binary"
	"errors"
	"math"
	"os"
	"os/signal"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/linkedin/goavro"
)

type AvroConsumer struct {
	Consumer             *kafka.Consumer
	SchemaRegistryClient *CachedSchemaRegistryClient
}

type AvroMessage struct {
	SchemaId       int
	TopicPartition kafka.TopicPartition
	Key            []byte
	Value          interface{}
}

// NewAvroConsumer creates a new AvroConsumer, a basic consumer to interact with schema registry, avro and kafka
func NewAvroConsumer(consumer *kafka.Consumer, schemaRegistryServers []string) *AvroConsumer {

	schemaRegistryClient := NewCachedSchemaRegistryClient(schemaRegistryServers)

	return &AvroConsumer{consumer, schemaRegistryClient}
}

// GetSchema gets an avro codec from schema-registry service using a schema id
func (ac *AvroConsumer) GetSchema(id int) (*goavro.Codec, error) {
	codec, err := ac.SchemaRegistryClient.GetSchema(id)
	if err != nil {
		return nil, err
	}
	return codec, nil
}

func (ac *AvroConsumer) Consume(timeout time.Duration) (*AvroMessage, error) {

	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	var absTimeout time.Time
	var timeoutMs int

	if timeout > 0 {
		absTimeout = time.Now().Add(timeout)
		timeoutMs = (int)(timeout.Seconds() * 1000.0)
	} else {
		timeoutMs = (int)(timeout)
	}

	for {
		ev := ac.Consumer.Poll(timeoutMs)

		switch e := ev.(type) {
		case *kafka.Message:

			if e.TopicPartition.Error != nil {
				return nil, e.TopicPartition.Error
			}

			msg, err := ac.ProcessAvroMsg(e)

			if err != nil {
				return nil, err
			}

			// ac.Consumer.MarkOffset(m, "")

			return &msg, nil

		case kafka.Error:
			return nil, e
		default:
			// Ignore other event types
		}

		if timeout > 0 {
			// Calculate remaining time
			timeoutMs = int(math.Max(0.0, absTimeout.Sub(time.Now()).Seconds()*1000.0))
		}

		if timeoutMs == 0 && ev == nil {
			return nil, errors.New("ErrTimedOut")
		}
	}
}

func (ac *AvroConsumer) ProcessAvroMsg(m *kafka.Message) (AvroMessage, error) {

	schemaId := binary.BigEndian.Uint32(m.Value[1:5])

	codec, err := ac.GetSchema(int(schemaId))

	if err != nil {
		return AvroMessage{}, err
	}

	// Convert binary Avro data back to native Go form
	native, _, err := codec.NativeFromBinary(m.Value[5:])
	if err != nil {
		return AvroMessage{}, err
	}

	return AvroMessage{int(schemaId), m.TopicPartition, m.Key, native}, nil
}

func (ac *AvroConsumer) Close() {
	ac.Consumer.Close()
}
