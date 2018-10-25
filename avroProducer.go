package kafka

import (
	"encoding/binary"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/linkedin/goavro"
)

type AvroProducer struct {
	producer             *kafka.Producer
	schemaRegistryClient *CachedSchemaRegistryClient
}

// NewAvroProducer is a basic producer to interact with schema registry, avro and kafka
func NewAvroProducer(producer *kafka.Producer, schemaRegistryServers []string) *AvroProducer {

	schemaRegistryClient := NewCachedSchemaRegistryClient(schemaRegistryServers)

	return &AvroProducer{producer, schemaRegistryClient}
}

// GetSchemaId gets schema id from schema-registry service
func (ap *AvroProducer) GetSchemaId(topic string, avroCodec *goavro.Codec) (int, error) {
	schemaId, err := ap.schemaRegistryClient.CreateSubject(topic, avroCodec)
	if err != nil {
		return 0, err
	}
	return schemaId, nil
}

func (ap *AvroProducer) Produce(topic kafka.TopicPartition, avroCodec *goavro.Codec, key []byte, value interface{}, deliveryChan chan kafka.Event) error {

	schemaId, err := ap.GetSchemaId(*topic.Topic, avroCodec)

	if err != nil {
		return err
	}

	binarySchemaId := make([]byte, 4)
	binary.BigEndian.PutUint32(binarySchemaId, uint32(schemaId))

	// Convert native Go form to binary Avro data
	binaryValue, err := avroCodec.BinaryFromNative(nil, value)

	if err != nil {
		return err
	}

	var binaryMsg []byte
	// first byte is magic byte, always 0 for now
	binaryMsg = append(binaryMsg, byte(0))
	//4-byte schema ID as returned by the Schema Registry
	binaryMsg = append(binaryMsg, binarySchemaId...)
	//avro serialized data in Avro’s binary encoding
	binaryMsg = append(binaryMsg, binaryValue...)

	msg := &kafka.Message{
		TopicPartition: topic,
		Key:            key,
		Value:          binaryMsg,
	}

	err = ap.producer.Produce(msg, deliveryChan)

	return err
}

func (ac *AvroProducer) Close() {
	ac.producer.Close()
}
