/*
* Copyright 2020 PickMe (Digital Mobility Solutions Lanka (PVT) Ltd).
* All rights reserved.
* Authors: Harshana Ranmuthu (harshana@pickme.lk)
* Created: 02 March 2020
 */

package producer

// ---------------------------------------------------------------------------
import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	sarama "github.com/Shopify/sarama"
	"github.com/kavindyasinthasilva/kafka-connector/avro"
	"github.com/kavindyasinthasilva/kafka-connector/kafka"
	log "github.com/kavindyasinthasilva/kafka-connector/log"
	"github.com/prometheus/client_golang/prometheus"
	"reflect"
)

// KProducer represent the Consumer obect of a kafka
// KStatus : 0=initial, 1=running
// ---------------------------------------------------------------------------
type KProducer struct {
	Topic            string
	PartCount        int                // total how many partitions are available
	PType            int                // 0=async, 1=sync
	Connection       *kafka.KConnection // connection structure
	sAsyncProducer   sarama.AsyncProducer
	sSyncProducer    sarama.SyncProducer
	sPartitioner     sarama.Partitioner
	sAsynSendError   OnAsyncSendErrorFunc
	inputChannel     chan<- *sarama.ProducerMessage
	successChannel   <-chan *sarama.ProducerMessage
	errorChannel     <-chan *sarama.ProducerError
	onAsyncSendError OnAsyncSendErrorFunc
	onAsyncSucess    OnAsyncSuccessFunc
	valType          int                // Type of Value
	UserData         interface{}        // apps may set this to associate KConsumer to it's reference.
	logger           log.PrefixedLogger // logger for the consumer
	metricSentCount  prometheus.Counter // total sent mesg count
}

const (
	// ManualPartitioner will use the partition value given in SendMesg
	ManualPartitioner = 0

	// HashPartitioner will use sarama.NewHashPartitioner
	HashPartitioner = 1

	// RandomPartitioner will use sarama.NewRandomPartitioner
	RandomPartitioner = 2

	// RounRobinPartitioner will use sarama.NewRounRobinPartitioner
	RounRobinPartitioner = 3

	// AsyncProducer is a KProducer type
	AsyncProducer = 0

	// SyncProducer is a KProducer type
	SyncProducer = 1
)

const (
	libname = "kafka-connector:producer"
)

// OnAsyncSendErrorFunc will throw errors on Async Produce
// ---------------------------------------------------------------------------
type OnAsyncSendErrorFunc func(kprod *KProducer, err error)

// OnAsyncSuccessFunc will call back on all successful sends
// ---------------------------------------------------------------------------
type OnAsyncSuccessFunc func(kprod *KProducer, partition int32, offset int64, key interface{})

// CreateSyncProducer will create a Kafka producer that can be used send messages with guarenteed deliver (to kafka)
// manualPartitioner : false - default partitioning, true - manual partitioning
// ---------------------------------------------------------------------------
func CreateSyncProducer(kc *kafka.KConnection, topic string, partitioner int) (*KProducer, error) {

	var kprod = KProducer{}

	c, err := kc.SClient.Partitions(topic)
	kprod.PartCount = len(c)

	kprod.Topic = topic
	kprod.PType = SyncProducer
	kprod.logger = kc.Logger
	//	kprod.reporter = kc.Reporter

	conf := sarama.NewConfig()
	conf.Version = sarama.V2_0_0_0
	conf.Producer.Return.Successes = true
	conf.Producer.RequiredAcks = sarama.WaitForLocal

	switch partitioner {
	case ManualPartitioner:
		conf.Producer.Partitioner = sarama.NewManualPartitioner
	case HashPartitioner:
		conf.Producer.Partitioner = sarama.NewHashPartitioner
	case RandomPartitioner:
		conf.Producer.Partitioner = sarama.NewHashPartitioner
	default:
		conf.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	}

	kprod.sPartitioner = conf.Producer.Partitioner(topic)

	kprod.Connection, err = kafka.NewSaramaClient(conf, kc.Brokers, kprod.logger)

	if err != nil {
		kprod.logger.Error(libname, fmt.Sprintf("SyncProducer Client creation failed. Topic:%s - %s", kprod.Topic, err))
		return nil, err
	}

	kprod.sSyncProducer, err = sarama.NewSyncProducerFromClient(kprod.Connection.SClient)

	if err != nil {
		kprod.logger.Error(libname, fmt.Sprintf("SyncProducer creation failed. Topic:%s - %s", kprod.Topic, err))
		return nil, err
	}

	kprod.setupMetrics()

	kprod.logger.Info(libname, fmt.Sprintf("SyncProducer created - %s", topic))

	return &kprod, err
}

// CreateAsyncProducer will publish messages in asynchronous manner.errors
// ---------------------------------------------------------------------------
func CreateAsyncProducer(kc *kafka.KConnection, topic string, partitioner int,
	onSendErr OnAsyncSendErrorFunc, onSendSuccess OnAsyncSuccessFunc) (*KProducer, error) {

	var kprod = KProducer{}

	c, err := kc.SClient.Partitions(topic)
	kprod.PartCount = len(c)

	kprod.Topic = topic
	kprod.PType = AsyncProducer // async producer
	kprod.onAsyncSendError = onSendErr
	kprod.onAsyncSucess = onSendSuccess
	kprod.logger = kc.Logger
	//	kprod.reporter = kc.Reporter

	conf := sarama.NewConfig()
	conf.Version = sarama.V2_0_0_0
	conf.Producer.RequiredAcks = sarama.WaitForLocal

	switch partitioner {
	case ManualPartitioner:
		conf.Producer.Partitioner = sarama.NewManualPartitioner
	case HashPartitioner:
		conf.Producer.Partitioner = sarama.NewHashPartitioner
	case RandomPartitioner:
		conf.Producer.Partitioner = sarama.NewHashPartitioner
	default:
		conf.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	}

	kprod.Connection, err = kafka.NewSaramaClient(conf, kc.Brokers, kprod.logger)

	if err != nil {
		kprod.logger.Error(libname, fmt.Sprintf("AyncProducer Client creation failed. Topic:%s - %s", kprod.Topic, err))
		return nil, err
	}

	kprod.sAsyncProducer, err = sarama.NewAsyncProducerFromClient(kprod.Connection.SClient)

	if err != nil {
		kprod.logger.Error(libname, fmt.Sprintf("AyncProducer creation failed. Topic:%s - %s", kprod.Topic, err))
		return nil, err
	}

	kprod.inputChannel = kprod.sAsyncProducer.Input()
	kprod.successChannel = kprod.sAsyncProducer.Successes()
	kprod.errorChannel = kprod.sAsyncProducer.Errors()

	kprod.setupMetrics()

	kprod.logger.Info(libname, fmt.Sprintf("AsyncProducer created - %s", topic))

	go kprod.asyncChannelListener()

	return &kprod, err
}

// GetPartition will return the possible partition for the key based on producer config
// it will return -1 for error
// ---------------------------------------------------------------------------
func (kprod *KProducer) GetPartition(key []byte) (part int32) {

	mesg := sarama.ProducerMessage{}
	mesg.Key = sarama.ByteEncoder(key)
	p, err := kprod.sPartitioner.Partition(&mesg, int32(kprod.PartCount))

	if err != nil {
		kprod.logger.Warn(libname, fmt.Sprintf("GetParitition error - %s", err))
		return -1
	}

	return p
}

// SendAvro will send the message after encoding to avro
// partition parameter is used only for the manul partitioner (others can set to 0)
// ---------------------------------------------------------------------------
func (kprod *KProducer) SendAvro(key []byte, mesg interface{}, partition int32, subject string,
	version int) (part int32, offset int64, e error) {

	if (mesg == nil) || (reflect.TypeOf(mesg).Kind() == reflect.Ptr && reflect.ValueOf(mesg).IsNil()) {
		return kprod.Send(key, nil, partition)
	}

	avmesg, err := avro.Encode(mesg, subject, version)

	if err != nil {
		kprod.logger.Error(libname, "Avro encode error", err)
		return 0, 0, err
	}

	return kprod.Send(key, avmesg, partition)
}

// Send will send the message to the corresponding topic.
// partition parameter is used only for the manul partitioner (others can set to 0)
// ---------------------------------------------------------------------------
func (kprod *KProducer) Send(key []byte, mesg []byte, partition int32) (part int32, offset int64, e error) {

	var p int32
	var o int64
	var err error

	msg := sarama.ProducerMessage{}
	msg.Topic = kprod.Topic
	msg.Partition = int32(partition)
	msg.Headers = append([]sarama.RecordHeader{}, sarama.RecordHeader{Key: []byte("Sender"), Value: kafka.IPPID})

	if mesg == nil {
		msg.Value = nil
	} else {
		msg.Value = sarama.ByteEncoder(mesg)
	}

	if key != nil {
		msg.Key = sarama.ByteEncoder(key)
	} else {
		if len(mesg) == 0 {
			return partition, -1, errors.New("Both key and mesg cannot be 0 length")
		}
	}

	kprod.metricSentCount.Inc()

	if kprod.PType == SyncProducer {
		p, o, err = kprod.sSyncProducer.SendMessage(&msg)

		if err != nil {
			kprod.logger.Error(libname, fmt.Sprintf("Error sending SyncProducer %s - %s", kprod.Topic, err))
		}

	} else { // AsyncProducer
		kprod.inputChannel <- &msg
		p = 0
		o = 0
		err = nil
	}

	return p, o, err
}

// SendStruct will send and encoded struct to the topic (Ideal for recovery - sends only exported variables (starting with Caps))
// Make sure no circular refernces (struct A refering to B and B refering to A via exported variables)
// partition parameter is used only for the manul partitioner (others can set to 0)
// ---------------------------------------------------------------------------
func (kprod *KProducer) SendStruct(key []byte, strct interface{}, partition int32) (part int32, offset int64, e error) {

	if (strct == nil) || (reflect.TypeOf(strct).Kind() == reflect.Ptr && reflect.ValueOf(strct).IsNil()) {
		return kprod.Send(key, nil, partition)
	}

	var buf bytes.Buffer // Stand-in for a network connection

	enc := gob.NewEncoder(&buf) // Will write to network.

	// Encode (send) some values.
	err := enc.Encode(strct)

	if err != nil {
		kprod.logger.Error(libname, fmt.Sprintf("Error in sendStruct %s - %s", kprod.Topic, err))
		return 0, 0, err
	}

	return kprod.Send(key, buf.Bytes(), partition)
}

// asyncChannelListener is a blocking call listening to Async Success and Error channels
// ---------------------------------------------------------------------------
func (kprod *KProducer) asyncChannelListener() {

	select {
	case errorMsg := <-kprod.errorChannel:
		if kprod.onAsyncSendError != nil {
			kprod.onAsyncSendError(kprod, errorMsg.Err)
		}
	case sucssMsg := <-kprod.successChannel:
		if kprod.onAsyncSucess != nil {
			kprod.onAsyncSucess(kprod, sucssMsg.Partition, sucssMsg.Offset, sucssMsg.Key)
		}
	}
}

// setupMetrics will setup metrics for producers
// ---------------------------------------------------------------------------
func (kprod *KProducer) setupMetrics() {

	kprod.metricSentCount = prometheus.NewCounter(prometheus.CounterOpts{Namespace: kafka.MetricName,
		Subsystem: "kafka_connector_producer", Name: "message_count", ConstLabels: map[string]string{"topic": kprod.Topic}})

	err := prometheus.Register(kprod.metricSentCount)

	if err != nil {
		kprod.logger.Error(libname, fmt.Sprintf("MericSentCount registration failed Topic:%s - %s", kprod.Topic, err.Error()))
	}
}
