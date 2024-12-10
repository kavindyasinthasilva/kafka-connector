package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"kafka-connector/avro"
	log "kafka-connector/log"
	"os"
	"strings"
)

// KConnection will represent the connection to Kafka
// ---------------------------------------------------------------------------
type KConnection struct {
	Brokers []string
	SClient sarama.Client
	Logger  log.PrefixedLogger
}

// StatInterval is used by consumer and producer to report stats
var StatInterval int

// MetricName is global variable
var MetricName string

// IPPID is used to set IP:PID in the mesg header
var IPPID []byte

const (
	libname = "kafka-connector:kafka"
)

// Init will initialize a kafka connection with a broker
// can be used to create KConsumers and KProducers
// brokers is a "host1:port1,host2:port2,..." type of a list
// ---------------------------------------------------------------------------
func Init(brokers string, logger log.PrefixedLogger, metricServiceName string, registryURL string) (*KConnection, error) {
	//func Init(brokers string, logger log.PrefixedLogger, registryURL string) (*KConnection, error) {
	// generic init

	host, err := os.Hostname()
	pid := os.Getpid()
	IPPID = []byte(fmt.Sprintf("%s:%d", host, pid))

	logger.Info(libname, fmt.Sprintf("Initializing kafka-connector %s", string(IPPID)))

	conf := sarama.NewConfig()
	conf.Version = sarama.V2_0_0_0

	brokerArr := strings.Split(brokers, ",")

	//	sarama.Logger = logger

	k, err := NewSaramaClient(conf, brokerArr, logger)

	if err != nil {
		return nil, err
	}

	if len(registryURL) > 0 {
		err = avro.Init(registryURL, k.SClient, logger)
	}

	MetricName = metricServiceName

	return k, err
}

// NewSaramaClient will create a sarama client
// ---------------------------------------------------------------------------
func NewSaramaClient(conf *sarama.Config, brokers []string, logger log.PrefixedLogger) (*KConnection, error) {

	cl, err := sarama.NewClient(brokers, conf)

	if err != nil {
		return nil, err
	}

	k := KConnection{}
	k.Brokers = brokers
	k.SClient = cl
	k.Logger = logger

	logger.Info(libname, "Connected to kafka client.")

	return &k, nil
}

// GetPartitionCount will return the partition count of a topic
// ---------------------------------------------------------------------------
func (conn *KConnection) GetPartitionCount(topic string) (part int32, err error) {
	p, err := conn.SClient.Partitions(topic)

	return int32(p[len(p)-1] + 1), err
}
