package consumer

// ---------------------------------------------------------------------------
import (
	context "context"
	"errors"
	"fmt"
	sarama "github.com/Shopify/sarama"
	"github.com/kavindyasinthasilva/kafka-connector/kafka"
	message "github.com/kavindyasinthasilva/kafka-connector/message"
)

// CreateGroupConsumer will create a KConsumer based on a kafka consumer group
// initialOffset may be either OffsetNewest or OffsetOldest
// Unlike SimpleConsumer, here the partitions are allocated by Kafka.
//
//	if autocommit is true and initialOffset is OffsetNewest - alway start from newest offset ignoring last commited offset
//
// when there's no recorded offset with kafka (new consumer group) then this value is used. It will be applied across all paritions.
// This will create a new connection with Kafka
// valType 		- const {FieldBytes, FieldInt, FieldString, FieldAvro, FieldAvroKV}
// ---------------------------------------------------------------------------
func CreateGroupConsumer(kc *kafka.KConnection, group string, topic string, initialOffset int64, autocommit bool,
	valType int, callbacks Callbacks) (*KConsumer, error) {

	c, err := kc.SClient.Partitions(topic)

	pCount := int(c[len(c)-1] + 1) // get the last value

	var kcons KConsumer
	kcons.CType = GroupConsumer // group consumer
	kcons.PartCount = pCount
	kcons.Topic = topic
	kcons.Partitions = make([]int32, pCount)
	kcons.initOffsets = make([]int64, 1)
	kcons.lastOffsets = make([]int64, pCount)
	kcons.channels = make([]<-chan *sarama.ConsumerMessage, pCount)
	kcons.callbacks = callbacks
	kcons.initOffsets[0] = initialOffset
	kcons.valType = valType
	kcons.logger = kc.Logger
	kcons.autoCommit = autocommit

	conf := sarama.NewConfig()
	conf.Version = sarama.V2_0_0_0
	conf.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky

	if initialOffset == OffsetAlwaysNew {
		conf.Consumer.Offsets.Initial = OffsetNewest
	} else {
		conf.Consumer.Offsets.Initial = initialOffset
	}

	kcons.Connection, err = kafka.NewSaramaClient(conf, kc.Brokers, kcons.logger)

	if err != nil {
		kcons.logger.Error(libname, fmt.Sprintf("GroupConsumer Topic:%s - %s", kcons.Topic, err))
	}

	kcons.sGroupConsumer, err = sarama.NewConsumerGroupFromClient(group, kcons.Connection.SClient)

	if err != nil {
		kcons.logger.Error(libname, fmt.Sprintf("GroupConsumer Topic:%s - %s", kcons.Topic, err))
		return &kcons, err
	}

	kcons.errorChannel = kcons.sGroupConsumer.Errors()

	kconsList.Lock()
	consumers = append(consumers, &kcons)
	kconsList.Unlock()

	kcons.setupMetrics()

	kcons.logger.Info(libname, fmt.Sprintf("GroupConsumer created - %s", kcons.Topic))

	return &kcons, nil
}

// Commit will inform Kafka that you are confirming the receipt upto offset
// KConsumer object recevied via OnData must be used for Commit() call
// ---------------------------------------------------------------------------
func (kcons *KConsumer) Commit(partition int32, offset int64, matadata string) error {
	// LOG - debug
	ksess := kcons.kSession

	if ksess == nil {
		return errors.New("kGPSession is nil")
	}

	ksess.SGroupSession.MarkOffset(kcons.Topic, partition, offset+1, matadata)
	return nil
}

// CommitMesg is same as Commit() except you can pass a KMessage instead of all parameters
// ---------------------------------------------------------------------------
func (kcons *KConsumer) CommitMesg(kmesg *message.KMessage, metadata string) error {
	// LOG - debug
	ksess := kcons.kSession

	if ksess == nil {
		return errors.New("kGPSession is nil")
	}

	ksess.SGroupSession.MarkOffset(kcons.Topic, kmesg.Partition, kmesg.Offset+1, metadata)
	return nil
}

// HardCommit will inform Kafka that you are confirming the receipt upto offset
// KConsumer object recevied via OnData must be used for Commit() call
// ---------------------------------------------------------------------------
func (kcons *KConsumer) HardCommit() error {
	ksess := kcons.kSession

	if ksess == nil {
		return errors.New("kGPSession is nil")
	}

	//	ksess.SGroupSession.Commit()

	return nil
}

// ResetOffsets will reset the topic offsets to provided values.
// Works only with GroupConsumers,
// Should call before calling Run() method.
// Length of slice has to be equal to no of parititions of the topic or 1
// Makesure you handle partition re-balancing case as well (same values will be applied during rebalance if not changed)
// ---------------------------------------------------------------------------
func (kcons *KConsumer) ResetOffsets(offsets []int64) error {

	if kcons.CType != GroupConsumer {
		err := errors.New("Work only with GroupConsumers")
		kcons.logger.Error(libname, fmt.Sprintf(" Unable to reset Offsets:%s - %s", kcons.Topic, err))
		return err
	}

	ll := len(offsets)
	if (ll != kcons.PartCount) && (ll != 1) {
		err := errors.New("Length of slice has to be same as total number of parititons or 1")
		kcons.logger.Error(libname, fmt.Sprintf(" Unable to reset Offsets:%s - %s", kcons.Topic, err))
		return err
	}

	kcons.objLock.Lock()
	defer kcons.objLock.Unlock()

	if kcons.Status > 1 {
		err := errors.New("Cannot be called after Startup functions are called")
		kcons.logger.Error(libname, fmt.Sprintf(" Unable to reset Offsets:%s - %s", kcons.Topic, err))
		return err
	}

	kcons.logger.Info(libname, fmt.Sprintf("GroupConsumer ResetOffsets() called %s, Status %d : %v",
		kcons.Topic, kcons.Status, offsets))

	kcons.initOffsets = offsets

	return nil
}

// Setup is run at the beginning of a new session, before ConsumeClaim.
// ---------------------------------------------------------------------------
func (ksess *kGPSession) Setup(cgs sarama.ConsumerGroupSession) error {

	kcons := ksess.KConsumer
	claims := cgs.Claims()

	kcons.objLock.Lock()
	defer kcons.objLock.Unlock()

	kcons.logger.Info(libname, fmt.Sprintf("GroupConsumer Setup() called - %s : %v", kcons.Topic, claims[kcons.Topic]))

	// call OnSetup()
	if kcons.callbacks.OnSetup != nil {
		var parts []int32

		for _, partition := range claims[kcons.Topic] {
			parts = append(parts, partition)
		}

		kcons.logger.Info(libname, fmt.Sprintf("GroupConsumer calling OnSetup - %s", kcons.Topic))
		kcons.callbacks.OnSetup(kcons, parts)
		kcons.logger.Info(libname, fmt.Sprintf("GroupConsumer OnSetup returned - %s", kcons.Topic))
	} else {
		kcons.logger.Info(libname, fmt.Sprintf("GroupConsumer OnSetup not defined to call - %s", kcons.Topic))
	}

	// initialize object variables
	kcons.Status = 1 // same as Run()
	kcons.metricPartGauge.Set(0)
	kcons.partInfo = nil
	kcons.onCatchup = false

	olen := int32(len(kcons.initOffsets))

	for _, partition := range claims[kcons.Topic] {
		if kcons.initOffsets[0] == OffsetAlwaysNew {
			offset, err := kcons.Connection.SClient.GetOffset(kcons.Topic, partition, sarama.OffsetNewest)
			if err != nil {
				kcons.logger.Warn(libname, fmt.Sprintf("GroupConsumer, could not set offset to Newest in %s:%d", kcons.Topic, partition))
				continue
			}
			kcons.logger.Info(libname, fmt.Sprintf("GroupConsumer, Marking offset to Newest %s:%d -%d\n", kcons.Topic, partition, offset))
			cgs.MarkOffset(kcons.Topic, partition, offset, "")
		} else if (olen > partition) && (kcons.initOffsets[partition]) >= 0 {
			kcons.logger.Info(libname, fmt.Sprintf("GroupConsumer, Reseting offset to %d in %s:%d\n",
				kcons.initOffsets[partition], kcons.Topic, partition))

			cgs.ResetOffset(kcons.Topic, partition, kcons.initOffsets[partition], "")
		}

		kcons.startWait.Add(1)
		kcons.stopWait.Add(1)

		kcons.metricPartGauge.Inc()
	}

	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
// but before the offsets are committed for the very last time.
// ---------------------------------------------------------------------------
func (ksess *kGPSession) Cleanup(cgs sarama.ConsumerGroupSession) error {

	kcons := ksess.KConsumer

	kcons.logger.Info(libname, fmt.Sprintf("GroupConsumer Cleanup() called - %s", ksess.KConsumer.Topic))
	kcons.metricPartGauge.Set(0)

	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// Once the Messages() channel is closed, the Handler must finish its processing
// loop and exit.
// ---------------------------------------------------------------------------
func (ksess *kGPSession) ConsumeClaim(cgs sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	kcons := ksess.KConsumer
	ksess.SGroupSession = cgs
	kcons.kSession = ksess
	partition := claim.Partition()

	kcons.logger.Info(libname, fmt.Sprintf("GroupConsumer starting ConsumeClaim() - %s:%d", kcons.Topic, partition))

	kcons.Partitions[partition] = 1
	kcons.channels[partition] = claim.Messages()

	pInfo := PartitionInfo{Partition: partition}
	pInfo.StartOffset = claim.InitialOffset()

	kcons.runCallbacks(&pInfo)

	kcons.logger.Info(libname, fmt.Sprintf("GroupConsumer returning ConsumeClaim() - %s:%d", kcons.Topic, partition))

	return nil
}

// ---------------------------------------------------------------------------
func (kcons *KConsumer) runGroupConsumer() error {
	ctx := context.Background()
	var err error
	var ksess kGPSession
	ksess.KConsumer = kcons

	go func() {
		for msg := range kcons.errorChannel {
			kcons.logger.Error(libname, fmt.Sprintf("Error from errorChannel:%s - %s", kcons.Topic, msg.Error()))
		}
		kcons.logger.Info(libname, fmt.Sprintf("GroupConsumer errorChannel closed:%s", kcons.Topic))
	}()

	for kcons.Status > 0 {

		kcons.logger.Info(libname, fmt.Sprintf("GroupConsumer starting to Consume() - %s", kcons.Topic))
		err = kcons.sGroupConsumer.Consume(ctx, []string{kcons.Topic}, &ksess)
		kcons.logger.Info(libname, fmt.Sprintf("GroupConsumer exiting Consume() - %s", kcons.Topic))

		if err != nil {
			kcons.logger.Error(libname, fmt.Sprintf("Error encountered on Consume():%s - %s", kcons.Topic, err))
		}
	}

	return err
}
