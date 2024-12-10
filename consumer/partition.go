package consumer

import (
	"errors"
	"fmt"
	sarama "github.com/Shopify/sarama"
	kafka "kafka-connector/kafka"
	message "kafka-connector/message"
)

// CreatePartitionConsumer will create a simple consumer without any GroupID
// partitions[] - list of partitions to subscribe
//   - empty array to AddPartition() later (create only the Consumer)
//   - nil to subscribe to all partitions
//
// offsets[] > 0 - start from given offset number, or use constants OffsetOldest or OffsetNewest
//   - if only offsets[0] is provided, it will be used for all partitions
//   - nil will defaults to OffsetNewest
//
// valType 		- const {FieldBytes, FieldInt, FieldString, FieldAvro, FieldAvroKV}
// ---------------------------------------------------------------------------
func CreatePartitionConsumer(kc *kafka.KConnection, topic string, partitions []int32, offsets []int64, valType int,
	callbacks Callbacks) (*KConsumer, error) {

	c, err := kc.SClient.Partitions(topic)

	if err != nil {
		kc.Logger.Error(libname, fmt.Sprintf("Topic:%s - %s", topic, err))
		return nil, err
	}

	pCount := int(c[len(c)-1] + 1) // get the last value
	plen := len(partitions)
	olen := len(offsets)

	if olen == 0 {
		offsets = []int64{OffsetNewest}
		olen = 1
	}

	var kcons KConsumer
	kcons.Connection = kc
	kcons.PartCount = pCount
	kcons.Topic = topic
	kcons.Partitions = make([]int32, pCount)
	kcons.initOffsets = make([]int64, pCount)
	kcons.lastOffsets = make([]int64, pCount)
	kcons.maxOffsets = make([]int64, pCount)
	kcons.sPartitionConsumers = make([]sarama.PartitionConsumer, pCount)
	kcons.channels = make([]<-chan *sarama.ConsumerMessage, pCount)
	kcons.callbacks = callbacks
	kcons.valType = valType
	kcons.logger = kc.Logger

	if plen > 0 {
		for i := 0; i < plen; i++ {
			kcons.Partitions[partitions[i]] = 1

			if olen == 1 {
				kcons.initOffsets[partitions[i]] = offsets[0]
			} else {
				kcons.initOffsets[partitions[i]] = offsets[i]
			}
		}
	} else if partitions != nil { // partition == empty array (not nil), just create consumer but no AddParition()
		kcons.logger.Info(libname, fmt.Sprintf("PartitionConsumer subscribing to 0 partitions - %s", topic))
		for i := 0; i < pCount; i++ {
			kcons.initOffsets[i] = offsets[0]
		}

	} else { // partitions == nil means subscribe to all parititions
		kcons.logger.Info(libname, fmt.Sprintf("PartitionConsumer subscribing to all partitions - %s", topic))
		for i := 0; i < pCount; i++ {
			kcons.Partitions[i] = 1
			kcons.initOffsets[i] = offsets[0]
		}
	}

	kcons.sConsumer, err = sarama.NewConsumerFromClient(kc.SClient)

	if err != nil {
		kcons.logger.Error(libname, fmt.Sprintf("Topic:%s - %s", topic, err))
		return &kcons, err
	}

	kconsList.Lock()
	consumers = append(consumers, &kcons)
	kconsList.Unlock()

	kcons.setupMetrics()

	for i := 0; i < pCount; i++ {
		if kcons.Partitions[i] > 0 {
			err = kcons.AddPartition(int32(i), kcons.initOffsets[i])
			if err != nil {
				kcons.logger.Error(libname, fmt.Sprintf("Topic:%s - %s", topic, err))
				return &kcons, err
			}
		}
	}

	kcons.logger.Info(libname, fmt.Sprintf("PartitionConsumer created - %s", topic))

	return &kcons, nil
}

// AddPartition will add consuming new partitions under the same KConsumer
// ---------------------------------------------------------------------------
func (kcons *KConsumer) AddPartition(partition int32, offset int64) error {

	kcons.logger.Info(libname, fmt.Sprintf("Adding partition - %s:%d, Offset:%d", kcons.Topic, partition, offset))
	var err error

	if kcons.CType != PartitionConsumer {
		return errors.New("Work only with PartitionConsumers")
	}

	if partition >= int32(kcons.PartCount) {
		return fmt.Errorf("%s has only %d partitions, cannot add parition %d", kcons.Topic, kcons.PartCount, partition)
	}

	kcons.lastOffsets[partition] = 0

	kcons.sPartitionConsumers[partition], err = kcons.sConsumer.ConsumePartition(kcons.Topic, int32(partition), offset)

	if err == nil {
		kcons.Partitions[partition] = 1
		kcons.initOffsets[partition] = offset

		kcons.channels[partition] = kcons.sPartitionConsumers[partition].Messages()
		if kcons.Status > 0 {
			kcons.runPartitionConsumer(partition) // start listning to the new topic
		}
	} else {
		kcons.logger.Error(libname, fmt.Sprintf("Addpartition Topic:%s:%d - %s", kcons.Topic, partition, err))
	}

	return err
}

// RemovePartition will stop consuming the given partition
// ---------------------------------------------------------------------------
func (kcons *KConsumer) RemovePartition(partition int32) error {
	if kcons.CType != PartitionConsumer {
		return errors.New("Work only with PartitionConsumers")
	}

	if partition >= int32(kcons.PartCount) {
		return fmt.Errorf("%s has only %d partitions, cannot remove parition %d", kcons.Topic, kcons.PartCount, partition)
	}

	if kcons.Partitions[partition] == 0 {
		return fmt.Errorf("Not subscribed to %s:%d partition, cannot remove parition %d", kcons.Topic, partition, partition)
	}

	err := kcons.sPartitionConsumers[partition].Close()

	if err != nil {
		kcons.logger.Error(libname, fmt.Sprintf("ClosePartition Topic:%s:%d - %s", kcons.Topic, partition, err))
		return err
	}

	kcons.channels[partition] = nil
	kcons.Partitions[partition] = 0
	kcons.sPartitionConsumers[partition] = nil

	kcons.metricPartGauge.Dec()

	// remove this parition from pInfo slice
	var pInfo []*PartitionInfo

	kcons.objLock.Lock()
	for _, p := range kcons.partInfo {
		if p.Partition != partition {
			pInfo = append(pInfo, p)
		}
	}
	kcons.partInfo = pInfo
	kcons.objLock.Unlock()

	kcons.logger.Info(libname, fmt.Sprintf("Partition closed - %s:%d", kcons.Topic, partition))
	return nil
}

// GetMessage will read messages one by one from a PartitionConsumer type KConsumer.
// Use this function with RegisterStruct() and producer.SendStruct() for recieve recovery topics
// ---------------------------------------------------------------------------
func (kcons *KConsumer) GetMessage(partition int32) (message *message.KMessage, err error) {

	// only works with Partition consumers
	if kcons.CType != PartitionConsumer {
		return nil, errors.New("Not partition consumer")
	}

	// are we at the tail
	if (kcons.maxOffsets[partition] == 0) || (kcons.lastOffsets[partition]+1 >= kcons.maxOffsets[partition]) {

		offset, err := kcons.Connection.SClient.GetOffset(kcons.Topic, partition, sarama.OffsetNewest)

		if err != nil {
			kcons.logger.Error(libname, fmt.Sprintf("GetMessage encountered a problem Topic:%s:%d", kcons.Topic, partition), err)
		} else {
			kcons.maxOffsets[partition] = offset
		}

		offset = -1

		// check for empty partitions
		if kcons.lastOffsets[partition] == 0 {
			offset, err = kcons.Connection.SClient.GetOffset(kcons.Topic, partition, sarama.OffsetOldest)

			if err != nil {
				kcons.logger.Error(libname, fmt.Sprintf("GetMessage encountered a problem2 Topic:%s:%d", kcons.Topic, partition), err)
			}
		}

		if (kcons.lastOffsets[partition]+1 >= kcons.maxOffsets[partition]) || (offset == kcons.maxOffsets[partition]) {
			return nil, nil
		}
	}

	msg, ok := <-kcons.channels[partition]

	if ok {
		kcons.lastOffsets[partition] = msg.Offset
		return kcons.createKMessage(msg), nil
	}

	s := fmt.Sprintf("GetMessage encountered a problem Topic:%s:%d", kcons.Topic, partition)
	kcons.logger.Error(libname, s)
	return nil, errors.New(s)
}

// ---------------------------------------------------------------------------
func (kcons *KConsumer) runPartitionConsumer(partition int32) {

	if kcons.callbacks.OnData == nil {
		return
	}

	kcons.runWait.Add(1)
	kcons.startWait.Add(1)
	kcons.stopWait.Add(1)

	go func() {
		defer kcons.runWait.Done()

		kcons.logger.Info(libname, fmt.Sprintf("%s:%d going into run loop ", kcons.Topic, partition))

		kcons.Partitions[partition] = 1
		pInfo := PartitionInfo{Partition: partition}
		pInfo.StartOffset = kcons.initOffsets[partition]

		kcons.runCallbacks(&pInfo)

		kcons.logger.Info(libname, fmt.Sprintf("%s:%d exiting the run loop ", kcons.Topic, partition))
	}()
}
