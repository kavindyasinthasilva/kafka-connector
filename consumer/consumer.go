package consumer

// ---------------------------------------------------------------------------
import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	log "github.com/kavindyasinthasilva/kafka-connector/log"
	"reflect"
	"sort"
	"sync"

	sarama "github.com/Shopify/sarama"
	avro "github.com/kavindyasinthasilva/kafka-connector/avro"
	kafka "github.com/kavindyasinthasilva/kafka-connector/kafka"
	message "github.com/kavindyasinthasilva/kafka-connector/message"
	"github.com/prometheus/client_golang/prometheus"
)

// KConsumer represent the Consumer obect of a kafka
// KStatus : 0=initial, 1=running
// ---------------------------------------------------------------------------
type KConsumer struct {
	Topic               string                     // topic name
	Partitions          []int32                    // partitions array, =1 if subscribed, else=0
	PartCount           int                        // total how many partitions are available
	CType               int                        // 0=PartitionConsumer, 1=GroupConsumer
	Status              int                        // 0=initial, 1=running, 2=OnReadyFIred, 3=OnCatchupFired
	Connection          *kafka.KConnection         // connection structure
	sConsumer           sarama.Consumer            // Sarama partition consumer object
	sPartitionConsumers []sarama.PartitionConsumer // Array of Sarama PartitionConsumers (one per partition)
	sGroupConsumer      sarama.ConsumerGroup       // Sarama consumer group object
	callbacks           Callbacks
	channels            []<-chan *sarama.ConsumerMessage // channels for data partitions
	errorChannel        <-chan error                     // error channel
	runWait             sync.WaitGroup                   // wait in Run() for partition consumer
	startWait           sync.WaitGroup                   // wait before for all threads to finish onStart() work in group consumers
	stopWait            sync.WaitGroup                   // wait before all threads to finish onStop()
	kSession            *kGPSession                      // kSession Object
	autoCommit          bool                             // autoCommit for GroupConsumer
	doNotExit           bool                             // Run() loop will not exist when this is set to true
	onCatchup           bool                             // have we called onCatchup()
	initOffsets         []int64                          // offset per partition (keep initial offset and change to running for GetMesg)
	lastOffsets         []int64                          // last consumed offset
	maxOffsets          []int64                          // max Offsets at startup. used for GetMesg() and onCatchup
	partInfo            []*PartitionInfo                 // partition information to be fired with OnReady()
	valType             int                              // Type of Value
	recoveryStruct      interface{}                      // struct set for recovery topics
	UserData            interface{}                      // apps may set this to associate KConsumer to it's reference.
	logger              log.PrefixedLogger               // logger for the consumer
	metricMesgCount     prometheus.Counter               // metric Counter for totoal received messages
	metricPartGauge     prometheus.Gauge                 // metric to track number of subscribed partitions
	objLock             sync.Mutex                       // lock for updating shared variables in this object (eg PartitionInfo)
	readyLock           sync.Mutex                       // lock for calling readyCallback()
	stopLock            sync.Mutex                       // lock for calling stopCallback()
}

// KGPSession is a temporary structure used for callbacks.
// ---------------------------------------------------------------------------
type kGPSession struct {
	KConsumer     *KConsumer
	SGroupSession sarama.ConsumerGroupSession
}

// PartitionInfo is used to fire OnReady() callback
// ---------------------------------------------------------------------------
type PartitionInfo struct {
	Partition    int32 // partition number
	OldestOffset int64 // oldest offset in topic
	StartOffset  int64 // starting offset number
	NewestOffset int64 // newest offset in topic
	doneCatchup  bool  // have we caughtup
}

// Callbacks is used to specify all callback functions to Groups consumers
// ---------------------------------------------------------------------------
type Callbacks struct {
	OnData    OnDataFunc    // fire with data messages (per partition)
	OnSetup   OnSetupFunc   // only when starting/rebalancing group consumer
	OnStart   OnStartFunc   // only when starting/rebalancing ( per partition)
	OnReady   OnReadyFunc   // only after returning all onStart() callback functions (once per rebalance)
	OnCatchup OnCatchupFunc // only after all partitions catchup to current offset (once per rebalance)
	OnStop    OnStopFunc    // only when stopping/rebalancing a group consumer (per partition)
	OnAllStop OnAllStopFunc // only after returning all onStop() callback functions (once per rebalance)
}

var consumers []*KConsumer // this is used to maintain list of consumers
var kconsList sync.Mutex   // this is used to lock before updating the consumers list

// ---------------------------------------------------------------------------
const (
	// OffsetNewest will get messages from latest offset
	OffsetNewest = sarama.OffsetNewest
	// OffsetOldest will get messages from oldest available offset
	OffsetOldest = sarama.OffsetOldest
	// OffsetAlwaysNew will get messages from newest (for GroupConsumer)
	OffsetAlwaysNew = -99
)

const (
	// PartitionConsumer type of KConsumer
	PartitionConsumer = 0
	// GroupConsumer type of KConsumer
	GroupConsumer = 1
)

const (
	// FieldBytes - KMessage value is []byte
	FieldBytes = 0

	// FieldInt64 - KMessage value is int64
	FieldInt64 = 1

	// FieldString - KMessage value is string
	FieldString = 2

	// FieldAvro - KMessage value is Avro decoded and unmarshelled to a struct if struct is provided via SetAvroCodec()
	// if struct is not provided it will be similar to KV
	FieldAvro = 3

	// FieldAvroKV - KMessage value is Avro decoded Key Value type. use KMessage.Get... methods
	FieldAvroKV = 4

	// FieldAvroString - KMessage value is Avro decoded json string
	FieldAvroString = 5

	// FieldStruct - KMessage value is a struct registered via RegisterStruct()
	FieldStruct = 6
)

// Auto learn Avro schemas
const (
	AvroLearnNone     = 0
	AvroLearnVersions = 1
	AvroLearnAll      = 2
)

// Status
const (
	StatusStopped  = 0
	StatusRunning  = 1
	StatusRunning2 = 2 // set for group consumers after calling OnReady()
)

const (
	libname = "kafka-connector:consumer"
)

// OnDataFunc is the callback for the application to get the messages delivered.
// It will deliver each mesg as a decoded string based on how it was sent.
// multiple threads may call this concurrently
// ---------------------------------------------------------------------------
type OnDataFunc func(kconsumer *KConsumer, message *message.KMessage)

// OnSetupFunc is called when kafka assign partitions to the ConsumerGroup.
// This will be called after calling Run() and later when partition re-balancing happens
// Applications can use this to reset offsets of a group consumer
// ---------------------------------------------------------------------------
type OnSetupFunc func(kconsumer *KConsumer, partitions []int32)

// OnStartFunc is called after calling Run() and later when partition re-balancing happens
// Applications may use this to initiate other recovery related work
// This will be called concurrently for all assigned partitions
// ---------------------------------------------------------------------------
type OnStartFunc func(kconsumer *KConsumer, partInfo *PartitionInfo)

// OnReadyFunc is called once, after calling OnStartFunc for all subscribed partitions and once all OnStartFuncs are returned.
// App can use this call to clean unncessary structures, subscribe to other topics etc.
// ---------------------------------------------------------------------------
type OnReadyFunc func(kconsumer *KConsumer, partInfo []*PartitionInfo)

// OnCatchupFunc is called once after all subscribed partitions catchup to the current offset.
// App can use this call to clean unncessary structures, subscribe to other topics etc.
// ---------------------------------------------------------------------------
type OnCatchupFunc func(kconsumer *KConsumer, partInfo []*PartitionInfo)

// OnStopFunc is called when kafka ends incoming messages for the partition.
// This will be call at application exit and partition re-balancing
// ---------------------------------------------------------------------------
type OnStopFunc func(kconsumer *KConsumer, partition int32)

// OnAllStopFunc is called after firing all OnStopFunc()s
// This will be call at application exit and partition re-balancing
// ---------------------------------------------------------------------------
type OnAllStopFunc func(kconsumer *KConsumer, partInfo []*PartitionInfo)

func init() {
	consumers = make([]*KConsumer, 0)
}

// CloseConsumer will close all consumers
// ---------------------------------------------------------------------------
func (kcons *KConsumer) CloseConsumer() error {
	// remove kConsumer obj from metric list
	kconsList.Lock()

	arr := make([]*KConsumer, 0)
	for _, k := range consumers {
		if k != kcons {
			arr = append(arr, k)
		}
	}

	consumers = arr
	kconsList.Unlock()

	kcons.logger.Info(libname, fmt.Sprintf("Closing Consumer Topic %s ", kcons.Topic))

	if kcons.CType == PartitionConsumer {

		if kcons.doNotExit {
			kcons.runWait.Done()
		}

		for i := 0; i < kcons.PartCount; i++ {
			if kcons.Partitions[i] == 1 {
				kcons.sPartitionConsumers[i].Close()
			}
		}

		return kcons.sConsumer.Close()
	}

	kcons.Status = 0

	return kcons.sGroupConsumer.Close()
}

// Run will start consuming data and will start firing the callback function
// NOTE than this function never return till all partitions are Removed from KConsumer
// In addition if doNotExit is set, Run loop will continue to run even after all partitions are closed except in CloseConsumer()
// Old behaviuor is false.
// ---------------------------------------------------------------------------
func (kcons *KConsumer) Run(doNotExit bool) error {
	var err error

	// LOG
	if kcons.Status > 0 {
		kcons.logger.Error(libname, fmt.Sprintf("Already in Run() Topic:%s", kcons.Topic))
		return fmt.Errorf("%s - Already running", kcons.Topic)
	}

	kcons.Status = 1 // status started

	kcons.logger.Info(libname, fmt.Sprintf("Topic : %s going into Run() loop", kcons.Topic))

	if kcons.CType == PartitionConsumer {

		if doNotExit {
			kcons.doNotExit = true
			kcons.runWait.Add(1) // add a dummy wait so Wait() below will never exist.
		}

		for i := 0; i < kcons.PartCount; i++ {
			if kcons.Partitions[i] == 1 {
				kcons.runPartitionConsumer(int32(i))
			}
		}

		kcons.runWait.Wait()
	} else {
		err = kcons.runGroupConsumer()
	}

	kcons.logger.Warn(libname, fmt.Sprintf("Exiting Run() Topic:%s", kcons.Topic))
	kcons.Status = 0 // status not running

	return err
}

// this function handles all on*Callback functions for consumers
// ---------------------------------------------------------------------------
func (kcons *KConsumer) runCallbacks(pInfo *PartitionInfo) {

	partition := pInfo.Partition
	name := "PartitionConsumer"

	if kcons.CType == GroupConsumer {
		name = "GroupConsumer"
	}

	pInfo.NewestOffset, _ = kcons.Connection.SClient.GetOffset(kcons.Topic, partition, sarama.OffsetNewest)
	pInfo.OldestOffset, _ = kcons.Connection.SClient.GetOffset(kcons.Topic, partition, sarama.OffsetOldest)

	kcons.logger.Info(libname, fmt.Sprintf("%s starting run loop - %s:%d - start:%d, newest:%d", name,
		kcons.Topic, partition, pInfo.StartOffset, pInfo.NewestOffset))

	// starting at newest(-1) or starting at newest(number) or oldest=newest
	if (pInfo.StartOffset == OffsetNewest) || (pInfo.StartOffset+1 >= pInfo.NewestOffset) ||
		(pInfo.OldestOffset == pInfo.NewestOffset) {
		kcons.logger.Info(libname, fmt.Sprintf("%s Catchup done at start - %s:%d", name, kcons.Topic, partition))
		pInfo.doneCatchup = true
	}

	kcons.objLock.Lock()
	kcons.partInfo = append(kcons.partInfo, pInfo)
	kcons.objLock.Unlock()

	// -------------------------------------------------------------------------------------------------- ON START
	if kcons.callbacks.OnStart != nil {
		kcons.logger.Info(libname, fmt.Sprintf("%s calling StartCallback - %s:%d", name, kcons.Topic, partition))
		kcons.callbacks.OnStart(kcons, pInfo)
		kcons.logger.Info(libname, fmt.Sprintf("%s StartCallback returned - %s:%d", name, kcons.Topic, partition))
	} else {
		kcons.logger.Info(libname, fmt.Sprintf("%s StartCallback not defined to call - %s:%d", name, kcons.Topic, partition))
	}

	kcons.startWait.Done() // done appending partition info
	kcons.readyLock.Lock() // try to call readyCallback

	// -------------------------------------------------------------------------------------------------- ON READY
	if kcons.Status == 1 { // no one has called readyCallback
		kcons.startWait.Wait() // wait everyone to finish startCallback()

		// no need to seperately lock as this area is safe
		sort.Slice(kcons.partInfo, func(i, j int) bool { return kcons.partInfo[i].Partition < kcons.partInfo[j].Partition })

		if kcons.callbacks.OnReady != nil {
			kcons.logger.Info(libname, fmt.Sprintf("%s calling OnReadyCallback - %s", name, kcons.Topic))
			kcons.callbacks.OnReady(kcons, kcons.partInfo)
			kcons.logger.Info(libname, fmt.Sprintf("%s OnReadyCallback returned - %s", name, kcons.Topic))
		} else {
			kcons.logger.Info(libname, fmt.Sprintf("%s OnReadyCallback not defined to call - %s", name, kcons.Topic))
		}

		kcons.Status = 2 // makesure other's will not call readyCallback()

		kcons.fireOnCatchup(name) // check if we can fire after OnReady()
	}

	kcons.readyLock.Unlock() // let other's continue

	autocommit := kcons.autoCommit

	if kcons.Partitions[partition] > 0 {

		for msg := range kcons.channels[partition] {
			// -------------------------------------------------------------------------------------------------- ON DATA
			kcons.callbacks.OnData(kcons, kcons.createKMessage(msg))
			kcons.lastOffsets[msg.Partition] = msg.Offset

			if autocommit && (kcons.CType == GroupConsumer) {
				kcons.kSession.SGroupSession.MarkMessage(msg, "")
			}

			// -------------------------------------------------------------------------------------------------- ON CATCHUP
			if (pInfo.doneCatchup == false) && (msg.Offset+1 >= pInfo.NewestOffset) {

				kcons.objLock.Lock()

				pInfo.doneCatchup = true
				kcons.logger.Info(libname, fmt.Sprintf("%s Catchup done in OnData - %s:%d", name, kcons.Topic, partition))

				kcons.fireOnCatchup(name)

				kcons.objLock.Unlock()
			}
		}

		kcons.logger.Info(libname, fmt.Sprintf("%s OnData loop terminated - %s:%d", name, kcons.Topic, partition))
	} else {
		kcons.logger.Info(libname, fmt.Sprintf("%s skipping OnData loop as partition is not active - %s:%d", name, kcons.Topic,
			partition))
	}

	// -------------------------------------------------------------------------------------------------- ON STOP
	if kcons.callbacks.OnStop != nil {
		kcons.logger.Info(libname, fmt.Sprintf("%s calling StopCallback - %s:%d", name, kcons.Topic, partition))
		kcons.callbacks.OnStop(kcons, partition)
		kcons.logger.Info(libname, fmt.Sprintf("%s StopCallback returned - %s:%d", name, kcons.Topic, partition))
	} else {
		kcons.logger.Info(libname, fmt.Sprintf("%s StopCallback not defined to call - %s:%d", name, kcons.Topic, partition))
	}

	// -------------------------------------------------------------------------------------------------- ON ALL STOP
	kcons.stopWait.Done() // I'm done with StopCallback
	kcons.stopLock.Lock()

	if kcons.Status > 1 {
		kcons.stopWait.Wait() // Wait for other to finish stopCallback

		if kcons.callbacks.OnAllStop != nil {
			kcons.logger.Info(libname, fmt.Sprintf("%s calling OnAllStopCallback - %s", name, kcons.Topic))
			kcons.callbacks.OnAllStop(kcons, kcons.partInfo)
			kcons.logger.Info(libname, fmt.Sprintf("%s OnAllStopCallback returned - %s", name, kcons.Topic))
		} else {
			kcons.logger.Info(libname, fmt.Sprintf("%s OnAllStopCallback not defined to call - %s", name, kcons.Topic))
		}

		kcons.Status = 1     // no one will now call onAllStop()
		kcons.partInfo = nil // no one should be accessing partInfo after onAllStop()
	}

	kcons.stopLock.Unlock()

	kcons.Partitions[partition] = 0
}

// this function checks the possibility of fireing on catchup
// ---------------------------------------------------------------------------
func (kcons *KConsumer) fireOnCatchup(name string) {

	for i := range kcons.partInfo {
		if kcons.partInfo[i].doneCatchup == false { // check if all have caughtup
			kcons.logger.Info(libname, fmt.Sprintf("%s waiting for - %s:%d to catchup", name, kcons.Topic,
				kcons.partInfo[i].Partition))
			return
		}
	}

	if kcons.callbacks.OnCatchup != nil {
		kcons.logger.Info(libname, fmt.Sprintf("%s calling OnCatchupCallback - %s", name, kcons.Topic))
		kcons.callbacks.OnCatchup(kcons, kcons.partInfo)
		kcons.logger.Info(libname, fmt.Sprintf("%s OnCatchupCallback returned - %s", name, kcons.Topic))
	} else {
		kcons.logger.Info(libname, fmt.Sprintf("%s OnCatchupCallback not defined to call - %s", name, kcons.Topic))
	}
}

// create KMessage from sarama.ConsumerMessage
// ---------------------------------------------------------------------------
func (kcons *KConsumer) createKMessage(msg *sarama.ConsumerMessage) *message.KMessage {
	var m message.KMessage
	var err error
	var kv []message.KeyVal

	m.Timestamp = msg.Timestamp
	m.Offset = msg.Offset
	m.Partition = msg.Partition
	m.Key = msg.Key
	m.Type = kcons.valType

	for _, h := range msg.Headers {
		kv = append(kv, message.KeyVal(*h))
	}

	m.Headers = kv

	kcons.metricMesgCount.Inc()

	if msg.Value == nil {
		m.ID = "NULL"
		return &m
	}

	switch kcons.valType {
	case FieldInt64:
		k := uint64(0)
		binary.BigEndian.PutUint64(msg.Value, k)
		m.Val = int64(k)
	case FieldString:
		m.Val = string(msg.Value)
	case FieldStruct:
		if kcons.recoveryStruct == nil {
			m.Error = errors.New("need to provide struct using RegisterStruct()")
			return &m
		}

		strct := reflect.New(reflect.ValueOf(kcons.recoveryStruct).Elem().Type()).Interface()
		buf := bytes.NewBuffer(msg.Value)
		dec := gob.NewDecoder(buf)
		err = dec.Decode(strct)
		m.Val = strct
	case FieldAvro, FieldAvroKV, FieldAvroString:
		vtype := 0
		m.Val, m.ID, m.ID2, vtype, err = avro.Decode(msg.Value, kcons.valType)

		if vtype >= 0 {
			m.Type = vtype
		}

		m.Error = err
	default:
		m.Val = msg.Value
	}

	return &m
}

// RegisterStruct - use this to register struct to decode "FieldStruct" type messages.
// Should be called with reference to a struct eg: RegisterStruct(&myStrct{})
// Caller can register only one struct per topic (ideally only one recovery topic should be available for an app)
// Must use the same type of struct as used for producer.SendStruct()
// Use GetMessage() to read messages in a loop - type cast : msg.Val.(*myStruct)
// ---------------------------------------------------------------------------
func (kcons *KConsumer) RegisterStruct(strct interface{}) error {

	if strct == nil {
		return errors.New("strct cannot be nil")
	}

	if reflect.TypeOf(strct).Kind() != reflect.Ptr {
		return errors.New("Need a reference to a structure")
	}

	kcons.recoveryStruct = strct
	kcons.valType = FieldStruct

	return nil
}

// GetNewestOffset will return the currently newest offset for the parition
// ---------------------------------------------------------------------------
func (kcons *KConsumer) GetNewestOffset(partition int32) int64 {

	f, _ := kcons.Connection.SClient.GetOffset(kcons.Topic, partition, sarama.OffsetNewest)
	return f
}

// setupMetrics will setup the metrics for the consumer
// ---------------------------------------------------------------------------
func (kcons *KConsumer) setupMetrics() {

	ss := "kafka_connector_consumer"
	labels := map[string]string{"topic": kcons.Topic}

	kcons.metricMesgCount = prometheus.NewCounter(prometheus.CounterOpts{Namespace: kafka.MetricName,
		Subsystem: ss, Name: "message_count", ConstLabels: labels})

	err := prometheus.Register(kcons.metricMesgCount)

	if err != nil {
		kcons.logger.Error(libname, fmt.Sprintf("MericMesgCount registration failed Topic:%s - %s", kcons.Topic, err.Error()))
	}

	kcons.metricPartGauge = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: kafka.MetricName,
		Subsystem: ss, Name: "subscribed_partitions", ConstLabels: labels})

	err = prometheus.Register(kcons.metricPartGauge)

	if err != nil {
		kcons.logger.Error(libname, fmt.Sprintf("MericPartCount registration failed Topic:%s - %s", kcons.Topic, err.Error()))
	}

	kcons.metricPartGauge.Set(0)
}

// AutoLearnAvroIDs will auto learn avro IDs and return a JSON string in KMessage.Value
// ---------------------------------------------------------------------------
func AutoLearnAvroIDs(level int) {
	avro.AutoLearn(level)
}

// SetAvroCodec will set the value type to avro and start decoding accordingly
// Call Function with pointer eg:
//
//	consumer.SetAvroCodec("com.pickme.events.trip.TripCreated", 6, &TripCreatedEvent{})
//
// In the data callback, application may check the KMessage.ID() and type cast with  corresponding struct pointer eg:
//
//	tripCreateEvent, ok := kmesg.Val.(*TripCreatedEvent)
//
// ---------------------------------------------------------------------------
func SetAvroCodec(subject string, version int, strct interface{}) error {
	var err error

	if !avro.IsInitialized() {
		return errors.New("avro package not initialized")
	}

	err = avro.SetCodec(subject, version, strct)

	return err
}

// ShutdownConsumers will gracefully stop all consumers.
// should be used when app want to gracefully shutdown
// ---------------------------------------------------------------------------
func ShutdownConsumers() {

	fmt.Println("Shutting down consumers")

	kconsList.Lock()
	temp := consumers
	kconsList.Unlock()

	for _, kcons := range temp {
		kcons.CloseConsumer()
	}
}
