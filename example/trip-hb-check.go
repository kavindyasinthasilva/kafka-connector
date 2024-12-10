package main

import (
	"flag"
	"fmt"
	consumer "kafka-connector/consumer"
	kafka "kafka-connector/kafka"
	log "kafka-connector/log"
	"kafka-connector/message"
	"kafka-connector/producer"
	"os"
	"strconv"
	"sync"
)

// ---------------------------------------------------------------------------
type driver struct {
	DID       int64
	Trips     []*trip
	Partition int32
	dlock     sync.RWMutex
}

// ---------------------------------------------------------------------------
type trip struct {
	TID       int64
	driver    *driver
	Count     int
	Buckets   [7]int // >100, >50, >30, >20, >10, <=10
	Partition int32
	Offset    int64
}

var inMemPartitions map[int32]int

var connection *kafka.KConnection
var heartbeatConsumer, tripConsumer, recoveryConsumer *consumer.KConsumer
var recoveryProducer *producer.KProducer

var driversMap map[int64]*driver
var tripsMap map[int64]*trip
var driverMapLock sync.RWMutex
var tripMapLock sync.RWMutex

// TestTripHBcheck is also a sample app we can use to test some functionalities of consumers
// main22
// ---------------------------------------------------------------------------
func main() {

	logger := log.Constructor.PrefixedLog()

	driversMap = make(map[int64]*driver)
	tripsMap = make(map[int64]*trip)
	inMemPartitions = make(map[int32]int)

	flag.Parse()

	broker := flag.Arg(0)
	schema := flag.Arg(1)

	connection, err := kafka.Init(broker, logger, "trip_hb_group", schema)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	consumer.AutoLearnAvroIDs(consumer.AvroLearnAll)

	// DEBUG // []int32{0}
	var callbacks consumer.Callbacks
	callbacks.OnData = onHB
	callbacks.OnReady = onHBReady
	callbacks.OnAllStop = onHBStop

	heartbeatConsumer, err = consumer.CreateGroupConsumer(connection, "trip_hb_group1", "driver_locations", consumer.OffsetOldest, true,
		consumer.FieldAvroKV, callbacks)

	off := make([]int64, 0)

	off = append(off, 1616223)
	off = append(off, 1588602)
	off = append(off, 1561151)
	off = append(off, 1605494)
	off = append(off, 1125330)
	off = append(off, 932863)

	//	err = heartbeatConsumer.ResetOffsets(off)

	if err != nil {
		fmt.Println("ResetOffset Error", err)
		return
	}

	// recovery topic has same no of partitions as "driver_locations", this is the primary partition for this service
	// no callback, empty partition array
	recoveryConsumer, err = consumer.CreatePartitionConsumer(connection, "trip_hb_recovery", []int32{}, []int64{consumer.OffsetOldest},
		consumer.FieldStruct, consumer.Callbacks{})

	recoveryConsumer.RegisterStruct(&driver{})

	var cb consumer.Callbacks
	cb.OnData = onTrip
	tripConsumer, err = consumer.CreatePartitionConsumer(connection, "trip", []int32{}, []int64{consumer.OffsetNewest},
		consumer.FieldAvroKV, cb)

	recoveryProducer, err = producer.CreateSyncProducer(connection, "trip_hb_recovery", producer.HashPartitioner)

	// do not call Run() for recovery consumer
	go tripConsumer.Run(true)
	heartbeatConsumer.Run(false)
}

// here's what we do for recovery
// ---------------------------------------------------------------------------
func onHBReady(kcons *consumer.KConsumer, partInfo []*consumer.PartitionInfo) {

	// locking is not required here as there won't be any parallel processing

	var nextPart int32 // used to remove old non existing partitions

	pcount := tripConsumer.PartCount

	tripOffsets := make([]int64, pcount) // create an offset array to record the max

	for _, p := range partInfo { // loop through the assigned partitions via group consumer

		part := p.Partition

		// detect if we have dropped partition subscriptions in the new list
		for i := nextPart; i < part; i++ {
			if inMemPartitions[i] == 1 {

				// remove drivers attached to this old partition
				deletePart(part)
				inMemPartitions[i] = 0
			}
		}

		nextPart = part + 1

		if inMemPartitions[part] == 1 {
			continue // we already have this partition in memory, so no need to process recovery
		}

		inMemPartitions[part] = 1

		// lets read messages from recovery topic
		recoveryConsumer.AddPartition(part, consumer.OffsetOldest) // add the correct recovery partition

		for {
			mesg, _ := recoveryConsumer.GetMessage(part) // read the entire partition in a loop

			if mesg == nil {
				break
			}

			d := mesg.Val.(*driver) // this recovery topic will have driver{} objects
			driversMap[d.DID] = d   // add the object to my main map

			for _, t := range d.Trips {
				tripsMap[t.TID] = t // traverse through the trip slice and add it to trip topic as well

				if t.Offset > tripOffsets[t.Partition] {
					tripOffsets[t.Partition] = t.Offset
				}
			}
		}

		recoveryConsumer.RemovePartition(part)
	}

	for i := 0; i < pcount; i++ {
		if tripOffsets[i] > 0 {
			tripConsumer.AddPartition(int32(i), tripOffsets[i])
		} else {
			tripConsumer.AddPartition(int32(i), consumer.OffsetOldest)
		}

	}

}

// ---------------------------------------------------------------------------
func onHB(kcons *consumer.KConsumer, mesg *message.KMessage) {

	if mesg.Error != nil {
		fmt.Printf("OnHB error %s\n", mesg.Error)
		return
	}

	if mesg.ID != "com.pickme.events.driver.DriverLocationChanged" {
		return
	}

	//.body.driver_id, .body.location.accuracy,

	did, err := mesg.GetInt64("body.driver_id")
	if err != nil {
		fmt.Printf("OnHB getDID error %s\n", err)
		return
	}

	facc, err := mesg.GetFloat64("body.location.accuracy")
	if err != nil {
		fmt.Printf("OnHB getACC %s\n", err)
		return
	}

	lon, err := mesg.GetFloat64("body.location.lng")
	if err != nil {
		fmt.Printf("OnHB getLon %s\n", err)
		return
	}

	lat, err := mesg.GetFloat64("body.location.lat")
	if err != nil {
		fmt.Printf("OnHB getLat %s\n", err)
		return
	}

	acc := int(facc)

	driverMapLock.RLock()
	dr, ok := driversMap[did]
	driverMapLock.RUnlock()

	if !ok {
		dr = create(did)
	}

	dr.dlock.RLock()
	defer dr.dlock.RUnlock()
	dr.Partition = mesg.Partition

	// traverse all trips for this driver

	for _, tr := range dr.Trips {
		fmt.Printf("trip_%d %d:%d lat: %f lon: %f acc: %d\n", tr.TID, mesg.Partition, mesg.Offset, lat, lon, acc)
		tr.calc(acc)
	}
}

// ---------------------------------------------------------------------------
func onHBStop(kconsumer *consumer.KConsumer, partInfo []*consumer.PartitionInfo) {

	pcount := tripConsumer.PartCount

	fmt.Printf("OnHBStop called - stopping all trip paritions\n")

	for i := 0; i < pcount; i++ {
		fmt.Printf("Removing trip partition %d\n", i)
		tripConsumer.RemovePartition(int32(i))
	}

}

// ---------------------------------------------------------------------------
func onTrip(kcons *consumer.KConsumer, mesg *message.KMessage) {

	if mesg.Error != nil {
		fmt.Printf("OnTrip error %s\n", mesg.Error)
		return
	}

	tid, err := mesg.GetInt64("body.trip_id")
	if err != nil {
		//		fmt.Printf("OnTrip getTID error %s\n", err)
		return
	}

	// fmt.Println(mesg.ID)

	if mesg.ID == "com.pickme.events.trip.TripAccepted" {
		did, err := mesg.GetInt64("body.driver_id")
		if err != nil {
			fmt.Printf("OnTrip getDID error %s\n", err)
			return
		}

		driverMapLock.RLock()
		dr, ok := driversMap[did]
		driverMapLock.RUnlock()

		if !ok {
			dr = create(did)
		}

		tr := trip{TID: tid, driver: dr}

		tripMapLock.Lock()
		tripsMap[tid] = &tr
		tripMapLock.Unlock()

		dr.dlock.Lock()

		dr.Trips = append(dr.Trips, &tr)
		recoveryProducer.SendStruct([]byte(strconv.FormatInt(dr.DID, 10)), dr, dr.Partition)

		dr.dlock.Unlock()
		//	fmt.Printf("trip accepted %d : %d\n", tid, did)
		return

	} else if mesg.ID == "com.pickme.events.trip.TripCancelled" {
		remove(tid, false)
	} else if mesg.ID == "com.pickme.events.trip.TripRejectedByDriver" {
		remove(tid, false)
	} else if mesg.ID == "com.pickme.events.trip.TripEnded" {
		remove(tid, true)
	}
}

// ---------------------------------------------------------------------------
func create(did int64) *driver {

	dr := driver{DID: did}
	dr.Trips = make([]*trip, 0, 5)

	driverMapLock.Lock()
	driversMap[did] = &dr
	driverMapLock.Unlock()

	return &dr
}

// ---------------------------------------------------------------------------
func deletePart(part int32) {
	driverMapLock.Lock()
	defer driverMapLock.Unlock()

	temp := make(map[int64]*driver)

	for _, d := range driversMap {
		if d.Partition != part {
			temp[d.DID] = d
		}
	}

	driversMap = temp
}

// ---------------------------------------------------------------------------
func (tr *trip) calc(acc int) {
	tr.Count++

	if (acc > 100) || (acc == 0) { // acc = 0 no location lock
		tr.Buckets[5]++
	} else if acc > 50 {
		tr.Buckets[4]++
	} else if acc > 30 {
		tr.Buckets[3]++
	} else if acc > 20 {
		tr.Buckets[2]++
	} else if acc > 10 {
		tr.Buckets[1]++
	} else {
		tr.Buckets[0]++
	}
}

// ---------------------------------------------------------------------------
func remove(tid int64, print bool) {

	//	fmt.Printf("trip remove %d\n", tid)
	tripMapLock.Lock()
	tr, ok := tripsMap[tid]
	delete(tripsMap, tid)
	tripMapLock.Unlock()

	if !ok {
		return
	}

	dr := tr.driver

	if dr == nil {
		return
	}

	if print {
		fmt.Printf("Trip: %d, Driver: %6d, Samples: %3d, 0-10: %3d, 10-20: %3d, 20-30: %3d, 30-50: %3d, 50-100: %3d, >100: %3d\n",
			tid, dr.DID, tr.Count, tr.Buckets[0], tr.Buckets[1], tr.Buckets[2], tr.Buckets[3],
			tr.Buckets[4], tr.Buckets[5])
	}

	temp := make([]*trip, 0)

	for _, t := range dr.Trips {
		if t != tr {
			temp = append(temp, t)
		}
	}

	dr.Trips = temp
}
