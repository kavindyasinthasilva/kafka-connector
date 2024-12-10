package main

import (
	"flag"
	"fmt"
	log "kafka-connector/log"
	"os"
	"sync"

	consumer "kafka-connector/consumer"
	kafka "kafka-connector/kafka"
	"kafka-connector/message"
)

// ---------------------------------------------------------------------------
// var count int64
var showIds *int
var outLock sync.Mutex

// ---------------------------------------------------------------------------
// main33
func main33() {

	reg := flag.String("schema", "", "schema registry ip:port")
	part := flag.Int("part", -1, "partition")
	offset := flag.Int64("offset", -2, "offset -1=newest, -2=oldest")
	bEnd := flag.Bool("end", false, "-end will stop consuming once latest offset is reached")
	enLog := flag.Bool("logs", false, "-logs")
	showIds = flag.Int("showIds", 0, "showIds 0=no IDs, 1=Show IDs, 2=Show only IDs")

	flag.Parse()

	broker := flag.Arg(0)
	topic := flag.Arg(1)

	var logger log.PrefixedLogger

	if *enLog {
		logger = log.Constructor.PrefixedLog()

	} else {
		logger = log.Constructor.PrefixedLog(log.WithLevel("")) // no logs are printed
	}

	if (broker == "") || (topic == "") {
		fmt.Println("Usage : kreader [options] <brokerIP:port> <topic>")
		flag.PrintDefaults()
		fmt.Println("Example : kreader -schema=schme_reg_url:port -offset=-1 broker:port mytopic")
		os.Exit(1)
	}

	conn, err := kafka.Init(broker, logger, "kreader", *reg) // dev

	if err != nil {
		fmt.Printf("Error %s\n", err.Error())
		os.Exit(1)
	}

	// os.Exit(1)

	vtype := consumer.FieldString

	var pa []int32

	if *part >= 0 {
		pa = []int32{int32(*part)}
	}

	ofa := []int64{*offset}

	if len(*reg) > 0 {
		vtype = consumer.FieldAvroString
		consumer.AutoLearnAvroIDs(consumer.AvroLearnAll)
	}

	var cb consumer.Callbacks
	cb.OnData = onData
	if *bEnd {
		cb.OnCatchup = onCatchup
	}
	cns, err := consumer.CreatePartitionConsumer(conn, topic, pa, ofa, vtype, cb)

	//cns, err := consumer.CreateGroupConsumer(conn, "kreader2", topic, *offset, true, vtype, onData, onReady, onStop)

	if err != nil {
		fmt.Printf("Error %s\n", err.Error())
		os.Exit(1)
	}

	//	err = consumer.SetAvroCodec("com.pickme.events.trip.TripCreated", 6, nil)

	//	http.Handle("/metrics", promhttp.Handler())

	//	go http.ListenAndServe(":8080", nil)

	cns.Run(false)

	fmt.Printf("Exiting Run()\n")
}

// onData will have all mesgs coming
// ---------------------------------------------------------------------------
func onData(kcons *consumer.KConsumer, mesg *message.KMessage) {

	if mesg.Error != nil {
		fmt.Printf("Error: %s\n", mesg.Error)
		return
	}

	outLock.Lock()

	if *showIds > 0 {
		ippid := "-"

		if mesg.Headers != nil {
			h := mesg.Headers[0]
			ippid = fmt.Sprintf("%s=%s", string(h.Key), string(h.Value))
		}

		fmt.Printf("showIds: part:%d offset:%d key:%s time:%s hdr:%s ID1:%s ID2:%d\n", mesg.Partition, mesg.Offset, string(mesg.Key),
			mesg.Timestamp.Format("2006-01-02 15:04:05.000"), ippid, mesg.ID, mesg.ID2)
	}

	if *showIds < 2 {
		if mesg.Val == nil {
			fmt.Println("NULL mesg")
		} else {
			fmt.Println(mesg.Val.(string))
		}
	}

	outLock.Unlock()
}

// ---------------------------------------------------------------------------
func onCatchup(kconsumer *consumer.KConsumer, partInfo []*consumer.PartitionInfo) {
	kconsumer.CloseConsumer()

	//	fmt.Println("Exiting on catchup")
}
