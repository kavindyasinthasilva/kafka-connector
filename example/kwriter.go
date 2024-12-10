package main

import (
	"fmt"
	kafka "kafka-connector/kafka"
	log "kafka-connector/log"
	"kafka-connector/producer"
	"os"
	"runtime"
	"strconv"
	"time"
)

// ---------------------------------------------------------------------------
var count int64

// ---------------------------------------------------------------------------
func main2() {

	logger := log.Constructor.PrefixedLog()

	// dev - 10.120.50.18:9092, stg - 10.20.4.14:9092, infr-auto - 10.148.0.18:9092
	//conn, err := kafka.Init([]string{"10.120.50.18:9092"}, logger, "") // dev
	//conn, err := kafka.Connect([]string{"10.148.0.18:9092"}) // partioner manual

	conn, err := kafka.Init("10.148.0.18:9092", logger, "kwriter", "") // infra-auto

	if err != nil {
		fmt.Printf("Error %s\n", err.Error())
		os.Exit(1)
	}

	// mqtt_session_subscribed

	//prd, err := producer.CreateSyncProducer(conn, "test-topic-2", producer.HashPartitioner)
	prd, err := producer.CreateAsyncProducer(conn, "test-topic-2", producer.HashPartitioner, onError, onSuccess)

	if err != nil {
		fmt.Printf("Error %s\n", err.Error())
		os.Exit(1)
	}

	go func() {
		for {
			x := count
			count = 0
			fmt.Printf("count=%d, go=%d\n", x, runtime.NumGoroutine())
			time.Sleep(1 * time.Second)
		}
	}()

	fmt.Println("Sending")

	for {
		mesg := fmt.Sprintf("message number %d", count)
		key := []byte(strconv.FormatInt(count, 10))
		bmesg := []byte(mesg)

		_, _, err := prd.Send(key, bmesg, 0)

		if err != nil {
			fmt.Printf("Error sending : %s\n", err.Error())
			break
		}

		// fmt.Printf("c=%d, p=%d, o=%d\n", count, p, off)
		count++
	}

}

func onError(kprod *producer.KProducer, err error) {
	fmt.Println(err)
}

func onSuccess(kprod *producer.KProducer, partition int32, offset int64, key interface{}) {
	fmt.Println(partition, offset)
}
