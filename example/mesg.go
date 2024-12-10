package main

import (
	"fmt"
	"kafka-connector/message"
)

// ---------------------------------------------------------------------------
// main44
func main44() {
	jsnStr := "{\"data\":{\"trip_id\":154694,\"key\":\"67e45ae2-49f7-424f-bb2d-b9967e0757e0\",\"distance\":{\"value\":13.46,\"unit\":\"km\"},\"duration\":{\"value\":2179,\"unit\":\"seconds\"},\"service_ids\":[823],\"fare_details\":{\"service_823\":{\"is_upfront\":true,\"is_package\":false,\"ride_hour_enabled\":true,\"estimated_fare\":{\"text\":\"LKR 146.2\",\"value\":146.2,\"currency\":\"LKR\",\"fare_info\":{\"exists\":false,\"min_km\":13.46,\"min_fare\":151.2,\"above_km_fare\":0,\"free_waiting_time\":36.32,\"waiting_fare\":1.98,\"ride_hours\":0,\"extra_ride_fare\":0,\"seat_cap\":0,\"night_fare\":[{\"from\":\"00:00:00\",\"to\":\"00:00:00\",\"fare\":0}],\"surge_status\":false,\"surcharge\":{\"status\":false,\"value\":0,\"type\":\"\"},\"fare_breakdown\":{\"distance_fare\":140,\"duration_fare\":1.2,\"additional_fare\":5}}},\"price_file\":{\"distance_fare\":[{\"base_fare\":146.2,\"km_fare\":0,\"distance\":0},{\"base_fare\":0,\"km_fare\":0,\"distance\":13.46},{\"base_fare\":0,\"km_fare\":42,\"distance\":200000}],\"waiting_fare\":[{\"end_time\":0,\"fare\":0},{\"end_time\":2179,\"fare\":0.00055}],\"night_fare\":[{\"from\":\"00:00:00\",\"to\":\"00:00:00\",\"fare\":0}],\"additional_charge\":[{\"id\":0,\"name\":\"Booking fee\",\"amount\":0,\"type\":\"\"},{\"id\":27,\"name\":\"PASSENGER_INSURANCE\",\"amount\":5,\"type\":\"FLA\"},{\"id\":30,\"name\":\"SHARING_DISCOUNT\",\"amount\":-0,\"type\":\"FLA\"}],\"surcharge\":{\"status\":false,\"value\":0,\"type\":\"\"},\"fare_info\":{\"exists\":false,\"min_km\":13.46,\"min_fare\":146.2,\"above_km_fare\":0,\"free_waiting_time\":36.32,\"waiting_fare\":1.98,\"ride_hours\":0,\"extra_ride_fare\":0,\"seat_cap\":0,\"night_fare\":null,\"surge_status\":false,\"surcharge\":{\"status\":false,\"value\":0,\"type\":\"\"},\"fare_breakdown\":{\"distance_fare\":140,\"duration_fare\":1.2,\"additional_fare\":5}}}}}}}"

	mm, _ := message.FlattenJSONStr([]byte(jsnStr))

	dfare, _ := mm.GetFloat64("data.fare_details.service_823.estimated_fare.fare_info.fare_breakdown.distance_fare")

	fmt.Println(dfare)
}
