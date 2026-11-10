package main

import (
	"math/rand/v2"
	"time"
)

type Measurement struct {
	StationId   int32   `parquet:"station_id" json:"station_id"`
	Timestamp   int64   `parquet:"timestamp" json:"timestamp"`
	Temperature float64 `parquet:"temperature" json:"temperature"`
}

func GenerateMeasurement(stationId int32, length int32) []Measurement {
	measurements := make([]Measurement, 0, length)

	baseTime := time.Now().Unix()

	for i := range length {
		measurement := Measurement{
			StationId:   stationId,
			Timestamp:   baseTime + int64(i),
			Temperature: float64(rand.Float64()*+3 + 25),
		}

		measurements = append(measurements, measurement)
	}

	return measurements
}
