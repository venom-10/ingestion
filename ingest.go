package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"
)

type batchSink interface {
	Save(context.Context, []Measurement) (int, error)
}
type Stats struct {
	Rows    int64 `json:"rows"`
	Batches int64 `json:"batches"`
	Blocks  int64 `json:"new_blocks"`
}

func makeGroup(station int64) uint8 { return sha256.Sum256([]byte(strconv.FormatInt(station, 10)))[0] }

func validate(m Measurement) error {
	if m.StationId <= 0 {
		return errors.New("station_id must be positive")
	}
	if m.Timestamp < 0 || m.Timestamp > 253402300799 {
		return errors.New("timestamp must be Unix seconds between 1970 and 9999")
	}
	if math.IsNaN(m.Temperature) || math.IsInf(m.Temperature, 0) {
		return errors.New("temperature must be finite")
	}
	return nil
}

// ingest fails fast. Previously committed batches remain safe to replay.
func ingest(ctx context.Context, r io.Reader, format string, batchSize int, sink batchSink) (Stats, error) {
	var stats Stats
	if batchSize <= 0 {
		return stats, errors.New("batch size must be positive")
	}
	batch := make([]Measurement, 0, batchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		n, err := sink.Save(ctx, batch)
		if err != nil {
			return err
		}
		stats.Rows += int64(len(batch))
		stats.Batches++
		stats.Blocks += int64(n)
		batch = batch[:0]
		return nil
	}
	accept := func(m Measurement, line int) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := validate(m); err != nil {
			return fmt.Errorf("record %d: %w", line, err)
		}
		batch = append(batch, m)
		if len(batch) == batchSize {
			return flush()
		}
		return nil
	}
	switch format {
	case "jsonl":
		scanner := bufio.NewScanner(r)
		scanner.Buffer(make([]byte, 4096), 1024*1024)
		line := 0
		for scanner.Scan() {
			line++
			if strings.TrimSpace(scanner.Text()) == "" {
				continue
			}
			var row struct {
				Station     *int32   `json:"station_id"`
				Timestamp   *int64   `json:"timestamp"`
				Temperature *float64 `json:"temperature"`
			}
			dec := json.NewDecoder(strings.NewReader(scanner.Text()))
			dec.DisallowUnknownFields()
			if err := dec.Decode(&row); err != nil {
				return stats, fmt.Errorf("line %d: %w", line, err)
			}
			if err := dec.Decode(new(any)); err != io.EOF {
				return stats, fmt.Errorf("line %d: expected one JSON object", line)
			}
			if row.Station == nil || row.Timestamp == nil || row.Temperature == nil {
				return stats, fmt.Errorf("line %d: station_id, timestamp and temperature are required", line)
			}
			if err := accept(Measurement{*row.Station, *row.Timestamp, *row.Temperature}, line); err != nil {
				return stats, err
			}
		}
		if err := scanner.Err(); err != nil {
			return stats, err
		}
	case "csv":
		reader := csv.NewReader(r)
		header, err := reader.Read()
		if err != nil {
			return stats, fmt.Errorf("CSV header: %w", err)
		}
		if strings.Join(header, ",") != "station_id,timestamp,temperature" {
			return stats, errors.New("CSV header must be station_id,timestamp,temperature")
		}
		for line := 2; ; line++ {
			fields, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				return stats, fmt.Errorf("CSV record %d: %w", line, err)
			}
			station, e1 := strconv.ParseInt(fields[0], 10, 32)
			timestamp, e2 := strconv.ParseInt(fields[1], 10, 64)
			temperature, e3 := strconv.ParseFloat(fields[2], 64)
			if err := errors.Join(e1, e2, e3); err != nil {
				return stats, fmt.Errorf("CSV record %d: %w", line, err)
			}
			if err := accept(Measurement{int32(station), timestamp, temperature}, line); err != nil {
				return stats, err
			}
		}
	default:
		return stats, fmt.Errorf("unsupported format %q", format)
	}
	return stats, flush()
}

func measurementDay(m Measurement) string {
	return time.Unix(m.Timestamp, 0).UTC().Format(time.DateOnly)
}
