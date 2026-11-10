package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
)

type recordingSink struct {
	batches [][]Measurement
	err     error
}

func (s *recordingSink) Save(_ context.Context, rows []Measurement) (int, error) {
	if s.err != nil {
		return 0, s.err
	}
	s.batches = append(s.batches, append([]Measurement(nil), rows...))
	return len(prepareBlocks(rows)), nil
}

func TestIngest(t *testing.T) {
	for _, format := range []string{"jsonl", "csv"} {
		t.Run(format, func(t *testing.T) {
			input := "{\"station_id\":1,\"timestamp\":0,\"temperature\":25}\n\n{\"station_id\":2,\"timestamp\":86400,\"temperature\":26}\n{\"station_id\":1,\"timestamp\":1,\"temperature\":27}\n"
			if format == "csv" {
				input = "station_id,timestamp,temperature\n1,0,25\n2,86400,26\n1,1,27\n"
			}
			sink := &recordingSink{}
			stats, err := ingest(context.Background(), strings.NewReader(input), format, 2, sink)
			if err != nil {
				t.Fatal(err)
			}
			if stats.Rows != 3 || stats.Batches != 2 || len(sink.batches[0]) != 2 || len(sink.batches[1]) != 1 {
				t.Fatalf("stats=%+v batches=%v", stats, sink.batches)
			}
		})
	}
}

func TestInvalidInput(t *testing.T) {
	cases := []struct{ format, input string }{
		{"jsonl", `{"station_id":1,"timestamp":0}`},
		{"jsonl", `{"station_id":0,"timestamp":0,"temperature":25}`},
		{"jsonl", `{"station_id":1,"timestamp":-1,"temperature":25}`},
		{"jsonl", `{"station_id":1,"timestamp":0,"temperature":25,"extra":0}`},
		{"jsonl", `{"station_id":1,"timestamp":0,"temperature":25} {}`},
		{"jsonl", `null`},
		{"csv", "station_id,timestamp,temperature\n1,0,NaN\n"},
		{"csv", "station_id,timestamp,temperature\n1,0\n"},
		{"csv", "wrong,header\n"},
		{"csv", "station_id,timestamp,temperature\n2147483648,0,25\n"},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			sink := &recordingSink{}
			if _, err := ingest(context.Background(), strings.NewReader(tc.input), tc.format, 10, sink); err == nil {
				t.Fatal("expected error")
			}
			if len(sink.batches) != 0 {
				t.Fatal("invalid batch was saved")
			}
		})
	}
}

func TestSinkFailureAndCancellation(t *testing.T) {
	input := `{"station_id":1,"timestamp":0,"temperature":25}`
	failure := errors.New("storage unavailable")
	if _, err := ingest(context.Background(), strings.NewReader(input), "jsonl", 1, &recordingSink{err: failure}); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ingest(ctx, strings.NewReader(input), "jsonl", 1, &recordingSink{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestBlocksDeterministicAndPartitioned(t *testing.T) {
	rows := []Measurement{{1, 86400, 25}, {1, 1, 27}, {1, 0, 26}, {1, 0, 25}}
	blocks := prepareBlocks(rows)
	if len(blocks) != 2 {
		t.Fatalf("got %d partitions", len(blocks))
	}
	reversed := []Measurement{rows[3], rows[2], rows[1], rows[0]}
	again := prepareBlocks(reversed)
	for i, b := range blocks {
		if b.key != again[i].key {
			t.Fatal("keys depend on row order")
		}
		for _, row := range b.rows {
			if measurementDay(row) != b.day || makeGroup(int64(row.StationId)) != b.group {
				t.Fatal("incorrect partition")
			}
		}
		var buf bytes.Buffer
		w := parquet.NewGenericWriter[Measurement](&buf)
		if _, err := w.Write(b.rows); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		decoded, err := parquet.Read[Measurement](bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		if err != nil {
			t.Fatal(err)
		}
		if len(decoded) != len(b.rows) {
			t.Fatal("roundtrip row count")
		}
		for j := range decoded {
			if decoded[j] != b.rows[j] {
				t.Fatal("roundtrip mismatch")
			}
		}
	}
	changed := prepareBlocks([]Measurement{{1, 0, 24}})
	if changed[0].key == prepareBlocks([]Measurement{{1, 0, 25}})[0].key {
		t.Fatal("different readings must have different keys")
	}
}

func TestGenerateCommand(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), []string{"generate", "-count", "3"}, &out); err != nil {
		t.Fatal(err)
	}
	sink := &recordingSink{}
	stats, err := ingest(context.Background(), &out, "jsonl", 2, sink)
	if err != nil || stats.Rows != 3 {
		t.Fatalf("%+v %v", stats, err)
	}
}
