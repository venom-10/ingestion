package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/parquet-go/parquet-go"
)

// Run against disposable services only: ARCHIVE_INTEGRATION=1 go test -run TestIntegration -v.
func TestIntegration(t *testing.T) {
	if os.Getenv("ARCHIVE_INTEGRATION") != "1" {
		t.Skip("set ARCHIVE_INTEGRATION=1 with PostgreSQL and S3 configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var out bytes.Buffer
	if err := run(ctx, []string{"migrate"}, &out); err != nil {
		t.Fatal(err)
	}
	db, err := ConnectDB(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// A unique test timestamp avoids reusing other test runs' manifests.
	timestamp := time.Now().Unix()
	rows := []Measurement{{2147483647, timestamp, float64(time.Now().UnixNano())}, {2147483647, timestamp, 23.456}}
	blocks := prepareBlocks(rows)
	t.Cleanup(func() {
		for _, b := range blocks {
			_, _ = db.Exec(context.Background(), `DELETE FROM archive_blocks WHERE object_key=$1`, b.key)
		}
	})
	var input bytes.Buffer
	for _, row := range rows {
		if err := json.NewEncoder(&input).Encode(row); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(t.TempDir(), "input.jsonl")
	if err := os.WriteFile(path, input.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		out.Reset()
		if err := run(ctx, []string{"ingest", "-input", path}, &out); err != nil {
			t.Fatal(err)
		}
	}
	client, err := newObjectClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range blocks {
		object, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(os.Getenv("S3_BUCKET")), Key: aws.String(b.key)})
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(object.Body)
		object.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := parquet.Read[Measurement](bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Fatal(err)
		}
		if len(decoded) != len(b.rows) {
			t.Fatal("uploaded Parquet row count mismatch")
		}
		for i := range decoded {
			if decoded[i] != b.rows[i] {
				t.Fatal("uploaded Parquet data mismatch")
			}
		}
		var count int
		if err := db.QueryRow(ctx, `SELECT count(*) FROM archive_blocks WHERE object_key=$1 AND row_count=2`, b.key).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("expected one manifest, got %d", count)
		}
	}
	out.Reset()
	if err := run(ctx, []string{"find", "-station", "2147483647", "-day", measurementDay(rows[0])}, &out); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(out.Bytes(), []byte(blocks[0].key)) {
		t.Fatal("lookup did not return ingested block")
	}
}
