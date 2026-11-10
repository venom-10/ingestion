package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
)

type objectStore interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}
type archiveSink struct {
	db     *pgxpool.Pool
	client objectStore
	bucket string
}
type partition struct {
	group uint8
	day   string
}
type block struct {
	partition
	key, checksum string
	rows          []Measurement
	min, max      int64
}

func prepareBlocks(rows []Measurement) []block {
	groups := map[partition][]Measurement{}
	for _, m := range rows {
		p := partition{makeGroup(int64(m.StationId)), measurementDay(m)}
		groups[p] = append(groups[p], m)
	}
	blocks := make([]block, 0, len(groups))
	for p, readings := range groups {
		sort.Slice(readings, func(i, j int) bool {
			a, b := readings[i], readings[j]
			if a.StationId != b.StationId {
				return a.StationId < b.StationId
			}
			if a.Timestamp != b.Timestamp {
				return a.Timestamp < b.Timestamp
			}
			return math.Float64bits(a.Temperature) < math.Float64bits(b.Temperature)
		})
		hash := sha256.New()
		var buf [20]byte
		min, max := readings[0].Timestamp, readings[0].Timestamp
		for _, m := range readings {
			binary.BigEndian.PutUint32(buf[:4], uint32(m.StationId))
			binary.BigEndian.PutUint64(buf[4:12], uint64(m.Timestamp))
			binary.BigEndian.PutUint64(buf[12:], math.Float64bits(m.Temperature))
			hash.Write(buf[:])
			if m.Timestamp < min {
				min = m.Timestamp
			}
			if m.Timestamp > max {
				max = m.Timestamp
			}
		}
		checksum := fmt.Sprintf("%x", hash.Sum(nil))
		blocks = append(blocks, block{p, fmt.Sprintf("v2/group=%03d/day=%s/%s.parquet", p.group, p.day, checksum), checksum, readings, min, max})
	}
	sort.Slice(blocks, func(i, j int) bool { return blocks[i].key < blocks[j].key })
	return blocks
}

// Objects are written before indexing. A crash may leave unindexed objects, but
// never publishes a manifest before upload succeeds. Replays reuse object keys.
func (s *archiveSink) Save(ctx context.Context, rows []Measurement) (int, error) {
	blocks := prepareBlocks(rows)
	pending := make([]block, 0, len(blocks))
	for _, b := range blocks {
		var exists bool
		if err := s.db.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM archive_blocks WHERE object_key=$1)`, b.key).Scan(&exists); err != nil {
			return 0, err
		}
		if exists {
			continue
		}
		var data bytes.Buffer
		writer := parquet.NewGenericWriter[Measurement](&data, parquet.Compression(&zstd.Codec{}))
		if _, err := writer.Write(b.rows); err != nil {
			return 0, err
		}
		if err := writer.Close(); err != nil {
			return 0, err
		}
		uploadCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		_, err := s.client.PutObject(uploadCtx, &s3.PutObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(b.key), Body: bytes.NewReader(data.Bytes()), ContentLength: aws.Int64(int64(data.Len())), ContentType: aws.String("application/vnd.apache.parquet")})
		cancel()
		if err != nil {
			return 0, fmt.Errorf("upload %s: %w", b.key, err)
		}
		pending = append(pending, b)
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(context.Background())
	inserted := 0
	for _, b := range pending {
		result, err := tx.Exec(ctx, `INSERT INTO archive_blocks (group_id,day,object_key,row_count,min_timestamp,max_timestamp,checksum) VALUES ($1,$2::date,$3,$4,$5,$6,$7) ON CONFLICT (object_key) DO NOTHING`, int16(b.group), b.day, b.key, len(b.rows), b.min, b.max, b.checksum)
		if err != nil {
			return 0, err
		}
		inserted += int(result.RowsAffected())
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	slog.Info("batch committed", "rows", len(rows), "partitions", len(blocks), "new_blocks", inserted)
	return inserted, nil
}
