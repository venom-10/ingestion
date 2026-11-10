package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5/pgxpool"
	// "github.com/parquet-go/parquet-go"
)

func findBlocks(
	ctx context.Context,
	db *pgxpool.Pool,
	stationID int64,
	day string,
) ([]string, error) {
	groupID := makeGroup(stationID)

	rows, err := db.Query(ctx, `
        SELECT object_key
        FROM archive_blocks
        WHERE group_id = $1
          AND day = $2::date
    `, int16(groupID), day)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var keys []string

	for rows.Next() {
		var key string

		if err := rows.Scan(&key); err != nil {
			return nil, err
		}

		keys = append(keys, key)
	}

	return keys, rows.Err()
}

func saveManifest(
	ctx context.Context,
	db *pgxpool.Pool,
	groupID uint8,
	day string,
	objectKey string,
) error {

	query := `
        INSERT INTO archive_blocks
            (group_id, day, object_key)
        VALUES ($1, $2::date, $3)
        ON CONFLICT (object_key) DO NOTHING
    `

	_, err := db.Exec(
		ctx,
		query,
		int16(groupID),
		day,
		objectKey,
	)

	return err
}

func makeGroup(stationId int64) uint8 {
	hash := sha256.Sum256([]byte(strconv.FormatInt(int64(stationId), 10)))
	return hash[0]
}

func generateGroupData() map[uint8][]Measurement {
	groups := make(map[uint8][]Measurement)

	for stationId := 1; stationId <= 10000; stationId++ {
		readings := GenerateMeasurement(int32(stationId), 1000)
		groupId := makeGroup(int64(stationId))

		groups[groupId] = append(groups[groupId], readings...)
	}

	return groups
}

func sortGroupData(groups map[uint8][]Measurement) {
	for groupID, readings := range groups {
		sort.Slice(readings, func(i, j int) bool {
			if readings[i].StationId != readings[j].StationId {
				return readings[i].StationId < readings[j].StationId
			}

			return readings[i].Timestamp < readings[j].Timestamp
		})

		groups[groupID] = readings
	}

}

func uploadGroup(ctx context.Context, client *s3.Client, db *pgxpool.Pool) error {
	day := time.Now().UTC().Format("2006-01-02")
	batchID := time.Now().UTC().Format("20060102T150405.000000000")

	for group := 0; group < 256; group++ {
		filename := fmt.Sprintf("group-%03d.parquet", group)
		file, err := os.Open(filename)
		if err != nil {
			return err
		}

		info, err := file.Stat()
		if err != nil {
			return err
		}
		defer file.Close()

		key := fmt.Sprintf("group=%03d/day=%s/batch-%s.parquet", group, day, batchID)

		uploadCtx, cancel := context.WithTimeout(ctx, 30*time.Second)

		_, err = client.PutObject(uploadCtx, &s3.PutObjectInput{
			Bucket:        aws.String("sensor-archive"),
			Key:           aws.String(key),
			Body:          file,
			ContentLength: aws.Int64(info.Size()),
		})

		cancel()
		if err != nil {
			return fmt.Errorf("upload group %d: %w", group, err)
		}
		fmt.Println("uploaded")
		if err := saveManifest(
			ctx,
			db,
			uint8(group),
			day,
			key,
		); err != nil {
			return fmt.Errorf("save manifest: %w", err)
		}
	}
	return nil
}

func main() {
	region := os.Getenv("REGION")
	key := os.Getenv("KEY")
	secret := os.Getenv("SECRET")
	creds := credentials.NewStaticCredentialsProvider(key, secret, "")

	cfg := aws.Config{
		Region:      region,
		Credentials: creds,
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String("http://localhost:9000")
		o.UsePathStyle = true
	})

	ctx := context.Background()

	res, err := client.ListBuckets(ctx, &s3.ListBucketsInput{})

	if err != nil {
		log.Fatal(err)
	}

	for _, buck := range res.Buckets {
		fmt.Println("Bucket:", aws.ToString(buck.Name))
	}

	// measurements := GenerateMeasurement(101, 10000)
	//
	// err = parquet.WriteFile("measurements.parquet", measurements)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// file, err := os.Open("measurements.parquet")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// defer file.Close()
	//
	// out, err := client.PutObject(ctx, &s3.PutObjectInput{
	// 	Bucket: aws.String("sensor-archive"),
	// 	Key:    aws.String("measurements.parquet"),
	// 	Body:   file,
	// 	ACL:    "private",
	// })
	//
	// if err != nil {
	// 	log.Fatal(err)
	// }
	//
	// fmt.Println("output", out)

	dbpool := ConnectDB(ctx)

	// groups := generateGroupData()
	// sortGroupData(groups)
	//
	// fmt.Println("Number of groups:", len(groups))
	//
	// for groupID, readings := range groups {
	// 	fmt.Printf(
	// 		"Group %d: %d readings\n",
	// 		groupID,
	// 		len(readings),
	// 	)
	// }
	//
	// for groupId, readings := range groups {
	// 	filename := fmt.Sprintf("group-%03d.parquet", groupId)
	//
	// 	if err := parquet.WriteFile(filename, readings); err != nil {
	// 		log.Fatal(err)
	// 	}
	//
	// }
	//
	// uploadGroup(ctx, client, dbpool)
	fmt.Print(findBlocks(ctx, dbpool, 101, "2026-11-10"))
	
}
