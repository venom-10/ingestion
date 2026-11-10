package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		slog.Error("command failed", "error", err)
		os.Exit(1)
	}
}

// resolveRegion determines the AWS region to use.
// When a custom S3_ENDPOINT is configured (RustFS, MinIO, etc.),
// we don't default to us-east-1. Instead, we let the SDK resolve
// the region from the endpoint or use the explicitly configured region.
//
// This fixes the PermanentRedirect error (HTTP 301) that occurs when
// the client defaults to us-east-1 but the bucket exists in a different
// region. The S3 service responds with a redirect to the correct regional
// endpoint, but with a custom BaseEndpoint the SDK cannot follow it.
func resolveRegion() string {
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = os.Getenv("REGION")
	}
	// When using a custom endpoint, don't default to us-east-1.
	// Let the SDK resolve the region from the endpoint.
	if region == "" && os.Getenv("S3_ENDPOINT") == "" {
		region = "us-east-1"
	}
	return region
}

func run(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: archive-lab <ingest|migrate|find|generate|create-bucket> [flags]")
	}
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	input := fs.String("input", "-", "input file, or - for stdin")
	format := fs.String("format", "jsonl", "jsonl or csv")
	batch := fs.Int("batch-size", 10000, "maximum measurements buffered")
	station := fs.Int64("station", 1, "station ID")
	day := fs.String("day", "", "UTC date YYYY-MM-DD")
	count := fs.Int("count", 1000, "generated measurements")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	switch args[0] {
	case "ingest", "migrate", "find", "generate", "create-bucket":
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
	if args[0] == "generate" {
		if *station <= 0 || *station > 2147483647 || *count < 0 {
			return errors.New("invalid station or count")
		}
		enc := json.NewEncoder(out)
		start := time.Now().UTC().Unix()
		for i := 0; i < *count; i++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := enc.Encode(Measurement{int32(*station), start + int64(i), 25 + float64(i%30)/10}); err != nil {
				return err
			}
		}
		return nil
	}
	if args[0] == "ingest" && (*batch <= 0 || (*format != "jsonl" && *format != "csv")) {
		return errors.New("batch-size must be positive and format must be jsonl or csv")
	}
	if args[0] == "find" {
		if *station <= 0 || *station > 2147483647 {
			return errors.New("station must be a positive int32")
		}
		if _, err := time.Parse(time.DateOnly, *day); err != nil {
			return fmt.Errorf("invalid day: %w", err)
		}
	}
	if args[0] == "create-bucket" {
		bucket := os.Getenv("S3_BUCKET")
		if bucket == "" {
			return errors.New("S3_BUCKET is required")
		}
		client, err := newObjectClient(ctx)
		if err != nil {
			return err
		}
		_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
		return err
	}
	db, err := ConnectDB(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		return err
	}
	defer db.Close()
	if args[0] == "migrate" {
		_, err = db.Exec(ctx, schema)
		return err
	}
	if args[0] == "find" {
		keys, err := findBlocks(ctx, db, *station, *day)
		if err != nil {
			return err
		}
		return json.NewEncoder(out).Encode(keys)
	}
	bucket := os.Getenv("S3_BUCKET")
	if bucket == "" {
		return errors.New("S3_BUCKET is required")
	}
	client, err := newObjectClient(ctx)
	if err != nil {
		return err
	}
	var r io.Reader = os.Stdin
	if *input != "-" {
		f, err := os.Open(*input)
		if err != nil {
			return err
		}
		defer f.Close()
		r = f
	}
	sink := &archiveSink{db: db, client: client, bucket: bucket}
	stats, err := ingest(ctx, r, *format, *batch, sink)
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(stats)
}

func newObjectClient(ctx context.Context) (*s3.Client, error) {
	opts := []func(*awsconfig.LoadOptions) error{}
	// Legacy prototype credentials remain supported; prefer standard AWS variables.
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" && os.Getenv("KEY") != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(os.Getenv("KEY"), os.Getenv("SECRET"), "")))
	}
	region := resolveRegion()
	if region != "" {
		opts = append(opts, awsconfig.WithRegion(region))
	}
	opts = append(opts, awsconfig.WithRetryMaxAttempts(5))
	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		if endpoint := os.Getenv("S3_ENDPOINT"); endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		}
	}), nil
}