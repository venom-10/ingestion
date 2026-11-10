# archive-lab

A Go ingestion CLI for sensor measurements: stream CSV/JSONL → validate → batch → partition by station hash and UTC day → sort → Zstandard-compressed Parquet → S3/RustFS → PostgreSQL manifest.

The original prototype is preserved at Git tag **`v1`**. Generated Parquet, binaries, and `.env` credentials are deliberately not committed.

## Local quick start

Requires Go 1.26.10 (see `go.mod`) and Docker Compose. The sample credentials below are for local development only. If you already have PostgreSQL/RustFS on ports 5433/9000/9001, use your existing services and credentials instead of starting another stack. The Compose file uses RustFS, not MinIO; pin its image digest for production.

```sh
docker compose up -d --wait
# Allow RustFS to finish startup before creating the bucket.
cp .env.example .env
# The CLI does not automatically load .env. Export it in your shell:
set -a; . ./.env; set +a

go build -o archive-lab .
./archive-lab migrate
./archive-lab create-bucket  # once, or create sensor-archive in the RustFS console
./archive-lab generate -station 101 -count 10000 > /tmp/readings.jsonl
./archive-lab ingest -input /tmp/readings.jsonl -batch-size 10000
./archive-lab find -station 101 -day "$(date -u +%F)"
```

`find` returns candidate object keys, not individual measurements: multiple stations share each hash group. Download Parquet objects and filter on `station_id` and `timestamp` for exact results. `find` also works with prototype manifest records.

## Input

JSONL (one object per line; blank lines ignored):

```json
{"station_id":101,"timestamp":1794268800,"temperature":25.4}
```

CSV requires this exact header and column order:

```csv
station_id,timestamp,temperature
101,1794268800,25.4
102,1794268860,26.1
```

```sh
./archive-lab ingest -format csv -input readings.csv -batch-size 10000
./archive-lab generate -count 1000 | ./archive-lab ingest
```

- `station_id`: positive signed 32-bit integer.
- `timestamp`: Unix **seconds**, UTC, between 1970-01-01 and 9999-12-31. Partition day comes from each reading, never ingestion time.
- `temperature`: required finite float64, including zero; no assumed physical range.
- Unknown JSON fields, missing/null values, trailing JSON, invalid numbers and malformed CSV fail ingestion. JSONL lines are limited to 1 MiB.
- Invalid input fails fast with record context. Earlier committed batches remain available; the incomplete batch is not committed. Fix the source and replay as described below.

## Storage and recovery

Station groups retain the prototype's algorithm: first byte of SHA-256(decimal station ID), producing 256 groups. Each input batch is split into `(group, UTC day)` partitions, sorted by station/time/temperature, then archived as:

```text
v2/group=042/day=2026-11-10/<sha256-of-sorted-logical-rows>.parquet
```

The manifest stores row count, timestamp bounds and logical checksum. These checksums identify canonical measurement values, **not Parquet file bytes**. Compression or writer metadata does not affect identity. The migration is explicit and idempotent; run `migrate` before ingestion. It extends compatible prototype tables without deleting data.

### Delivery semantics

- Manifests act as block-level checkpoints. Replay the **same file with the same batch size** after failure: committed blocks are skipped, unfinished blocks reuse deterministic S3 keys. No local offset checkpoint is needed, but replay scans the source from the beginning.
- S3 uploads use the AWS SDK's standard retry behavior (up to five attempts), seekable bodies, and a two-minute deadline per object. PostgreSQL failures stop the command; replay after restoring the database.
- All new manifests for one batch commit in a single database transaction **after** all corresponding uploads succeed. Readers never see a manifest published before a successful upload.
- A crash after upload but before commit can leave unindexed objects. Replay repairs indexing; there is no automated orphan garbage collection.
- Concurrent identical batches converge on the same keys; manifest uniqueness prevents duplicate index entries.
- This is **block-level replay safety, not global row-level deduplication**. Changed batch boundaries, overlapping input files, or changed rows can create overlapping blocks. Repeated rows inside a block are preserved. Keep batch size/source stable on retries; upstream event IDs and a deduplication stage are needed for arbitrary exactly-once event ingestion.
- The pipeline holds at most one configured batch of measurements plus its partition copies and one encoded block, rather than the entire input. Processing/upload is sequential to bound resource use. Batch size bounds record count, not raw CSV field bytes.
- SIGINT/SIGTERM cancels database and upload work. An interactive/blocking stdin read may not exit until the reader returns.

## Configuration

| Variable | Purpose |
| --- | --- |
| `DATABASE_URL` | Required PostgreSQL URL (use TLS outside local development) |
| `S3_BUCKET` | Required archive bucket; `ingest` requires it to exist. `create-bucket` creates it explicitly (RustFS/us-east-1 setup) |
| `S3_ENDPOINT` | Optional custom S3 endpoint; enables path-style addressing for RustFS/MinIO |
| `AWS_REGION` | AWS region; defaults to `us-east-1` when no custom endpoint is configured |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` | Standard SDK credentials; IAM roles/profiles/default chain also supported |

**Important**: When using a custom `S3_ENDPOINT` (RustFS, MinIO, or other S3-compatible storage), **do not rely on the `us-east-1` default**. Either:
- Set `AWS_REGION` to match your bucket's actual region, or
- Leave `AWS_REGION` unset to let the SDK auto-resolve the region from the endpoint

The default `us-east-1` only applies when using AWS S3 directly (no custom endpoint). Using the wrong region with a custom endpoint causes `PermanentRedirect` (HTTP 301) errors because the S3 service redirects to the correct regional endpoint, but the SDK cannot follow the redirect when `BaseEndpoint` is explicitly set.

Prototype `REGION`, `KEY`, `SECRET` remain fallback aliases. Do not commit production credentials. Use least-privilege S3 write access scoped to the archive prefix and database permissions appropriate to the command. Bucket encryption, retention, access policy, backups and lifecycle are deployment responsibilities. Logs go to stderr; successful ingestion emits JSON row/batch/new-block counters on stdout. Row counts include replayed rows; `new_blocks` counts newly indexed blocks only.

## Commands

| Command | Description |
| --- | --- |
| `ingest` | Stream, validate, partition, and upload measurements |
| `migrate` | Create/upgrade the PostgreSQL manifest schema (idempotent) |
| `create-bucket` | Create the S3 bucket (useful for local RustFS/MinIO) |
| `generate` | Emit synthetic JSONL measurements to stdout |
| `find` | Look up candidate object keys by station and day |

## Verification

```sh
go test ./...
go test -race ./...
go vet ./...
# With disposable PostgreSQL/RustFS running and environment exported:
ARCHIVE_INTEGRATION=1 go test -run TestIntegration -v
```

Unit tests cover parsing, validation, batching, cancellation, sink failures, deterministic partitioning, and Parquet round trips. The opt-in integration test covers migration, upload, replay and lookup against real services; it removes its manifests but leaves test objects in S3. It must not target production.

## Architecture

```
┌─────────────┐    ┌──────────────┐    ┌──────────────┐    ┌────────────┐    ┌──────────┐    ┌────────────┐    ┌───────────┐    ┌──────────────────┐
│ CSV/JSONL   │───▶│ Validate &   │───▶│ Partition by │───▶│ Sort by    │───▶│ Zstd     │───▶│ Upload   │───▶│ PostgreSQL│
│ Input       │    │ Normalize    │    │ (group, day) │    │ station/   │    │ Compress │    │ to S3    │    │ Manifest  │
└─────────────┘    └──────────────┘    └──────────────┘    │ time/      │    └──────────┘    └────────────┘    └───────────┘
                                                          │ temp       │
                                                          └────────────┘
```

- **Deterministic keys**: S3 object keys are SHA-256 of sorted logical rows (station, timestamp, temperature). Identical input produces identical keys, enabling safe replay.
- **Idempotent migration**: `migrate` is safe to run repeatedly; it extends compatible prototype tables without data loss.
- **Path-style addressing**: Enabled automatically when `S3_ENDPOINT` is set, required for RustFS/MinIO.
- **ARN region resolution**: When using custom endpoints, `UseARNRegion=true` allows the SDK to resolve the bucket's region from its ARN, enabling proper cross-region redirect handling.

## Troubleshooting

### PermanentRedirect (HTTP 301) on upload

```
api error PermanentRedirect: The bucket you are attempting to access must be addressed using the specified endpoint.
```

**Cause**: The S3 client is configured for the wrong region. With a custom `S3_ENDPOINT`, the SDK defaults to `us-east-1` unless explicitly configured otherwise.

**Fix**: Set `AWS_REGION` to your bucket's actual region, or leave it unset to let the SDK auto-resolve from the endpoint:

```sh
# Option 1: Explicit region
export AWS_REGION=eu-west-1

# Option 2: Auto-resolve (recommended for RustFS/MinIO)
unset AWS_REGION
export S3_ENDPOINT=http://localhost:9000
```

### Connection refused / timeout

- Verify `S3_ENDPOINT` is correct and the service is running (`curl $S3_ENDPOINT/minio/health/live`)
- Check firewall/security groups allow the port
- For Docker Compose, ensure services are healthy (`docker compose ps`)

### Database connection failed

- Verify `DATABASE_URL` format: `postgres://user:pass@host:port/db?sslmode=disable`
- Check PostgreSQL is running and accepting connections
- Run `./archive-lab migrate` to ensure schema exists

## License

MIT