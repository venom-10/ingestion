-- Compatible with the prototype's group_id/day/object_key manifest.
CREATE TABLE IF NOT EXISTS archive_blocks (
    group_id smallint NOT NULL CHECK (group_id BETWEEN 0 AND 255),
    day date NOT NULL,
    object_key text PRIMARY KEY
);
ALTER TABLE archive_blocks ADD COLUMN IF NOT EXISTS row_count bigint;
ALTER TABLE archive_blocks ADD COLUMN IF NOT EXISTS min_timestamp bigint;
ALTER TABLE archive_blocks ADD COLUMN IF NOT EXISTS max_timestamp bigint;
ALTER TABLE archive_blocks ADD COLUMN IF NOT EXISTS checksum text;
ALTER TABLE archive_blocks ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now();
CREATE INDEX IF NOT EXISTS archive_blocks_partition_idx ON archive_blocks (group_id, day);
