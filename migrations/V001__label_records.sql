CREATE TABLE IF NOT EXISTS label_records(
    id INTEGER PRIMARY KEY,
    src TEXT NOT NULL,
    seq INTEGER NOT NULL,
    create_timestamp TEXT NOT NULL,
    expiry_timestamp TEXT,
    neg BOOL NOT NULL,
    target_uri TEXT NOT NULL,
    target_cid TEXT,
    val TEXT NOT NULL,
    sig BLOB,
    last_seen_timestamp TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS label_records_outgoing
    ON label_records (src, val, target_uri, seq);
CREATE INDEX IF NOT EXISTS label_records_incoming
    ON label_records (target_uri, val);
