CREATE TABLE IF NOT EXISTS label_records(
   src TEXT NOT NULL,
   seq INT NOT NULL,
   create_timestamp TEXT NOT NULL,
   expiry_timestamp TEXT,
   neg BOOL NOT NULL,
   target_uri TEXT NOT NULL,
   target_cid TEXT,
   val TEXT NOT NULL,
   sig BLOB
);
CREATE UNIQUE INDEX IF NOT EXISTS label_records_seq
ON label_records (src, seq);
CREATE UNIQUE INDEX IF NOT EXISTS label_records_target
ON label_records (target_uri, target_cid, val, neg);
