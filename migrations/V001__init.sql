CREATE TABLE IF NOT EXISTS imports(
    id INTEGER PRIMARY KEY,
    start_time TEXT NOT NULL,
    program_args TEXT JSON NOT NULL
);

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
    import_id INTEGER NOT NULL,
    last_seen_timestamp TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS label_records_outgoing
    ON label_records (src, val, target_uri, seq);
CREATE INDEX IF NOT EXISTS label_records_incoming
    ON label_records (target_uri, val);

CREATE TABLE IF NOT EXISTS suspicious_records(
    id INTEGER PRIMARY KEY,
    import_id INTEGER NOT NULL,
    suspicion_timestamp TEXT NOT NULL,
    problem_category TEXT NOT NULL,
    src TEXT NOT NULL,
    seq INTEGER NOT NULL,
    create_timestamp TEXT NOT NULL,
    expiry_timestamp TEXT,
    neg BOOL NOT NULL,
    target_uri TEXT NOT NULL,
    target_cid TEXT,
    val TEXT NOT NULL,
    original_import_id INTEGER,
    original_last_seen_timestamp TEXT
);
CREATE INDEX IF NOT EXISTS suspicious_records_by_import
    ON suspicious_records(import_id, problem_category);

CREATE TABLE IF NOT EXISTS known_handles(
    did TEXT NOT NULL,
    handle TEXT NOT NULL,
    witnessed_timestamp TEXT NOT NULL,
    PRIMARY KEY (did, handle)
) WITHOUT ROWID;
CREATE UNIQUE INDEX IF NOT EXISTS handle_dids
    ON known_handles(handle, did);
