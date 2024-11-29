CREATE TABLE IF NOT EXISTS known_handles(
    did TEXT NOT NULL,
    handle TEXT NOT NULL,
    witnessed_timestamp TEXT NOT NULL
    PRIMARY KEY (did, handle)
) WITHOUT ROWID;
CREATE UNIQUE INDEX IF NOT EXISTS handle_dids
    ON known_handles(handle, did);
