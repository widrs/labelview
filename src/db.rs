use eyre::{bail, eyre, Result};
use rusqlite::named_params;
use std::{borrow::Borrow, ops::RangeInclusive, path::PathBuf, rc::Rc};

pub use rusqlite::Connection;

pub type DateTime = chrono::DateTime<chrono::Utc>;

pub fn now() -> DateTime {
    chrono::Utc::now()
}

pub fn parse_datetime(s: &str) -> Option<DateTime> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|d| d.to_utc())
}

mod embedded {
    refinery::embed_migrations!("migrations");
}

/// Returns the path to the application's sqlite database file
pub fn get_data_dir() -> Result<PathBuf> {
    directories::ProjectDirs::from("", "", "labelview")
        .map(|dirs| dirs.data_dir().to_path_buf())
        .ok_or(eyre!("could not find data directory"))
}

/// Connects to the application's database
pub fn connect() -> Result<Connection> {
    let data_dir = get_data_dir()?;
    std::fs::create_dir_all(&data_dir)
        .map_err(|err| eyre!("couldn't create data directory {data_dir:?}: {err}"))?;
    let mut db = Connection::open(data_dir.join("data.sqlite"))?;
    db.set_db_config(
        rusqlite::config::DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY,
        true,
    )?;
    db.pragma_update(None, "journal_mode", "WAL")
        .map_err(|e| eyre!("error setting up db connection: {e}"))?;
    db.pragma_update(None, "synchronous", "NORMAL")
        .map_err(|e| eyre!("error setting up db connection: {e}"))?;
    embedded::migrations::runner()
        .run(&mut db)
        .map_err(|e| eyre!("error running db migrations: {e}"))?;
    Ok(db)
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LabelKey {
    pub src: String,
    pub target_uri: String,
    pub val: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LabelDbKey {
    pub key: LabelKey,
    pub seq: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LabelRecord {
    pub dbkey: LabelDbKey,
    pub create_timestamp: Rc<str>,
    pub expiry_timestamp: Option<String>,
    pub neg: bool,
    pub target_cid: Option<String>,
}

impl Borrow<LabelDbKey> for LabelRecord {
    fn borrow(&self) -> &LabelDbKey {
        &self.dbkey
    }
}

impl Borrow<LabelKey> for LabelRecord {
    fn borrow(&self) -> &LabelKey {
        &self.dbkey.key
    }
}

impl LabelRecord {
    /// https://atproto.com/specs/label#schema-and-data-model
    pub fn from_subscription_record(bin: &mut &[u8]) -> Result<Vec<Self>> {
        let labels: atrium_api::com::atproto::label::subscribe_labels::Labels =
            ciborium::from_reader(bin)
                .map_err(|e| eyre!("error decoding label record event stream body: {e}"))?;
        let seq = labels.seq;
        if !(1..i64::MAX).contains(&seq) {
            bail!("non-positive sequence number in label update: {seq}");
        }
        labels
            .data
            .labels
            .into_iter()
            .map(|label| {
                let label = label.data;
                if label.ver != Some(1) {
                    let ver = label.ver;
                    bail!("unsupported or missing label record version {ver:?}");
                }
                // TODO(widders): can we check the signature? do we know how
                Ok(Self {
                    dbkey: LabelDbKey {
                        key: LabelKey {
                            src: label.src.to_string(),
                            target_uri: label.uri,
                            val: label.val,
                        },
                        seq,
                    },
                    target_cid: label.cid.map(|cid| cid.as_ref().to_string()),
                    create_timestamp: label.cts.as_str().into(),
                    expiry_timestamp: label.exp.map(|exp| exp.as_str().to_owned()),
                    neg: label.neg.unwrap_or(false),
                })
            })
            .collect()
    }

    fn from_row(row: &rusqlite::Row) -> rusqlite::Result<Self> {
        Ok(Self {
            dbkey: LabelDbKey {
                key: LabelKey {
                    src: row.get("src")?,
                    target_uri: row.get("target_uri")?,
                    val: row.get("val")?,
                },
                seq: row.get("seq")?,
            },
            create_timestamp: row.get("create_timestamp")?,
            expiry_timestamp: row.get("expiry_timestamp")?,
            neg: row.get("neg")?,
            target_cid: row.get("target_cid")?,
        })
    }

    pub fn fetch_by_key(db: &Connection, key: &LabelKey, seq: i64) -> Result<(Self, DateTime)> {
        let mut stmt = db.prepare_cached(
            r#"
            SELECT
                src, target_uri, val,
                seq, create_timestamp, expiry_timestamp,
                neg, target_cid, last_seen_timestamp
            FROM label_records
            WHERE (src, target_uri, val, seq) = (:src, :uri, :val, :seq);
            "#,
        )?;
        Ok(stmt.query_row(
            named_params!(
                ":src": &key.src,
                ":uri": &key.target_uri,
                ":val": &key.val,
                ":seq": seq,
            ),
            |row| Ok((Self::from_row(row)?, row.get("last_seen_timestamp")?)),
        )?)
    }

    pub fn is_expired(&self, now: &DateTime) -> bool {
        let Some(exp) = &self.expiry_timestamp else {
            return false;
        };
        let Some(exp) = parse_datetime(exp) else {
            return false;
        };
        exp > *now
    }

    pub fn load_known_range(
        db: &Connection,
        src: &str,
        seq_range: RangeInclusive<i64>,
    ) -> Result<Vec<Self>> {
        let mut stmt = db.prepare_cached(
            r#"
            SELECT
                src, target_uri, val,
                seq, create_timestamp, expiry_timestamp,
                neg, target_cid
            FROM label_records
            WHERE
                src = :src AND
                seq >= :seq_start AND seq <= :seq_end
            "#,
        )?;
        let result: Result<Vec<_>, _> = stmt
            .query_map(
                named_params!(
                    ":src": src,
                    ":seq_start": seq_range.start(),
                    ":seq_end": seq_range.end(),
                ),
                Self::from_row,
            )?
            .collect();
        Ok(result?)
    }

    /// tries to insert the record, returning true if it was inserted and false if there was a key
    /// conflict
    pub fn insert(&self, db: &Connection, import_id: i64, now: &DateTime) -> Result<bool> {
        let mut stmt = db.prepare_cached(
            r#"
            INSERT INTO label_records(
                src, target_uri, val, seq,
                create_timestamp, expiry_timestamp, neg,
                target_cid, import_id, last_seen_timestamp
            )
            VALUES (
                :src, :uri, :val, :seq,
                :cts, :exp, :neg,
                :cid, :import_id, :last_seen
            );
            "#,
        )?;
        Ok(
            match stmt.execute(named_params!(
                ":src": &self.dbkey.key.src,
                ":uri": &self.dbkey.key.target_uri,
                ":val": &self.dbkey.key.val,
                ":seq": &self.dbkey.seq,
                ":cts": &self.create_timestamp,
                ":exp": &self.expiry_timestamp,
                ":neg": &self.neg,
                ":cid": &self.target_cid,
                ":import_id": import_id,
                ":last_seen": now,
            )) {
                Ok(..) => true,
                Err(e) => {
                    if is_constraint_violation(&e) {
                        false
                    } else {
                        bail!("error inserting label record: {e}");
                    }
                }
            },
        )
    }

    // update a label record by its key the last seen timestamp of records that already exactly exist instead of
    // failing
    pub fn update(&self, db: &Connection, import_id: i64, now: &DateTime) -> Result<bool> {
        let mut stmt = db.prepare_cached(
            r#"
            UPDATE label_records SET
                create_timestamp = :cts,
                expiry_timestamp = :exp,
                neg = :neg,
                target_cid = :cid,
                import_id = :import_id,
                last_seen_timestamp = :last_seen
            WHERE (src, target_uri, val, seq) = (:src, :uri, :val, :seq)
            "#,
        )?;
        let updated = stmt.execute(named_params!(
            ":src": &self.dbkey.key.src,
            ":uri": &self.dbkey.key.target_uri,
            ":val": &self.dbkey.key.val,
            ":seq": &self.dbkey.seq,
            ":cts": &self.create_timestamp,
            ":exp": &self.expiry_timestamp,
            ":neg": &self.neg,
            ":cid": &self.target_cid,
            ":import_id": import_id,
            ":last_seen": now,
        ))?;
        if updated > 1 {
            bail!("more than 1 label record updated by a single key :(");
        }
        Ok(updated == 1)
    }

    pub fn suspicious(
        &self,
        db: &Connection,
        problem: &str,
        import_id: i64,
        now: &DateTime,
    ) -> Result<()> {
        let mut stmt = db.prepare_cached(
            r#"
            INSERT INTO suspicious_records(
                import_id, suspicion_timestamp, problem_category,
                src, target_uri, val, seq,
                create_timestamp, expiry_timestamp, neg,
                target_cid
            )
            VALUES (
                :import_id, :sus_time, :problem,
                :src, :uri, :val, :seq,
                :cts, :exp, :neg,
                :cid
            );
            "#,
        )?;
        stmt.execute(named_params!(
            ":import_id": import_id,
            ":sus_time": now,
            ":problem": problem,
            ":src": &self.dbkey.key.src,
            ":uri": &self.dbkey.key.target_uri,
            ":val": &self.dbkey.key.val,
            ":seq": &self.dbkey.seq,
            ":cts": &self.create_timestamp,
            ":exp": &self.expiry_timestamp,
            ":neg": &self.neg,
            ":cid": &self.target_cid,
        ))?;
        Ok(())
    }
}

impl LabelDbKey {
    /// copy an existing record
    pub fn suspicious_from_old_record(
        &self,
        db: &Connection,
        problem: &str,
        import_id: i64,
        now: &DateTime,
    ) -> Result<bool> {
        let mut stmt = db.prepare_cached(
            r#"
            INSERT INTO suspicious_records(
                import_id, suspicion_timestamp, problem_category,
                src, target_uri, val, seq,
                create_timestamp, expiry_timestamp, neg,
                target_cid, original_import_id, original_last_seen_timestamp
            )
            SELECT
                :import_id, :sus_time, :problem,
                src, target_uri, val, seq,
                create_timestamp, expiry_timestamp, neg,
                target_cid, import_id, last_seen_timestamp
            FROM label_records
            WHERE (src, target_uri, val, seq) = (:src, :uri, :val, :seq);
            "#,
        )?;
        let inserted = stmt.execute(named_params!(
            ":import_id": import_id,
            ":sus_time": now,
            ":problem": problem,
            ":src": &self.key.src,
            ":uri": &self.key.target_uri,
            ":val": &self.key.val,
            ":seq": &self.seq,
        ))?;
        if inserted > 1 {
            bail!("found more than 1 label record by a single key :(")
        }
        Ok(inserted == 1)
    }
}

/// Record the association between a handle and a did
pub fn witness_handle_did(db: &Connection, handle: &str, did: &str) -> Result<()> {
    let mut stmt = db.prepare_cached(
        r#"
        INSERT OR REPLACE INTO known_handles(did, handle, witnessed_timestamp)
        VALUES (:did, :handle, :witness_time);
        "#,
    )?;
    stmt.execute(named_params!(
        ":did": handle,
        ":handle": did,
        ":witness_time": now(),
    ))?;
    Ok(())
}

/// Fetch the last-seen label stream sequence for a labeler's update stream
pub fn seq_for_src(db: &Connection, src_did: &str) -> Result<i64> {
    let mut stmt = db.prepare_cached(
        r#"
        SELECT coalesce(max(seq), 0) AS last_seq
        FROM label_records
        WHERE src = :src;
        "#,
    )?;
    Ok(stmt.query_row(
        named_params!(
            ":src": src_did,
        ),
        |row| row.get("last_seq"),
    )?)
}

pub fn begin_import(db: &Connection, now: DateTime) -> Result<i64> {
    let program_args =
        serde_json::Value::Array(std::env::args().map(serde_json::Value::String).collect());
    let mut stmt = db.prepare_cached(
        r#"
        INSERT INTO imports(start_time, program_args)
        VALUES (:start_time, :args)
        RETURNING id;
        "#,
    )?;
    Ok(stmt.query_row(
        named_params!(
            ":start_time": now,
            ":args": program_args,
        ),
        |row| row.get("id"),
    )?)
}

fn is_constraint_violation(err: &rusqlite::Error) -> bool {
    matches!(
        err.sqlite_error(),
        Some(&rusqlite::ffi::Error {
            code: libsqlite3_sys::ErrorCode::ConstraintViolation,
            ..
        })
    )
}
