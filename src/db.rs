use anyhow::{anyhow, bail, Result};
use rusqlite::params;
use std::{borrow::Borrow, collections::HashSet, ops::RangeInclusive, path::PathBuf, rc::Rc};

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
        .ok_or(anyhow!("could not find data directory"))
}

/// Connects to the application's database
pub fn connect() -> Result<Connection> {
    let data_dir = get_data_dir()?;
    std::fs::create_dir_all(&data_dir)
        .map_err(|err| anyhow!("couldn't create data directory {data_dir:?}: {err}"))?;
    let mut db = Connection::open(data_dir.join("data.sqlite"))?;
    db.set_db_config(
        rusqlite::config::DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY,
        true,
    )?;
    db.pragma_update(None, "journal_mode", "WAL")
        .map_err(|e| anyhow!("error setting up db connection: {e}"))?;
    db.pragma_update(None, "synchronous", "NORMAL")
        .map_err(|e| anyhow!("error setting up db connection: {e}"))?;
    embedded::migrations::runner()
        .run(&mut db)
        .map_err(|e| anyhow!("error running db migrations: {e}"))?;
    Ok(db)
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LabelKey {
    pub src: String,
    pub target_uri: String,
    pub val: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LabelRecord {
    pub key: LabelKey,
    pub seq: i64,
    pub create_timestamp: Rc<str>,
    pub expiry_timestamp: Option<String>,
    pub neg: bool,
    pub target_cid: Option<String>,
}

impl Borrow<LabelKey> for LabelRecord {
    fn borrow(&self) -> &LabelKey {
        &self.key
    }
}

impl LabelRecord {
    /// https://atproto.com/specs/label#schema-and-data-model
    pub fn from_subscription_record(bin: &mut &[u8]) -> Result<Vec<Self>> {
        let labels: atrium_api::com::atproto::label::subscribe_labels::Labels =
            ciborium::from_reader(bin)
                .map_err(|e| anyhow!("error decoding label record event stream body: {e}"))?;
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
                    key: LabelKey {
                        src: label.src.to_string(),
                        target_uri: label.uri,
                        val: label.val,
                    },
                    target_cid: label.cid.map(|cid| cid.as_ref().to_string()),
                    seq,
                    create_timestamp: label.cts.as_str().into(),
                    expiry_timestamp: label.exp.map(|exp| exp.as_str().to_owned()),
                    neg: label.neg.unwrap_or(false),
                })
            })
            .collect()
    }

    fn from_row(row: &rusqlite::Row) -> rusqlite::Result<Self> {
        Ok(Self {
            key: LabelKey {
                src: row.get("src")?,
                target_uri: row.get("target_uri")?,
                val: row.get("val")?,
            },
            seq: row.get("seq")?,
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
            WHERE (src, target_uri, val, seq) = (?1, ?2, ?3, ?4);
            "#,
        )?;
        Ok(
            stmt.query_row(params!(&key.src, &key.target_uri, &key.val, seq), |row| {
                Ok((Self::from_row(row)?, row.get("last_seen_timestamp")?))
            })?,
        )
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
    ) -> Result<HashSet<Self>> {
        let mut stmt = db.prepare_cached(
            r#"
            SELECT
                src, target_uri, val,
                seq, create_timestamp, expiry_timestamp,
                neg, target_cid
            FROM label_records
            WHERE
                src = ?1 AND
                seq >= ?2 AND seq <= ?3
            "#,
        )?;
        let result: Result<HashSet<_>, _> = stmt
            .query_map(
                params!(src, seq_range.start(), seq_range.end()),
                Self::from_row,
            )?
            .collect();
        Ok(result?)
    }

    pub fn insert(&self, db: &Connection, import_id: i64, now: &DateTime) -> Result<()> {
        let mut stmt = db.prepare_cached(
            r#"
            INSERT INTO label_records(
                src, target_uri, val, seq,
                create_timestamp, expiry_timestamp, neg,
                target_cid, import_id, last_seen_timestamp
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10);
            "#,
        )?;
        stmt.execute(params!(
            &self.key.src,
            &self.key.val,
            &self.key.target_uri,
            &self.seq,
            &self.create_timestamp,
            &self.expiry_timestamp,
            &self.neg,
            &self.target_cid,
            import_id,
            now,
        ))
        .map_err(|e| {
            if is_constraint_violation(&e) {
                anyhow!("uh oh! conflicting label record already exists")
            } else {
                anyhow!("error inserting label record: {e}")
            }
        })?;
        Ok(())
    }

    // upsert, updating the last seen timestamp of records that already exactly exist instead of
    // failing
    pub fn upsert(&self, db: &Connection, import_id: i64, now: &DateTime) -> Result<()> {
        let mut stmt = db.prepare_cached(
            r#"
            INSERT INTO label_records(
                src, target_uri, val, seq,
                create_timestamp, expiry_timestamp, neg,
                target_cid, import_id, last_seen_timestamp
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
            ON CONFLICT (src, val, target_uri, seq)
                WHERE (create_timestamp, expiry_timestamp, neg, target_cid) IS
                    (?5, ?6, ?7, ?8)
                DO UPDATE SET last_seen_timestamp = ?10;
            "#,
        )?;
        stmt.execute(params!(
            &self.key.src,
            &self.key.val,
            &self.key.target_uri,
            &self.seq,
            &self.create_timestamp,
            &self.expiry_timestamp,
            &self.neg,
            &self.target_cid,
            import_id,
            now,
        ))
        .map_err(|e| {
            if is_constraint_violation(&e) {
                let Ok((conflicting_record, was_last_seen)) =
                    Self::fetch_by_key(db, &self.key, self.seq)
                else {
                    return anyhow!(e);
                };
                println!("ERROR: label record changed since it was last read!");
                let ago = match (now.clone() - was_last_seen).to_std() {
                    Ok(ago) => &format!("{} ago", humantime::format_duration(ago)),
                    Err(..) => "unfortunately this is in the future...",
                };
                println!("previous label record was seen at: {was_last_seen} ({ago})");
                println!("previous label record: {conflicting_record:#?}");
                println!("new label record: {self:#?}");
                anyhow!("no support for inserting conflicting records at this time")
            } else {
                anyhow!("error upserting label record: {e}")
            }
        })?;
        Ok(())
    }
}

/// Record the association between a handle and a did
pub fn witness_handle_did(db: &Connection, handle: &str, did: &str) -> Result<()> {
    let mut stmt = db.prepare_cached(
        r#"
        INSERT OR REPLACE INTO known_handles(did, handle, witnessed_timestamp)
        VALUES (?1, ?2, ?3);
        "#,
    )?;
    stmt.execute(params!(handle, did, now()))?;
    Ok(())
}

/// Fetch the last-seen label stream sequence for a labeler's update stream
pub fn seq_for_src(db: &Connection, src_did: &str) -> Result<i64> {
    let mut stmt = db.prepare_cached(
        r#"
        SELECT coalesce(max(seq), 0) AS last_seq
        FROM label_records
        WHERE src = ?1;
        "#,
    )?;
    Ok(stmt.query_row(params!(src_did), |row| row.get("last_seq"))?)
}

pub fn begin_import(db: &Connection, now: DateTime) -> Result<i64> {
    let program_args =
        serde_json::Value::Array(std::env::args().map(serde_json::Value::String).collect());
    let mut stmt = db.prepare_cached(
        r#"
        INSERT INTO imports(start_time, program_args)
        VALUES (?1, ?2)
        RETURNING id;
        "#,
    )?;
    Ok(stmt.query_row(params!(now, program_args), |row| row.get("id"))?)
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
