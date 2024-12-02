use anyhow::{anyhow, bail, Result};
use rusqlite::params;
use std::{ops::RangeInclusive, path::PathBuf};

pub use rusqlite::Connection;

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LabelRecord {
    pub src: String,
    pub seq: i64,
    pub create_timestamp: String,
    pub expiry_timestamp: Option<String>,
    pub neg: bool,
    pub target_uri: String,
    pub target_cid: Option<String>,
    pub val: String,
    pub sig: Option<Vec<u8>>,
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
                Ok(Self {
                    src: label.src.to_string(),
                    seq,
                    create_timestamp: label.cts.as_str().to_owned(),
                    expiry_timestamp: label.exp.map(|exp| exp.as_str().to_owned()),
                    neg: label.neg.unwrap_or(false),
                    target_uri: label.uri,
                    target_cid: label.cid.map(|cid| cid.as_ref().to_string()),
                    val: label.val,
                    sig: label.sig,
                })
            })
            .collect()
    }

    pub fn load_known_range(
        db: &Connection,
        src: &str,
        seq_range: RangeInclusive<i64>,
    ) -> Result<Vec<Self>> {
        todo!()
    }

    pub fn save(&self, db: &Connection, now: &chrono::DateTime<chrono::Utc>) -> Result<()> {
        let mut stmt = db.prepare_cached(
            r#"
            INSERT INTO label_records(
                src, seq, create_timestamp,
                expiry_timestamp, neg, target_uri,
                target_cid, val, sig,
                last_seen_timestamp
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10);
            "#,
        )?;
        stmt.execute(params!(
            &self.src,
            &self.seq,
            &self.create_timestamp,
            &self.expiry_timestamp,
            &self.neg,
            &self.target_uri,
            &self.target_cid,
            &self.val,
            &self.sig,
            now,
        ))?; // TODO(widders): probably need a better error message here
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
    stmt.execute(params!(handle, did, chrono::Utc::now()))?;
    Ok(())
}

/// Fetch the last-seen label stream sequence for a labeler's update stream
pub fn seq_for_src(db: &Connection, src_did: &str) -> Result<i64> {
    let mut stmt = db.prepare_cached(
        r#"
        SELECT coalesce(max(seq), 0)
        FROM label_records
        WHERE src = ?1;
        "#,
    )?;
    Ok(stmt.query_row(params!(src_did), |row| row.get(1))?)
}
