use std::path::PathBuf;

use anyhow::{anyhow, bail, Result};
use cbor4ii::core::{dec::Decode, utils::SliceReader, Value};
use rusqlite::{params, Connection};

mod embedded {
    refinery::embed_migrations!("migrations");
}

pub fn get_data_dir() -> Result<PathBuf> {
    directories::ProjectDirs::from("", "", "labelview")
        .map(|dirs| dirs.data_dir().to_path_buf())
        .ok_or(anyhow!("could not find data directory"))
}

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
    src: String,
    seq: i64,
    create_timestamp: String,
    expiry_timestamp: Option<String>,
    neg: bool,
    target_uri: String,
    target_cid: Option<String>,
    val: String,
    sig: Option<Vec<u8>>,
}

impl LabelRecord {
    /// https://atproto.com/specs/label#schema-and-data-model
    pub fn from_subscription_record(bin: &mut SliceReader) -> Result<Vec<Self>> {
        let body = Value::decode(bin)
            .map_err(|e| anyhow!("error decoding label record event stream body: {e}"))?;
        let mut record_seq = None;
        let mut record_labels = None;
        let Value::Map(items) = body else {
            bail!("subscription record is not a cbor map");
        };
        for item in items {
            let (Value::Text(ref k), v) = item else {
                bail!("non-string key in label record");
            };
            match (k.as_str(), v) {
                ("seq", Value::Integer(seq)) => {
                    record_seq =
                        Some(i64::try_from(seq).map_err(|_| {
                            anyhow!("subscription record sequence number too large")
                        })?)
                }
                ("labels", Value::Array(labels)) => record_labels = Some(labels),
                _ => bail!("subscription record has unrecognized map entry"),
            }
        }
        let (Some(seq), Some(labels)) = (record_seq, record_labels) else {
            bail!("incomplete subscription record is missing required key(s)");
        };
        labels
            .into_iter()
            .map(|val| Self::from_cbor(seq, val))
            .collect()
    }

    fn from_cbor(seq: i64, cbor_val: Value) -> Result<Self> {
        let mut ver = None;
        let mut src = None;
        let mut create_timestamp = None;
        let mut expiry_timestamp = None;
        let mut neg = None;
        let mut target_uri = None;
        let mut target_cid = None;
        let mut val = None;
        let mut sig = None;

        let Value::Map(items) = cbor_val else {
            bail!("label record item is not a cbor map");
        };
        for item in items {
            let (Value::Text(ref k), v) = item else {
                bail!("non-string key in label record");
            };
            match (k.as_str(), v) {
                ("ver", Value::Integer(v)) => ver = Some(v),
                ("src", Value::Text(v)) => src = Some(v),
                ("uri", Value::Text(v)) => target_uri = Some(v),
                ("cid", Value::Text(v)) => target_cid = Some(v),
                ("val", Value::Text(v)) => val = Some(v),
                ("neg", Value::Bool(v)) => neg = Some(v),
                ("cts", Value::Text(v)) => create_timestamp = Some(v),
                ("exp", Value::Text(v)) => expiry_timestamp = Some(v),
                ("sig", Value::Bytes(v)) => sig = Some(v),
                _ => bail!("unexpected item in label record"),
            }
        }

        if ver != Some(1) {
            bail!("unsupported or missing label schema version: {ver:?}");
        }
        let (Some(src), Some(target_uri), Some(val), Some(create_timestamp)) =
            (src, target_uri, val, create_timestamp)
        else {
            bail!("label record item has missing required fields");
        };
        Ok(LabelRecord {
            src,
            seq,
            create_timestamp,
            expiry_timestamp,
            neg: neg.unwrap_or(false),
            target_uri,
            target_cid,
            val,
            sig,
        })
    }

    pub fn save(&self, db: &mut Connection) -> Result<()> {
        let mut stmt = db.prepare_cached(
            r#"
            INSERT INTO label_records(
                src, seq, create_timestamp,
                expiry_timestamp, neg, target_uri,
                target_cid, val, sig
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9);
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
            &self.sig
        ))?;
        Ok(())
    }
}
