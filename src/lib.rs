use std::path::PathBuf;

use anyhow::{anyhow, bail, Result};
use cbor4ii::core::{
    dec::{Decode, Read},
    utils::SliceReader,
    Value,
};
use rusqlite::Connection;

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
    db.pragma_update(None, "journal_mode", "WAL")?;
    db.pragma_update(None, "synchronous", "NORMAL")?;
    embedded::migrations::runner().run(&mut db)?;
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
        let body = Value::decode(bin)?;
        let mut record_seq = None;
        let mut record_labels = None;
        match body {
            Value::Map(items) => {
                for item in items {
                    match item {
                        (Value::Text(k), Value::Integer(seq)) if k == "seq" => {
                            record_seq = Some(
                                i64::try_from(seq)
                                    .map_err(|e| anyhow!("sequence number too large"))?,
                            )
                        }
                        (Value::Text(k), Value::Array(labels)) if k == "labels" => {
                            record_labels = Some(labels)
                        }
                        _ => bail!("unrecognized map entry"),
                    }
                }
            }
            _ => {
                bail!("subscription record is not a cbor map");
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
            match item {
                (Value::Text(k), Value::Integer(v)) if k == "ver" => {
                    ver = Some(v);
                }
                (Value::Text(k), Value::Text(v)) if k == "src" => {
                    src = Some(v);
                }
                (Value::Text(k), Value::Text(v)) if k == "uri" => {
                    target_uri = Some(v);
                }
                (Value::Text(k), Value::Text(v)) if k == "cid" => {
                    target_cid = Some(v);
                }
                (Value::Text(k), Value::Text(v)) if k == "val" => {
                    val = Some(v);
                }
                (Value::Text(k), Value::Bool(v)) if k == "neg" => {
                    neg = Some(v);
                }
                (Value::Text(k), Value::Text(v)) if k == "cts" => {
                    create_timestamp = Some(v);
                }
                (Value::Text(k), Value::Text(v)) if k == "exp" => {
                    expiry_timestamp = Some(v);
                }
                (Value::Text(k), Value::Bytes(v)) if k == "sig" => {
                    sig = Some(v);
                }
                _ => {
                    bail!("unexpected item in label record");
                }
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
}
