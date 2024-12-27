use eyre::{bail, eyre as err, Result};
use rusqlite::named_params;
use std::{borrow::Borrow, path::Path, rc::Rc};

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

/// Connects to the application's database
pub fn connect(path: &Path) -> Result<Connection> {
    let db = Connection::open(path)?;
    db.set_db_config(
        rusqlite::config::DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY,
        true,
    )?;
    db.pragma_update(None, "journal_mode", "WAL")
        .map_err(|e| err!("error setting up db connection: {e}"))?;
    db.pragma_update(None, "synchronous", "NORMAL")
        .map_err(|e| err!("error setting up db connection: {e}"))?;
    db.execute(
        r#"
        CREATE TABLE IF NOT EXISTS label_records(
            src TEXT NOT NULL,
            target_uri TEXT NOT NULL,
            val TEXT NOT NULL,
            seq INTEGER NOT NULL,
            create_timestamp TEXT NOT NULL,
            expiry_timestamp TEXT,
            neg BOOL NOT NULL,
            target_cid TEXT,
            sig BLOB,
            seen_at_timestamp TEXT NOT NULL
        );
        "#,
        [],
    )?;
    Ok(db)
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LabelKey {
    pub src: Rc<str>,
    pub target_uri: Rc<str>,
    pub val: Rc<str>,
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
    pub sig: Option<Vec<u8>>,
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
    /// Returns the seq and labels from a subscription stream message.
    ///
    /// https://atproto.com/specs/label#schema-and-data-model
    pub fn from_subscription_record(bin: &mut &[u8]) -> Result<(i64, Vec<Self>)> {
        let labels: atrium_api::com::atproto::label::subscribe_labels::Labels =
            ciborium::from_reader(bin)
                .map_err(|e| err!("error decoding label record event stream body: {e}"))?;
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
                            src: label.src.to_string().into(),
                            target_uri: label.uri.into(),
                            val: label.val.into(),
                        },
                        seq,
                    },
                    target_cid: label.cid.map(|cid| cid.as_ref().to_string()),
                    create_timestamp: label.cts.as_str().into(),
                    expiry_timestamp: label.exp.map(|exp| exp.as_str().to_owned()),
                    neg: label.neg.unwrap_or(false),
                    sig: label.sig,
                })
            })
            .collect::<Result<_>>()
            .map(|labels| (seq, labels))
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

    /// tries to insert the record, returning true if it was inserted and false if there was a key
    /// conflict
    pub fn insert(&self, db: &Connection, now: &DateTime) -> Result<()> {
        let mut stmt = db.prepare_cached(
            r#"
            INSERT INTO label_records(
                src, target_uri, val, seq,
                create_timestamp, expiry_timestamp, neg,
                target_cid, sig, seen_at_timestamp
            )
            VALUES (
                :src, :uri, :val, :seq,
                :cts, :exp, :neg,
                :cid, :sig, :last_seen
            );
            "#,
        )?;
        stmt.execute(named_params!(
            ":src": &self.dbkey.key.src,
            ":uri": &self.dbkey.key.target_uri,
            ":val": &self.dbkey.key.val,
            ":seq": &self.dbkey.seq,
            ":cts": &self.create_timestamp,
            ":exp": &self.expiry_timestamp,
            ":neg": &self.neg,
            ":cid": &self.target_cid,
            ":sig": &self.sig,
            ":last_seen": now,
        ))?;
        Ok(())
    }
}
