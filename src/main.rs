use clap::{Args, Parser};
use eyre::{bail, eyre as err, Result};
use futures_util::StreamExt;
use itertools::Itertools;
use serde::Deserialize;
use std::{
    borrow::Borrow,
    collections::{HashMap, HashSet},
    rc::Rc,
    time::Duration,
};
use tokio::{select, time::sleep};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

mod lookup;

pub type DateTime = chrono::DateTime<chrono::Utc>;

pub fn now() -> DateTime {
    chrono::Utc::now()
}

pub fn parse_datetime(s: &str) -> Option<DateTime> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|d| d.to_utc())
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
                })
            })
            .collect()
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
}

#[derive(Debug, Parser)]
enum GetCmd {
    /// Get labels looking up the labeler via handle or did
    Lookup(GetLookupCmd),
    /// Get labels directly from the labeler service
    Direct(GetDirectCmd),
}

#[derive(Debug, Args)]
struct GetCommonArgs {
    /// Timeout when the stream's updates start slowing down to assume that it is caught up, in
    /// seconds. Non-positive values wait forever
    #[arg(long, default_value = "5")]
    stream_timeout: f64,
}

#[derive(Debug, Args)]
struct GetLookupCmd {
    #[clap(flatten)]
    common: GetCommonArgs,
    /// Handle or DID of the labeler to read from
    handle_or_did: String,
    /// Directory service to use for plc lookups
    #[arg(long, default_value = "plc.directory")]
    plc_directory: String,
}

#[derive(Debug, Args)]
struct GetDirectCmd {
    #[clap(flatten)]
    common: GetCommonArgs,
    /// Domain name for the labeler service
    labeler_service: String,
}

impl GetCmd {
    /// Reads an event stream frame header type
    ///
    /// https://atproto.com/specs/event-stream#streaming-wire-protocol-v0
    fn header_type(bin: &mut &[u8]) -> Result<String> {
        #[derive(Deserialize)]
        struct Header {
            op: i64,
            t: String,
            error: Option<String>,
            message: Option<String>,
        }
        let header: Header = ciborium::from_reader(bin)
            .map_err(|e| err!("error decoding event stream header: {e}"))?;
        if header.op != 1 {
            let error_1 = header.error.as_deref().unwrap_or("(no error type)");
            let error_2 = header.message.as_deref().unwrap_or("(no error message)");
            bail!("received an error from event stream: {error_1}: {error_2}");
        }
        Ok(header.t)
    }

    async fn go(self) -> Result<()> {
        let mut store = LabelStore::new()?;

        let common_args; // common arguments

        println!("looking up did...");
        let labeler_domain = match self {
            GetCmd::Lookup(cmd) => {
                common_args = cmd.common;
                // make sure we have a did
                let did = lookup::did(&cmd.handle_or_did).await?;
                // because we are looking up the did document to find the service, we will know
                // ahead of time what the src did should be for all the label records
                store.set_known_did(did.clone().into())?;
                // get the document
                let doc = lookup::did_doc(&cmd.plc_directory, &did).await?;
                // get all the bits from the did-doc and print some of them out
                let handle = lookup::handle_from_doc(&doc);
                let handle_text = handle.unwrap_or("(no handle listed in did)");
                // read the handle, did, and pds & labeler endpoint urls from the response
                let pds =
                    lookup::service_from_doc(&doc, "#atproto_pds", "AtprotoPersonalDataServer");
                let labeler = lookup::service_from_doc(&doc, "#atproto_labeler", "AtprotoLabeler");

                println!();
                println!("handle: {handle_text}");
                println!("did:    {did}");
                println!();
                let pds_text = pds.as_deref().unwrap_or("(no pds endpoint defined)");
                let labeler_text = labeler
                    .as_deref()
                    .unwrap_or("(no labeler endpoint defined)");
                println!("pds:     {pds_text}");
                println!("labeler: {labeler_text}");

                let Some(labeler) = labeler else {
                    bail!("that entity doesn't seem to be a labeler.");
                };

                let labeler_url = Url::parse(&labeler)
                    .map_err(|e| err!("could not parse labeler endpoint as url: {e}"))?;
                let Some(labeler_domain) = labeler_url.domain() else {
                    bail!("labeler endpoint url does not seem to specify a domain");
                };
                labeler_domain.to_owned()
            }
            GetCmd::Direct(cmd) => {
                common_args = cmd.common;
                cmd.labeler_service
            }
        };

        let address = Url::parse(&format!(
            "wss://{labeler_domain}/xrpc/com.atproto.label.subscribeLabels?cursor=0"
        ))?;
        println!();
        println!("streaming from labeler service");
        let (stream, _response) = connect_async(&address).await?;
        let (_write, mut read) = stream.split();
        // TODO(widders): progress bar?
        // read websocket messages from the connection until they slow down
        let sleep_duration = Duration::try_from_secs_f64(common_args.stream_timeout).ok();
        loop {
            let timeout = sleep_duration.clone().map(sleep);
            let next_frame_read = read.next();
            let message;
            select! {
                Some(()) = conditional_sleep(timeout) => {
                    println!("label subscription stream slowed and crawled; terminating");
                    store.finalize()?;
                    return Ok(());
                }
                websocket_frame = next_frame_read => {
                    let Some(msg) = websocket_frame else {
                        continue;
                    };
                    message = msg;
                }
            }

            match message.map_err(|e| err!("error reading websocket message: {e}"))? {
                Message::Text(text) => {
                    println!("text message: {text:?}")
                }
                Message::Binary(bin) => {
                    let mut bin = bin.as_slice();
                    // the schema for this endpoint is declared here:
                    // https://github.com/bluesky-social/atproto/blob/main/lexicons/com/atproto/label/subscribeLabels.json
                    let ty = Self::header_type(&mut bin)?;
                    if ty == "#labels" {
                        let labels = LabelRecord::from_subscription_record(&mut bin)?;
                        store.process_labels(labels)?;
                    } else if ty == "#info" {
                        let info: atrium_api::com::atproto::label::subscribe_labels::Info =
                            ciborium::from_reader(&mut bin)
                                .map_err(|e| err!("error parsing #info message: {e}"))?;
                        let name = &info.name;
                        let message = &info.message;
                        println!("info: {name:?}: {message:?}");
                    } else {
                        bail!("unknown event stream message type: {ty:?}");
                    }
                    if !bin.is_empty() {
                        let extra_bytes = bin.len();
                        println!(
                            "EXTRA DATA: received {extra_bytes} at end of event stream message"
                        );
                    };
                }
                _ => {}
            }
        }
    }
}

/// waits for the timer only if a one is provided
async fn conditional_sleep(t: Option<tokio::time::Sleep>) -> Option<()> {
    match t {
        Some(timer) => Some(timer.await),
        None => None,
    }
}

struct LabelStore {
    /// set of all src dids we have seen from the labeler stream so far, paired with their prior seq
    labeler_dids: HashSet<Rc<str>>,
    /// tracked effective labels
    effective: HashMap<LabelKey, LabelRecord>,
    /// greatest create timestamp of a label we've seen this trip
    latest_create_timestamp: Option<Rc<str>>,
}

impl LabelStore {
    fn new() -> Result<Self> {
        Ok(Self {
            effective: HashMap::new(),
            labeler_dids: HashSet::new(),
            latest_create_timestamp: None,
        })
    }

    /// record the foreknowledge of an expected src did
    fn set_known_did(&mut self, did: Rc<str>) -> Result<()> {
        if !self.labeler_dids.is_empty() {
            bail!("label store already knows of a labeler did");
        }
        self.labeler_dids.insert(did);
        Ok(())
    }

    fn process_labels(&mut self, labels: Vec<LabelRecord>) -> Result<()> {
        for label in labels {
            if !self.labeler_dids.contains(&label.dbkey.key.src) {
                self.labeler_dids.insert(label.dbkey.key.src.clone());
            }

            // keep track of the latest create timestamp
            if Some(label.create_timestamp.as_ref()) > self.latest_create_timestamp.as_deref() {
                self.latest_create_timestamp = Some(label.create_timestamp.clone());
            }

            self.effective.insert(label.dbkey.key.clone(), label);
        }

        Ok(())
    }

    // TODO(widders): finalize with probably one last fetch to the last possible seq, reporting of
    //  disappeared records, etc
    fn finalize(self) -> Result<()> {
        let now = now();

        println!();
        println!("--------------------");
        println!("--> UPDATE SUMMARY");
        println!("--------------------");
        println!();

        // TODO(widders): count labels gotten, positive and negative

        if let Some(latest_created_at) = &self.latest_create_timestamp {
            let ago =
                match parse_datetime(latest_created_at).and_then(|cts| (now - cts).to_std().ok()) {
                    Some(ago) => &format!("{} ago", humantime::format_duration(ago)),
                    None => "in the future :(",
                };
            println!(
                "== --> last label update received was at {latest_created_at:?}, which is {ago}"
            );
        } else {
            println!("== --> received no labels this time.");
        }

        match self.labeler_dids.len() {
            0 => {}
            1 => println!("OK --> got label records from exactly 1 labeler did (this is good)"),
            2.. => println!(
                "XX --> got label records from {} labeler dids from the same source (WEIRD!)",
                self.labeler_dids.len(),
            ),
        }

        println!("(info) --> all source dids:");
        for did in self.labeler_dids.into_iter().sorted() {
            println!("   {did}");
        }
        println!();

        println!("--------------------");
        todo!("summarize")
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    GetCmd::parse().go().await
}
