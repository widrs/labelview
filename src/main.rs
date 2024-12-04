use crate::db::{get_data_dir, LabelKey, LabelRecord};
use anyhow::{anyhow, bail, Result};
use clap::{Args, Parser, Subcommand};
use futures_util::StreamExt;
use itertools::Itertools;
use serde::Deserialize;
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    ops::Deref,
    time::Duration,
};
use tokio::{select, time::sleep};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

mod db;
mod lookup;

#[derive(Debug, Parser)]
#[command(version)]
enum Command {
    /// Actions for the data directory
    #[clap(subcommand)]
    Data(ConfigCmd),
    /// Reads the labels from a labeler service
    #[clap(subcommand)]
    Get(GetCmd),
    // TODO(widders): summarize commands: sum labels, sum by type (account or what kind of record),
    //  probably do a validation pass on seq/cts ordering validity
}

#[derive(Debug, Subcommand)]
enum ConfigCmd {
    /// Shows the location of the data directory
    Where,
    /// Try to open the database file
    Connect,
}

impl ConfigCmd {
    async fn go(self) -> Result<()> {
        match self {
            ConfigCmd::Where => {
                let data_dir = get_data_dir()?.display().to_string();
                println!("{data_dir}");
                Ok(())
            }
            ConfigCmd::Connect => {
                db::connect()?;
                println!("ok");
                Ok(())
            }
        }
    }
}

#[derive(Debug, Subcommand)]
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
    /// Retread the entire label stream, re-checking currently provided labels for inconsistencies
    /// against already-seen labels from that labeler
    #[arg(long)]
    retread: bool,
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
            .map_err(|e| anyhow!("error decoding event stream header: {e}"))?;
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
        let retreading; // retread argument
        let cursor; // resume cursor

        println!("looking up did...");
        let labeler_domain = match self {
            GetCmd::Lookup(cmd) => {
                common_args = cmd.common;
                retreading = cmd.retread;
                // make sure we have a did
                let did = lookup::did(&cmd.handle_or_did).await?;
                // because we are looking up the did document to find the service, we will know
                // ahead of time what the src did should be for all the label records
                store.set_known_did(did.clone())?;
                cursor = if retreading {
                    0
                } else {
                    db::seq_for_src(&store, &did)?
                };
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

                // record the handle/did association we got
                if let Some(handle) = handle {
                    db::witness_handle_did(&store, handle, &did)?;
                }
                let Some(labeler) = labeler else {
                    bail!("that entity doesn't seem to be a labeler.");
                };

                let labeler_url = Url::parse(&labeler)
                    .map_err(|e| anyhow!("could not parse labeler endpoint as url: {e}"))?;
                let Some(labeler_domain) = labeler_url.domain() else {
                    bail!("labeler endpoint url does not seem to specify a domain");
                };
                labeler_domain.to_owned()
            }
            GetCmd::Direct(cmd) => {
                common_args = cmd.common;
                cursor = 0;
                retreading = true; // we don't know the did, and must retread
                cmd.labeler_service
            }
        };

        let address = Url::parse(&format!(
            "wss://{labeler_domain}/xrpc/com.atproto.label.subscribeLabels?cursor={cursor}"
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

            match message.map_err(|e| anyhow!("error reading websocket message: {e}"))? {
                Message::Text(text) => {
                    println!("text message: {text:?}")
                }
                Message::Binary(bin) => {
                    let now = chrono::Utc::now();
                    let mut bin = bin.as_slice();
                    // the schema for this endpoint is declared here:
                    // https://github.com/bluesky-social/atproto/blob/main/lexicons/com/atproto/label/subscribeLabels.json
                    let ty = Self::header_type(&mut bin)?;
                    if ty == "#labels" {
                        let labels = LabelRecord::from_subscription_record(&mut bin)?;
                        store.process_labels(labels, now, retreading)?;
                    } else if ty == "#info" {
                        let info: atrium_api::com::atproto::label::subscribe_labels::Info =
                            ciborium::from_reader(&mut bin)
                                .map_err(|e| anyhow!("error parsing #info message: {e}"))?;
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
    store: db::Connection,
    /// set of all src dids we have seen from the labeler stream so far
    labeler_dids: HashSet<String>,
    /// count of newly added records found during a retread
    suspicious_new_records: usize,
    /// count of negative records that have gone missing during a retread
    disappeared_negative_records: usize,
    /// last seq seen for each label src
    last_seq: HashMap<String, i64>,
    // set of old unexpired non-negative records which were missing from the stream and, if they are
    // not negated in a future seq later in the stream, would still be in effect.
    disappeared_old_records: HashMap<LabelKey, LabelRecord>,
}

impl LabelStore {
    fn new() -> Result<Self> {
        Ok(Self {
            store: db::connect()?,
            labeler_dids: HashSet::new(),
            suspicious_new_records: 0,
            disappeared_negative_records: 0,
            last_seq: HashMap::new(),
            disappeared_old_records: HashMap::new(),
        })
    }

    /// record the foreknowledge of an expected src did
    fn set_known_did(&mut self, did: String) -> Result<()> {
        if !self.labeler_dids.is_empty() {
            bail!("label store already knows of a labeler did");
        }
        self.labeler_dids.insert(did);
        Ok(())
    }

    fn process_labels(
        &mut self,
        labels: Vec<LabelRecord>,
        now: chrono::DateTime<chrono::Utc>,
        retreading: bool,
    ) -> Result<()> {
        let batch_seq = if let Some(label) = labels.first() {
            label.seq
        } else {
            return Ok(());
        };
        // most of the time we expect labelers to only output records from a single did, which
        // should always be the did that we probably looked up to find it. however, we didn't
        // necessarily look up the labeler, and there's actually nothing about the data that
        // enforces that it couldn't be wildly admixed.
        let batch_src_dids = labels
            .iter()
            .map(|label| &label.key.src)
            .unique()
            .map(ToOwned::to_owned)
            .collect_vec();
        // process retreading logic for each labeler src did we know about
        for src in &batch_src_dids {
            if !self.labeler_dids.contains(src) {
                // if labeler_dids is already empty, it's probably because we went directly to the
                // labeler service without actually knowing what the labeler did is supposed to be
                // first.
                if !self.labeler_dids.is_empty() {
                    println!("WARNING -- additional did appeared in label stream: {src}");
                }
                self.labeler_dids.insert(src.clone());
            }
            if retreading {
                // fetch known old labels starting after the last received seq, up to and including
                // the current seq. if we've seen this src before use its last_seq + 1 otherwise 0
                // as the lower bound, and always leave this batch's seq in the map.
                let seq_range = match self.last_seq.entry(src.clone()) {
                    Entry::Occupied(mut entry) => {
                        let out = entry.get() + 1;
                        *entry.get_mut() = batch_seq;
                        out..=batch_seq
                    }
                    Entry::Vacant(entry) => {
                        entry.insert(batch_seq);
                        0..=batch_seq
                    }
                };
                let mut known_old_labels = LabelRecord::load_known_range(&self, src, seq_range)?;
                // subtract the received labels in this seq from that range
                for label in &labels {
                    if &label.key.src != src {
                        continue;
                    }
                    if !known_old_labels.remove(label) {
                        println!(
                            "WARNING -- NEW label record appeared in label stream: {label:#?}"
                        );
                        self.suspicious_new_records += 1;
                    }
                    if label.neg {
                        self.disappeared_old_records.remove(&label.key);
                    }
                }
                // catalog all the old label records that we didn't see again this time
                for disappeared in known_old_labels {
                    if disappeared.neg {
                        match self.disappeared_old_records.get(&disappeared.key) {
                            Some(positive) if positive.is_expired(&now) => {
                                // the negation record disappeared, but the record it was negating
                                // was expired
                                self.disappeared_old_records.remove(&disappeared.key);
                            }
                            _ => {
                                println!(
                                    "WARNING -- negation record disappeared from label stream: \
                                    {disappeared:#?}"
                                );
                                self.disappeared_negative_records += 1;
                            }
                        }
                    } else {
                        // positive records all go into the disappeared log
                        self.disappeared_old_records
                            .insert(disappeared.key.clone(), disappeared);
                    }
                }
            }
        }
        let tx = self.store.transaction()?;
        if retreading {
            // when retreading, we upsert
            for label in labels {
                label.upsert(&tx, &now)?;
            }
        } else {
            // when not retreading, we simply slam it all in there
            for label in labels {
                label
                    .insert(&tx, &now)?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    // TODO(widders): finalize with probably one last fetch to the last possible seq, reporting of
    //  disappeared records, etc
    fn finalize(self) -> Result<()> {
        todo!()
    }
}

impl Deref for LabelStore {
    type Target = db::Connection;

    fn deref(&self) -> &Self::Target {
        &self.store
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    match Command::parse() {
        Command::Data(cmd) => cmd.go().await,
        Command::Get(cmd) => cmd.go().await,
    }
}
