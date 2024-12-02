use crate::db::{get_data_dir, LabelRecord};
use anyhow::{anyhow, bail, Result};
use clap::{Args, Parser, Subcommand};
use futures_util::StreamExt;
use itertools::Itertools;
use serde::Deserialize;
use std::{collections::BTreeSet, time::Duration};
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
    // TODO(widders): summarize commands
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
        let mut store = db::connect()?;
        // set of all src dids we have seen from the labeler stream so far
        let mut labeler_dids = BTreeSet::<String>::new();
        let common_args;
        let cursor;
        let retreading;
        println!("looking up did...");
        let labeler_domain = match self {
            GetCmd::Lookup(cmd) => {
                common_args = cmd.common;
                retreading = cmd.retread;
                // make sure we have a did
                let did = lookup::did(&cmd.handle_or_did).await?;
                // because we are looking up the did document to find the service, we will know
                // ahead of time what the src did should be for all the label records
                labeler_dids.insert(did.clone());
                cursor = if cmd.retread {
                    0
                } else {
                    db::seq_for_src(&mut store, &did)?
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
                    db::witness_handle_did(&mut store, handle, &did)?;
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
        // TODO(widders): catch-up or re-tread data we already have to look for changes
        let (stream, _response) = connect_async(&address).await?;
        let (_write, mut read) = stream.split();
        // TODO(widders): progress bar?
        // read websocket messages from the connection until they slow down
        let sleep_duration = Duration::try_from_secs_f64(common_args.stream_timeout).ok();
        let mut last_seq = None;
        loop {
            let timeout = sleep_duration.clone().map(sleep);
            let next_frame_read = read.next();
            let message;
            select! {
                Some(()) = conditional_sleep(timeout) => {
                    println!("label subscription stream slowed and crawled; terminating");
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
                        let mut seq_range = None;
                        if let Some(label) = labels.first() {
                            seq_range = Some(last_seq.unwrap_or(0)..=label.seq);
                            last_seq = Some(label.seq);
                        };
                        // most of the time we expect labelers to only output records from a single
                        // did, which should always be the did that we probably looked up to find
                        // it. however, we didn't necessarily look up the labeler, and there's
                        // actually nothing about the data that enforces that it couldn't be wildly
                        // admixed.
                        let batch_src_dids = labels
                            .iter()
                            .map(|label| &label.src)
                            .unique()
                            .map(ToOwned::to_owned)
                            .collect_vec();
                        for src in &batch_src_dids {
                            if !labeler_dids.contains(src) {
                                // if labeler_dids is already empty, it's probably because we
                                // went directly to the labeler service without actually knowing
                                // what the labeler did is supposed to be first.
                                if !labeler_dids.is_empty() {
                                    println!(
                                        "WARNING -- additional did appeared in label stream: {src}"
                                    );
                                }
                                labeler_dids.insert(src.clone());
                            }
                            if retreading {
                                let seq_range = seq_range.clone().unwrap();
                                let known_labels = LabelRecord::load_known_range(
                                    &mut store,
                                    src,
                                    seq_range,
                                )?;
                                // TODO(widders): filter labels down by src
                                // TODO(widders): create a set of the old labels and subtract the
                                //  newly received ones from it. alert when we find new ones, and
                                //  when there are left-over ones that have been deleted since add
                                //  them to a carry-forward struct that we will slowly eliminate
                                //  from when we see those src/target pairs get superceded later
                                //  or if we can see that the entry has simply expired

                                // TODO(widders): finally, upsert the last-seen timestamp of the
                                //  received label records
                            }
                        }
                        if !retreading {
                            // when not retreading, we simply slam it all in there
                            let tx = store.transaction()?;
                            for label in labels {
                                label
                                    .save(&tx, &now)
                                    .map_err(|e| anyhow!("error saving label record: {e}"))?;
                                // TODO(widders): can we check the signature? do we know how
                            }
                            tx.commit()?;
                        }
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

#[tokio::main]
async fn main() -> Result<()> {
    match Command::parse() {
        Command::Data(cmd) => cmd.go().await,
        Command::Get(cmd) => cmd.go().await,
    }
}
