use crate::db::{now, parse_datetime, Connection, DateTime, LabelKey, LabelRecord};
use clap::{Args, Parser};
use eyre::{bail, eyre as err, Result};
use futures_util::StreamExt;
use itertools::Itertools;
use serde::Deserialize;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    num::NonZeroUsize,
    path::PathBuf,
    rc::Rc,
    time::Duration,
};
use tokio::{select, sync::mpsc::channel, time::sleep};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

mod db;
mod lookup;

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
    /// Save all records read from the labeler into the specified Sqlite file.
    ///
    /// A table named "label_records" will be created and the data inserted into it, plus the time
    /// that it is received from the labeling service.
    #[arg(long)]
    save_to_db: Option<PathBuf>,
    /// Maximum number of messages to buffer while processing. Increasing this can speed up
    /// ingestion at the network level at the cost of more memory usage.
    #[arg(long, default_value = "10000")]
    buffer_size: NonZeroUsize,
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

enum StreamHeaderType {
    Type(String),
    Error,
}

impl GetCmd {
    /// Reads an event stream frame header type
    ///
    /// https://atproto.com/specs/event-stream#streaming-wire-protocol-v0
    fn header_type(bin: &mut &[u8]) -> Result<StreamHeaderType> {
        #[derive(Deserialize)]
        struct Header {
            op: i64,
            t: Option<String>,
        }
        Ok(
            match ciborium::from_reader(bin)
                .map_err(|e| err!("error decoding event stream header: {e}"))?
            {
                Header {
                    op: 1,
                    t: Some(t),
                } => StreamHeaderType::Type(t),
                Header {
                    op: -1,
                    t: None,
                } => StreamHeaderType::Error,
                malformed => bail!(
                    "received a malformed event stream header: op {op}",
                    op = malformed.op,
                ),
            },
        )
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

        if let Some(db_path) = common_args.save_to_db {
            store.store = Some(db::connect(&db_path)?);
        }

        let address = Url::parse(&format!(
            "wss://{labeler_domain}/xrpc/com.atproto.label.subscribeLabels?cursor=0"
        ))?;
        println!();
        println!("streaming from labeler service");
        let (stream, _response) = connect_async(&address).await?;
        let (_write, mut read) = stream.split();
        // TODO(widders): progress bar?

        let (send, mut recv) = channel(common_args.buffer_size.get());

        tokio::spawn(async move {
            // read websocket messages from the connection until they slow down
            let sleep_duration = Duration::try_from_secs_f64(common_args.stream_timeout).ok();
            loop {
                let timeout = sleep_duration.clone().map(sleep);
                let next_frame_read = read.next();
                select! {
                    Some(()) = conditional_sleep(timeout) => {
                        println!("label subscription stream slowed and crawled; terminating");
                        break;
                    }
                    websocket_frame = next_frame_read => {
                        let Some(msg) = websocket_frame else {
                            continue;
                        };
                        let Ok(()) = send.send(msg).await else {
                            return; // channel closed; shut down
                        };
                    }
                }
            }
        });

        let begin = now();
        while let Some(message) = recv.recv().await {
            match message.map_err(|e| err!("error reading websocket message: {e}"))? {
                Message::Text(text) => {
                    println!("text message: {text:?}")
                }
                Message::Binary(bin) => {
                    let now = now();
                    let mut bin: &[u8] = &*bin;
                    // the schema for this endpoint is declared here:
                    // https://github.com/bluesky-social/atproto/blob/main/lexicons/com/atproto/label/subscribeLabels.json
                    match Self::header_type(&mut bin)? {
                        StreamHeaderType::Error => {
                            #[derive(Deserialize)]
                            struct ErrorPayload {
                                error: String,
                                message: Option<String>,
                            }
                            let ErrorPayload{error, message} = ciborium::from_reader(&mut bin)
                                .map_err(|e| err!("malformed stream error: {e}"))?;
                            println!(
                                "label subscription stream returned an error: {error}: {message}",
                                message = message.as_deref().unwrap_or("(no error message)"),
                            );
                            if !bin.is_empty() {
                                let extra_bytes = bin.len();
                                println!(
                                    "EXTRA DATA: received {extra_bytes} at end of event stream \
                                    error message"
                                );
                            };
                            break;
                        }
                        StreamHeaderType::Type(ty) => {
                            if ty == "#labels" {
                                let labels = LabelRecord::from_subscription_record(&mut bin)?;
                                store.process_labels(labels, &now)?;
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
                                    "EXTRA DATA: received {extra_bytes} at end of event stream \
                                    message"
                                );
                            };
                        }
                    }
                }
                Message::Close(frame) => {
                    if let Some(frame) = frame {
                        println!(
                            "label subscription stream closed: {code:?} {reason:?}",
                            code = frame.code,
                            reason = frame.reason.as_str(),
                        );
                    } else {
                        println!("label subscription stream closed");
                    }
                    break;
                }
                _ => {}
            }
        }
        let end = now();
        drop(recv);
        println!("elapsed: {}", humantime::format_duration((end - begin).to_std()?));
        Ok(store.finalize()?)
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
    /// database we are saving labels into
    store: Option<Connection>,
    /// set of all src dids we have seen from the labeler stream so far, paired with their prior seq
    labeler_dids: HashSet<Rc<str>>,
    /// total labels read
    total_labels: usize,
    /// tracked effective labels
    effective: HashMap<LabelKey, LabelRecord>,
    /// greatest create timestamp of a label we've seen this trip
    latest_create_timestamp: Option<Rc<str>>,
}

impl LabelStore {
    fn new() -> Result<Self> {
        Ok(Self {
            store: None,
            total_labels: 0,
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

    fn process_labels(&mut self, labels: Vec<LabelRecord>, now: &DateTime) -> Result<()> {
        self.total_labels += labels.len();
        for mut label in labels {
            if !self.labeler_dids.contains(&label.dbkey.key.src) {
                self.labeler_dids.insert(label.dbkey.key.src.clone());
            }

            // keep track of the latest create timestamp
            if Some(label.create_timestamp.as_ref()) > self.latest_create_timestamp.as_deref() {
                self.latest_create_timestamp = Some(label.create_timestamp.clone());
            }

            if let Some(store) = &self.store {
                label.insert(store, now)?;
            }

            // discard the signature data after it's been stored in the db, we no longer need it by
            // this point
            label.sig = None;

            // TODO(widders): make sure the label we're effecting over has an older create timestamp
            self.effective.insert(label.dbkey.key.clone(), label);
        }
        Ok(())
    }

    fn finalize(self) -> Result<()> {
        let now = now();

        println!();
        println!("--------------------");
        println!("--> UPDATE SUMMARY");
        println!("--------------------");
        println!();
        println!(
            "received a total of {total} label record(s)",
            total = self.total_labels
        );
        println!();

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

        let global_labels: HashSet<_> = [
            "!hide",
            "!warn",
            "porn",
            "sexual",
            "graphic-media",
            "nudity",
        ]
        .into_iter()
        .collect();

        let mut effective_counts = BTreeMap::<_, usize>::new();
        let mut total_effective = 0usize;
        for (
            LabelKey {
                src,
                val,
                target_uri,
            },
            label,
        ) in self.effective
        {
            if !label.neg && !label.is_expired(&now) {
                *effective_counts
                    .entry((
                        src.clone(),
                        val.clone(),
                        TargetKind::from_target_uri(&target_uri),
                    ))
                    .or_default() += 1;
                total_effective += 1;
            }
        }

        println!("labeler defined {total_effective} effective label(s)");
        println!("--------------------");

        for ((src, val, target_kind), count) in effective_counts {
            let global_tag = if global_labels.contains(val.as_ref()) {
                " (global)"
            } else {
                ""
            };
            println!("{src} labels {count:>8} x: {val:?}{global_tag} -> {target_kind:?}");
        }

        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
enum TargetKind {
    Account,
    Record { kind: String },
    Unknown,
}

impl TargetKind {
    fn from_target_uri(uri: &str) -> Self {
        if let Some(rest) = uri.strip_prefix("at://") {
            let mut split = rest.split('/');
            if let (Some(_did), Some(middle)) = (split.next(), split.next()) {
                Self::Record {
                    kind: middle.to_owned(),
                }
            } else {
                Self::Unknown
            }
        } else {
            // assume it's a did
            Self::Account
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    GetCmd::parse().go().await
}
