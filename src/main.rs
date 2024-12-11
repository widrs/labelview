use crate::db::{get_data_dir, now, parse_datetime, DateTime, LabelDbKey, LabelKey, LabelRecord};
use clap::{Args, Parser, Subcommand};
use eyre::{bail, eyre as err, Result};
use futures_util::StreamExt;
use itertools::Itertools;
use serde::Deserialize;
use std::{
    borrow::Borrow,
    collections::{hash_map::Entry, HashMap},
    ops::Deref,
    rc::Rc,
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
                    .map_err(|e| err!("could not parse labeler endpoint as url: {e}"))?;
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

            match message.map_err(|e| err!("error reading websocket message: {e}"))? {
                Message::Text(text) => {
                    println!("text message: {text:?}")
                }
                Message::Binary(bin) => {
                    let now = now();
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
    store: db::Connection,
    /// the unique id for this run of the program and its associated rows
    import_id: i64,
    /// set of all src dids we have seen from the labeler stream so far, paired with their prior seq
    labeler_dids: HashMap<String, i64>,
    /// count of newly added records found during a retread
    suspicious_new_records: usize,
    /// count of negative records that have gone missing during a retread
    disappeared_negative_records: usize,
    /// last seq seen for each label src
    last_seq: HashMap<String, i64>,
    /// set of old unexpired non-negative records which were missing from the stream and, if they
    /// are not negated in a future seq later in the stream, would still be in effect.
    disappeared_old_records: HashMap<LabelKey, LabelRecord>,
    /// greatest create timestamp of a label we've seen this trip
    latest_create_timestamp: Option<Rc<str>>,
}

impl LabelStore {
    fn new() -> Result<Self> {
        let store = db::connect()?;
        let import_id = db::begin_import(&store, now())?;
        Ok(Self {
            store,
            import_id,
            labeler_dids: HashMap::new(),
            suspicious_new_records: 0,
            disappeared_negative_records: 0,
            last_seq: HashMap::new(),
            disappeared_old_records: HashMap::new(),
            latest_create_timestamp: None,
        })
    }

    /// record the foreknowledge of an expected src did
    fn set_known_did(&mut self, did: String) -> Result<()> {
        if !self.labeler_dids.is_empty() {
            bail!("label store already knows of a labeler did");
        }
        let prior_seq = db::seq_for_src(self, &did)?;
        self.labeler_dids.insert(did, prior_seq);
        Ok(())
    }

    fn process_labels(
        &mut self,
        labels: Vec<LabelRecord>,
        now: DateTime,
        retreading: bool,
    ) -> Result<()> {
        let batch_seq = if let Some(label) = labels.first() {
            label.dbkey.seq
        } else {
            return Ok(());
        };
        // keep track of the latest create timestamp
        self.latest_create_timestamp = labels
            .iter()
            .map(|l| &l.create_timestamp)
            .chain(&self.latest_create_timestamp)
            .max()
            .cloned();
        // most of the time we expect labelers to only output records from a single did, which
        // should always be the did that we probably looked up to find it. however, we didn't
        // necessarily look up the labeler, and there's actually nothing about the data that
        // enforces that it couldn't be wildly admixed.
        let batch_src_dids: Vec<String> = labels
            .iter()
            .map(|label| &label.dbkey.key.src)
            .unique()
            .cloned()
            .collect();
        // old label records to be copied out as suspicious record entries
        let mut suspicious_old_records: Vec<(LabelDbKey, &str)> = vec![];
        // label records to be recorded as first seen this time, paired with the problem string
        let mut suspicious_new_records: Vec<(LabelRecord, &str)> = vec![];
        // label records to be updated in place
        let mut update_labels: Vec<LabelRecord> = vec![];
        // label records to be inserted
        let mut insert_labels: Vec<LabelRecord> = vec![];
        // indexes the old records by their database key
        let mut indexed_old_records: HashMap<LabelDbKey, LabelRecord> = Default::default();

        // process retreading logic for each labeler src did we know about, one src at a time.
        // usually this will mean everything in a single pass
        for src in &batch_src_dids {
            if !self.labeler_dids.contains(src) {
                // if labeler_dids is already empty, it's probably because we went directly to the
                // labeler service without actually knowing what the labeler did is supposed to be
                // first.
                if !self.labeler_dids.is_empty() {
                    println!("WARNING -- additional did appeared in label stream: {src}");
                }
                self.labeler_dids
                    .insert(src.clone(), db::seq_for_src(self, src)?);
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
                indexed_old_records.extend(
                    LabelRecord::load_known_range(&self, src, seq_range)?
                        .into_iter()
                        .map(|disappeared| (disappeared.dbkey.clone(), disappeared)),
                );
            }
        }

        // we want to cover these scenarios:
        //
        // 1. seq is greater than we expect to see from any old records
        //  * all records will be inserted
        // 2. a new and old record exist for the same db key, and match exactly
        //  * the record will be updated only to bump its last seen
        // 3. a new and old record exist for the same db key, but they differ
        //  * the old record should get a suspicious entry as "replaced"
        //  * the new record gets a suspicious entry as "replacing"
        //  * the record will be updated
        // 4. a new record exists but an old one does not
        //  * the new record gets a suspicious entry as "new"
        //  * the new record will be inserted
        // 5. an old record exists but a new one does not
        //  5a. ...and the old record, or the record it was negating, were expired
        //    * no action, does not count as disappeared
        //  5b. ...and the old record, or the record it was negating, were NOT expired
        //    * add the old record to self.disappeared_old_records
        //    * if by the end of the update it has not been matched, emit a suspicious entry as
        //      "disappeared-while-effective"
        //    * TODO(widders): maybe we always sus negatives disappearing, as they are never
        //       supposed to move according to the docs
        //
        // TODO(widders):
        // regardless of the above logic, all new & old records seen should continue to modulate the
        // record in self.disappeared_old_records to mark what has been superceded in the current
        // view.
        //
        // we don't need to cover these scenarios:
        //
        // 1. new labels don't have unique keys. unclear what we do with this for now, maybe we will
        //    just keep the last of each in the seq, but it's not even very clear from the docs
        //    whether you're supposed to be able to emit successive positive records to update the
        //    fine details of a label without negating it, either.

        for new_label in labels {
            // TODO(widders): we can validate that an old record is valid to have disappeared like
            //  this, but we need to actually know that it should have been there first.
            //  before we do this we need to add the indexed old records to disappeared old records,
            //  and we may want to do that in seq order
            // self.disappeared_old_records.remove(new_label.borrow());

            if self.labeler_dids.get(&new_label.dbkey.key.src).unwrap() < &new_label.dbkey.seq {
                // case 1
                insert_labels.push(new_label);
                continue;
            }
            // subtract the received labels in this seq from the old labels
            match indexed_old_records.remove(new_label.borrow()) {
                Some(old_label) if old_label == new_label => {
                    // case 2
                    update_labels.push(new_label);
                }
                Some(mismatched_old_label) => {
                    // case 3
                    suspicious_old_records.push((mismatched_old_label.dbkey, "replaced"));
                    suspicious_new_records.push((new_label.clone(), "replacing"));
                    update_labels.push(new_label);
                    self.suspicious_new_records += 1; // TODO(widders): update this stat later
                }
                None => {
                    // case 4
                    suspicious_new_records.push((new_label.clone(), "new"));
                    insert_labels.push(new_label);
                }
            }
        }
        // catalog all the old label records that we didn't see again this time
        for disappeared in indexed_old_records.into_values() {
            if disappeared.neg {
                match self.disappeared_old_records.get(disappeared.borrow()) {
                    Some(positive) if positive.is_expired(&now) => {
                        // the negation record disappeared, but the record it was negating
                        // was expired
                        self.disappeared_old_records.remove(disappeared.borrow());
                    }
                    _ => {
                        suspicious_old_records
                            .push((disappeared.dbkey.clone(), "disappeared-while-effective"));
                        self.disappeared_negative_records += 1;
                    }
                }
            } else {
                // positive records all go into the disappeared log
                self.disappeared_old_records
                    .insert(disappeared.dbkey.key.clone(), disappeared);
            }
        }

        // write all the updates for this batch in one transaction
        let tx = self.store.transaction()?;
        for (label_db_id, problem) in suspicious_old_records {
            label_db_id.suspicious_from_old_record(&tx, problem, self.import_id, &now)?;
        }
        for (new_record, problem) in suspicious_new_records {
            new_record.suspicious(&tx, problem, self.import_id, &now)?
        }
        for update_label in update_labels {
            update_label.update(&tx, self.import_id, &now)?;
        }
        for insert_label in insert_labels {
            insert_label.insert(&tx, self.import_id, &now)?;
        }
        tx.commit()?;

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
        for (did, _) in self.labeler_dids.into_iter().sorted() {
            println!("   {did}");
        }
        println!();

        if !self.disappeared_old_records.is_empty() {
            println!(
                "XX --> some label records that were observed on prior runs were not seen this \
                time and may otherwise have been taking effect, because no records that supercede \
                them appeared! SUSPICIOUS"
            );
            for disappeared_record in self.disappeared_old_records {
                println!("XX --> disappeared record: {disappeared_record:#?}");
            }
        }

        if self.disappeared_negative_records > 0 {
            println!(
                "XX --> a total of {} negation records disappeared from the stream that were \
                probably still taking effect by negating labels that have not expired! (these were \
                fully logged up above.) SUSPICIOUS",
                self.disappeared_negative_records,
            );
        }

        if self.suspicious_new_records > 0 {
            println!(
                "XX --> a total of {} new records appeared in the stream that hadn't been seen in \
                prior passes (logged up above). SUSPICIOUS",
                self.suspicious_new_records,
            );
        }

        println!("--------------------");
        todo!("summarize")
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
