use anyhow::{anyhow, bail, Result};
use clap::{Args, Parser, Subcommand};
use futures_util::StreamExt;
use labelview::{get_data_dir, LabelRecord};
use serde::Deserialize;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

#[derive(Debug, Parser)]
#[command(version)]
enum Command {
    /// Actions for the data directory
    #[clap(subcommand)]
    Data(ConfigCmd),
    /// Reads the labels from a labeler service
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
                labelview::connect()?;
                println!("ok");
                Ok(())
            }
        }
    }
}

#[derive(Debug, Args)]
struct GetCmd {
    /// Handle or DID of the labeler to read from
    #[arg(required = true, value_name = "HANDLE-OR-DID")]
    handle_or_did: String,
    /// Entryway service to use for did lookups
    #[arg(long, default_value = "bsky.social", value_name = "ENTRYWAY-SERVICE")]
    entryway_service: String,
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
        let mut db = labelview::connect()?;

        // read the did document from the entryway to get the service endpoints for the labeler
        let http_client = reqwest::Client::new();
        print!("looking up did...");
        let entryway_service = &self.entryway_service;
        let did_lookup = http_client
            .get(format!(
                "https://{entryway_service}/xrpc/com.atproto.repo.describeRepo"
            ))
            .query(&[("repo", self.handle_or_did.as_str())])
            .send()
            .await
            .map_err(|e| anyhow!("error fetching did: {e}"))?;

        // https://docs.bsky.app/docs/api/com-atproto-repo-describe-repo
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct DidDescription {
            handle: String,
            did: String,
            handle_is_correct: bool,
            did_doc: atrium_api::did_doc::DidDocument,
            #[allow(dead_code)]
            collections: Vec<String>,
        }

        // parse the json response
        let desc: DidDescription = serde_json::from_slice(
            &did_lookup
                .bytes()
                .await
                .map_err(|e| anyhow!("error reading did from response: {e}"))?,
        )
        .map_err(|e| anyhow!("error parsing did document from entryway: {e}"))?;
        println!("ok");

        // read the handle, did, and pds & labeler endpoint urls from the response
        let DidDescription {
            handle,
            did,
            handle_is_correct,
            ..
        } = &desc;
        let pds = desc.did_doc.service.iter().flatten().find_map(|service| {
            if service.id.ends_with("#atproto_pds") && service.r#type == "AtprotoPersonalDataServer"
            {
                Some(service.service_endpoint.clone())
            } else {
                None
            }
        });
        let labeler = desc.did_doc.service.iter().flatten().find_map(|service| {
            if service.id.ends_with("#atproto_labeler") && service.r#type == "AtprotoLabeler" {
                Some(service.service_endpoint.clone())
            } else {
                None
            }
        });
        println!("handle: {handle}");
        println!("did:    {did}");
        println!("handle is correct: {handle_is_correct:?}");
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
            .map_err(|e| anyhow!("could not parse labeler endpoint as url: {e}"))?;
        let Some(labeler_domain) = labeler_url.domain() else {
            bail!("labeler endpoint url does not seem to specify a domain");
        };

        println!();
        println!("connecting to labeler service");
        // TODO(widders): catch-up or re-tread data we already have to look for changes
        let address = Url::parse(&format!(
            "wss://{labeler_domain}/xrpc/com.atproto.label.subscribeLabels?cursor=0"
        ))?;
        let (stream, _response) = connect_async(&address).await?;
        let (_write, mut read) = stream.split();
        // TODO(widders): progress bar?
        loop {
            let Some(message) = read.next().await else {
                continue;
            };
            match message.map_err(|e| anyhow!("error reading websocket message: {e}"))? {
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
                        for label in labels {
                            label
                                .save(&mut db)
                                .map_err(|e| anyhow!("error saving label record: {e}"))?;
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
            // TODO(widders): timeout when stream catches up and crawls
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    match Command::parse() {
        Command::Data(cmd) => cmd.go().await,
        Command::Get(cmd) => cmd.go().await,
    }
}
