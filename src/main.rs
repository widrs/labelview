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
    /// Domain name of the labeler to read from
    #[arg(required = true, value_name = "DOMAIN-NAME")]
    domain_name: String,
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
        // TODO(widders): get the service domain from the did service record
        let domain_name = &self.domain_name;
        // TODO(widders): catch-up or re-tread data we already have to look for changes
        let address = Url::parse(&format!(
            "wss://{domain_name}/xrpc/com.atproto.label.subscribeLabels?cursor=0"
        ))?;
        if address.domain() != Some(domain_name) {
            bail!("invalid domain")
        }
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
