use anyhow::{anyhow, bail, Result};
use cbor4ii::core::{
    dec::{Decode, Read},
    utils::SliceReader,
    Value,
};
use clap::{Args, Parser, Subcommand};
use futures_util::StreamExt;
use labelview::{get_data_dir, LabelRecord};
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
    fn header_type(bin: &mut SliceReader) -> Result<String> {
        // TODO(widders): maybe move this into the lib
        let mut bailing = false;
        let mut error_kind = None;
        let mut error_msg = None;
        let mut header_ty = None;
        match Value::decode(bin).map_err(|e| anyhow!("error decoding event stream header: {e}"))? {
            Value::Map(items) => {
                for item in items {
                    let (Value::Text(ref k), v) = item else {
                        bail!("non-string key in event stream header");
                    };
                    match (k.as_str(), v) {
                        ("op", v) => {
                            if let Value::Integer(i) = v {
                                if i != 1 {
                                    bailing = true;
                                }
                            } else {
                                bail!("malformed event stream header op key");
                            }
                        }
                        ("t", Value::Text(ty)) => header_ty = Some(ty),
                        ("error", Value::Text(err)) => error_kind = Some(err),
                        ("message", Value::Text(msg)) => error_msg = Some(msg),
                        _ => {}
                    }
                }
            }
            _ => bail!("found a non-map value in place of the event stream header"),
        }
        if bailing {
            let error_1 = error_kind.as_deref().unwrap_or("(no error type)");
            let error_2 = error_msg.as_deref().unwrap_or("(no error message)");
            bail!("received an error from event stream: {error_1}: {error_2}");
        }
        header_ty.ok_or(anyhow!("missing type in event stream header"))
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
                    println!("text message: {text}")
                }
                Message::Binary(bin) => {
                    let mut reader = SliceReader::new(&bin);
                    // the schema for this endpoint is declared here:
                    // https://github.com/bluesky-social/atproto/blob/main/lexicons/com/atproto/label/subscribeLabels.json
                    let ty = Self::header_type(&mut reader)?;
                    if ty == "#labels" {
                        let labels = LabelRecord::from_subscription_record(&mut reader)?;
                        for label in labels {
                            label
                                .save(&mut db)
                                .map_err(|e| anyhow!("error saving label record: {e}"))?;
                        }
                    } else {
                        let body = Value::decode(&mut reader)?;
                        let extra = if reader.fill(1)?.as_ref().is_empty() {
                            ""
                        } else {
                            ", EXTRA DATA!"
                        };
                        println!("bin message with type {ty:?}: {body:?}{extra}");
                    }
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
