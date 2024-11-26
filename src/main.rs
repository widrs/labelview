use anyhow::{bail, Result};
use cbor4ii::core::Value;
use cbor4ii::core::dec::{Decode, Read};
use cbor4ii::core::utils::SliceReader;
use clap::{Args, Parser};
use futures_util::StreamExt;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use url::Url;

#[derive(Debug, Parser)]
#[command(version)]
enum Command {
    /// Reads the labels from a labeler service
    Get(GetCmd),
}

#[derive(Debug, Args)]
struct GetCmd {
    /// Domain name of the labeler to read from
    #[arg(required = true, value_name = "DOMAIN-NAME")]
    domain_name: String,
}

impl GetCmd {
    async fn go(self) -> Result<()> {
        // TODO(widders): get the service domain from the did service record
        let domain_name = &self.domain_name;
        let address = Url::parse(&format!(
            "wss://{domain_name}/xrpc/com.atproto.label.subscribeLabels?cursor=0"
        ))?;
        if address.domain() != Some(domain_name) {
            bail!("invalid domain")
        }
        let (stream, _response) = connect_async(&address).await?;
        let (_write, mut read) = stream.split();
        loop {
            let Some(message) = read.next().await else {
                continue;
            };
            match message? {
                Message::Text(text) => {
                    println!("text message: {text}")
                }
                Message::Binary(bin) => {
                    let mut reader = SliceReader::new(&bin);
                    let header = Value::decode(&mut reader)?;
                    let body = Value::decode(&mut reader)?;
                    let extra = if reader.fill(1)?.as_ref().is_empty() {
                        ""
                    } else {
                        ", EXTRA DATA!"
                    };
                    println!("bin message: {header:?}, {body:?}{extra}");
                }
                _ => {}
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    match Command::parse() {
        Command::Get(get_cmd) => get_cmd.go().await,
    }
}
