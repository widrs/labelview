use anyhow::{anyhow, bail, Result};
use cbor4ii::core::{
    dec::{Decode, Read},
    utils::SliceReader,
    Value,
};
use clap::{Args, Parser, Subcommand};
use futures_util::StreamExt;
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
                let data_dir = labelview::get_data_dir()?.display().to_string();
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
        let mut bailing = false;
        let mut error_kind = None;
        let mut error_msg = None;
        let mut header_ty = None;
        match Value::decode(bin)? {
            Value::Map(items) => {
                for item in items {
                    match item {
                        (Value::Text(k), v) if k == "op" => {
                            if let Value::Integer(i) = v {
                                if i != 1 {
                                    bailing = true;
                                }
                            } else {
                                bail!("malformed event stream header op key");
                            }
                        }
                        (Value::Text(k), Value::Text(ty)) if k == "t" => {
                            header_ty = Some(ty);
                        }
                        (Value::Text(k), Value::Text(err)) if k == "error" => {
                            error_kind = Some(err);
                        }
                        (Value::Text(k), Value::Text(msg)) if k == "message" => {
                            error_msg = Some(msg);
                        }
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
                    let ty = Self::header_type(&mut reader)?;
                    let body = Value::decode(&mut reader)?;
                    let extra = if reader.fill(1)?.as_ref().is_empty() {
                        ""
                    } else {
                        ", EXTRA DATA!"
                    };
                    println!("bin message with type {ty:?}: {body:?}{extra}");
                }
                _ => {}
            }
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
