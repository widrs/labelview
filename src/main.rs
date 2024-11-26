use anyhow::{bail, Result};
use clap::{Args, Parser};
use tokio_tungstenite::connect_async;
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
        let domain_name = &self.domain_name;
        let address = Url::parse(&format!(
            "wss://{domain_name}/com.atproto.label.subscribeLabels?cursor=0"
        ))?;
        if address.domain() != Some(domain_name) {
            bail!("invalid domain")
        }
        let (stream, _response) = connect_async(&address).await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    match Command::parse() {
        Command::Get(get_cmd) => get_cmd.go().await,
    }
}
