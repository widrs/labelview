use anyhow::{anyhow, bail, Result};
use clap::{Args, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(version)]
struct LabelView {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Get(GetCmd),
}

#[derive(Debug, Args)]
struct GetCmd {
    /// Domain name of the labeler to read from
    #[arg(required = true, value_name = "DOMAIN-NAME")]
    domain_name: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    match LabelView::parse().command {
        Command::Get(get_cmd) => {
            let domain = get_cmd.domain_name.as_str();
            println!("get {domain}");
        }
    }
    Ok(())
}
