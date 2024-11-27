use std::path::PathBuf;

use anyhow::{anyhow, Result};
use sqlx::Connection;

pub fn get_data_dir() -> Result<PathBuf> {
    directories::ProjectDirs::from("", "", "labelview")
        .map(|dirs| dirs.data_dir().to_path_buf())
        .ok_or(anyhow!("could not find data directory"))
}

async fn get_database() -> Result<sqlx::SqliteConnection> {
    let data_dir = get_data_dir()?;
    std::fs::create_dir_all(&data_dir)
        .map_err(|err| anyhow!("couldn't create data directory {data_dir:?}: {err}"))?;
    Ok(sqlx::SqliteConnection::connect(
        url::Url::from_file_path(data_dir.join("data.sqlite"))
            .map_err(|()| anyhow!("problem building database path"))?
            .as_str(),
    ).await?)
}
