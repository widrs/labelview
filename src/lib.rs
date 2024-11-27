use std::path::PathBuf;

use anyhow::{anyhow, Result};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqliteSynchronous},
    Connection,
};

pub fn get_data_dir() -> Result<PathBuf> {
    directories::ProjectDirs::from("", "", "labelview")
        .map(|dirs| dirs.data_dir().to_path_buf())
        .ok_or(anyhow!("could not find data directory"))
}

pub async fn get_database() -> Result<sqlx::SqliteConnection> {
    let data_dir = get_data_dir()?;
    std::fs::create_dir_all(&data_dir)
        .map_err(|err| anyhow!("couldn't create data directory {data_dir:?}: {err}"))?;
    Ok(sqlx::SqliteConnection::connect_with(
        &SqliteConnectOptions::new()
            .filename(data_dir.join("data.sqlite"))
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal),
    )
    .await?)
}
