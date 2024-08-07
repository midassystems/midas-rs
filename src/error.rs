use reqwest;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("SQL error: {0}")]
    SqlError(String),
    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),
    #[error("Request error: {0}")]
    RequestError(#[from] reqwest::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
