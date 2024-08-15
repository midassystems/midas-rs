use chrono;
use reqwest;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("SQL error: {0}")]
    SqlError(String),
    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),
    #[error("Parse Error : {0}")]
    ParseError(#[from] chrono::ParseError),
    #[error("Io error: {0}")]
    IOError(#[from] std::io::Error),
    #[error("Request error: {0}")]
    RequestError(#[from] reqwest::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
