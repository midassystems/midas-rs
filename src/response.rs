// use axum::{
//     http::StatusCode,
//     response::{IntoResponse, Response},
//     Json,
// };
use derive_more::From;
use reqwest;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Deserialize, Serialize)]
pub struct ApiResponse<T> {
    pub status: String,
    pub message: String,
    pub code: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
}

// #[derive(Debug, Error)]
// pub enum Error {
//     #[error("SQL error: {0}")]
//     SqlError(String),
//     #[error("JSON error: {0}")]
//     JsonError(#[from] serde_json::Error), // Other error types
//     #[error("Request error: {0}")]
//     RequestError(#[from] reqwest::Error),
// }

// pub type Result<T> = std::result::Result<T, Error>;
