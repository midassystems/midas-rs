use axum::http::StatusCode;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ApiResponse<T> {
    pub status: String,
    pub message: String,
    pub code: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
}

impl<T> ApiResponse<T> {
    pub fn new(status: &str, message: &str, code: StatusCode, data: Option<T>) -> Self {
        Self {
            status: status.to_string(),
            message: message.to_string(),
            code: code.as_u16(),
            data,
        }
    }
}
