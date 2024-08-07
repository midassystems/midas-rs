use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct ApiResponse<T> {
    pub status: String,
    pub message: String,
    pub code: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
}
