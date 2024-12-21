use reqwest::{Response, StatusCode};
use serde::{Deserialize, Serialize};

pub trait ApiDefault {
    fn default_value() -> Self;
}

impl ApiDefault for i32 {
    fn default_value() -> Self {
        0
    }
}
impl ApiDefault for u32 {
    fn default_value() -> Self {
        0
    }
}

impl ApiDefault for String {
    fn default_value() -> Self {
        "".to_string()
    }
}

impl<T> ApiDefault for Vec<T> {
    fn default_value() -> Self {
        vec![]
    }
}

impl<T> ApiDefault for Option<T> {
    fn default_value() -> Self {
        None
    }
}

#[derive(Debug, Deserialize)]
pub struct RawApiResponse {
    pub status: String,
    pub message: String,
    pub code: u16,
}

impl<T: serde::de::DeserializeOwned + ApiDefault> Into<ApiResponse<T>> for RawApiResponse {
    fn into(self) -> ApiResponse<T> {
        ApiResponse::with_default(&self.status, &self.message, self.code)
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct ApiResponse<T> {
    pub status: String,
    pub message: String,
    pub code: u16,
    pub data: T,
}

impl<T: serde::de::DeserializeOwned + ApiDefault> ApiResponse<T> {
    pub fn new(status: &str, message: &str, code: StatusCode, data: T) -> Self {
        Self {
            status: status.to_string(),
            message: message.to_string(),
            code: code.as_u16(),
            data,
        }
    }

    pub async fn from_response(response: Response) -> crate::Result<ApiResponse<T>> {
        // Read the body as a string first to avoid consuming it multiple times
        let body = response.text().await?;

        // Try to deserialize as ApiResponse
        match serde_json::from_str::<ApiResponse<T>>(&body) {
            Ok(api_response) => Ok(api_response),
            Err(_) => {
                // Fallback: Deserialize into RawApiResponse or log the raw response
                let raw_response: RawApiResponse = serde_json::from_str(&body)?;
                let api_response: ApiResponse<T> = raw_response.into();
                Ok(api_response)
            }
        }
    }
    pub fn with_default(status: &str, message: &str, code: u16) -> Self {
        Self {
            status: status.to_string(),
            message: message.to_string(),
            code,
            data: T::default_value(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_response() {
        let status = "success";
        let msg = "message";
        let code = StatusCode::OK;
        let response = ApiResponse::new(status, msg, code, "".to_string());

        // Test
        assert_eq!(response.status, status);
        assert_eq!(response.code, 200);
        assert_eq!(response.message, msg);
        assert_eq!(response.data, "");
    }

    #[test]
    fn test_response_from_json() {
        let json = serde_json::json!({"status": "success", "message": "Testing msg.", "code" : 200, "data": "12345"}).to_string();

        // Test
        let api_response: ApiResponse<String> = serde_json::from_str(&json).unwrap();

        // Validate
        let status = "success";
        let msg = "Testing msg.";
        let code = StatusCode::OK;
        let response = ApiResponse::new(status, msg, code, "12345".to_string());
        assert_eq!(response, api_response);
    }
}
