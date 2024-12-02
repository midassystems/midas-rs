use crate::error::Result;
use crate::response::ApiResponse;
use mbn::{backtest::BacktestData, live::LiveData};
use reqwest::StatusCode;
use reqwest::{self, Client, ClientBuilder};
use std::time::Duration;

#[derive(Clone)]
pub struct Trading {
    base_url: String,
    client: Client,
}

impl Trading {
    pub fn new(base_url: &str) -> Self {
        let client = ClientBuilder::new()
            .timeout(Duration::from_secs(20000)) // Set timeout to 120 seconds
            .build()
            .expect("Failed to build HTTP client");

        Trading {
            base_url: base_url.to_string(),
            client,
        }
    }

    fn url(&self, endpoint: &str) -> String {
        format!(
            "{}{}{}",
            self.base_url,
            "/trading/".to_string(),
            endpoint.to_string()
        )
    }

    // Live
    pub async fn create_live(&self, data: &LiveData) -> Result<ApiResponse<i32>> {
        let url = self.url("live/create");
        let response = self.client.post(&url).json(data).send().await?;

        // Check for HTTP status
        if response.status() != StatusCode::OK {
            // Deserialize the API response and return it, even if it indicates failure
            return ApiResponse::<i32>::from_response(response).await;
        }

        let api_response = ApiResponse::<i32>::from_response(response).await?;
        Ok(api_response)
    }

    pub async fn list_live(&self) -> Result<ApiResponse<Vec<(i32, String)>>> {
        let url = self.url("live/list");
        let response = self.client.get(&url).send().await?;

        // Check for HTTP status
        if response.status() != StatusCode::OK {
            // Deserialize the API response and return it, even if it indicates failure
            return ApiResponse::<Vec<(i32, String)>>::from_response(response).await;
        }

        let api_response = ApiResponse::<Vec<(i32, String)>>::from_response(response).await?;
        Ok(api_response)
    }

    pub async fn delete_live(&self, id: &i32) -> Result<ApiResponse<String>> {
        let url = self.url("live/delete");
        let response = self.client.delete(&url).json(id).send().await?;

        if response.status() != StatusCode::OK {
            // Deserialize the API response and return it, even if it indicates failure
            return ApiResponse::<String>::from_response(response).await;
        }

        let api_response = ApiResponse::<String>::from_response(response).await?;
        Ok(api_response)
    }

    pub async fn get_live(&self, id: &i32) -> Result<ApiResponse<Vec<LiveData>>> {
        let url = self.url(&format!("live/get?id={}", id));
        let response = self.client.get(&url).send().await?;

        // Check for HTTP status
        if response.status() != StatusCode::OK {
            // Deserialize the API response and return it, even if it indicates failure
            return ApiResponse::<Vec<LiveData>>::from_response(response).await;
        }

        let api_response = ApiResponse::<Vec<LiveData>>::from_response(response).await?;
        Ok(api_response)
    }

    // Backtest
    pub async fn create_backtest(&self, backtest: &BacktestData) -> Result<ApiResponse<i32>> {
        let url = self.url("backtest/create");
        let response = self.client.post(&url).json(backtest).send().await?;

        if response.status() != StatusCode::OK {
            // Deserialize the API response and return it, even if it indicates failure
            return ApiResponse::<i32>::from_response(response).await;
        }

        let api_response = ApiResponse::<i32>::from_response(response).await?;
        Ok(api_response)
    }

    pub async fn list_backtest(&self) -> Result<ApiResponse<Vec<(i32, String)>>> {
        let url = self.url("backtest/list");
        let response = self.client.get(&url).send().await?;

        // Check for HTTP status
        if response.status() != StatusCode::OK {
            // Deserialize the API response and return it, even if it indicates failure
            return ApiResponse::<Vec<(i32, String)>>::from_response(response).await;
        }

        let api_response = ApiResponse::<Vec<(i32, String)>>::from_response(response).await?;
        Ok(api_response)
    }

    pub async fn delete_backtest(&self, id: &i32) -> Result<ApiResponse<String>> {
        let url = self.url("backtest/delete");
        let response = self.client.delete(&url).json(id).send().await?;

        // Check for HTTP status
        if response.status() != StatusCode::OK {
            // Deserialize the API response and return it, even if it indicates failure
            return ApiResponse::<String>::from_response(response).await;
        }

        let api_response = ApiResponse::<String>::from_response(response).await?;
        Ok(api_response)
    }

    pub async fn get_backtest(&self, id: &i32) -> Result<ApiResponse<Vec<BacktestData>>> {
        let url = self.url(&format!("backtest/get?id={}", id));
        let response = self.client.get(&url).send().await?;

        // Check for HTTP status
        if response.status() != StatusCode::OK {
            // Deserialize the API response and return it, even if it indicates failure
            return ApiResponse::<Vec<BacktestData>>::from_response(response).await;
        }

        let api_response = ApiResponse::<Vec<BacktestData>>::from_response(response).await?;
        Ok(api_response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dotenv::dotenv;
    use regex::Regex;
    use serial_test::serial;
    use std::fs;

    fn get_id_from_string(message: &str) -> Option<i32> {
        let re = Regex::new(r"\d+$").unwrap();

        if let Some(captures) = re.captures(message) {
            if let Some(matched) = captures.get(0) {
                let number: i32 = matched.as_str().parse().unwrap();
                return Some(number);
            }
        }
        None
    }

    #[tokio::test]
    #[serial]
    // #[ignore]
    async fn test_create_backtest() -> Result<()> {
        dotenv().ok();
        let base_url = std::env::var("TRADING_URL").expect("Expected database_url.");
        let client = Trading::new(&base_url);

        // Pull test data
        let mock_data =
            fs::read_to_string("tests/data/test_data.backtest.json").expect("Unable to read file");
        let backtest_data: BacktestData =
            serde_json::from_str(&mock_data).expect("JSON was not well-formatted");

        // Test
        let response = client.create_backtest(&backtest_data).await?;

        // Validate
        assert_eq!(response.code, 200);
        assert_eq!(response.status, "success");

        // Cleanup
        let id = get_id_from_string(&response.message).expect("Error getting id from message.");
        let _ = client.delete_backtest(&id).await?;

        Ok(())
    }

    #[tokio::test]
    #[serial]
    // #[ignore]
    async fn test_list_backtest() -> Result<()> {
        dotenv().ok();
        let base_url = std::env::var("TRADING_URL").expect("Expected database_url.");
        let client = Trading::new(&base_url);

        // Pull test data
        let mock_data =
            fs::read_to_string("tests/data/test_data.backtest.json").expect("Unable to read file");
        let backtest_data: BacktestData =
            serde_json::from_str(&mock_data).expect("JSON was not well-formatted");

        let response = client.create_backtest(&backtest_data).await?;
        let id = get_id_from_string(&response.message).expect("Error getting id from message.");

        // Test
        let response = client.list_backtest().await?;

        // Validate
        assert_eq!(response.code, 200);
        assert_eq!(response.status, "success");

        // Cleanup
        let _ = client.delete_backtest(&id).await?;

        Ok(())
    }

    #[tokio::test]
    #[serial]
    // #[ignore]
    async fn test_get_backtest() -> Result<()> {
        dotenv().ok();
        let base_url = std::env::var("TRADING_URL").expect("Expected database_url.");
        let client = Trading::new(&base_url);

        // Pull test data
        let mock_data =
            fs::read_to_string("tests/data/test_data.backtest.json").expect("Unable to read file");
        let backtest_data: BacktestData =
            serde_json::from_str(&mock_data).expect("JSON was not well-formatted");

        let response = client.create_backtest(&backtest_data).await?;
        let id = get_id_from_string(&response.message).expect("Error getting id from message.");

        // Test
        let response = client.get_backtest(&id).await?;

        // Validate
        assert_eq!(response.code, 200);
        assert_eq!(response.status, "success");

        // Cleanup
        let _ = client.delete_backtest(&id).await?;

        Ok(())
    }

    #[tokio::test]
    #[serial]
    // #[ignore]
    async fn test_create_live() -> Result<()> {
        dotenv().ok();
        let base_url = std::env::var("TRADING_URL").expect("Expected database_url.");
        let client = Trading::new(&base_url);

        // Pull test data
        let mock_data =
            fs::read_to_string("tests/data/test_data.live.json").expect("Unable to read file");
        let live_data: LiveData =
            serde_json::from_str(&mock_data).expect("JSON was not well-formatted");

        // Test
        let response = client.create_live(&live_data).await?;

        // Validate
        assert_eq!(response.code, 200);
        assert_eq!(response.status, "success");

        // Cleanup
        let id = get_id_from_string(&response.message).expect("Error getting id from message.");
        let _ = client.delete_live(&id).await?;

        Ok(())
    }

    #[tokio::test]
    #[serial]
    // #[ignore]
    async fn test_list_live() -> Result<()> {
        dotenv().ok();
        let base_url = std::env::var("TRADING_URL").expect("Expected database_url.");
        let client = Trading::new(&base_url);

        // Pull test data
        let mock_data =
            fs::read_to_string("tests/data/test_data.live.json").expect("Unable to read file");
        let live_data: LiveData =
            serde_json::from_str(&mock_data).expect("JSON was not well-formatted");

        let response = client.create_live(&live_data).await?;
        let id = get_id_from_string(&response.message).expect("Error getting id from message.");

        // Test
        let response = client.list_live().await?;

        // Validate
        assert_eq!(response.code, 200);
        assert_eq!(response.status, "success");

        // Cleanup
        let _ = client.delete_live(&id).await?;

        Ok(())
    }

    #[tokio::test]
    #[serial]
    // #[ignore]
    async fn test_get_live() -> Result<()> {
        dotenv().ok();
        let base_url = std::env::var("TRADING_URL").expect("Expected database_url.");
        let client = Trading::new(&base_url);

        // Pull test data
        let mock_data =
            fs::read_to_string("tests/data/test_data.live.json").expect("Unable to read file");
        let live_data: LiveData =
            serde_json::from_str(&mock_data).expect("JSON was not well-formatted");

        let response = client.create_live(&live_data).await?;
        let id = get_id_from_string(&response.message).expect("Error getting id from message.");

        // Test
        let response = client.get_live(&id).await?;

        // Validate
        assert_eq!(response.code, 200);
        assert_eq!(response.status, "success");

        // Cleanup
        let _ = client.delete_live(&id).await?;

        Ok(())
    }
}
