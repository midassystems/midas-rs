use crate::response::ApiResponse;
use crate::{error::Error, error::Result, utils::date_to_unix_nanos};
use axum::http::StatusCode;
use futures_util::StreamExt;
use mbn::backtest::BacktestData;
use mbn::symbols::Instrument;
use reqwest::{self, Client, ClientBuilder};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Write;
use std::time::Duration;

#[derive(Debug, Serialize, Deserialize)]
pub struct RetrieveParams {
    pub symbols: Vec<String>,
    pub start_ts: i64,
    pub end_ts: i64,
    pub schema: String,
}

impl RetrieveParams {
    pub fn new(symbols: Vec<String>, start: &str, end: &str, schema: &str) -> Result<Self> {
        Ok(RetrieveParams {
            symbols,
            start_ts: date_to_unix_nanos(start)?,
            end_ts: date_to_unix_nanos(end)?,
            schema: schema.to_string(),
        })
    }
}

pub struct ApiClient {
    base_url: String,
    client: Client,
}

impl ApiClient {
    pub fn new(base_url: &str) -> Self {
        let client = ClientBuilder::new()
            .timeout(Duration::from_secs(1000)) // Set timeout to 120 seconds
            .build()
            .expect("Failed to build HTTP client");

        ApiClient {
            base_url: base_url.to_string(),
            client,
        }
    }

    fn url(&self, endpoint: &str) -> String {
        format!("{}{}", self.base_url, endpoint.to_string())
    }

    // Instruments
    pub async fn create_symbol(&self, instrument: &Instrument) -> Result<ApiResponse<u32>> {
        let url = self.url("/market_data/instruments/create");
        let response = self
            .client
            .post(&url)
            .json(instrument)
            .send()
            .await?
            .text()
            .await?;

        let api_response: ApiResponse<u32> = serde_json::from_str(&response)?;
        Ok(api_response)
    }

    pub async fn get_symbol(&self, ticker: &String) -> Result<ApiResponse<u32>> {
        let url = self.url("/market_data/instruments/get");
        let response = self
            .client
            .get(&url)
            .json(ticker)
            .send()
            .await?
            .text()
            .await?;

        let api_response: ApiResponse<u32> = serde_json::from_str(&response)?;
        Ok(api_response)
    }

    pub async fn delete_symbol(&self, id: &i32) -> Result<ApiResponse<()>> {
        let url = self.url("/market_data/instruments/delete");
        let response = self
            .client
            .delete(&url)
            .json(id)
            .send()
            .await?
            .text()
            .await?;

        let api_response: ApiResponse<()> = serde_json::from_str(&response)?;
        Ok(api_response)
    }

    pub async fn list_symbols(&self) -> Result<ApiResponse<Vec<Instrument>>> {
        let url = self.url("/market_data/instruments/list");
        let response = self.client.get(&url).send().await?.text().await?;
        let api_response: ApiResponse<Vec<Instrument>> = serde_json::from_str(&response)?;

        Ok(api_response)
    }

    pub async fn update_symbol(
        &self,
        instrument: &Instrument,
        id: &i32,
    ) -> Result<ApiResponse<()>> {
        let url = self.url("/market_data/instruments/update");
        let response = self
            .client
            .put(&url)
            .json(&(instrument, id))
            .send()
            .await?
            .text()
            .await?;

        let api_response: ApiResponse<()> = serde_json::from_str(&response)?;
        Ok(api_response)
    }

    // Backtest
    pub async fn create_backtest(&self, backtest: &BacktestData) -> Result<ApiResponse<i32>> {
        let url = self.url("/trading/backtest/create");
        let response = self
            .client
            .post(&url)
            .json(backtest)
            .send()
            .await?
            .text()
            .await?;

        let api_response: ApiResponse<i32> = serde_json::from_str(&response)?;
        Ok(api_response)
    }

    pub async fn list_backtest(&self) -> Result<ApiResponse<Vec<(i32, String)>>> {
        let url = self.url("/trading/backtest/list");
        let response = self.client.get(&url).send().await?.text().await?;

        let api_response: ApiResponse<Vec<(i32, String)>> = serde_json::from_str(&response)?;
        Ok(api_response)
    }

    pub async fn delete_backtest(&self, id: &i32) -> Result<ApiResponse<()>> {
        let url = self.url("/trading/backtest/delete");
        let response = self
            .client
            .delete(&url)
            .json(id)
            .send()
            .await?
            .text()
            .await?;

        let api_response: ApiResponse<()> = serde_json::from_str(&response)?;
        Ok(api_response)
    }

    pub async fn get_backtest(&self, id: &i32) -> Result<ApiResponse<BacktestData>> {
        let url = self.url(&format!("/trading/backtest/get?id={}", id));
        let response = self.client.get(&url).send().await?.text().await?;

        let api_response: ApiResponse<BacktestData> = serde_json::from_str(&response)?;
        Ok(api_response)
    }

    // Market data
    pub async fn create_mbp(&self, data: &[u8]) -> Result<ApiResponse<()>> {
        let url = self.url("/market_data/mbp/create");
        let response = self
            .client
            .post(&url)
            .json(data)
            .send()
            .await?
            .text()
            .await?;

        let api_response: ApiResponse<()> = serde_json::from_str(&response)?;
        Ok(api_response)
    }

    pub async fn create_mbp_from_file(&self, file_path: &str) -> Result<ApiResponse<()>> {
        let url = self.url("/market_data/mbp/bulk_upload");
        let response = self
            .client
            .post(&url)
            .json(file_path)
            .send()
            .await?
            .text()
            .await?;

        let api_response: ApiResponse<()> = serde_json::from_str(&response)?;
        Ok(api_response)
    }

    pub async fn get_records(&self, params: &RetrieveParams) -> Result<ApiResponse<Vec<u8>>> {
        let url = self.url("/market_data/mbp/get");
        let response = self.client.get(&url).json(params).send().await?;

        // Ensure the response is streamed properly
        let mut data = Vec::new();
        let mut stream = response.bytes_stream(); // Correct usage of bytes_stream here

        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => data.extend_from_slice(&bytes),
                Err(e) => {
                    println!("Error while receiving chunk: {:?}", e);
                    return Err(Error::from(e));
                }
            }
        }

        // Deserialize the data into the ApiResponse
        let api_response = ApiResponse::new("success", "", StatusCode::OK, Some(data));

        Ok(api_response)
    }

    pub async fn get_records_to_file(
        &self,
        params: &RetrieveParams,
        file_path: &str,
    ) -> Result<()> {
        let response = self.get_records(params).await?;

        // Create or open the file
        let mut file = File::create(file_path)?;

        // Write the binary data to the file
        file.write_all(&response.data.ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::Other, "Error with returned buffer")
        })?)?;

        // let api_response: ApiResponse<Vec<u8>> = serde_json::from_str(&response)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dotenv::dotenv;
    use mbn::decode::CombinedDecoder;
    use mbn::encode::RecordEncoder;
    use mbn::enums::{Action, Schema};
    use mbn::record_ref::RecordRef;
    use mbn::records::{BidAskPair, Mbp1Msg, RecordHeader};
    use mbn::symbols::Instrument;
    use regex::Regex;
    use serial_test::serial;
    use std::fs;
    use std::io::Cursor;

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
    async fn test_instrument_create() -> Result<()> {
        dotenv().ok();
        let base_url = std::env::var("DATABASE_URL").expect("Expected database_url.");
        let client = ApiClient::new(&base_url);

        let instrument = Instrument {
            ticker: "AAP00001".to_string(),
            name: "Apple tester client".to_string(),
            instrument_id: None,
        };

        // Test
        let response = client.create_symbol(&instrument).await?;
        let id = get_id_from_string(&response.message).expect("Error getting id from message.");

        // Validate
        assert_eq!(response.code, 200);
        assert_eq!(response.status, "success");

        // Cleanup
        let _ = client.delete_symbol(&id).await?;

        Ok(())
    }

    #[tokio::test]
    #[serial]
    // #[ignore]
    async fn test_get_instrument() -> Result<()> {
        dotenv().ok();
        let base_url = std::env::var("DATABASE_URL").expect("Expected database_url.");
        let client = ApiClient::new(&base_url);

        let instrument = Instrument {
            ticker: "AAPL2098".to_string(),
            name: "Apple tester client2".to_string(),
            instrument_id: None,
        };

        let response = client.create_symbol(&instrument).await?;
        let id = get_id_from_string(&response.message).expect("Error getting id from message.");

        // Test
        let response = client.get_symbol(&"AAPL2098".to_string()).await?;

        // Validate
        assert_eq!(response.code, 200);
        assert_eq!(response.status, "success");
        assert!(response.data.unwrap() > 0);

        // Cleanup
        let _ = client.delete_symbol(&id).await?;

        Ok(())
    }

    #[tokio::test]
    #[serial]
    // #[ignore]
    async fn test_get_instrument_none() -> Result<()> {
        dotenv().ok();
        let base_url = std::env::var("DATABASE_URL").expect("Expected database_url.");
        let client = ApiClient::new(&base_url);

        // Test
        let response = client.get_symbol(&"AAPL".to_string()).await?;

        // Validate
        assert_eq!(response.code, 404);
        assert_eq!(response.status, "success");

        Ok(())
    }

    #[tokio::test]
    #[serial]
    // #[ignore]
    async fn test_list_instruments() -> Result<()> {
        dotenv().ok();
        let base_url = std::env::var("DATABASE_URL").expect("Expected database_url.");
        let client = ApiClient::new(&base_url);

        let instrument = Instrument {
            ticker: "AAP0003".to_string(),
            name: "Apple tester client".to_string(),
            instrument_id: None,
        };

        let create_response = client.create_symbol(&instrument).await?;

        // Test
        let response = client.list_symbols().await?;

        // Validate
        assert_eq!(response.code, 200);
        assert_eq!(response.status, "success");

        // Cleanup
        let id =
            get_id_from_string(&create_response.message).expect("Error getting id from message.");
        let _ = client.delete_symbol(&id).await?;

        Ok(())
    }

    #[tokio::test]
    #[serial]
    // #[ignore]
    async fn test_update_instrument() -> Result<()> {
        dotenv().ok();
        let base_url = std::env::var("DATABASE_URL").expect("Expected database_url.");
        let client = ApiClient::new(&base_url);

        let instrument = Instrument {
            ticker: "AAP0005".to_string(),
            name: "Apple tester client".to_string(),
            instrument_id: None,
        };

        let create_response = client.create_symbol(&instrument).await?;
        let id =
            get_id_from_string(&create_response.message).expect("Error getting id from message.");

        // Test
        let instrument = Instrument {
            ticker: "TTT0005".to_string(),
            name: "New name".to_string(),
            instrument_id: None,
        };

        let response = client.update_symbol(&instrument, &id).await?;

        // Validate
        assert_eq!(response.code, 200);
        assert_eq!(response.status, "success");

        // Cleanup
        let _ = client.delete_symbol(&id).await?;

        Ok(())
    }

    #[tokio::test]
    #[serial]
    // #[ignore]
    async fn test_create_backtest() -> Result<()> {
        dotenv().ok();
        let base_url = std::env::var("DATABASE_URL").expect("Expected database_url.");
        let client = ApiClient::new(&base_url);

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
        let base_url = std::env::var("DATABASE_URL").expect("Expected database_url.");
        let client = ApiClient::new(&base_url);

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
        let base_url = std::env::var("DATABASE_URL").expect("Expected database_url.");
        let client = ApiClient::new(&base_url);

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
    async fn test_create_mbp() -> Result<()> {
        dotenv().ok();
        let base_url = std::env::var("DATABASE_URL").expect("Expected database_url.");
        let client = ApiClient::new(&base_url);

        // Create instrument
        let instrument = Instrument {
            ticker: "AAP0003".to_string(),
            name: "Apple tester client".to_string(),
            instrument_id: None,
        };

        let create_response = client.create_symbol(&instrument).await?;
        let id =
            get_id_from_string(&create_response.message).expect("Error getting id from message.");

        // Pull test data
        let mbp_1 = Mbp1Msg {
            hd: { RecordHeader::new::<Mbp1Msg>(id as u32, 1704209103644092564) },
            price: 6770,
            size: 1,
            action: 1,
            side: 2,
            depth: 0,
            flags: 0,
            ts_recv: 1704209103644092564,
            ts_in_delta: 17493,
            sequence: 739763,
            levels: [BidAskPair {
                ask_px: 1,
                bid_px: 1,
                bid_sz: 2,
                ask_sz: 2,
                bid_ct: 10,
                ask_ct: 20,
            }],
        };
        let mbp_2 = Mbp1Msg {
            hd: { RecordHeader::new::<Mbp1Msg>(id as u32, 1704239109644092565) },
            price: 6870,
            size: 2,
            action: 1,
            side: 1,
            depth: 0,
            flags: 0,
            ts_recv: 1704209103644092565,
            ts_in_delta: 17493,
            sequence: 739763,
            levels: [BidAskPair {
                ask_px: 1,
                bid_px: 1,
                bid_sz: 2,
                ask_sz: 2,
                bid_ct: 10,
                ask_ct: 20,
            }],
        };
        let record_ref1: RecordRef = (&mbp_1).into();
        let record_ref2: RecordRef = (&mbp_2).into();

        let mut buffer = Vec::new();
        let mut encoder = RecordEncoder::new(&mut buffer);
        encoder
            .encode_records(&[record_ref1, record_ref2])
            .expect("Encoding failed");

        // Test
        let response = client.create_mbp(&buffer).await?;

        // Validate
        assert_eq!(response.code, 200);
        assert_eq!(response.status, "success");

        // Cleanup
        let _ = client.delete_symbol(&id).await?;

        Ok(())
    }

    #[tokio::test]
    #[serial]
    // #[ignore]
    async fn test_get_mbp() -> Result<()> {
        dotenv().ok();
        let base_url = std::env::var("DATABASE_URL").expect("Expected database_url.");
        let client = ApiClient::new(&base_url);

        // Create instrument
        let instrument = Instrument {
            ticker: "AAP3".to_string(),
            name: "Apple tester client".to_string(),
            instrument_id: None,
        };

        let create_response = client.create_symbol(&instrument).await?;
        let id =
            get_id_from_string(&create_response.message).expect("Error getting id from message.");

        // Pull test data
        let mbp_1 = Mbp1Msg {
            hd: { RecordHeader::new::<Mbp1Msg>(id as u32, 1704209103644092564) },
            price: 6770,
            size: 1,
            action: Action::Trade as i8,
            side: 2,
            depth: 0,
            flags: 0,
            ts_recv: 1704209103644092564,
            ts_in_delta: 17493,
            sequence: 739763,
            levels: [BidAskPair {
                ask_px: 1,
                bid_px: 1,
                bid_sz: 2,
                ask_sz: 2,
                bid_ct: 10,
                ask_ct: 20,
            }],
        };
        let mbp_2 = Mbp1Msg {
            hd: { RecordHeader::new::<Mbp1Msg>(id as u32, 1704239109644092564) },
            price: 6870,
            size: 2,
            action: Action::Trade as i8,
            side: 1,
            depth: 0,
            flags: 0,
            ts_recv: 1704209103644092565,
            ts_in_delta: 17493,
            sequence: 739763,
            levels: [BidAskPair {
                ask_px: 1,
                bid_px: 1,
                bid_sz: 2,
                ask_sz: 2,
                bid_ct: 10,
                ask_ct: 20,
            }],
        };
        let record_ref1: RecordRef = (&mbp_1).into();
        let record_ref2: RecordRef = (&mbp_2).into();

        let mut buffer = Vec::new();
        let mut encoder = RecordEncoder::new(&mut buffer);
        encoder
            .encode_records(&[record_ref1, record_ref2])
            .expect("Encoding failed");

        // Create records
        let _ = client.create_mbp(&buffer).await?;

        // Test
        let query_params = RetrieveParams {
            symbols: vec!["AAP3".to_string()],
            start_ts: 1704209103644092563,
            end_ts: 1704239109644092565,
            schema: Schema::Mbp1.to_string(),
        };

        let response = client.get_records(&query_params).await?;

        let data = response.data.unwrap();
        let cursor = Cursor::new(data);
        let mut decoder = CombinedDecoder::new(cursor);
        let _decoded = decoder
            .decode_metadata_and_records()
            .expect("Error decoding metadata.");

        // Validate
        assert_eq!(response.code, 200);
        assert_eq!(response.status, "success");

        // Cleanup
        let _ = client.delete_symbol(&id).await?;

        Ok(())
    }

    #[tokio::test]
    #[serial]
    // #[ignore]
    async fn test_get_records_to_file() -> Result<()> {
        dotenv().ok();
        let base_url = std::env::var("DATABASE_URL").expect("Expected database_url.");
        let client = ApiClient::new(&base_url);

        // Create instrument
        let instrument = Instrument {
            ticker: "AAP19".to_string(),
            name: "Apple tester client".to_string(),
            instrument_id: None,
        };

        let create_response = client.create_symbol(&instrument).await?;
        let id =
            get_id_from_string(&create_response.message).expect("Error getting id from message.");

        // Pull test data
        let mbp_1 = Mbp1Msg {
            hd: { RecordHeader::new::<Mbp1Msg>(id as u32, 1704209103644092564) },
            price: 6770,
            size: 1,
            action: 1,
            side: 2,
            depth: 0,
            flags: 0,
            ts_recv: 1704209103644092564,
            ts_in_delta: 17493,
            sequence: 739763,
            levels: [BidAskPair {
                ask_px: 1,
                bid_px: 1,
                bid_sz: 2,
                ask_sz: 2,
                bid_ct: 10,
                ask_ct: 20,
            }],
        };
        let mbp_2 = Mbp1Msg {
            hd: { RecordHeader::new::<Mbp1Msg>(id as u32, 1704239109644092564) },
            price: 6870,
            size: 2,
            action: 1,
            side: 1,
            depth: 0,
            flags: 0,
            ts_recv: 1704209103644092564,
            ts_in_delta: 17493,
            sequence: 739763,
            levels: [BidAskPair {
                ask_px: 1,
                bid_px: 1,
                bid_sz: 2,
                ask_sz: 2,
                bid_ct: 10,
                ask_ct: 20,
            }],
        };
        let record_ref1: RecordRef = (&mbp_1).into();
        let record_ref2: RecordRef = (&mbp_2).into();

        let mut buffer = Vec::new();
        let mut encoder = RecordEncoder::new(&mut buffer);
        encoder
            .encode_records(&[record_ref1, record_ref2])
            .expect("Encoding failed");

        // Create records
        let _ = client.create_mbp(&buffer).await?;

        // Test
        let query_params = RetrieveParams {
            symbols: vec!["AAP19".to_string()],
            start_ts: 1704209103644092563,
            end_ts: 1704239109644092565,
            schema: Schema::Mbp1.to_string(),
        };

        let response = client
            .get_records_to_file(&query_params, "tests/test_data_pull.bin")
            .await?;

        // Validate
        assert_eq!(response, ());

        // Cleanup
        let _ = client.delete_symbol(&id).await?;

        Ok(())
    }

    #[tokio::test]
    #[serial]
    // #[ignore]
    async fn test_get_ohlcv() -> Result<()> {
        dotenv().ok();
        let base_url = std::env::var("DATABASE_URL").expect("Expected database_url.");
        let client = ApiClient::new(&base_url);

        // Create instrument
        let instrument = Instrument {
            ticker: "AAP9".to_string(),
            name: "Apple tester client".to_string(),
            instrument_id: None,
        };

        let create_response = client.create_symbol(&instrument).await?;
        let id =
            get_id_from_string(&create_response.message).expect("Error getting id from message.");

        // Pull test data
        let mbp_1 = Mbp1Msg {
            hd: { RecordHeader::new::<Mbp1Msg>(id as u32, 1704209103644092564) },
            price: 6770,
            size: 1,
            action: Action::Trade as i8,
            side: 2,
            depth: 0,
            flags: 0,
            ts_recv: 1704209103644092564,
            ts_in_delta: 17493,
            sequence: 739763,
            levels: [BidAskPair {
                ask_px: 1,
                bid_px: 1,
                bid_sz: 2,
                ask_sz: 2,
                bid_ct: 10,
                ask_ct: 20,
            }],
        };
        let mbp_2 = Mbp1Msg {
            hd: { RecordHeader::new::<Mbp1Msg>(id as u32, 1704209103644092565) },
            price: 6870,
            size: 2,
            action: Action::Trade as i8,
            side: 1,
            depth: 0,
            flags: 0,
            ts_recv: 1704209103644092565,
            ts_in_delta: 17493,
            sequence: 739763,
            levels: [BidAskPair {
                ask_px: 1,
                bid_px: 1,
                bid_sz: 2,
                ask_sz: 2,
                bid_ct: 10,
                ask_ct: 20,
            }],
        };
        let record_ref1: RecordRef = (&mbp_1).into();
        let record_ref2: RecordRef = (&mbp_2).into();

        let mut buffer = Vec::new();
        let mut encoder = RecordEncoder::new(&mut buffer);
        encoder
            .encode_records(&[record_ref1, record_ref2])
            .expect("Encoding failed");

        // Create records
        let _ = client.create_mbp(&buffer).await?;

        // Test
        let query_params = RetrieveParams {
            symbols: vec!["AAP9".to_string()],
            start_ts: 1704209103644092563,
            end_ts: 1704239109644092565,
            schema: Schema::Ohlcv1D.to_string(),
        };

        let response = client.get_records(&query_params).await?;

        let data = response.data.unwrap();
        let cursor = Cursor::new(data);
        let mut decoder = CombinedDecoder::new(cursor);
        let _decoded = decoder
            .decode_metadata_and_records()
            .expect("Error decoding metadata.");

        // Validate
        assert_eq!(response.code, 200);
        assert_eq!(response.status, "success");

        // Cleanup
        let _ = client.delete_symbol(&id).await?;

        Ok(())
    }

    // Used to test pull files from server
    #[tokio::test]
    #[serial]
    #[ignore]
    async fn test_get_records_to_file_server() -> Result<()> {
        dotenv().ok();
        let base_url = std::env::var("DATABASE_URL").expect("Expected database_url.");
        let client = ApiClient::new(&base_url);

        // Test
        let query_params = RetrieveParams::new(
            vec!["HE.n.0".to_string(), "ZC.n.0".to_string()],
            "2024-08-18",
            "2024-08-22",
            "ohlcv-1s",
        )?;

        let response = client
            .get_records_to_file(&query_params, "tests/ohlcv1stesting.bin")
            .await?;

        println!("{:?}", response);

        // Validate
        // assert_eq!(response, ());

        // Cleanup
        // let _ = client.delete_symbol(&id).await?;

        Ok(())
    }
}
