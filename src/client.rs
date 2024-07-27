use crate::error::Result;
use mbn::backtest::BacktestData;
use mbn::decode::RecordDecoder;
use mbn::symbols::Instrument;
use midasbackend::database::market_data::RetrieveParams;
use reqwest::{self, Client};
use serde::{Deserialize, Serialize};
use std::io::Cursor;

// #[derive(Debug, Serialize, Deserialize)]
// pub struct RetrieveParams {
//     pub start_ts: i64,
//     pub end_ts: i64,
//     pub schema: String,
// }

#[derive(Debug, Deserialize, Serialize)]
pub struct ApiResponse<T> {
    pub status: String,
    pub message: String,
    pub code: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
}

pub struct ApiClient {
    base_url: String,
    client: Client,
}

impl ApiClient {
    pub fn new(base_url: &str) -> Self {
        ApiClient {
            base_url: base_url.to_string(),
            client: Client::new(),
        }
    }

    fn url(&self, endpoint: &str) -> String {
        format!("{}{}", self.base_url, endpoint.to_string())
    }

    // Instruments
    pub async fn create_symbol(&self, instrument: &Instrument) -> Result<ApiResponse<i32>> {
        let url = self.url("/market_data/instruments/create");
        let response = self
            .client
            .post(&url)
            .json(instrument)
            .send()
            .await?
            .text()
            .await?;

        let api_response: ApiResponse<i32> = serde_json::from_str(&response)?;
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
        let url = self.url("/trading/backtest/get");
        let response = self.client.get(&url).json(id).send().await?.text().await?;

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

    pub async fn get_records(&self, params: &RetrieveParams) -> Result<ApiResponse<Vec<u8>>> {
        let url = self.url("/market_data/mbp/get");
        let response = self
            .client
            .get(&url)
            .json(params)
            .send()
            .await?
            .text()
            .await?;

        let api_response: ApiResponse<Vec<u8>> = serde_json::from_str(&response)?;
        Ok(api_response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mbn::decode::{CombinedDecoder, RecordDecoder};
    use mbn::encode::RecordEncoder;
    use mbn::enums::Schema;
    use mbn::record_ref::RecordRef;
    use mbn::records::{BidAskPair, Mbp1Msg, RecordHeader};
    use mbn::symbols::Instrument;
    use regex::Regex;
    use serial_test::serial;
    use std::fs;

    // use serde_json::json;

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
    async fn test_instrument_create() -> Result<()> {
        let base_url = "http://localhost:8080"; // Update with your actual base URL
        let client = ApiClient::new(base_url);

        let instrument = Instrument {
            ticker: "AAP00001".to_string(),
            name: "Apple tester client".to_string(),
            // Include other fields as necessary
        };

        // Test
        let response = client.create_symbol(&instrument).await?;

        // Validate
        assert_eq!(response.code, 200);
        assert_eq!(response.status, "success");

        // Cleanup
        let id = get_id_from_string(&response.message).expect("Error getting id from message.");
        let _ = client.delete_symbol(&id).await?;

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_list_instruments() -> Result<()> {
        let base_url = "http://localhost:8080"; // Update with your actual base URL
        let client = ApiClient::new(base_url);

        let instrument = Instrument {
            ticker: "AAP0003".to_string(),
            name: "Apple tester client".to_string(),
            // Include other fields as necessary
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
    async fn test_update_instrument() -> Result<()> {
        let base_url = "http://localhost:8080"; // Update with your actual base URL
        let client = ApiClient::new(base_url);

        let instrument = Instrument {
            ticker: "AAP0005".to_string(),
            name: "Apple tester client".to_string(),
        };

        let create_response = client.create_symbol(&instrument).await?;
        let id =
            get_id_from_string(&create_response.message).expect("Error getting id from message.");

        // Test
        let instrument = Instrument {
            ticker: "TTT0005".to_string(),
            name: "New name".to_string(),
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
    async fn test_create_backtest() -> Result<()> {
        let base_url = "http://localhost:8080"; // Update with your actual base URL
        let client = ApiClient::new(base_url);

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
    async fn test_get_backtest() -> Result<()> {
        let base_url = "http://localhost:8080"; // Update with your actual base URL
        let client = ApiClient::new(base_url);

        // Pull test data
        let mock_data =
            fs::read_to_string("tests/data/test_data.backtest.json").expect("Unable to read file");
        let backtest_data: BacktestData =
            serde_json::from_str(&mock_data).expect("JSON was not well-formatted");

        let response = client.create_backtest(&backtest_data).await?;
        let id = get_id_from_string(&response.message).expect("Error getting id from message.");

        // Test
        let result = client.get_backtest(&id).await?;

        // Validate
        assert_eq!(response.code, 200);
        assert_eq!(response.status, "success");

        // Cleanup
        let _ = client.delete_backtest(&id).await?;

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_create_mbp() -> Result<()> {
        let base_url = "http://localhost:8080"; // Update with your actual base URL
        let client = ApiClient::new(base_url);

        // Create instrument
        let instrument = Instrument {
            ticker: "AAP0003".to_string(),
            name: "Apple tester client".to_string(),
            // Include other fields as necessary
        };

        let create_response = client.create_symbol(&instrument).await?;
        let id =
            get_id_from_string(&create_response.message).expect("Error getting id from message.");
        println!("{}", id);

        // Pull test data
        let mbp_1 = Mbp1Msg {
            hd: { RecordHeader::new::<Mbp1Msg>(id as u32, 1704209103644092564) },
            price: 6770,
            size: 1,
            action: 1,
            side: 2,
            depth: 0,
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

        println!("{:?}", buffer);

        // Test
        let response = client.create_mbp(&buffer).await?;
        println!("{:?}", response);

        // Validate
        assert_eq!(response.code, 200);
        assert_eq!(response.status, "success");

        // Cleanup
        let _ = client.delete_symbol(&id).await?;

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_get_mbp() -> Result<()> {
        let base_url = "http://localhost:8080"; // Update with your actual base URL
        let client = ApiClient::new(base_url);

        // Create instrument
        let instrument = Instrument {
            ticker: "AAP3".to_string(),
            name: "Apple tester client".to_string(),
            // Include other fields as necessary
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
        let response = client.create_mbp(&buffer).await?;

        // Test
        let query_params = RetrieveParams {
            symbols: vec!["AAP3".to_string()],
            start_ts: 1704209103644092563,
            end_ts: 1704239109644092565,
            schema: Schema::Ohlcv1D.to_string(),
        };

        let response = client.get_records(&query_params).await?;

        let mut data = response.data.unwrap();
        let cursor = Cursor::new(data);
        let mut decoder = CombinedDecoder::new(cursor);
        // let mut decoder = RecordDecoder::new(cursor);  /
        let decoded = decoder
            .decode_metadata_and_records()
            .expect("Error decoding metadata.");
        println!("{:?}", decoded);

        // Validate

        // assert_eq!(response.code, 200);
        // assert_eq!(response.status, "success");

        // Cleanup
        let _ = client.delete_symbol(&id).await?;

        Ok(())
    }
}
