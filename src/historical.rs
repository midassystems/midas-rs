use crate::response::ApiResponse;
use crate::{error::Error, error::Result, utils::date_to_unix_nanos};
use axum::http::StatusCode;
use futures_util::StreamExt;
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

pub struct Historical {
    base_url: String,
    client: Client,
}

impl Historical {
    pub fn new(base_url: &str) -> Self {
        let client = ClientBuilder::new()
            .timeout(Duration::from_secs(20000)) // Set timeout to 120 seconds
            .build()
            .expect("Failed to build HTTP client");

        Historical {
            base_url: base_url.to_string(),
            client,
        }
    }

    fn url(&self, endpoint: &str) -> String {
        format!(
            "{}{}{}",
            self.base_url,
            "/historical/".to_string(),
            endpoint.to_string()
        )
    }

    // Instruments
    pub async fn create_symbol(&self, instrument: &Instrument) -> Result<ApiResponse<u32>> {
        let url = self.url("instruments/create");
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
        let url = self.url("instruments/get");
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
        let url = self.url("instruments/delete");
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
        let url = self.url("instruments/list");
        let response = self.client.get(&url).send().await?.text().await?;
        let api_response: ApiResponse<Vec<Instrument>> = serde_json::from_str(&response)?;

        Ok(api_response)
    }

    pub async fn update_symbol(
        &self,
        instrument: &Instrument,
        id: &i32,
    ) -> Result<ApiResponse<()>> {
        let url = self.url("instruments/update");
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

    // Market data
    pub async fn create_mbp(&self, data: &[u8]) -> Result<ApiResponse<()>> {
        let url = self.url("mbp/create");
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

    pub async fn create_mbp_from_file(&self, file_path: &str) -> Result<()> {
        let url = self.url("mbp/bulk_upload");
        let response = self
            .client
            .post(&url)
            .json(&file_path) // Ensure you send the file path correctly
            .send()
            .await?;

        // Stream the server's response
        let mut stream = response.bytes_stream();

        // Output the streamed response directly to the user
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => {
                    println!("{}", String::from_utf8_lossy(&bytes));
                }
                Err(e) => {
                    println!("Error while receiving chunk: {:?}", e);
                    return Err(Error::from(e));
                }
            }
        }

        Ok(())
    }

    pub async fn get_records(&self, params: &RetrieveParams) -> Result<ApiResponse<Vec<u8>>> {
        let url = self.url("mbp/get");
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

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dotenv::dotenv;
    use mbn::decode::Decoder;
    use mbn::encode::RecordEncoder;
    use mbn::enums::{Action, Schema};
    use mbn::record_ref::RecordRef;
    use mbn::records::{BidAskPair, Mbp1Msg, RecordHeader};
    use mbn::symbols::Instrument;
    use regex::Regex;
    use serial_test::serial;
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

    #[allow(dead_code)]
    async fn create_dummy_records() -> Result<()> {
        dotenv().ok();
        let base_url = std::env::var("HISTORICAL_URL").expect("Expected database_url.");
        let client = Historical::new(&base_url);

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
        Ok(())
    }

    #[tokio::test]
    #[serial]
    // #[ignore]
    async fn test_instrument_create() -> Result<()> {
        dotenv().ok();
        let base_url = std::env::var("HISTORICAL_URL").expect("Expected database_url.");
        let client = Historical::new(&base_url);

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
        let base_url = std::env::var("HISTORICAL_URL").expect("Expected database_url.");
        let client = Historical::new(&base_url);

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
        let base_url = std::env::var("HISTORICAL_URL").expect("Expected database_url.");
        let client = Historical::new(&base_url);

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
        let base_url = std::env::var("HISTORICAL_URL").expect("Expected database_url.");
        let client = Historical::new(&base_url);

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
        let base_url = std::env::var("HISTORICAL_URL").expect("Expected database_url.");
        let client = Historical::new(&base_url);

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
    async fn test_create_mbp() -> Result<()> {
        dotenv().ok();
        let base_url = std::env::var("HISTORICAL_URL").expect("Expected database_url.");
        let client = Historical::new(&base_url);

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
        let base_url = std::env::var("HISTORICAL_URL").expect("Expected database_url.");
        let client = Historical::new(&base_url);

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
        let mut decoder = Decoder::new(cursor)?;
        let _decoded = decoder.decode().expect("Error decoding metadata.");

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
        let base_url = std::env::var("HISTORICAL_URL").expect("Expected database_url.");
        let client = Historical::new(&base_url);

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
        let base_url = std::env::var("HISTORICAL_URL").expect("Expected database_url.");
        let client = Historical::new(&base_url);

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
            end_ts: 1704209203654092563,
            schema: Schema::Ohlcv1S.to_string(),
        };

        let response = client.get_records(&query_params).await?;

        let data = response.data.unwrap();
        let cursor = Cursor::new(data);
        let mut decoder = Decoder::new(cursor)?;
        let _record = decoder.decode().expect("Error decoding metadata.");

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
    async fn test_get_trades() -> Result<()> {
        dotenv().ok();
        let base_url = std::env::var("HISTORICAL_URL").expect("Expected database_url.");
        let client = Historical::new(&base_url);

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
            end_ts: 1704209203654092563,
            schema: Schema::Trade.to_string(),
        };

        let response = client.get_records(&query_params).await?;

        let data = response.data.unwrap();
        let cursor = Cursor::new(data);
        let mut decoder = Decoder::new(cursor)?;
        let _record = decoder.decode().expect("Error decoding metadata.");

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
    async fn test_get_tbbo() -> Result<()> {
        dotenv().ok();
        let base_url = std::env::var("HISTORICAL_URL").expect("Expected database_url.");
        let client = Historical::new(&base_url);

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
            end_ts: 1704209203654092563,
            schema: Schema::Tbbo.to_string(),
        };

        let response = client.get_records(&query_params).await?;

        let data = response.data.unwrap();
        let cursor = Cursor::new(data);
        let mut decoder = Decoder::new(cursor)?;
        let _record = decoder.decode().expect("Error decoding metadata.");

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
    async fn test_get_bbo() -> Result<()> {
        dotenv().ok();
        let base_url = std::env::var("HISTORICAL_URL").expect("Expected database_url.");
        let client = Historical::new(&base_url);

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
            end_ts: 1704209203654092563,
            schema: Schema::Bbo1S.to_string(),
        };

        let response = client.get_records(&query_params).await?;

        let data = response.data.unwrap();
        let cursor = Cursor::new(data);
        let mut decoder = Decoder::new(cursor)?;
        let _record = decoder.decode().expect("Error decoding metadata.");

        // Validate
        assert_eq!(response.code, 200);
        assert_eq!(response.status, "success");

        // Cleanup
        let _ = client.delete_symbol(&id).await?;

        Ok(())
    }

    /// Used to test pull files from server
    #[tokio::test]
    #[serial]
    #[ignore]
    async fn test_get_records_to_file_server() -> Result<()> {
        dotenv().ok();

        let base_url = std::env::var("HISTORICAL_URL").expect("Expected database_url.");
        let client = Historical::new(&base_url);

        // Test
        let query_params = RetrieveParams::new(
            vec!["HE.n.0".to_string(), "ZC.n.0".to_string()],
            "2024-01-01 00:00:00",
            "2024-01-03 23:00:00",
            "bbo-1m",
        )?;

        let _response = client.get_records_to_file(&query_params, "bbo.bin").await?;

        Ok(())
    }
}
