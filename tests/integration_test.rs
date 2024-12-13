use dotenv::dotenv;
use mbn::encode::RecordEncoder;
use mbn::enums::Action;
use mbn::record_ref::RecordRef;
use mbn::records::{BidAskPair, Mbp1Msg, RecordHeader};
use mbn::symbols::Instrument;
use mbn::symbols::Vendors;
use midas_client::historical::Historical;
use regex::Regex;
use serial_test::serial;
use std::path::PathBuf;

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

async fn create_dummy_instrument(client: &Historical) -> anyhow::Result<i32> {
    // Create instrument
    let instrument = Instrument::new(
        None,
        "AAPL9",
        "Apple tester client",
        Vendors::Databento,
        Some("continuous".to_string()),
        Some("GLBX.MDP3".to_string()),
        1,
        1,
        true,
    );

    let create_response = client.create_symbol(&instrument).await?;
    let id = get_id_from_string(&create_response.message).expect("Error getting id from message.");
    Ok(id)
}

async fn create_dummy_records_file(filename: &PathBuf) -> anyhow::Result<i32> {
    dotenv().ok();
    let base_url = std::env::var("HISTORICAL_URL").expect("Expected database_url.");
    let client = Historical::new(&base_url);

    let id = create_dummy_instrument(&client).await?;

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
        discriminator: 0,
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
        discriminator: 0,
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

    // Create records file
    let _ = encoder.write_to_file(filename, false)?;

    Ok(id)
}

// -- Tests
#[tokio::test]
#[serial]
// #[ignore]
async fn test_create_mbp_from_file() -> anyhow::Result<()> {
    dotenv().ok();
    let base_url = std::env::var("HISTORICAL_URL").expect("Expected database_url.");
    let client = Historical::new(&base_url);

    let filename = "midas_client_test_mbp-1.bin";
    let path = PathBuf::from("../midas-server/data/processed_data").join(filename);

    let id = create_dummy_records_file(&path).await?;

    // Test
    let result = client.create_mbp_from_file(filename).await?;

    // Validate
    assert_eq!(result.status, "success");

    // Cleanup
    let _ = client.delete_symbol(&id).await?;
    let _ = tokio::fs::remove_file(path).await;

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_create_mbp_from_file_duplicate_error() -> anyhow::Result<()> {
    dotenv().ok();
    let base_url = std::env::var("HISTORICAL_URL").expect("Expected database_url.");
    let client = Historical::new(&base_url);
    let filename = "midas_client_test_mbp-1.bin";
    let path = PathBuf::from("../midas-server/data/processed_data").join(filename);
    let id = create_dummy_instrument(&client).await?;

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
        discriminator: 0,
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
        discriminator: 0,
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

    // Create records file
    let _ = encoder.write_to_file(&path, false)?;

    // Test
    let result = client.create_mbp_from_file(filename).await?;
    println!(" Duplicate result {:?}", result);

    // Validate
    assert_eq!(result.status, "failed");

    // Cleanup
    let _ = client.delete_symbol(&id).await?;
    let _ = tokio::fs::remove_file(path).await;

    Ok(())
}
