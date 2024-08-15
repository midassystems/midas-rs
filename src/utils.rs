use crate::error::Result;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};

pub fn date_to_unix_nanos(date_str: &str) -> Result<i64> {
    let naive_datetime = if date_str.len() == 10 {
        // Parse date-only format YYYY-MM-DD
        let naive_date = NaiveDate::parse_from_str(date_str, "%Y-%m-%d")?;
        naive_date.and_hms_opt(0, 0, 0).unwrap() // Set time to midnight
    } else {
        // Parse datetime format YYYY-MM-DD HH:MM:SS
        NaiveDateTime::parse_from_str(date_str, "%Y-%m-%d %H:%M:%S")?
    };
    // Convert the NaiveDateTime to a DateTime<Utc>
    let datetime_utc: DateTime<Utc> = DateTime::from_naive_utc_and_offset(naive_datetime, Utc);

    // Convert to Unix time in nanoseconds
    let unix_nanos = datetime_utc.timestamp_nanos_opt().unwrap();

    Ok(unix_nanos)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_datetime_to_unix_nanos() -> Result<()> {
        let date_str = "2021-11-01 01:01:01";

        // Test
        let unix_nanos = date_to_unix_nanos(date_str)?;

        // Validate
        assert_eq!(1635728461000000000, unix_nanos);
        Ok(())
    }

    #[test]
    fn test_date_to_unix_nanos() -> Result<()> {
        let date_str = "2021-11-01";

        // Test
        let unix_nanos = date_to_unix_nanos(date_str)?;

        // Validate
        assert_eq!(1635724800000000000, unix_nanos);

        Ok(())
    }
}
