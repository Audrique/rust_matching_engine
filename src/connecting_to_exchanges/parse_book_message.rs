use serde::{Deserialize, Deserializer};
use serde_json::Value;
use crate::matching_engine::engine::TradingPair;


#[derive(Debug, Deserialize, Clone)]
pub struct BookUpdate {
    pub asks: Vec<OrderBookEntry>,
    pub bids: Vec<OrderBookEntry>,
    pub change_id: u64,
    #[serde(rename = "instrument_name", deserialize_with = "deserialize_trading_pair")]
    pub trading_pair: TradingPair,
    pub prev_change_id: u64,
    pub timestamp: u64,
    #[serde(rename = "type")]
    pub update_type: String,
}

#[derive(Debug, PartialEq, Clone)]
pub struct OrderBookEntry {
    pub action: String,
    pub price: f64,
    pub volume: f64,
}

impl<'de> Deserialize<'de> for OrderBookEntry {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let array = Vec::<Value>::deserialize(deserializer)?;

        if array.len() != 3 {
            return Err(serde::de::Error::custom("Expected array of 3 elements"));
        }

        let action = array[0].as_str()
            .ok_or_else(|| serde::de::Error::custom("Expected string for action"))?
            .to_string();

        let price = array[1].as_f64()
            .ok_or_else(|| serde::de::Error::custom("Expected number for price"))?;

        let volume = array[2].as_f64()
            .ok_or_else(|| serde::de::Error::custom("Expected number for volume"))?;

        Ok(OrderBookEntry {
            action,
            price,
            volume,
        })
    }
}

fn deserialize_trading_pair<'de, D>(deserializer: D) -> Result<TradingPair, D::Error>
where
    D: Deserializer<'de>,
{
    let instrument_name = String::deserialize(deserializer)?;
    let base_and_quote: Vec<&str> = instrument_name.split('_').collect();

    Ok(TradingPair::new(
        base_and_quote.get(0).unwrap_or(&"").to_string(),
        base_and_quote.get(1).unwrap_or(&"").to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_book_update_deserialize() {
        let json_str = r#"{
            "asks": [["delete", 62944.0, 0.0], ["new", 62949.0, 16500.0]],
            "bids": [["change", 62933.0, 7550.0]],
            "change_id": 78693046768,
            "instrument_name": "BTC_USDT",
            "prev_change_id": 78693046766,
            "timestamp": 1728299690099,
            "type": "change"
        }"#;

        let book_update: BookUpdate = serde_json::from_str(json_str).unwrap();

        // Test trading pair parsing
        assert_eq!(book_update.trading_pair.base, "BTC");
        assert_eq!(book_update.trading_pair.quote, "USDT");

        // Test asks parsing
        assert_eq!(book_update.asks.len(), 2);
        assert_eq!(book_update.asks[0], OrderBookEntry {
            action: "delete".to_string(),
            price: 62944.0,
            volume: 0.0,
        });
        assert_eq!(book_update.asks[1], OrderBookEntry {
            action: "new".to_string(),
            price: 62949.0,
            volume: 16500.0,
        });

        // Test bids parsing
        assert_eq!(book_update.bids.len(), 1);
        assert_eq!(book_update.bids[0], OrderBookEntry {
            action: "change".to_string(),
            price: 62933.0,
            volume: 7550.0,
        });

        // Test other fields
        assert_eq!(book_update.change_id, 78693046768);
        assert_eq!(book_update.prev_change_id, 78693046766);
        assert_eq!(book_update.timestamp, 1728299690099);
        assert_eq!(book_update.update_type, "change");
        println!("{:?}", book_update);
    }

    #[test]
    fn test_empty_book_update() {
        let json_str = r#"{
            "asks": [],
            "bids": [],
            "change_id": 78693046768,
            "instrument_name": "ETH-PERPETUAL",
            "prev_change_id": 78693046766,
            "timestamp": 1728299690099,
            "type": "change"
        }"#;

        let book_update: BookUpdate = serde_json::from_str(json_str).unwrap();

        assert!(book_update.asks.is_empty());
        assert!(book_update.bids.is_empty());
        assert_eq!(book_update.trading_pair.base, "ETH-PERPETUAL");
        assert_eq!(book_update.trading_pair.quote, "");
    }

    #[test]
    fn test_invalid_trading_pair() {
        let json_str = r#"{
            "asks": [],
            "bids": [],
            "change_id": 78693046768,
            "instrument_name": "INVALID",
            "prev_change_id": 78693046766,
            "timestamp": 1728299690099,
            "type": "change"
        }"#;

        let book_update: BookUpdate = serde_json::from_str(json_str).unwrap();

        assert_eq!(book_update.trading_pair.base, "INVALID");
        assert_eq!(book_update.trading_pair.quote, "");
    }

    #[test]
    fn test_invalid_order_book_entry() {
        let json_str = r#"{
            "asks": [["delete", "not_a_number", 0.0]],
            "bids": [],
            "change_id": 78693046768,
            "instrument_name": "BTC-PERPETUAL",
            "prev_change_id": 78693046766,
            "timestamp": 1728299690099,
            "type": "change"
        }"#;

        let result = serde_json::from_str::<BookUpdate>(json_str);
        assert!(result.is_err());
    }
}