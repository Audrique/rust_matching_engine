use std::collections::HashMap;
use futures_util::SinkExt;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tokio_tungstenite::tungstenite::Message;

// In this file we will add functions/structs/... that all traders can use
#[derive(Deserialize, Serialize, Debug)]
pub struct RegisterResponse {
    pub url: String,
}

#[derive(Debug, Deserialize)]
pub struct EngineData {
    pub message: String,
    pub topic: String,
    pub user_id: u32,
    pub update_counter: u32,
}

#[derive(Debug, Deserialize)]
pub struct BestPriceUpdate {
    pub best_bid_price: String,
    pub best_ask_price: String,
    pub changed_side: String,
}
#[derive(Debug)]
struct LocalOrder {
    asks: HashMap<Decimal, f64>, // price --> volume
    bids: HashMap<Decimal, f64>,
}

#[derive(Debug)]
struct ClientData {
    open_orders: HashMap<String, LocalOrder>, // TradingPair --> open orders
    positions: HashMap<String, f64>, // TradingPair --> position
}

pub async fn place_limit_order(trading_pair_base: &str,
                               trading_pair_quote: &str,
                               price: f64,
                               side: &str,
                               volume: f64,
                               trader_id: &str,
                               order_id: &str,
                               ws_stream_ref: &mut WebSocketStream<MaybeTlsStream<TcpStream>>
) {
    let add_limit_order_msg = json!({"action": "add_limit_order",
            "trading_pair_base": trading_pair_base,
            "trading_pair_quote": trading_pair_quote,
            "price": price,
            "side": side,
            "volume": volume,
            "trader_id": trader_id,
            "order_id": order_id
        });
    ws_stream_ref.send(Message::text(add_limit_order_msg.to_string())).await.unwrap();
    println!("Sent the order request to the engine.");
}

pub async fn request_open_orders_and_position(trading_pair_base: &str,
                               trading_pair_quote: &str,
                               trader_id: &str,
                               ws_stream_ref: &mut WebSocketStream<MaybeTlsStream<TcpStream>>
) {
    let request_open_orders_and_position_msg = json!({"action": "open_orders_and_positions",
            "trading_pair_base": trading_pair_base,
            "trading_pair_quote": trading_pair_quote,
            "trader_id": trader_id
    });
    ws_stream_ref.send(Message::text(request_open_orders_and_position_msg.to_string())).await.unwrap();
    println!("Sent the order request to the engine.");
}



