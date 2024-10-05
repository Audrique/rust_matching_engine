#![allow(dead_code)]
#![allow(unused_imports)]

use std::collections::HashMap;
use std::fmt::format;
use std::str::FromStr;
use futures_util::{StreamExt, SinkExt};
use serde::{Deserialize, Serialize};
use reqwest::{Client, RequestBuilder};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use tokio_tungstenite::tungstenite::Message;
use warp::body::json;
use rust_matching_engine::{place_limit_order,
                           RegisterResponse,
                           EngineData,
                           BestPriceUpdate
};
use taos::*;

struct BestAskAndBidData {
    best_ask_price: Option<f32>,
    best_bid_price: Option<f32>,
}
impl BestAskAndBidData {
    pub fn new() -> BestAskAndBidData {
        BestAskAndBidData {
            best_ask_price: None,
            best_bid_price: None,
        }
    }
}

#[derive(Debug, Deserialize)]
struct Trade {
    direction : String,
    maker_fee: f32,
    price: String, // This is a string since we start from a Decimal and is not identified as a number when sent.
    taker_fee: f32,
    timestamp: u64,
    trader_id_maker: String,
    trader_id_taker: String,
    volume: f32,
}
// Combined struct for aggregating data
#[derive(Debug, Deserialize)]
struct TradeData {
    trades: Vec<Trade>,
}


async fn build_taos_ws() -> Result<Taos, Box<dyn std::error::Error>> {
    let dsn = "ws://127.0.0.1:6041";
    let builder = TaosBuilder::from_dsn(dsn)?;
    let taos = builder.build().await?;
    let _ = taos.query("show databases").await?;
    println!("Connected to taos");
    Ok(taos)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let http_client = Client::new();
    let register_url = "http://127.0.0.1:8000/register";

    let exchange = "deribit".to_string();
    let trading_pairs = ["BTC_USDT".to_string(), "BTC-PERPETUAL".to_string()];

    // create a hashmap for all trading pairs to keep track of the best bid/ask price
    let mut best_price_data: HashMap<String, BestAskAndBidData> = HashMap::new();
    for trading_pair in &trading_pairs {
        best_price_data.insert(trading_pair.clone(), BestAskAndBidData::new());
    }
    // Subscribe to the BTC_USDT pair on best_bid_change
    let mut topics: Vec<String> = vec![];
    for trading_pair in trading_pairs {
        topics.push(format!("best_bid_change.{}.{}", exchange.clone(), trading_pair.clone()));
        topics.push(format!("best_ask_change.{}.{}", exchange.clone(), trading_pair.clone()));
        topics.push(format!("trades.{}.{}", exchange.clone(), trading_pair.clone()));
    }
    let register_request = json!({ "user_id": 1, "topic": ""});

    let response = http_client.post(register_url)
        .json(&register_request)  // Send as JSON
        .send()
        .await
        .expect("Failed to send request");

    if response.status().is_success() {
        let register_response: RegisterResponse = response.json().await.expect("Failed to parse response");
        println!("Successfully registered client! WebSocket URL: {}", register_response.url);

        // add the topic of the best ask
        let add_topic_url = "http://127.0.0.1:8000/add_topic";
        let client_id = register_response.url.split('/').last().unwrap_or("");

        for topic in topics {
            let add_topic_request = json!({"topic": topic, "client_id": client_id});
            let add_topic_response = http_client.post(add_topic_url)
                .json(&add_topic_request)  // Send as JSON
                .send()
                .await
                .expect("Failed to send request");
            let add_topic_response: String = add_topic_response.text().await.expect("Failed to parse response");
            println!("Successfully added {:?} for the client: {}", &topic, add_topic_response);
        }
        // Now we have a URL which we are registered with under register_response.url and can now get a websocket_connection
        let (mut ws_stream, _) = connect_async(register_response.url).await.expect("Failed to connect");

        println!("WebSocket connected");

        // // Send a message to the WebSocket server
        ws_stream.send(Message::text("ping")).await.unwrap();
        println!("Message send to the server");

        let price_update_table_name = "MICRO_SEC_DB.DERIBIT_BTC_USDT_AND_DERIBIT_BTC_PERPETUAL_BEST_PRICES2";
        let trade_update_table_name = "MICRO_SEC_DB.DERIBIT_BTC_USDT_AND_DERIBIT_BTC_PERPETUAL_TRADES2";
        let taos = build_taos_ws().await.unwrap();

        // Receive messages from the WebSocket server
        while let Some(message) = ws_stream.next().await {
            match message {
                Ok(msg) => {
                    match msg {
                        Message::Text(text) => {
                            match serde_json::from_str::<EngineData>(&text) {
                                Ok(parsed_msg) => {
                                    if let Some(trading_pair) = parsed_msg.topic.split(".").last() {
                                        if parsed_msg.topic.starts_with("best") {
                                            match serde_json::from_str::<BestPriceUpdate>(&parsed_msg.message) {
                                                Ok(best_price_update) => {
                                                    // println!("Parsed message: {:?}", &best_price_update);
                                                    let new_best_price = f32::from_str(&best_price_update.price).unwrap();
                                                    let side = best_price_update.side.clone();
                                                    if let Some(data) = best_price_data.get_mut(trading_pair) {
                                                        match side.as_str() {
                                                            "ask" => { data.best_ask_price = Some(new_best_price); },
                                                            "bid" => { data.best_bid_price = Some(new_best_price); },
                                                            _ => eprintln!("Side is not bid or ask!")
                                                        }
                                                        if let (Some(best_ask_p), Some(best_bid_p)) =
                                                            (data.best_ask_price, data.best_bid_price) {
                                                            let sql = format!(
                                                                "INSERT INTO {} (time, instrument, best_ask_price, best_bid_price) VALUES (NOW, '{}', {}, {});",
                                                                price_update_table_name,
                                                                trading_pair,
                                                                best_ask_p,
                                                                best_bid_p
                                                            );
                                                            taos.exec(sql).await?;
                                                            // println!("Executed the inserting of price data");
                                                        }
                                                    } else { eprintln!("Trading pair not found!"); }
                                                },
                                                Err(e) => println!("Failed to parse message content into price update data: {:?}", e),
                                            }
                                        } else if parsed_msg.topic.starts_with("trades") {
                                            match serde_json::from_str::<TradeData>(&parsed_msg.message) {
                                                Ok(trades) => {
                                                    // println!("Parsed message: {:?}", &trades);
                                                    for trade in trades.trades {
                                                        let sql = format!(
                                                            "INSERT INTO {} (time, instrument, direction, price, volume) VALUES (NOW, '{}', '{}', {}, {});",
                                                            trade_update_table_name,
                                                            trading_pair,
                                                            trade.direction,
                                                            trade.price,
                                                            trade.volume,
                                                        );
                                                        taos.exec(sql).await?;
                                                        // println!("Executed the inserting of trade");
                                                    }
                                                },
                                                Err(e) => println!("Failed to parse message content into trade data: {:?}", e),
                                            }
                                        }
                                    } else { println!("Failed to find a tradingpair") }
                                },
                                Err(e) => println!("Failed to parse message: {:?}", e),
                            }
                        }
                        _ => {
                            println!("Received a different type of message than expected: {:?}", msg);
                        }
                    }
                },
                Err(e) => println!("Error: {:?}", e),
            }
        }
    } else {
        eprintln!("Failed to register client: {:?}", response.status());
        // Exit the program with a non-zero status code to indicate failure
        std::process::exit(1);
    }
    Ok(())
}