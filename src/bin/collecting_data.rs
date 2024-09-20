#![allow(dead_code)]
#![allow(unused_imports)]
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
                           BestBidOrAskData,
                           MessageContent
};
use taos::*;

async fn build_taos_ws() -> Result<(), Box<dyn std::error::Error>> {
    let dsn = "ws://127.0.0.1:6041";
    let builder = TaosBuilder::from_dsn(dsn)?;
    let taos = builder.build().await?;
    let _ = taos.query("show databases").await?;
    println!("Connected to taos");
    Ok(())
}

#[tokio::main]
async fn main() {
    let http_client = Client::new();
    let register_url = "http://127.0.0.1:8000/register";

    let pair_string = "BTC_USDT".to_string();
    let exchange = "deribit".to_string();
    // Subscribe to the BTC_USDT pair on best_bid_change
    let topic_bid = format!("{}_{}_best_bid_change", pair_string.clone(), exchange.clone());
    let register_request = json!({ "user_id": 1, "topic": topic_bid});

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
        let topic_ask = format!("{}_{}_best_ask_change", pair_string.clone(), exchange.clone());
        let client_id = register_response.url.split('/').last().unwrap_or("");
        let add_topic_request = json!({"topic": topic_ask, "client_id": client_id});
        let add_topic_response = http_client.post(add_topic_url)
            .json(&add_topic_request)  // Send as JSON
            .send()
            .await
            .expect("Failed to send request");
        let add_topic_response2: String  = add_topic_response.text().await.expect("Failed to parse response");
        println!("Successfully added a topic for the client: {}", add_topic_response2);

        // Now we have a URL which we are registered with under register_response.url and can now get a websocket_connection
        let (mut ws_stream, _) = connect_async(register_response.url).await.expect("Failed to connect");

        println!("WebSocket connected");

        // // Send a message to the WebSocket server
        ws_stream.send(Message::text("ping")).await.unwrap();
        println!("Message send to the server");

        build_taos_ws().await.unwrap();
        // Receive messages from the WebSocket server
        while let Some(message) = ws_stream.next().await {
            match message {
                Ok(msg) => {
                    match msg {
                        Message::Text(text) => {
                            match serde_json::from_str::<BestBidOrAskData>(&text) {
                                Ok(parsed_msg) => {
                                    if parsed_msg.topic == "BTC_USDT_deribit_best_bid_change" {
                                        match serde_json::from_str::<MessageContent>(&parsed_msg.message) {
                                            Ok(content) => {
                                                println!("Parsed message: {:?}", &content);
                                                let new_best_price = f32::from_str(&content.price).unwrap();
                                                let side = &content.side;
                                                // TODO: in here we send our stuff to the TDengine database

                                            },
                                            Err(e) => println!("Failed to parse message content: {:?}", e),
                                        }
                                    }
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
}