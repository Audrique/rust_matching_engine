#![allow(dead_code)]
#![allow(unused_imports)]

use std::fmt::format;
use futures_util::{StreamExt, SinkExt};
use serde::{Deserialize, Serialize};
use reqwest::{Client, RequestBuilder};
use serde_json::json;
use tokio_tungstenite::connect_async;
// use warp::ws::Message;
use tokio_tungstenite::tungstenite::Message;

#[derive(Deserialize, Serialize, Debug)]
pub struct RegisterRequest {
    user_id: usize,
    topic: String,
}

#[derive(Deserialize)]
pub struct TopicActionRequest {
    topic: String,
    client_id: String,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct RegisterResponse {
    url: String,
}

#[derive(Deserialize, Debug)]
pub struct Event {
    topic: String,
    user_id: Option<usize>,
    message: String,
}

#[tokio::main]
async fn main() {
    let http_client = Client::new();
    let register_url = "http://localhost:8000/register";

    let pair_string = "BTC_USDT".to_string();
    let exchange = "deribit".to_string();
    // Subscribe to the BTC_USDT pair on best_bid_change
    let topic_ask = format!("{}_{}_best_bid_change", pair_string, exchange);
    // let topic_2 = format!("{}_{}_best_bid_change", pair_string, exchange);
    let register_request = json!({ "user_id": 1, "topic": topic_ask});
    // add the bid topic with the add_topic handler

    let response = http_client.post(register_url)
        .json(&register_request)  // Send as JSON
        .send()
        .await
        .expect("Failed to send request");

    if response.status().is_success() {
        let register_response: RegisterResponse = response.json().await.expect("Failed to parse response");
        println!("Successfully registered client! WebSocket URL: {}", register_response.url);

        // Now we have a URL which we are registered with under register_response.url and can now get a websocket_connection
        let (mut ws_stream, _) = connect_async(register_response.url).await.expect("Failed to connect");

        println!("WebSocket connected");

        // // Send a message to the WebSocket server
        ws_stream.send(Message::text("ping")).await.unwrap();
        println!("Message send to the server");

        // Receive messages from the WebSocket server
        while let Some(message) = ws_stream.next().await {
            match message {
                Ok(msg) => println!("Received: {:?}", msg),
                Err(e) => println!("Error: {:?}", e),
            }
        }
    } else {
        eprintln!("Failed to register client: {:?}", response.status());
        // Exit the program with a non-zero status code to indicate failure
        std::process::exit(1);
    }
}
