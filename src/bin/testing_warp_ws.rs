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
                           EngineData,
                           BestPriceUpdate
};

#[tokio::main]
async fn main() {
    let http_client = Client::new();
    let register_url = "http://127.0.0.1:8000/register";

    let pair_string = "BTC_USDT".to_string();
    let exchange = "deribit".to_string();
    // Subscribe to the BTC_USDT pair on best_price_change
    let best_price_topic = format!("best_price_change.{}.{}", exchange.clone(), pair_string.clone());
    let register_request = json!({ "user_id": 1, "topic": best_price_topic});

    let response = http_client.post(register_url)
        .json(&register_request)  // Send as JSON
        .send()
        .await
        .expect("Failed to send request");

    if response.status().is_success() {
        let register_response: RegisterResponse = response.json().await.expect("Failed to parse response");
        println!("Successfully registered client! WebSocket URL: {}", register_response.url);
        // // add the topic of the best ask
        // let add_topic_url = "http://127.0.0.1:8000/add_topic";
        // let topic_ask = format!("best_ask_change.{}.{}", exchange.clone(), pair_string.clone());
        // let client_id = register_response.url.split('/').last().unwrap_or("");
        // let add_topic_request = json!({"topic": topic_ask, "client_id": client_id});
        // let add_topic_response = http_client.post(add_topic_url)
        //     .json(&add_topic_request)  // Send as JSON
        //     .send()
        //     .await
        //     .expect("Failed to send request");
        // let add_topic_response2: String  = add_topic_response.text().await.expect("Failed to parse response");
        // println!("Successfully added a topic for the client: {}", add_topic_response2);

        // Now we have a URL which we are registered with under register_response.url and can now get a websocket_connection
        let (mut ws_stream, _) = connect_async(register_response.url).await.expect("Failed to connect");

        println!("WebSocket connected");

        // // Send a message to the WebSocket server
        ws_stream.send(Message::text("ping")).await.unwrap();
        println!("Message send to the server");

        // Receive messages from the WebSocket server
        while let Some(message) = ws_stream.next().await {
            match message {
                Ok(msg) => {
                    match msg {
                        Message::Text(text) => {
                            match serde_json::from_str::<EngineData>(&text) {
                                Ok(parsed_msg) => {
                                    if parsed_msg.topic == "best_bid_change.deribit.BTC_USDT" ||
                                        parsed_msg.topic == "best_ask_change.deribit.BTC_USDT" {
                                        match serde_json::from_str::<BestPriceUpdate>(&parsed_msg.message) {
                                            Ok(content) => {
                                                println!("Parsed message: {:?}", &content);
                                                // Only place bid orders for the moment since
                                                // we do not keep track of our open orders and then we get negative spread
                                                // Note that the changed side is not nessecarily the only side that was changed
                                                // However, it is the side that 'caused' the change
                                                let side = &content.changed_side;

                                                let order_price = match side.as_str() {
                                                    "bid" => { f64::from_str(&content.best_bid_price).unwrap() + 30.0 },
                                                    "ask" => { f64::from_str(&content.best_ask_price).unwrap() - 30.0 },
                                                    _ => -1.0,
                                                };

                                                place_limit_order("BTC",
                                                                  "USDT",
                                                                  order_price,
                                                                  side,
                                                                  0.002,
                                                                  "testing_trader",
                                                                  "33",
                                                                  &mut ws_stream
                                                ).await;
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
