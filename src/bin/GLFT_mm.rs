use std::str::FromStr;
use futures_util::{StreamExt, SinkExt};
use reqwest::Client;
use rust_decimal::Decimal;
use serde_json::json;
use tokio::net::TcpStream;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use rust_matching_engine::{place_limit_order,
                           RegisterResponse,
                           EngineData,
                           BestPriceUpdate,
                           ClientData,
                           BidOrAsk,
                           TradesUpdate,
};

#[tokio::main]
async fn main() {
    let trader_id = "GLFT_mm";
    let mut client_data = ClientData::new();
    let http_client = Client::new();
    let register_url = "http://127.0.0.1:8000/register";

    let pair_string = "BTC-PERPETUAL".to_string();
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

        let add_topic_url = "http://127.0.0.1:8000/add_topic";
        let topic_trades = format!("trades.{}", trader_id);
        let client_id = register_response.url.split('/').last().unwrap_or("");
        let add_topic_request = json!({"topic": topic_trades.clone(), "client_id": client_id});
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

        // Receive messages from the WebSocket server
        while let Some(message) = ws_stream.next().await {
            match message {
                Ok(msg) => {
                    match msg {
                        Message::Text(text) => {
                            match serde_json::from_str::<EngineData>(&text) {
                                Ok(parsed_msg) => {
                                    println!("{:?}", parsed_msg);
                                    if parsed_msg.topic == "best_price_change.deribit.BTC-PERPETUAL" {
                                        match serde_json::from_str::<BestPriceUpdate>(&parsed_msg.message) {
                                            Ok(content) => {

                                                let mut side = content.changed_side.clone();
                                                if side == "Both".to_string() {
                                                    side = "Bid".to_string();
                                                }

                                                let (order_price, bid_or_ask) = match side.as_str() {
                                                    "Bid" => { (f64::from_str(&content.best_bid_price).unwrap() + 30.0, BidOrAsk::Bid) },
                                                    "Ask" => { (f64::from_str(&content.best_ask_price).unwrap() - 30.0, BidOrAsk::Ask) },
                                                    "Both" => { (f64::from_str(&content.best_bid_price).unwrap() + 30.0, BidOrAsk::Bid) }
                                                    _ => (-1.0, BidOrAsk::Bid),
                                                };
                                                let volume = 0.002;
                                                place_limit_order("BTC-PERPETUAL",
                                                                  "",
                                                                  order_price.clone(),
                                                                  side.as_str(),
                                                                  volume.clone(),
                                                                  trader_id,
                                                                  "89",
                                                                  &mut ws_stream
                                                ).await;
                                                client_data.add_order("BTC-PERPETUAL", bid_or_ask, Decimal::from_str(&order_price.to_string()).unwrap(), volume, "5".to_string());
                                            },
                                            Err(e) => println!("Failed to parse message content: {:?}", e),
                                        }
                                    }
                                    if parsed_msg.topic == topic_trades.as_str() {
                                        match serde_json::from_str::<TradesUpdate>(&parsed_msg.message) {
                                            Ok(content) => {
                                                println!("Parsed message: {:?}", &content);
                                            },
                                            Err(e) => println!("Failed to parse message content: {:?}", e),
                                        }
                                        //TODO: Update here the ClientData positions and open orders
                                        let place_holder = "test";
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