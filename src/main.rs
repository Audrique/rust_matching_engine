#![allow(warnings)]
mod matching_engine;
mod connecting_to_exchanges;
use tokio_tungstenite::connect_async;
use tokio;
use tokio_tungstenite::tungstenite::Message;
use serde_json::{json, Value};
use futures_util::{StreamExt, SinkExt};
use std::fs;
// TODO: two main things
// 1) Prevent placing orders that will self trade (so cancel them immediately after when in the matching they would have matched with a self trade
//    and then disconnect the trader
// 2) Return trades that happen (if 1 trader with an order trades with multiple traders or with one trader at different price limits, consider them as two trades)
// 99) Then if this works test this with a lot of orders and then start connecting this engine to some real exchange and start keeping a local orderbook.
//     Before fully implementing this think carefully how one would convert the events into orders for the orderbook.
//     Then also implement a flag of 'shadow_order' which is a bool, which will trade as usual but does not remove liquidity from the orderbook
//     This is done so that we can keep a proper local orderbook which is similar to the exchange.



#[tokio::main]
async fn main() {
    let url = "wss://www.deribit.com/ws/api/v2";
    println!("Trying to connect to: {}", url);
    let (mut ws_stream, _) = connect_async(url).await.expect("Failed to connect");
    println!("Connected to Deribit exchange");

    // Get the client ID and client secret from the config file
    let config_file = fs::read_to_string("config.json").expect("Unable to read config file");
    let config: Value = serde_json::from_str(&config_file).expect("Unable to parse config file");

    let client_id = config["client_id"].as_str().expect("Client ID not found");
    let client_secret = config["client_secret"].as_str().expect("Client Secret not found");
    println!("id: {}, secret: {}", client_id, client_secret);
    // Authenticate with Deribit
    let auth_msg = json!({
        "jsonrpc": "2.0",
        "id": 9929,
        "method": "public/auth",
        "params": {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret
        }
    });

    let auth_msg_text = auth_msg.to_string();
    ws_stream.send(Message::Text(auth_msg_text)).await.expect("Failed to send auth message");

    // Wait for authentication response
    if let Some(Ok(auth_response)) = ws_stream.next().await {
        if let Message::Text(auth_text) = auth_response {
            println!("Received auth response: {}", auth_text);

            // Parse the response to check for errors
            let auth_response_json: serde_json::Value = serde_json::from_str(&auth_text).expect("Failed to parse auth response");
            if auth_response_json.get("error").is_some() {
                println!("Authentication failed: {:?}", auth_response_json["error"]);
                return;
            }
        }
    }

    // Subscribe to the raw data channel after authentication
    let msg = json!({
        "method":"public/subscribe",
        "params":{
            "channels":[
                "book.BTC_USDT.raw"
            ]
        },
        "jsonrpc":"2.0",
        "id":9929
    });
    let msg_text = msg.to_string();
    ws_stream.send(Message::Text(msg_text)).await.expect("Failed to send message");

    // Handle incoming messages
    while let Some(message) = ws_stream.next().await {
        match message {
            Ok(msg) => match msg {
                Message::Text(text) => {
                    println!("Received: {}", text);
                    // Add your logic here to handle the response
                }
                Message::Binary(bin) => {
                    println!("Received binary data: {:?}", bin);
                }
                Message::Close(_) => {
                    println!("Connection closed");
                    break;
                }
                _ => (),
            },
            Err(e) => {
                println!("Error receiving message: {}", e);
                break;
            }
        }
    }
}



