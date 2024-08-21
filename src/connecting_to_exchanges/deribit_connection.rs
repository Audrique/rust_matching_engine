
use std::fs;
use serde_json::{json, Value};
use tokio::{net::TcpStream,
            task};
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use tokio_tungstenite::tungstenite::Message;
use futures_util::{StreamExt, SinkExt};

//TODO: Remove the prints and replace it by Ok(), Err()
pub async fn establish_connection(url: &str) -> WebSocketStream<MaybeTlsStream<TcpStream>>{
    println!("Trying to connect to: {}", url);
    let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");
    println!("Connected to Deribit exchange");
    ws_stream
}

pub fn read_config_file() -> (String, String) {
    let config_file = fs::read_to_string("config.json").expect("Unable to read config file");
    let config: Value = serde_json::from_str(&config_file).expect("Unable to parse config file");

    let client_id = config["client_id"].as_str().expect("Client ID not found").to_string();
    let client_secret = config["client_secret"].as_str().expect("Client Secret not found").to_string();
    (client_id, client_secret)
}

pub async fn authenticate_deribit(ws_stream_ref: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
                              client_id: &str, client_secret: &str) {
    let auth_msg = json!({
        "jsonrpc": "2.0",
        "id": 9929, // I don't understand what the ID is for since we can authenticate with an ID and subscribe with a different ID and it works
        "method": "public/auth",
        "params": {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret
        }
    });

    let auth_msg_text = auth_msg.to_string();
    ws_stream_ref.send(Message::Text(auth_msg_text)).await.expect("Failed to send auth message");

    // Wait for authentication response
    if let Some(Ok(auth_response)) = ws_stream_ref.next().await {
        if let Message::Text(auth_text) = auth_response {
            println!("Received auth response: {}", auth_text);

            // Parse the response to check for errors
            let auth_response_json: Value = serde_json::from_str(&auth_text).expect("Failed to parse auth response");
            if auth_response_json.get("error").is_some() {
                println!("Authentication failed: {:?}", auth_response_json["error"]);
            }
        }
    }
}

pub async fn subscribe_to_channel(ws_stream_ref: &mut WebSocketStream<MaybeTlsStream<TcpStream>>, channels: Vec<&String>) {
    // Subscribe to the raw data channel after authentication
    let msg = json!({
        "method":"public/subscribe",
        "params":{
            "channels": channels
        },
        "jsonrpc":"2.0",
        "id":9922
    });
    let msg_text = msg.to_string();
    ws_stream_ref.send(Message::Text(msg_text)).await.expect("Failed to send message");
}

pub async fn on_incoming_message(ws_stream_ref: &mut WebSocketStream<MaybeTlsStream<TcpStream>>) {
    loop {
        match ws_stream_ref.next().await {
            Some(Ok(msg)) => {
                // Immediately offload the message processing to another function/task.
                task::spawn(process_message(msg));
            },
            Some(Err(e)) => {
                eprintln!("Error receiving message: {:?}", e);
                break;
            }
            None => break, // Exit the loop if the stream is closed.
        }
    }
}

// fn parse_message(message: &str) -> Result<WebSocketMessage> {
//     let parsed: WebSocketMessage = serde_json::from_str(message)?;
//     Ok(parsed)
// }

//TODO: process the message here such that the receiving of the messages is the least amount blocked
async fn process_message(msg: Message, ) {
    // Check this again
    let update_msg = match msg {
        Message::Text(text) => {text},
        _ => {panic!()}
    };
    let parsed: serde_json::Value = serde_json::from_str(&update_msg).expect("Can't parse to JSON");
    if let Some(params) = parsed.get("params") {
        if let Some(data) = params.get("data") {
            if let Some(Value::Array(asks_update)) = data.get("asks") {
                if asks_update.len() != 0 {
                    // here loop over the asks_updates
                    // If "new": put it into the orderbook
                    // -> Question: does new just add to the existing or is it the new total volume at that level
                    // If "delete", cancel the order (the limit)
                    // println!("{:?}", asks_update);
                }
            }
            if let Some(Value::Array(bids_update)) = data.get("bids") {
                if bids_update.len() != 0 {
                    for price_level in bids_update {
                        let type_of_update = &price_level[0];
                        let price = &price_level[1];
                        let volume = &price_level[2];
                        // println!("Price: {}, Quantity: {}, type of update: {}", price, volume, type_of_update);
                        if type_of_update == "delete" {
                            println!("{:?}", volume);
                        }
                    }
                }
            }
        }
    }
}