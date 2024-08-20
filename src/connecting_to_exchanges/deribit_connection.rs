use std::fs;
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use tokio_tungstenite::tungstenite::Message;
use futures_util::{StreamExt, SinkExt};

//TODO: Remove the prints and replace it by Ok(), Err()
pub async fn establish_connection(url: &str) -> WebSocketStream<MaybeTlsStream<TcpStream>>{
    println!("Trying to connect to: {}", url);
    let (mut ws_stream, _) = connect_async(url).await.expect("Failed to connect");
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

pub async fn subscribe_to_channel(ws_stream_ref: &mut WebSocketStream<MaybeTlsStream<TcpStream>>, channels: Vec<&str>) {
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

// TODO: run this on a different thread or channel or whatever it is called
// To make sure we minimize the 'missing of updates'
pub async fn on_incoming_message(ws_stream_ref: &mut WebSocketStream<MaybeTlsStream<TcpStream>>) {
    while let Some(message) = ws_stream_ref.next().await {
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