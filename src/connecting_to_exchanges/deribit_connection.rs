use std::collections::HashMap;
use std::fs;
use std::error::Error;
use serde_json::{json, Value};
use tokio::{net::TcpStream, task};
use tokio_tungstenite::{tungstenite::Message,
                        connect_async,
                        MaybeTlsStream,
                        WebSocketStream};
use std::sync::Arc;
use futures_util::{StreamExt, SinkExt};
use rust_decimal::{Decimal, prelude::FromPrimitive};
use tokio::sync::{RwLock, Mutex as TokioMutex};
use tokio::time::{timeout, Duration};
use crate::{Clients};
use reqwest::Client;
use crate::matching_engine::{engine::{MatchingEngine, TradingPair},
                             orderbook::{Order, BidOrAsk}};
use crate::warp_websocket::handler::{Event, publish_handler};

//  Pass an Arc<mutex<MatchingEngine>>> to the ws_handler inside the route definition since
//  ws_handler connects to the functions in ws.rs which receive messages from the clients (actions to be taken on the matching engine)

fn make_trading_pair_type(input_from_deribit: &String) -> TradingPair {
    let parts: Vec<&str> = input_from_deribit.split('.').collect();
    let together = parts.get(1).unwrap_or(&"").to_string();
    let base_and_quote:  Vec<&str> = together.split('_').collect();
    let trading_pair = TradingPair::new(base_and_quote.get(0).unwrap_or(&"").to_string(),
                                        base_and_quote.get(1).unwrap_or(&"").to_string());
    trading_pair
}

pub async fn establish_connection(url: &str) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, Box<dyn Error>>{
    println!("Trying to connect to: {}", url);
    let (ws_stream, _) = connect_async(url).await?;
    println!("Connected to Deribit exchange");
    Ok(ws_stream)
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
        "id": 9929,
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
            println!("Received auth response");

            // Parse the response to check for errors, change this with error propagation?
            let auth_response_json: Value = serde_json::from_str(&auth_text).expect("Failed to parse auth response");
            if auth_response_json.get("error").is_some() {
                println!("Authentication failed: {:?}", auth_response_json["error"]);
            }
        }
    }
}

pub async fn subscribe_to_channel(ws_stream_ref: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
                                  channels: Vec<&String>) -> Result<(), Box<dyn Error>> {
    // Subscribe to the raw data channel after authentication
    let msg = json!({
        "method":"public/subscribe",
        "params":{"channels": channels},
        "jsonrpc":"2.0",
        "id":9922
    });
    let msg_text = msg.to_string();
    ws_stream_ref.send(Message::Text(msg_text)).await?;
    Ok(())
}

async fn send_heartbeat(ws_stream_ref: &mut WebSocketStream<MaybeTlsStream<TcpStream>>) -> Result<(), Box<dyn Error>> {
    let heartbeat_response = json!({
                        "jsonrpc": "2.0",
                        "id": 888,
                        "method": "public/test",
                        "params": {}
                    });
    let heartbeat_msg_text = heartbeat_response.to_string();
    ws_stream_ref.send(Message::Text(heartbeat_msg_text)).await?;
    Ok(())
}

pub async fn establish_heartbeat(ws_stream: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
                                 interval_in_secs: u32) -> Result<(), Box<dyn Error>> {
    let msg = json!({
                    "jsonrpc": "2.0",
                    "id": 9098,
                    "method": "public/set_heartbeat",
                    "params": {
                              "interval": interval_in_secs
                               }
    });
    let heartbeat_msg_text = msg.to_string();
    ws_stream.send(Message::Text(heartbeat_msg_text)).await?;
    println!("Sent message to establish heartbeat every {interval_in_secs} seconds");
    // Set a maximum waiting time of 5 seconds for the response
    match timeout(Duration::from_secs(5), ws_stream.next()).await {
        Ok(Some(Ok(Message::Text(text)))) => {
            // Parse the incoming message as JSON
            let data: Value = serde_json::from_str(&text)?;

            // Check if the response matches the expected structure
            if data["id"] == 9098 && data["result"] == "ok" {
                println!("Heartbeat successfully established.");
                Ok(())
            } else {
                eprintln!("Unexpected response: {:?}", data);
                Err("Failed to establish heartbeat".into())
            }
        }
        Ok(Some(Ok(_))) => {
            // Received a message, but it's not a text message
            Err("Received unexpected message type".into())
        }
        Ok(Some(Err(e))) => {
            // Error while receiving the message
            eprintln!("Error receiving response: {:?}", e);
            Err("Failed to receive a valid response".into())
        }
        Ok(None) => {
            // WebSocket stream closed
            Err("WebSocket stream closed unexpectedly".into())
        }
        Err(_) => {
            // Timeout occurred
            Err("Failed to establish heartbeat: no response received within 5 seconds".into())
        }
    }
}

pub async fn on_incoming_deribit_message(
    ws_stream_ref: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    matching_engine: Arc<TokioMutex<MatchingEngine>>
) {
    let previous_best_bids: Arc<TokioMutex<HashMap<TradingPair, Decimal>>> = Arc::new(TokioMutex::new(HashMap::new()));
    let previous_best_asks: Arc<TokioMutex<HashMap<TradingPair, Decimal>>> = Arc::new(TokioMutex::new(HashMap::new()));
    let publish_client = Arc::new(Client::new());
    let mut previous_change_id: Option<u64> = None;
    let mut number_of_missed_changes: u32 = 0;
    loop {
        // Only handle messages from the WebSocket stream.
        match ws_stream_ref.next().await {
            Some(Ok(msg)) => {
                let text = msg.to_string();
                let data: Value = match serde_json::from_str(&text) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("Error parsing message: {:?}", e);
                        continue;
                    }
                };
                if data["method"] == "heartbeat" {
                    println!("Heartbeat received");
                    send_heartbeat(ws_stream_ref).await.expect("Problem sending heartbeat");
                    println!("Heartbeat sent");
                    // This is the id with which we send the heartbeat, the response will have the same id
                } else if data["id"] == 888 {
                    continue
                } else if data["id"] == 9922 {
                    println!("Subscription response result: {:?}", data["result"]);
                    continue
                } else if data["method"] == "subscription" {
                    let previous_best_bids = Arc::clone(&previous_best_bids);
                    let previous_best_asks = Arc::clone(&previous_best_asks);
                    let matching_engine = Arc::clone(&matching_engine);
                    let publish_client = Arc::clone(&publish_client);
                    // Immediately offload the message processing to another function/task.
                    tokio::spawn(async move {
                        // TODO: change the process_message and underlying functions to take the parsed data object already
                        if let Err(e) = process_message(msg, matching_engine, previous_best_bids, previous_best_asks, publish_client).await {
                            eprintln!("Error processing message: {:?}", e);
                        }
                    });
                    // Checking for missed updates
                    let current_change_id = data["params"]["data"]["change_id"].as_u64();
                    let current_previous_change_id = data["params"]["data"]["prev_change_id"].as_u64();
                    if let (Some(current_change_id), Some(current_previous_change_id)) = (current_change_id, current_previous_change_id) {
                        if let Some(prev_id) = previous_change_id {
                            if current_previous_change_id != prev_id {
                                number_of_missed_changes += 1;
                                println!("The number of missed updates: {number_of_missed_changes}");
                                println!("The previous change was: {:?}, and the expected previous change: {:?}", &previous_change_id, &current_previous_change_id);
                            }
                        }
                        previous_change_id = Some(current_change_id.clone());
                    }
                } else { println!("Received message which we do not process: {:?}", data) }
            },
            Some(Err(e)) => {
                eprintln!("Error receiving message: {:?}", e);
                break;
            }
            None => break, // Exit the loop if the stream is closed.
        }
    }
}

fn place_orders(update: &Vec<Value>,
                bid_or_ask: BidOrAsk,
                trading_pair: TradingPair,
                matching_engine: &mut MatchingEngine) {
    for price_level in update {
        if let Some(Value::String(type_of_update)) = &price_level.get(0) {
            if let Some(Value::Number(price)) = &price_level.get(1) {
                let price = Decimal::from_f64(price.as_f64().unwrap()).unwrap();
                if let Some(Value::Number(volume)) = &price_level.get(2){
                    let volume = volume.as_f64().unwrap();
                    match type_of_update.as_str() {
                        "delete" => {
                            matching_engine.leave_volume_from_exchange(
                                trading_pair.clone(),
                                bid_or_ask.clone(),
                                price,
                                volume // In this case the volume is always 0.0 -> maybe change it? or keep it as an extra 'check'
                            ).unwrap();
                        },
                        "new" => { // This case is correct
                            matching_engine.place_limit_order(
                                trading_pair.clone(),
                                price,
                                Order::new(bid_or_ask.clone(),
                                           volume,
                                           "deribit".to_string(),
                                           "-1".to_string())
                            ).unwrap();
                        },
                        "change" => {
                            matching_engine.leave_volume_from_exchange(
                                trading_pair.clone(),
                                bid_or_ask.clone(),
                                price,
                                volume
                            ).unwrap();
                        },
                        _ => () // maybe put a panic here or something
                    }
                }
            }
        }
    }
}

async fn publish_message(msg: String, topic: String, client: &Client) -> Result<(), Box<dyn Error + Send + Sync>> {
    let payload = json!({
        "user_id": 1,
        "topic": topic,
        "message": msg
    });
    let url = "http://127.0.0.1:8000/publish";
    let response = client.post(url).json(&payload).send().await?;
    // Check the response status
    if !response.status().is_success() {
        return Err(format!("Failed to publish message: {}", response.status()).into());
    }
    Ok(())
}

// In here also call the publish_handler from handler.ws (for now only the {pair}_{deribit}_best_bid_change
// and {pair}_{deribit}_best_ask_change)

async fn process_message(
    msg: Message,
    matching_engine: Arc<TokioMutex<MatchingEngine>>,
    previous_best_bids: Arc<TokioMutex<HashMap<TradingPair, Decimal>>>,
    previous_best_asks: Arc<TokioMutex<HashMap<TradingPair, Decimal>>>,
    publish_client: Arc<Client>
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let update_msg = match msg {
        Message::Text(text) => text,
        _ => return Err("Unexpected message type".into()),
    };

    let parsed: Value = serde_json::from_str(&update_msg)?;
    let (trading_pair, data) = extract_trading_pair_and_data(&parsed)?;
    process_order_updates(
        &data,
        "asks",
        BidOrAsk::Ask,
        &trading_pair,
        matching_engine.clone(),
        previous_best_asks,
        publish_client.clone(),
    ).await?;

    process_order_updates(
        &data,
        "bids",
        BidOrAsk::Bid,
        &trading_pair,
        matching_engine,
        previous_best_bids,
        publish_client,
    ).await?;

    Ok(())
}

fn extract_trading_pair_and_data(parsed: &Value) -> Result<(TradingPair, &Value), Box<dyn Error + Send + Sync>> {
    let params = parsed.get("params").ok_or("Missing params")?;
    let data = params.get("data").ok_or("Missing data")?;
    let trading_pair_string = params.get("channel")
        .and_then(Value::as_str)
        .ok_or("Invalid channel")?.to_string();
    let trading_pair = make_trading_pair_type(&trading_pair_string);
    Ok((trading_pair, data))
}

async fn process_order_updates(
    data: &Value,
    side: &str,
    bid_or_ask: BidOrAsk,
    trading_pair: &TradingPair,
    matching_engine: Arc<TokioMutex<MatchingEngine>>,
    previous_best_prices: Arc<TokioMutex<HashMap<TradingPair, Decimal>>>,
    publish_client: Arc<Client>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if let Some(Value::Array(updates)) = data.get(side) {
        if !updates.is_empty() {
            let best_price = update_orders_and_get_best_price(
                updates,
                bid_or_ask,
                trading_pair,
                matching_engine,
            ).await?;

            check_and_publish_price_change(
                best_price,
                trading_pair,
                previous_best_prices,
                publish_client,
                side,
            ).await?;
        }
    }
    Ok(())
}

async fn update_orders_and_get_best_price(
    updates: &Vec<Value>,
    bid_or_ask: BidOrAsk,
    trading_pair: &TradingPair,
    matching_engine: Arc<TokioMutex<MatchingEngine>>,
) -> Result<Decimal, Box<dyn Error + Send + Sync>> {
    let mut engine = matching_engine.lock().await;
    place_orders(updates, bid_or_ask.clone(), trading_pair.clone(), &mut engine);

    let orderbook = engine.orderbooks.get(trading_pair).ok_or("Orderbook not found")?;
    let (best_price, _) = match bid_or_ask {
        BidOrAsk::Ask => orderbook.asks.first_key_value(),
        BidOrAsk::Bid => orderbook.bids.last_key_value(),
    }.ok_or("No orders found")?;

    Ok(best_price.clone())
}

async fn check_and_publish_price_change(
    new_price: Decimal,
    trading_pair: &TradingPair,
    previous_best_prices: Arc<TokioMutex<HashMap<TradingPair, Decimal>>>,
    publish_client: Arc<Client>,
    side: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut previous_best = previous_best_prices.lock().await;
    let price_changed = previous_best
        .get(trading_pair)
        .map_or(true, |prev| (new_price - prev).abs() > Decimal::new(1, 4));

    if price_changed {
        previous_best.insert(trading_pair.clone(), new_price);
        drop(previous_best);

        let topic = format!("{}_deribit_best_{}_change", trading_pair.clone().to_string(), side.trim_end_matches('s'));
        let message = format!("Best {}: {}", side.trim_end_matches('s'), new_price);
        publish_message(message.clone(), topic, &publish_client).await?;
        println!("published {}", message);
    }

    Ok(())
}