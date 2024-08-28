use std::collections::HashMap;
use std::fs;
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
use crate::{Client, Clients};
use crate::matching_engine::{engine::{MatchingEngine, TradingPair},
                             orderbook::{Order, BidOrAsk}};
use crate::warp_websocket::handler::{Event, publish_handler};

// TODO: create more helper functions and refactor the code

// Helper function
fn make_trading_pair_type(input_from_deribit: &String) -> TradingPair {
    let parts: Vec<&str> = input_from_deribit.split('.').collect();
    let together = parts.get(1).unwrap_or(&"").to_string();
    let base_and_quote:  Vec<&str> = together.split('_').collect();
    let trading_pair = TradingPair::new(base_and_quote.get(0).unwrap_or(&"").to_string(),
                                        base_and_quote.get(1).unwrap_or(&"").to_string());
    trading_pair
}

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

pub async fn subscribe_to_channel(ws_stream_ref: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
                                  channels: Vec<&String>) {
    // Subscribe to the raw data channel after authentication
    let msg = json!({
        "method":"public/subscribe",
        "params":{"channels": channels},
        "jsonrpc":"2.0",
        "id":9922
    });
    let msg_text = msg.to_string();
    ws_stream_ref.send(Message::Text(msg_text)).await.expect("Failed to send message");
}

pub async fn on_incoming_deribit_message(ws_stream_ref: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
                                  matching_engine: Arc<TokioMutex<MatchingEngine>>,
                                         clients: Clients) {
    // Keeps track of the previous best bids/asks such that we can send updates to the subscribed clients
    let previous_best_bids: Arc<TokioMutex<HashMap<TradingPair, Decimal>>> = Arc::new(TokioMutex::new(HashMap::new()));
    let previous_best_asks: Arc<TokioMutex<HashMap<TradingPair, Decimal>>> = Arc::new(TokioMutex::new(HashMap::new()));
    loop {
        match ws_stream_ref.next().await {
            Some(Ok(msg)) => {
                let previous_best_bids = Arc::clone(&previous_best_bids);
                let previous_best_asks = Arc::clone(&previous_best_asks);
                let matching_engine = Arc::clone(&matching_engine);
                let clients = Arc::clone(&clients);
                // Immediately offload the message processing to another function/task.
                tokio::spawn(async move {
                    if let Err(e) = process_message(msg, matching_engine, previous_best_bids, previous_best_asks, clients).await {
                        eprintln!("Error processing message: {:?}", e);
                    }
                });
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


// In here also call the publish_handler from handler.ws (for now only the {pair}_{deribit}_best_bid_change
// and {pair}_{deribit}_best_ask_change)
// TODO: refactor and abstract this function (and probably other functions in this file)
// TODO: also make sure the previous_best_bids and asks are correct since I think they are not implemented correctly (they dont overwrite the previous one correctly or something)
async fn process_message(msg: Message,
                         matching_engine: Arc<TokioMutex<MatchingEngine>>,
                         mut previous_best_bids: Arc<TokioMutex<HashMap<TradingPair, Decimal>>>,
                         mut previous_best_asks: Arc<TokioMutex<HashMap<TradingPair, Decimal>>>,
                         clients: Clients
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let update_msg = match msg {
        Message::Text(text) => {text},
        _ => {panic!()}
    };
    let parsed: Value = serde_json::from_str(&update_msg).expect("Can't parse to JSON");

    if let Some(params) = parsed.get("params") {
        if let Some(data) = params.get("data") {
            if let Some(Value::String(trading_pair_string_ref)) = params.get("channel") {
                let trading_pair = make_trading_pair_type(trading_pair_string_ref);

                // Process asks
                if let Some(Value::Array(asks_update)) = data.get("asks") {
                    if !asks_update.is_empty() {
                        let mut engine = matching_engine.lock().await;
                        place_orders(asks_update, BidOrAsk::Ask, trading_pair.clone(), &mut engine);

                        // Now check if the best ask price has changed
                        let orderbook = engine.orderbooks.get(&trading_pair).ok_or("Orderbook not found")?;
                        let (best_ask_price, _) = orderbook.asks.first_key_value().ok_or("No asks found")?;
                        let best_ask_price = best_ask_price.clone();

                        // Release the lock on the engine
                        drop(engine);

                        let mut previous_best_a = previous_best_asks.lock().await;
                        //refactor this a lot
                        if let Some(previous_ba) = previous_best_a.get(&trading_pair) {
                            // If the difference is more than 0.0001 we consider it a new best ask
                            // we can't directly use the != operator
                            if (best_ask_price - previous_ba).abs() > Decimal::new(1, 4) {
                                // Update the previous best ask
                                // TODO: make sure we overwrite the previous_best_a
                                previous_best_a.entry(trading_pair.clone())
                                    .and_modify(|e| *e = best_ask_price)
                                    .or_insert(best_ask_price);
                                drop(previous_best_a);
                                // Send an update to the subscribed clients
                                let topic = format!("{}_deribit_best_ask_change", trading_pair.clone().to_string());
                                println!("{}", &topic);
                                let message = format!("Best ask: {}", best_ask_price);
                                let event = Event::new(topic.clone(), None, message);
                                // Call the publish_handler to broadcast the message
                                // We have to clone the clients since we also want to call it in the bids case
                                let clients_clone = Arc::clone(&clients);
                                if let Err(e) = publish_handler(event, clients_clone).await {
                                    eprintln!("Failed to publish message for topic {}: {:?}", topic, e);
                                }
                            }
                        } else {
                            // Update the previous best ask
                            previous_best_a.insert(trading_pair.clone(), best_ask_price);
                            drop(previous_best_a);
                            // Send an update to the subscribed clients
                            let topic = format!("{}_deribit_best_ask_change", trading_pair.clone().to_string());
                            let message = format!("Best ask: {}", best_ask_price);
                            let event = Event::new(topic.clone(), None, message);
                            // Call the publish_handler to broadcast the message
                            let clients_clone = Arc::clone(&clients);
                            if let Err(e) = publish_handler(event, clients_clone).await {
                                eprintln!("Failed to publish message for topic {}: {:?}", topic, e);
                            }
                        }
                    }
                }
                // Process the bids
                if let Some(Value::Array(bids_update)) = data.get("bids") {
                    if !bids_update.is_empty() {
                        let mut engine = matching_engine.lock().await;
                        place_orders(bids_update, BidOrAsk::Bid, trading_pair.clone(), &mut engine);

                        // Now check if the best ask price has changed
                        let orderbook = engine.orderbooks.get(&trading_pair).ok_or("Orderbook not found")?;
                        let (best_bid_price, _) = orderbook.bids.last_key_value().ok_or("No asks found")?;
                        let best_bid_price = best_bid_price.clone();

                        // Release the lock on the engine
                        drop(engine);

                        let mut previous_best_b = previous_best_bids.lock().await;

                        if let Some(previous_bb) = previous_best_b.get(&trading_pair) {

                            if (best_bid_price - previous_bb).abs() > Decimal::new(1, 4) {
                                // Update the previous best ask
                                previous_best_b.entry(trading_pair.clone())
                                    .and_modify(|e| *e = best_bid_price)
                                    .or_insert(best_bid_price);
                                drop(previous_best_b);
                                // Send an update to the subscribed clients
                                let topic = format!("{}_deribit_best_bid_change", trading_pair.clone().to_string());
                                println!("{}", &topic);
                                let message = format!("Best bid: {}", best_bid_price);
                                let event = Event::new(topic.clone(), None, message);
                                // Call the publish_handler to broadcast the message
                                let clients_clone = Arc::clone(&clients);
                                if let Err(e) = publish_handler(event, clients_clone).await {
                                    eprintln!("Failed to publish message for topic {}: {:?}", topic, e);
                                }
                            }
                        } else {
                            // Update the previous best ask
                            previous_best_b.insert(trading_pair.clone(), best_bid_price);
                            drop(previous_best_b);
                            // Send an update to the subscribed clients
                            let topic = format!("{}_deribit_best_bid_change", trading_pair.clone().to_string());
                            let message = format!("Best bid: {}", best_bid_price);
                            let event = Event::new(topic.clone(), None, message);
                            // Call the publish_handler to broadcast the message
                            if let Err(e) = publish_handler(event, clients).await {
                                eprintln!("Failed to publish message for topic {}: {:?}", topic, e);
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(())
}