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
use tokio::time::{timeout, Duration, Instant};
use crate::{Clients};
use reqwest::Client;
use serde::ser::SerializeTuple;
use crate::matching_engine::{engine::{MatchingEngine, TradingPair},
                             orderbook::{Order, BidOrAsk}};
use crate::warp_websocket::handler::{Event};
use crate::connecting_to_exchanges::for_all_exchanges::{TraderData, update_trader_data};

// TODO: remove the Box<> in the Results as much as we can as it is slower (uses the heap)
//  Just replace it with the correct error type.

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
fn initialize_on_hold_changes(trading_pairs: Vec<TradingPair>,
) -> Arc<TokioMutex<HashMap<TradingPair, HashMap<BidOrAsk, HashMap<Decimal, (f64, Instant)>>>>> {
    let mut outer_map = HashMap::new();

    for pair in trading_pairs {
        let mut inner_map = HashMap::new();
        inner_map.insert(BidOrAsk::Bid, HashMap::new());
        inner_map.insert(BidOrAsk::Ask, HashMap::new());
        outer_map.insert(pair, inner_map);
    }

    Arc::new(TokioMutex::new(outer_map))
}

// deribit can change to exchange later
// Topics: {pair}_{deribit}_best_bid_change, sends the best bid price if it changes (volume could be included later)
//         {pair}_{deribit}_best_ask_change, sends the best ask price if it changes (idem as above)
//         {pair}_{deribit}_top_10_asks_bids_periodically, sends the top 10 bids and asks every 100ms (prices and volume)
pub async fn on_incoming_deribit_message(
    ws_stream_ref: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    matching_engine: Arc<TokioMutex<MatchingEngine>>,
    trading_pairs: Vec<TradingPair>,
    traders_data: Arc<TokioMutex<HashMap<String, TraderData>>>,
) {
    let previous_best_bids: Arc<TokioMutex<HashMap<TradingPair, Decimal>>> = Arc::new(TokioMutex::new(HashMap::new()));
    let previous_best_asks: Arc<TokioMutex<HashMap<TradingPair, Decimal>>> = Arc::new(TokioMutex::new(HashMap::new()));
    let on_hold_changes = initialize_on_hold_changes(trading_pairs.clone());
    let publish_client = Arc::new(Client::new());
    let mut previous_change_id: Option<u64> = None;
    let mut number_of_missed_changes: u32 = 0;

    // Spawn a new task for periodic publishing
    let periodic_publish_task = {
        let matching_engine = Arc::clone(&matching_engine);
        let publish_client = Arc::clone(&publish_client);
        let traders_data_cloned = Arc::clone(&traders_data);
        tokio::spawn(async move {
            loop {
                // Use this to automatically close the lock
                let _ = {
                    let engine = matching_engine.lock().await;
                    for t_pair in &trading_pairs {
                        let orderbook = engine.orderbooks.get(t_pair).ok_or("Orderbook not found").unwrap();
                        let top_10_asks: Vec<(Decimal, f64)> = orderbook.asks.iter()
                            .take(10)
                            .map(|(price, limit)| (price.clone(), limit.total_volume()))
                            .collect();
                        let top_10_bids: Vec<(Decimal, f64)> = orderbook.bids.iter()
                            .rev()
                            .take(10)
                            .map(|(price, limit)| (price.clone(), limit.total_volume()))
                            .collect();
                        let topic = format!("{}_deribit_top_10_asks_bids_periodically", t_pair.clone().to_string());
                        let message = format!("Top 10 asks: {:?}; Top 10 bids: {:?}", top_10_asks, top_10_bids);
                        publish_message(message, topic, &publish_client).await.unwrap();

                        // Also publish the traders data
                        let topic2 = "traders_data_periodically".to_string();
                        let traders_data2 = traders_data_cloned.lock().await;
                        let message2 = format!("{:?}", traders_data2.clone());
                        publish_message(message2, topic2, &publish_client).await.unwrap();
                    }
                };
                // Wait for 250 ms
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        })
    };


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
                    let on_hold_changes = Arc::clone(&on_hold_changes);
                    let traders_data_cloned = Arc::clone(&traders_data);
                    // Immediately offload the message processing to another function/task.
                    let msg = data.clone();
                    tokio::spawn(async move {
                        if let Err(e) = process_message(
                            msg,
                            matching_engine,
                            previous_best_bids,
                            previous_best_asks,
                            publish_client,
                            on_hold_changes,
                            traders_data_cloned
                        ).await {
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
    periodic_publish_task.abort();
}


// TODO: put the arc mutex in here instead of the engine so we can have
//  the least amount of locking we can
async fn place_orders(update: &Vec<Value>,
                bid_or_ask: BidOrAsk,
                trading_pair: TradingPair,
                matching_engine: &mut MatchingEngine,
                on_hold_changes: &mut HashMap<TradingPair, HashMap<BidOrAsk, HashMap<Decimal, (f64, Instant)>>>,
                time_until_forget_on_hold: Duration,
                traders_data: Arc<TokioMutex<HashMap<String, TraderData>>>
) {
    for price_level in update {
        if let Some(Value::String(type_of_update)) = &price_level.get(0) {
            if let Some(Value::Number(price)) = &price_level.get(1) {
                let price = Decimal::from_f64(price.as_f64().unwrap()).unwrap();
                let check_exchange_order_in_orderbook = match bid_or_ask {
                    BidOrAsk::Ask => {matching_engine
                        .orderbooks
                        .get(&trading_pair).unwrap()
                        .asks.get(&price).map_or(false, |limit| {
                        limit.check_exchange_order_in_limit()
                        })
                    },
                    BidOrAsk::Bid => {matching_engine
                        .orderbooks
                        .get(&trading_pair).unwrap()
                        .bids.get(&price).map_or(false, |limit| {
                        limit.check_exchange_order_in_limit()
                        })
                    }
                };
                if let Some(Value::Number(volume)) = &price_level.get(2){
                    let volume = volume.as_f64().unwrap();
                    match type_of_update.as_str() {
                        "delete" => {
                            if check_exchange_order_in_orderbook {
                                matching_engine.leave_volume_from_exchange(
                                    trading_pair.clone(),
                                    bid_or_ask.clone(),
                                    price,
                                    volume // In this case the volume is always 0.0 -> maybe change it? or keep it as an extra 'check'
                                ).unwrap();
                            } else {on_hold_changes
                                .get_mut(&trading_pair)
                                .unwrap()
                                .get_mut(&bid_or_ask)
                                .unwrap()
                                .insert(price, (volume, Instant::now()));}
                        },
                        "new" => {
                            // TODO: publish the trades at the correct topic '{deribit}_{pair}_trades, topic or something
                            //  Also, in the client_msg function in ws.rs (since this places orders from clients)
                            let (pair, trades) = matching_engine.place_limit_order(
                                trading_pair.clone(),
                                price.clone(),
                                Order::new(bid_or_ask.clone(),
                                           volume,
                                           "deribit".to_string(),
                                           "-1".to_string())
                            ).unwrap();
                            update_trader_data(pair, trades, traders_data.clone()).await;

                            let volume_on_hold = on_hold_changes.
                                get_mut(&trading_pair)
                                .unwrap()
                                .get_mut(&bid_or_ask)
                                .unwrap()
                                .get(&price)
                                .map(|&(vol, _)| vol)
                                .unwrap_or(-999.0);

                            if volume_on_hold >= 0.0 {
                                matching_engine.leave_volume_from_exchange(
                                    trading_pair.clone(),
                                    bid_or_ask.clone(),
                                    price.clone(),
                                    volume_on_hold
                                ).unwrap();
                                on_hold_changes
                                    .get_mut(&trading_pair)
                                    .unwrap()
                                    .get_mut(&bid_or_ask)
                                    .unwrap()
                                    .remove(&price);
                            }
                        },
                        "change" => {
                            if check_exchange_order_in_orderbook {
                                matching_engine.leave_volume_from_exchange(
                                    trading_pair.clone(),
                                    bid_or_ask.clone(),
                                    price,
                                    volume
                                ).unwrap();
                            } {on_hold_changes
                                .get_mut(&trading_pair)
                                .unwrap()
                                .get_mut(&bid_or_ask)
                                .unwrap()
                                .insert(price, (volume, Instant::now()));}
                        },
                        _ => {eprintln!("Update-type not one of 'change', 'new' or 'delete'! ")}
                    }
                }
            }
        }
    }
    // Clean up on_hold_change
    let cleanup_time = Instant::now();
    for (_, bid_ask_map) in on_hold_changes.iter_mut() {
        for (_, price_map) in bid_ask_map.iter_mut() {
            price_map.retain(|_, (_, timestamp)| cleanup_time.duration_since(*timestamp) <= time_until_forget_on_hold);
        }
    }
    // println!("{:?}", on_hold_changes);
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

async fn process_message(
    msg: Value,
    matching_engine: Arc<TokioMutex<MatchingEngine>>,
    previous_best_bids: Arc<TokioMutex<HashMap<TradingPair, Decimal>>>,
    previous_best_asks: Arc<TokioMutex<HashMap<TradingPair, Decimal>>>,
    publish_client: Arc<Client>,
    on_hold_changes: Arc<TokioMutex<HashMap<TradingPair, HashMap<BidOrAsk, HashMap<Decimal, (f64, Instant)>>>>>,
    traders_data: Arc<TokioMutex<HashMap<String, TraderData>>>
) -> Result<(), Box<dyn Error + Send + Sync>> {

    let (trading_pair, data) = extract_trading_pair_and_data(&msg)?;
    process_order_updates(
        &data,
        "asks",
        BidOrAsk::Ask,
        &trading_pair,
        matching_engine.clone(),
        previous_best_asks,
        publish_client.clone(),
        on_hold_changes.clone(),
        traders_data.clone()
    ).await?;
    process_order_updates(
        &data,
        "bids",
        BidOrAsk::Bid,
        &trading_pair,
        matching_engine,
        previous_best_bids,
        publish_client,
        on_hold_changes,
        traders_data
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
    on_hold_changes: Arc<TokioMutex<HashMap<TradingPair, HashMap<BidOrAsk, HashMap<Decimal, (f64, Instant)>>>>>,
    traders_data: Arc<TokioMutex<HashMap<String, TraderData>>>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // TODO? maybe change this such that the updates is still of type Value and we can access the elements by the 'key'
    if let Some(Value::Array(updates)) = data.get(side) {
        if !updates.is_empty() {
            let best_price = update_orders_and_get_best_price(
                updates,
                bid_or_ask,
                trading_pair,
                matching_engine,
                on_hold_changes,
                traders_data
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
    on_hold_changes: Arc<TokioMutex<HashMap<TradingPair, HashMap<BidOrAsk, HashMap<Decimal, (f64, Instant)>>>>>,
    traders_data: Arc<TokioMutex<HashMap<String, TraderData>>>,
) -> Result<Decimal, Box<dyn Error + Send + Sync>> {
    let mut engine = matching_engine.lock().await;
    let mut on_hold_changes_ = on_hold_changes.lock().await;
    place_orders(updates,
                 bid_or_ask.clone(),
                 trading_pair.clone(),
                 &mut engine,
                 &mut on_hold_changes_,
                 Duration::from_secs(2),
                 traders_data).await;

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