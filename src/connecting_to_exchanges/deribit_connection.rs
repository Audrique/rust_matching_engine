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
                             orderbook::{Order, BidOrAsk, Trade, BuyOrSell}};
use crate::warp_websocket::handler::{Event};
use crate::connecting_to_exchanges::for_all_exchanges::{TraderData, update_trader_data};
use crate::connecting_to_exchanges::parse_book_message::{BookUpdate, OrderBookEntry};
use rust_decimal_macros::dec;
use serde::Serialize;
use taos::Itertools;
// TODO: refactor error handling (make it with '?' and Box< dyn std::error:Error>)

#[derive(Debug, Clone, Serialize)]
pub enum ChangedSide {
    Ask,
    Bid,
    Both,
}

impl ChangedSide {
    pub fn to_string(&self) -> String {
        match self {
            ChangedSide::Ask => "Ask".to_string(),
            ChangedSide::Bid => "Bid".to_string(),
            ChangedSide::Both => "Both".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
struct BestPriceData {
    best_ask_price: Decimal,
    best_bid_price: Decimal,
}

impl BestPriceData {
    fn new(best_ask_price: Decimal, best_bid_price: Decimal) -> BestPriceData {
        BestPriceData {
            best_ask_price,
            best_bid_price,
        }
    }
}

fn parse_book_update(msg: Value) -> Result<BookUpdate, serde_json::Error> {
    let book_update: BookUpdate = serde_json::from_value(msg)?;
    Ok(book_update)
}

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
                                  channels: Vec<String>) -> Result<(), Box<dyn Error>> {
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


// deribit can change to exchange later when we add other exchanges
// Topics: best_price_change.{deribit}.{pair}, sends the best bid and ask price if one of them changes (volume could be included later)
//         top_10_asks_bids_periodically.{deribit}.{pair}, sends the top 10 bids and asks every 250ms (prices and volume)
//         trades.{deribit}.{pair}, sends trades that happen for the given exchange and trading_pair
//         traders_data_periodically, sends all connected traders positions and trading_profit every 250ms
//         trades.{trader_id}, sends trades which involve a specific trader_id when they happen
pub async fn on_incoming_deribit_message(
    ws_stream_ref: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    matching_engine: Arc<TokioMutex<MatchingEngine>>,
    trading_pairs: Vec<TradingPair>,
    traders_data: Arc<TokioMutex<HashMap<String, TraderData>>>,
) {
    let previous_best_prices: Arc<TokioMutex<HashMap<TradingPair, BestPriceData>>> = Arc::new(TokioMutex::new(HashMap::new()));
    let on_hold_changes = initialize_on_hold_changes(trading_pairs.clone());
    let publish_client = Arc::new(Client::new());
    let mut previous_change_ids: HashMap<String, u64> = HashMap::new();
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
                        let topic = format!("top_10_asks_bids_periodically.deribit.{}", t_pair.clone().to_string());
                        // TODO: Change this to JSON format instead of how it is now
                        let message = format!("Top 10 asks: {:?}; Top 10 bids: {:?}", top_10_asks, top_10_bids);
                        publish_message(message, topic, &publish_client).await.unwrap();
                    }
                    // Also publish the traders data
                    let topic2 = "traders_data_periodically".to_string();
                    let traders_data2 = traders_data_cloned.lock().await;
                    let message2 = serde_json::to_string(&*traders_data2).unwrap();
                    publish_message(message2, topic2, &publish_client).await.unwrap();
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
                    // 888 is the id with which we send the heartbeat, the response will have the same id
                } else if data["id"] == 888 {
                    continue
                } else if data["id"] == 9922 {
                    println!("Subscription response result: {:?}", data["result"]);
                    continue
                } else if data["method"] == "subscription" {
                    let channel = data["params"]["channel"]
                        .as_str()
                        .expect("Converting to string failed!")
                        .split('.')
                        .next()
                        .expect("No part before the first '.'");
                    match channel {
                        "book" => {
                            let previous_best_prices = Arc::clone(&previous_best_prices);
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
                                    previous_best_prices,
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
                            let current_channel = data["params"]["channel"].as_str();
                            if let (Some(current_change_id), Some(current_previous_change_id), Some(channel_str)) =
                                (current_change_id, current_previous_change_id, current_channel) {
                                let channel = channel_str.to_string();
                                if let Some(prev_id) = previous_change_ids.get(&channel) {
                                    if current_previous_change_id != prev_id.clone() {
                                        number_of_missed_changes += 1;
                                        println!("The number of missed updates: {number_of_missed_changes}");
                                        println!("The previous change was: {:?}, and the expected previous change: {:?}", prev_id, &current_previous_change_id);
                                    }
                                }
                                previous_change_ids.insert(channel, current_change_id.clone());
                            } else {
                                let update_type = data["params"]["data"]["type"].as_str().ok_or("No type in received data!").unwrap();
                                if update_type == "snapshot" {
                                    println!("Snapshot received!");
                                }
                            }
                        },
                        "trades" => {
                            let trading_pair = data["params"]["channel"]
                                .as_str()
                                .expect("Converting to string failed!")
                                .split('.')
                                .nth(1)
                                .expect("No second '.' part found!");

                            let mut trades: Vec<Trade> = vec!();
                            if let Value::Array(data_arr) = data["params"]["data"].clone() {
                                for trade_data in data_arr {
                                    let volume = trade_data["amount"].as_f64().expect("Converting amount to f64 failed!");
                                    let timestamp = trade_data["timestamp"].as_u64().expect("Converting timestamp to u64 failed!");
                                    let price = trade_data["price"].as_f64().expect("Converting price to f64 failed!");
                                    let price_dec = Decimal::from_f64(price).expect("Problem converting price");
                                    let direction_str = trade_data["direction"].as_str().expect("Converting direction to str, failed!");
                                    let direction = match direction_str {
                                        "buy" => {Ok(BuyOrSell::Buy)},
                                        "sell"=> {Ok(BuyOrSell::Sell)},
                                        _ => {eprintln!("Error converting direction to Buy or Sell!");
                                            Err("Error converting direction to Buy or Sell!")}
                                    };
                                    let trade = Trade::new("deribit_exchange".to_string(),
                                                           "deribit_exchange".to_string(),
                                                           volume,
                                                           price_dec,
                                                           timestamp,
                                                           0.0,
                                                           0.0,
                                                           direction.unwrap(),
                                    );
                                    trades.push(trade);
                                }
                            }
                            let topic = format!("trades.deribit.{}", trading_pair.to_string());
                            let message = json!({
                                "trades": trades,
                            }).to_string();
                            publish_message(message.clone(), topic, &publish_client).await.unwrap();
                            // println!("Published trade");
                        },
                        _ => {eprintln!("Unexpected channel message!")}
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

async fn place_orders(parsed_data: BookUpdate,
                matching_engine: &mut MatchingEngine,
                on_hold_changes: &mut HashMap<TradingPair, HashMap<BidOrAsk, HashMap<Decimal, (f64, Instant)>>>,
                time_until_forget_on_hold: Duration,
                traders_data: Arc<TokioMutex<HashMap<String, TraderData>>>,
                publish_client: &Arc<Client>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let trading_pair = parsed_data.trading_pair.clone();

    // Process asks
    for entry in parsed_data.asks {
        process_order_entry(
            entry,
            BidOrAsk::Ask,
            &trading_pair,
            matching_engine,
            on_hold_changes,
            traders_data.clone(),
            publish_client
        ).await?;
    }

    // Process bids
    for entry in parsed_data.bids {
        process_order_entry(
            entry,
            BidOrAsk::Bid,
            &trading_pair,
            matching_engine,
            on_hold_changes,
            traders_data.clone(),
            &publish_client
        ).await?;
    }

    // Clean up on_hold_change
    let cleanup_time = Instant::now();
    for (_, bid_ask_map) in on_hold_changes.iter_mut() {
        for (_, price_map) in bid_ask_map.iter_mut() {
            price_map.retain(|_, (_, timestamp)| cleanup_time.duration_since(*timestamp) <= time_until_forget_on_hold);
        }
    }
    Ok(())
}

async fn process_order_entry(
    entry: OrderBookEntry,
    bid_or_ask: BidOrAsk,
    trading_pair: &TradingPair,
    matching_engine: &mut MatchingEngine,
    on_hold_changes: &mut HashMap<TradingPair, HashMap<BidOrAsk, HashMap<Decimal, (f64, Instant)>>>,
    traders_data: Arc<TokioMutex<HashMap<String, TraderData>>>,
    publish_client_ref: &Arc<Client>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let price = Decimal::from_f64(entry.price).unwrap();
    let volume = entry.volume;

    let check_exchange_order_in_orderbook = matching_engine
        .orderbooks
        .get(trading_pair)
        .unwrap()
        .get_side(&bid_or_ask)
        .get(&price)
        .map_or(false, |limit| limit.check_exchange_order_in_limit());

    let on_hold_entry = on_hold_changes
        .get_mut(trading_pair)
        .unwrap()
        .get_mut(&bid_or_ask)
        .unwrap();

    match entry.action.as_str() {
        "delete" => {
            if check_exchange_order_in_orderbook {
                matching_engine.leave_volume_from_exchange(
                    trading_pair.clone(),
                    bid_or_ask.clone(),
                    price,
                    volume // In this case the volume is always 0.0 -> maybe change it? or keep it as an extra 'check'
                )?;
            } else {
                on_hold_entry.insert(price, (volume, Instant::now()));
            };
        },
        "new" => {
            let (pair, trades) = matching_engine.place_limit_order(
                trading_pair.clone(),
                price,
                Order::new(
                    bid_or_ask.clone(),
                    volume,
                    "deribit-exchange".to_string(),
                    "-1".to_string()
                )
            )?;
            update_trader_data(pair, trades.clone(), traders_data.clone()).await;
            publish_trades_for_trader_id(trades, publish_client_ref, trading_pair.clone().to_string()).await?;

            if let Some(&(volume_on_hold, _)) = on_hold_entry.get(&price) {
                if volume_on_hold >= 0.0 {
                    matching_engine.leave_volume_from_exchange(
                        trading_pair.clone(),
                        bid_or_ask.clone(),
                        price,
                        volume_on_hold
                    )?;
                    on_hold_entry.remove(&price);
                };
            };
        },
        "change" => {
            if check_exchange_order_in_orderbook {
                matching_engine.leave_volume_from_exchange(
                    trading_pair.clone(),
                    bid_or_ask.clone(),
                    price,
                    volume
                )?;
            };
            on_hold_entry.insert(price, (volume, Instant::now()));
        },
        _ => return Err("Update-type not one of 'change', 'new' or 'delete'!".into())
    }
    Ok(())
}

async fn publish_trades_for_trader_id(trades: Vec<Trade>,
                                      publish_client_ref: &Arc<Client>,
                                      trading_pair: String,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut grouped_trades: HashMap<String, Vec<Trade>> = HashMap::new();
    for trade in trades {
        if !trade.trader_id_maker.ends_with("exchange") {
            grouped_trades
                .entry(trade.trader_id_maker.clone())
                .or_insert_with(Vec::new)
                .push(trade.clone());
        }
        if !trade.trader_id_taker.ends_with("exchange") {
            grouped_trades
                .entry(trade.trader_id_taker.clone())
                .or_insert_with(Vec::new)
                .push(trade.clone());
        }
    }

    for (trader_id, trades) in grouped_trades {
        let msg = json!({
            "trading_pair": trading_pair.clone(),
            "trades": trades,
        }).to_string();
        let topic = format!("trades.{}", trader_id);
        publish_message(msg.clone(), topic, publish_client_ref).await?;
    }
    Ok(())
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
    previous_best_prices: Arc<TokioMutex<HashMap<TradingPair, BestPriceData>>>,
    publish_client: Arc<Client>,
    on_hold_changes: Arc<TokioMutex<HashMap<TradingPair, HashMap<BidOrAsk, HashMap<Decimal, (f64, Instant)>>>>>,
    traders_data: Arc<TokioMutex<HashMap<String, TraderData>>>
) -> Result<(), Box<dyn Error + Send + Sync>> {

    let data = msg["params"]["data"].clone();
    let parsed_update = parse_book_update(data.clone())?;
    process_order_updates(
        parsed_update,
        matching_engine.clone(),
        previous_best_prices.clone(),
        publish_client.clone(),
        on_hold_changes.clone(),
        traders_data.clone()
    ).await?;
    Ok(())
}

async fn process_order_updates(
    parsed_data: BookUpdate,
    matching_engine: Arc<TokioMutex<MatchingEngine>>,
    previous_best_prices: Arc<TokioMutex<HashMap<TradingPair, BestPriceData>>>,
    publish_client: Arc<Client>,
    on_hold_changes: Arc<TokioMutex<HashMap<TradingPair, HashMap<BidOrAsk, HashMap<Decimal, (f64, Instant)>>>>>,
    traders_data: Arc<TokioMutex<HashMap<String, TraderData>>>,
) -> Result<(), Box<dyn Error + Send + Sync>> {

    let trading_pair = &parsed_data.trading_pair.clone();
    let (best_ask_price, best_bid_price) = update_orders_and_get_best_price(
        parsed_data,
        matching_engine,
        on_hold_changes,
        traders_data,
        &publish_client,
    ).await?;

    check_and_publish_price_change(
        best_ask_price,
        best_bid_price,
        trading_pair,
        previous_best_prices,
        publish_client,
    ).await?;


    Ok(())
}

async fn update_orders_and_get_best_price(
    parsed_data: BookUpdate,
    matching_engine: Arc<TokioMutex<MatchingEngine>>,
    on_hold_changes: Arc<TokioMutex<HashMap<TradingPair, HashMap<BidOrAsk, HashMap<Decimal, (f64, Instant)>>>>>,
    traders_data: Arc<TokioMutex<HashMap<String, TraderData>>>,
    publish_client: &Arc<Client>,
) -> Result<(Decimal, Decimal), Box<dyn Error + Send + Sync>> {
    let mut engine = matching_engine.lock().await;
    let mut on_hold_changes_ = on_hold_changes.lock().await;
    place_orders(parsed_data.clone(),
                 &mut engine,
                 &mut on_hold_changes_,
                 Duration::from_millis(200), // TODO: Play with this value to make sure we keep the correct orderbook and look if we publish wrong things
                 traders_data,
                 publish_client
    ).await?;
    let orderbook = engine.orderbooks.get(&parsed_data.trading_pair).ok_or("Orderbook not found")?;
    let (best_ask_price, _) = orderbook.asks.first_key_value().ok_or("No orders found for asks")?;
    let (best_bid_price, _) = orderbook.bids.last_key_value().ok_or("No orders found for bids")?;
    Ok((best_ask_price.clone(), best_bid_price.clone()))
}

async fn check_and_publish_price_change(
    new_best_ask_price: Decimal,
    new_best_bid_price: Decimal,
    trading_pair: &TradingPair,
    previous_best_prices: Arc<TokioMutex<HashMap<TradingPair, BestPriceData>>>,
    publish_client: Arc<Client>
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut previous_best = previous_best_prices.lock().await;

    let best_bid_changed = previous_best
        .get(trading_pair)
        .map_or(true, |prev| (new_best_bid_price - prev.best_bid_price).abs() > Decimal::new(1, 4));

    let best_ask_changed = previous_best
        .get(trading_pair)
        .map_or(true, |prev| (new_best_ask_price - prev.best_ask_price).abs() > Decimal::new(1, 4));

    if best_bid_changed || best_ask_changed {
        // Get existing data or create new BestPriceData
        let mut best_price_data = previous_best
            .get(trading_pair)
            .cloned()
            .unwrap_or_else(|| BestPriceData::new(dec!(0.0), dec!(0.0)));
        best_price_data.best_bid_price = new_best_bid_price;
        best_price_data.best_ask_price = new_best_ask_price;
        let mut changed_side = ChangedSide::Ask;
        if best_bid_changed {
            if best_ask_changed {
                changed_side = ChangedSide::Both;
            } else {
                changed_side = ChangedSide::Bid;
            }
        }


        // Insert updated data
        previous_best.insert(trading_pair.clone(), best_price_data.clone());

        // Prepare and publish message
        let topic = format!("best_price_change.deribit.{}", trading_pair.clone().to_string());

        let message = json!({
            "changed_side": changed_side.to_string(),
            "best_ask_price": best_price_data.best_ask_price,
            "best_bid_price": best_price_data.best_bid_price
        }).to_string();

        publish_message(message.clone(), topic, &publish_client).await?;
        drop(previous_best);
        // println!("For {:?} published {}", trading_pair, message);
    }

    Ok(())
}