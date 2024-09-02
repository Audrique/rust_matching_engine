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
use crate::{Clients};
use reqwest::Client;
use tokio_tungstenite::tungstenite::client;
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
                                  matching_engine: Arc<TokioMutex<MatchingEngine>>) {
    // Keeps track of the previous best bids/asks such that we can send updates to the subscribed clients
    let previous_best_bids: Arc<TokioMutex<HashMap<TradingPair, Decimal>>> = Arc::new(TokioMutex::new(HashMap::new()));
    let previous_best_asks: Arc<TokioMutex<HashMap<TradingPair, Decimal>>> = Arc::new(TokioMutex::new(HashMap::new()));
    let publish_client = Arc::new(Client::new());
    loop {
        match ws_stream_ref.next().await {
            Some(Ok(msg)) => {
                let previous_best_bids = Arc::clone(&previous_best_bids);
                let previous_best_asks = Arc::clone(&previous_best_asks);
                let matching_engine = Arc::clone(&matching_engine);
                let publish_client = Arc::clone(&publish_client);
                // Immediately offload the message processing to another function/task.
                tokio::spawn(async move {
                    if let Err(e) = process_message(msg, matching_engine, previous_best_bids, previous_best_asks, publish_client).await {
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

// TODO: Add a client with user_id = 98326 and match this with the below user_id, which we thus use for publishing (now it is matched
//  with the test_warp_ws.rs file
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