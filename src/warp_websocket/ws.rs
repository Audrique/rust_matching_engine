use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex as TokioMutex;
use crate::{Client, Clients};
use futures::{FutureExt, StreamExt};
use futures_util::SinkExt;
use futures_util::stream::SplitSink;
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use serde::Deserialize;
use serde_json::{from_str, Value};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use warp::ws::{Message, WebSocket};
use crate::connecting_to_exchanges::for_all_exchanges::{TraderData, update_trader_data};
use crate::matching_engine::engine::{MatchingEngine, TradingPair};
use crate::matching_engine::orderbook::{BidOrAsk, Trade, Order};

#[derive(Deserialize, Debug)]
pub struct TopicsRequest {
    topics: Vec<String>,
}

pub async fn client_connection(ws: WebSocket,
                               id: String,
                               clients: Clients,
                               mut client: Client,
                               matching_engine: Arc<TokioMutex<MatchingEngine>>,
                               traders_data: Arc<TokioMutex<HashMap<String, TraderData>>>
) {
    let (client_ws_sender, mut client_ws_rcv) = ws.split();
    let (client_sender, client_rcv) = mpsc::unbounded_channel();

    let client_rcv = UnboundedReceiverStream::new(client_rcv);
    tokio::task::spawn(client_rcv.forward(client_ws_sender).map(|result| {
        if let Err(e) = result {
            eprintln!("error sending websocket msg: {}", e);
        }
    }));

    client.sender = Some(client_sender);
    clients.write().await.insert(id.clone(), client);

    println!("{} connected", id);

    while let Some(result) = client_ws_rcv.next().await {
        let msg = match result {
            Ok(msg) => msg,
            Err(e) => {
                eprintln!("error receiving ws message for id: {}): {}", id.clone(), e);
                break;
            }
        };
        client_msg(&id, msg, &clients, matching_engine.clone(), traders_data.clone()).await;
    }

    // Only get here when we get an error and then remove the id
    clients.write().await.remove(&id);
    println!("{} disconnected", id);
}

async fn client_msg(id: &str,
                    msg: Message,
                    clients: &Clients,
                    matching_engine: Arc<TokioMutex<MatchingEngine>>,
                    traders_data: Arc<TokioMutex<HashMap<String, TraderData>>>
) {
    println!("received message from {}: {:?}", id, msg);
    let message = match msg.to_str() {
        Ok(v) => v,
        Err(_) => return
    };
    println!("{:?}", message);

    if message == "ping" || message == "ping\n" {
        println!("Received ping from {}", id);
        let pong_message = Message::text("pong");
        let locked_clients = clients.write().await;
        if let Some(client) = locked_clients.get(id) {
            if let Err(e) = client.sender.as_ref().unwrap().send(Ok(pong_message)) {
                eprintln!("error sending pong message to {}: {}", id, e);
            }
        }
        return;
    }
    let data: Value = match from_str(&message) {
        Ok(v) => v,
        Err(_) => {eprintln!("Error parsing message in client_msg"); return}
    };
    if let Some(action) = data.get("action"){
        println!("The parsed data: {:?}", data);
        let str_action = action.as_str().unwrap();
        match str_action {
            "add_limit_order" => {
                let (trading_pair, price, order_to_place) = prepare_limit_order(data);
                let mut engine = matching_engine.lock().await;
                let (pair, trades) = engine.place_limit_order(trading_pair, price, order_to_place).unwrap();
                drop(engine); // Drop the lock on the engine as soon as possible
                update_trader_data(pair, trades.clone(), traders_data).await;
                // TODO: publish these trades to the correct topic
                println!("The trades that occurred after placing an order from the client trader: {:?}", trades);
            },
            "cancel_limit_order" => {
                let (trading_pair,side, price, order_id) = prepare_cancel_order(data);
                let mut engine = matching_engine.lock().await;
                engine.cancel_order(trading_pair, side, price, order_id).unwrap();
            },
            "market_order" => {
                let (trading_pair, mut order_to_place) = prepare_market_order(data);
                let mut engine = matching_engine.lock().await;
                let (pair, trades) = engine.place_market_order(trading_pair, &mut order_to_place).unwrap();
                drop(engine);
                update_trader_data(pair, trades.clone(), traders_data).await;
                // TODO: publish these trades to the correct topic
                println!("The trades that occurred after placing an order from the client trader: {:?}", trades);
            },
            _ => {panic!("Client request was not a placement or cancelation of a limit order, nor a market order.")}
        }
        return;
    }


    let topics_req: TopicsRequest = match from_str(&message) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("error while parsing message to topics request: {}", e);
            return;
        }
    };

    let mut locked = clients.write().await;
    if let Some(v) = locked.get_mut(id) {
        v.topics = topics_req.topics;
    }
}
async fn send_message_to_client(client: &Client, msg: Message) {
    if let Some(sender) = &client.sender {
        if let Err(e) = sender.send(Ok(msg)) {
            eprintln!("error sending message to client: {}", e);
        }
    }
}

fn prepare_limit_order(data: Value) -> (TradingPair, Decimal, Order) {
    let bid_or_ask = match data["side"].as_str() {
        Some("bid") => {BidOrAsk::Bid},
        Some("ask") => {BidOrAsk::Ask},
        _ => {panic!("Unexpected value for side received from the client!");} // Handle this better potentially?
    };
    let order_to_place =  Order::new(bid_or_ask,
                                     data["volume"].as_f64().unwrap(),
                                     data["trader_id"].to_string(),
                                     data["order_id"].to_string()
    );
    let trading_pair_base = data["trading_pair_base"].as_str().ok_or("Missing trading_pair_base").unwrap();
    let trading_pair_quote = data["trading_pair_quote"].as_str().ok_or("Missing trading_pair_quote").unwrap();

    let trading_pair = TradingPair::new(trading_pair_base.to_string(), trading_pair_quote.to_string());
    let price = Decimal::from_f64(data["price"].as_f64().unwrap()).expect("Failed to convert f64 to Decimal");
    (trading_pair, price, order_to_place)
}

fn prepare_cancel_order(data: Value) -> (TradingPair, BidOrAsk, Decimal, String) {
    let bid_or_ask = match data["side"].as_str() {
        Some("bid") => {BidOrAsk::Bid},
        Some("ask") => {BidOrAsk::Ask},
        _ => {panic!("Unexpected value for side received from the client!");} // Handle this better potentially?
    };
    let trading_pair_base = data["trading_pair_base"].as_str().ok_or("Missing trading_pair_base").unwrap();
    let trading_pair_quote = data["trading_pair_quote"].as_str().ok_or("Missing trading_pair_quote").unwrap();

    let trading_pair = TradingPair::new(trading_pair_base.to_string(), trading_pair_quote.to_string());
    let price = Decimal::from_f64(data["price"].as_f64().unwrap()).expect("Failed to convert f64 to Decimal");
    (trading_pair, bid_or_ask, price, data["order_id"].to_string())
}

fn prepare_market_order(data: Value) -> (TradingPair, Order) {
    let bid_or_ask = match data["side"].as_str() {
        Some("bid") => {BidOrAsk::Bid},
        Some("ask") => {BidOrAsk::Ask},
        _ => {panic!("Unexpected value for side received from the client!");} // Handle this better potentially?
    };
    let order_to_place =  Order::new(bid_or_ask,
                                     data["volume"].as_f64().unwrap(),
                                     data["trader_id"].to_string(),
                                     data["order_id"].to_string()
    );
    let trading_pair_base = data["trading_pair_base"].as_str().ok_or("Missing trading_pair_base").unwrap();
    let trading_pair_quote = data["trading_pair_quote"].as_str().ok_or("Missing trading_pair_quote").unwrap();

    let trading_pair = TradingPair::new(trading_pair_base.to_string(), trading_pair_quote.to_string());
    (trading_pair, order_to_place)
}
