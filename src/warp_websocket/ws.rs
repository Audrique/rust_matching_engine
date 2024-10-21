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
use serde_json::{from_str, json, Value};
use tokio::sync::mpsc;
use std::error::Error;
use tokio_stream::wrappers::UnboundedReceiverStream;
use warp::ws::{Message, WebSocket};
use crate::connecting_to_exchanges::for_all_exchanges::{TraderData, update_trader_data};
use crate::matching_engine::engine::{MatchingEngine, TradingPair};
use crate::matching_engine::orderbook::{BidOrAsk, Trade, Order};
use reqwest::Client as ReqClient;

#[derive(Deserialize, Debug)]
pub struct TopicsRequest {
    topics: Vec<String>,
}

pub async fn client_connection(ws: WebSocket,
                               id: String,
                               clients: Clients,
                               mut client: Client,
                               matching_engine: Arc<TokioMutex<MatchingEngine>>,
                               traders_data: Arc<TokioMutex<HashMap<String, TraderData>>>,
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
        client_msg(&id, msg, &clients, matching_engine.clone(), traders_data.clone()).await.unwrap();
    }

    // Only get here when we get an error and then remove the id
    clients.write().await.remove(&id);
    println!("{} disconnected", id);
}

async fn client_msg(
    id: &str,
    msg: Message,
    clients: &Clients,
    matching_engine: Arc<TokioMutex<MatchingEngine>>,
    traders_data: Arc<TokioMutex<HashMap<String, TraderData>>>
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let message = msg.to_str().unwrap();

    if message.trim() == "ping" {
        return handle_ping(id, clients).await;
    }

    let data: Value = from_str(message)?;

    if let Some(action) = data.get("action").and_then(Value::as_str) {
        match action {
            "add_limit_order" => handle_add_limit_order(&data, matching_engine, traders_data).await?,
            "cancel_limit_order" => handle_cancel_limit_order(&data, matching_engine).await?,
            "market_order" => handle_market_order(&data, matching_engine, traders_data).await?,
            "open_orders_and_positions" => handle_open_orders_and_positions(id, &data, clients, matching_engine, traders_data).await?,
            _ => return Err(format!("Unknown action: {}", action).into()),
        }
    } else {
        handle_topics_request(id, message, clients).await?;
    }

    Ok(())
}

async fn handle_ping(id: &str, clients: &Clients) -> Result<(), Box<dyn Error + Send + Sync>> {
    println!("Received ping from {}", id);
    let pong_message = Message::text("pong");
    let locked_clients = clients.write().await;
    if let Some(client) = locked_clients.get(id) {
        client.sender.as_ref().unwrap().send(Ok(pong_message))?;
    }
    Ok(())
}

async fn handle_add_limit_order(
    data: &Value,
    matching_engine: Arc<TokioMutex<MatchingEngine>>,
    traders_data: Arc<TokioMutex<HashMap<String, TraderData>>>
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let (trading_pair, price, order_to_place) = prepare_limit_order(data)?;
    let mut engine = matching_engine.lock().await;
    let (pair, trades) = engine.place_limit_order(trading_pair.clone(), price, order_to_place)?;
    drop(engine);

    update_trader_data(pair, trades.clone(), traders_data).await;
    publish_trades(trades, trading_pair.to_string()).await?;
    Ok(())
}

async fn handle_cancel_limit_order(
    data: &Value,
    matching_engine: Arc<TokioMutex<MatchingEngine>>
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let (trading_pair, side, price, order_id) = prepare_cancel_order(data)?;
    let mut engine = matching_engine.lock().await;
    engine.cancel_order(trading_pair, side, price, order_id)?;
    Ok(())
}

async fn handle_market_order(
    data: &Value,
    matching_engine: Arc<TokioMutex<MatchingEngine>>,
    traders_data: Arc<TokioMutex<HashMap<String, TraderData>>>
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let (trading_pair, mut order_to_place) = prepare_market_order(data)?;
    let mut engine = matching_engine.lock().await;
    let (pair, trades) = engine.place_market_order(trading_pair.clone(), &mut order_to_place)?;
    drop(engine);
    update_trader_data(pair, trades.clone(), traders_data).await;
    publish_trades(trades, trading_pair.to_string()).await?;
    Ok(())
}

async fn handle_open_orders_and_positions(
    id: &str,
    data: &Value,
    clients: &Clients,
    matching_engine: Arc<TokioMutex<MatchingEngine>>,
    traders_data: Arc<TokioMutex<HashMap<String, TraderData>>>
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let engine = matching_engine.lock().await;
    if let (Some(Value::String(trader_id)),
        Some(Value::String(trading_pair_base)),
        Some(Value::String(trading_pair_quote)))
        = (data.get("trader_id"),
           data.get("trading_pair_base"),
           data.get("trading_pair_quote"))
    {
        let open_orders = engine.open_orders(trader_id.clone());
        drop(engine);

        let trading_pair = TradingPair::new(trading_pair_base.clone(), trading_pair_quote.clone());
        let requested_open_orders = open_orders.get(&trading_pair);

        let open_orders = match requested_open_orders {
            Some(orders) if !orders.is_empty() => orders,
            _ => &HashMap::new(),
        };

        let td = traders_data.lock().await;
        let trader_data = td.get(trader_id);

        let position = trader_data
            .and_then(|td| td.positions.get(&trading_pair.to_string()))
            .cloned()
            .unwrap_or(0.0);
        drop(td);
        let msg = Message::text(json!({
            "open_orders": open_orders,
            "position": position,
        }).to_string());
        let locked_clients = clients.write().await;
        if let Some(client) = locked_clients.get(id) {
            client.sender.as_ref().unwrap().send(Ok(msg))?;
        }
    } else {
        return Err("open_orders_and_positions request was not properly formatted!".into());
    }
    Ok(())
}

async fn handle_topics_request(id: &str, message: &str, clients: &Clients) -> Result<(), Box<dyn Error + Send + Sync>> {
    let topics_req: TopicsRequest = serde_json::from_str(message)?;
    let mut locked = clients.write().await;
    if let Some(v) = locked.get_mut(id) {
        v.topics = topics_req.topics;
    }
    Ok(())
}

async fn send_message_to_client(client: &Client, msg: Message) {
    if let Some(sender) = &client.sender {
        if let Err(e) = sender.send(Ok(msg)) {
            eprintln!("error sending message to client: {}", e);
        }
    }
}
fn prepare_limit_order(data: &Value) -> Result<(TradingPair, Decimal, Order), Box<dyn Error + Send + Sync>> {
    let bid_or_ask = match data["side"].as_str() {
        Some("Bid") => BidOrAsk::Bid,
        Some("Ask") => BidOrAsk::Ask,
        _ => return Err("Invalid side for order".into()),
    };

    let trader_id = data["trader_id"].as_str()
        .ok_or("Missing trader_id")?.to_string();
    let order_id = data["order_id"].as_str()
        .ok_or("Missing order_id")?.to_string();
    let volume = data["volume"].as_f64()
        .ok_or("Invalid volume")?;

    let order_to_place = Order::new(bid_or_ask, volume, trader_id, order_id);

    let trading_pair_base = data["trading_pair_base"].as_str()
        .ok_or("Missing trading_pair_base")?;
    let trading_pair_quote = data["trading_pair_quote"].as_str()
        .ok_or("Missing trading_pair_quote")?;

    let trading_pair = TradingPair::new(trading_pair_base.to_string(), trading_pair_quote.to_string());

    let price = Decimal::from_f64(data["price"].as_f64()
        .ok_or("Invalid price")?)
        .ok_or("Failed to convert price to Decimal")?;

    Ok((trading_pair, price, order_to_place))
}

fn prepare_cancel_order(data: &Value) -> Result<(TradingPair, BidOrAsk, Decimal, String), Box<dyn Error + Send + Sync>> {
    let bid_or_ask = match data["side"].as_str() {
        Some("Bid") => BidOrAsk::Bid,
        Some("Ask") => BidOrAsk::Ask,
        _ => return Err("Invalid side for order".into()),
    };

    let trading_pair_base = data["trading_pair_base"].as_str()
        .ok_or("Missing trading_pair_base")?;
    let trading_pair_quote = data["trading_pair_quote"].as_str()
        .ok_or("Missing trading_pair_quote")?;

    let trading_pair = TradingPair::new(trading_pair_base.to_string(), trading_pair_quote.to_string());

    let price = Decimal::from_f64(data["price"].as_f64()
        .ok_or("Invalid price")?)
        .ok_or("Failed to convert price to Decimal")?;

    let order_id = data["order_id"].as_str()
        .ok_or("Missing order_id")?.to_string();

    Ok((trading_pair, bid_or_ask, price, order_id))
}

fn prepare_market_order(data: &Value) -> Result<(TradingPair, Order), Box<dyn Error + Send + Sync>> {
    let bid_or_ask = match data["side"].as_str() {
        Some("Bid") => BidOrAsk::Bid,
        Some("Ask") => BidOrAsk::Ask,
        _ => return Err("Invalid side for order".into()),
    };

    let trader_id = data["trader_id"].as_str()
        .ok_or("Missing trader_id")?.to_string();
    let order_id = data["order_id"].as_str()
        .ok_or("Missing order_id")?.to_string();
    let volume = data["volume"].as_f64()
        .ok_or("Invalid volume")?;

    let order_to_place = Order::new(bid_or_ask, volume, trader_id, order_id);

    let trading_pair_base = data["trading_pair_base"].as_str()
        .ok_or("Missing trading_pair_base")?;
    let trading_pair_quote = data["trading_pair_quote"].as_str()
        .ok_or("Missing trading_pair_quote")?;

    let trading_pair = TradingPair::new(trading_pair_base.to_string(), trading_pair_quote.to_string());

    Ok((trading_pair, order_to_place))
}

async fn publish_trades(trades: Vec<Trade>,
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
        publish_message(msg.clone(), topic).await?;
    }
    Ok(())
}

async fn publish_message(msg: String,
                         topic: String
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let client = ReqClient::new();
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
