#![allow(dead_code)]
#![allow(unused_imports)]
mod warp_websocket;
mod connecting_to_exchanges;
mod matching_engine;

use std::collections::HashMap;
use std::convert::Infallible;
use std::error::Error;
use tokio::sync::{mpsc, oneshot, RwLock, Mutex as TokioMutex};
use warp::{ws::Message, Filter, Rejection};
use crate::warp_websocket::{handler, create_server};
use std::sync::Arc;
use matching_engine::{engine::{MatchingEngine, TradingPair}, orderbook::{BidOrAsk, Order}};
use connecting_to_exchanges::deribit_connection::{authenticate_deribit,
                                                  establish_connection,
                                                  on_incoming_deribit_message,
                                                  read_config_file,
                                                  subscribe_to_channel,
                                                  establish_heartbeat};
use tokio;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};

type Result<T> = std::result::Result<T, Rejection>;
type Clients = Arc<RwLock<HashMap<String, Client>>>;
#[derive(Debug, Clone)]
pub struct Client {
    pub user_id: usize,
    pub topics: Vec<String>,
    pub sender: Option<mpsc::UnboundedSender<std::result::Result<Message, warp::Error>>>,
}

pub struct TraderData {
    pub position: f64,
    pub trading_profit: f64,
    pub holding_profit: f64,
}
impl TraderData {
    pub fn new(position: f64, trading_profit: f64, holding_profit: f64) -> TraderData {
        TraderData{
            position,
            trading_profit,
            holding_profit,
        }
    }
}

async fn wait_for_server(rx: oneshot::Receiver<()>) {
    match rx.await {
        Ok(_) => println!("Server is ready, starting to process Deribit messages"),
        Err(_) => eprintln!("Failed to receive server ready signal"),
    }
}

#[tokio::main]
async fn main() {
    let trading_pair = TradingPair::new("BTC".to_string(), "USDT".to_string());
    let trading_pairs = vec![trading_pair.clone()];
    // We put the trader data inside an arc mutex since we will add other exchanges later
    // all on_incoming_{exchange}_message (currently only the deribit exchange)
    let mut traders_data: Arc<TokioMutex<HashMap<String, TraderData>>> = Arc::new(TokioMutex::new(HashMap::new()));
    let mut engine = MatchingEngine::new();
    engine.add_new_market(trading_pair.clone(), 0.0, 0.0);

    let engine = Arc::new(TokioMutex::new(engine));

    // Create an oneshot-channel for signaling server readiness
    let (tx, rx) = oneshot::channel();

    // Spawn the Deribit connection handling
    let deribit_engine = engine.clone();
    tokio::spawn(async move {
        let url_deribit = "wss://www.deribit.com/ws/api/v2";
        let mut ws_stream = establish_connection(url_deribit).await.unwrap();
        let (client_id, client_secret) = read_config_file();

        authenticate_deribit(&mut ws_stream, &client_id, &client_secret).await;
        let channel_btc_usdt = &format!("book.{}.raw", trading_pair.to_string());
        let channels = vec![channel_btc_usdt];
        establish_heartbeat(&mut ws_stream, 10).await.unwrap();
        subscribe_to_channel(&mut ws_stream, channels).await.unwrap();
        // Wait for the server to be ready before processing messages
        wait_for_server(rx).await;
        on_incoming_deribit_message(&mut ws_stream, deribit_engine, trading_pairs).await;
    });

    // Start the warp websocket
    create_server::start_server(tx, engine.clone()).await;
}



